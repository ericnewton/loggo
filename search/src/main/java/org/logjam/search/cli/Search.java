/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logjam.search.cli;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.logjam.client.LogEntry;
import org.logjam.schema.Schema;
import org.logjam.search.cli.options.Options;
import org.logjam.search.iterators.CountingIterator;
import org.logjam.search.iterators.GrepValueFilter;
import org.logjam.search.iterators.HostAndApplicationFilter;

public class Search {
  public static final String SHORT_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String DATE_ONLY = "yyyy-MM-dd";

  public static final String[] FORMATS = new String[] {LogEntry.DATE_FORMAT, SHORT_FORMAT, DATE_ONLY};

  private static String ROW_FORMAT = Schema.SHARD_FORMAT + " %s";

  private final Connector conn;
  private final Options opts;
  private final PrintStream printer;

  public Search(Connector conn, Options opts, PrintStream printer) {
    this.conn = conn;
    this.opts = opts;
    this.printer = printer;
  }

  public void query() throws Exception {
    BatchScanner bs = conn.createBatchScanner(opts.table, Authorizations.EMPTY, 8);
    try {
      // Compute the user's date range, if any
      SimpleDateFormat sdf = new SimpleDateFormat(LogEntry.DATE_FORMAT);
      String startDate = "";
      if (opts.start != null) {
        startDate = sdf.format(new Date(opts.start));
      }
      String endDate = "9999";
      if (opts.end != null) {
        endDate = sdf.format(new Date(opts.end));
      }
      if (opts.start != null || opts.end != null) {
        // Set the date ranges for each shard
        List<Range> ranges = new ArrayList<>(Schema.SHARDS);
        for (int i = 0; i < Schema.SHARDS; i++) {
          Range r = new Range(String.format(ROW_FORMAT, i, startDate), String.format(ROW_FORMAT, i, endDate));
          ranges.add(r);
        }
        bs.setRanges(ranges);
      } else {
        // full table scan
        bs.setRanges(Collections.singletonList(new Range()));
      }

      // Set the filter for applications and host
      int priority = 100;
      if (!opts.hosts.isEmpty() || !opts.applications.isEmpty()) {
        IteratorSetting is = new IteratorSetting(priority++, HostAndApplicationFilter.class);
        HostAndApplicationFilter.setApps(is, opts.applications);
        HostAndApplicationFilter.setHosts(is, opts.hosts);
        bs.addScanIterator(is);
      }
      // stack the iterators for multiple terms: each term must match to return results
      if (!opts.terms.isEmpty()) {
        for (int i = 0; i < opts.terms.size(); i++) {
          IteratorSetting is;
          if (opts.regexp) {
            is = new IteratorSetting(priority++, RegExFilter.class);
            RegExFilter.setRegexs(is, null, null, null, opts.terms.get(i), false);
          } else {
            is = new IteratorSetting(priority++, "name" + i, GrepValueFilter.class);
            GrepValueFilter.setTerm(is, opts.terms.get(i));
          }
          bs.addScanIterator(is);
        }
      }

      // Just get the count: don't bother returning whole records
      if (opts.count) {
        IteratorSetting is = new IteratorSetting(priority++, CountingIterator.class);
        bs.addScanIterator(is);
        long total = 0;
        for (Entry<Key,Value> entry : bs) {
          total += Long.parseLong(entry.getValue().toString());
        }
        printer.println(total);
        return;
      }

      // Read the whole list for sorting. Unfortunately this means it has to fit into memory.
      ArrayList<Entry<Key,Value>> results = new ArrayList<Entry<Key,Value>>();
      for (Entry<Key,Value> entry : bs) {
        results.add(entry);
      }

      if (opts.sort || opts.reverse) {
        final int order = opts.reverse ? -1 : 1;
        Collections.sort(results, new Comparator<Entry<Key,Value>>() {
          @Override
          public int compare(Entry<Key,Value> o1, Entry<Key,Value> o2) {
            Text row = o1.getKey().getRow();
            Text row2 = o2.getKey().getRow();
            return order * BytesWritable.Comparator.compareBytes(row.getBytes(), Schema.SHARD_LENGTH, row.getLength() - Schema.SHARD_LENGTH, row2.getBytes(),
                Schema.SHARD_LENGTH, row2.getLength() - Schema.SHARD_LENGTH);
          }
        });
      }
      for (Entry<Key,Value> entry : results) {
        String cq = entry.getKey().getColumnQualifier().toString();
        String parts[] = cq.split(Schema.APP_HOST_SEPARATOR);
        String row = entry.getKey().getRow().toString();
        String value = entry.getValue().toString();
        printer.println(String.format("%s\t%s\t%s\t%s", row.substring(Schema.SHARD_LENGTH), parts[0], parts[1], value));
      }
    } finally {
      bs.close();
    }
  }
}
