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
package org.loggo.search.cli;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

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
import org.loggo.client.LogEntry;
import org.loggo.schema.Schema;
import org.loggo.search.cli.options.SearchOptions;
import org.loggo.search.iterators.CountingIterator;
import org.loggo.search.iterators.GrepValueFilter;
import org.loggo.search.iterators.HostAndApplicationFilter;
import org.loggo.search.iterators.StatsIterator;

import com.google.common.base.Joiner;

public class Search {
  public static final String SHORT_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String DATE_ONLY = "yyyy-MM-dd";

  public static final String[] FORMATS = new String[] {LogEntry.DATE_FORMAT, SHORT_FORMAT, DATE_ONLY};

  private static String ROW_FORMAT = Schema.SHARD_FORMAT + " %s";

  private final Connector conn;
  private final SearchOptions opts;
  private final PrintStream printer;

  public Search(Connector conn, SearchOptions opts, PrintStream printer) {
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
      List<String> families = Arrays.asList(Schema.FAMILIES);
      if (!opts.terms.isEmpty()) {
        for (int i = 0; i < opts.terms.size(); i++) {
          String term = opts.terms.get(i);
          IteratorSetting is;
          if (opts.regexp) {
            is = new IteratorSetting(priority++, RegExFilter.class);
            RegExFilter.setRegexs(is, null, null, null, term, false);
          } else {
            is = new IteratorSetting(priority++, "name" + i, GrepValueFilter.class);
            GrepValueFilter.setTerm(is, term);
            if (families.contains(term)) {
              bs.fetchColumnFamily(new Text(term));
            }
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

      // Get stats, not logs
      if (opts.duration != null) {
        final long duration = opts.duration;
        SimpleDateFormat fmt = new SimpleDateFormat(LogEntry.DATE_FORMAT);
        // Stats iterator pulls out counts by CF
        IteratorSetting is = new IteratorSetting(priority++, StatsIterator.class);
        StatsIterator.duration(is, opts.duration, TimeUnit.MILLISECONDS);
        bs.addScanIterator(is);
        // Group counts under the right "bucket" of time
        SortedMap<Long,Map<String,Long>> stats = new TreeMap<>();
        for (Entry<Key,Value> entry : bs) {
          Key key = entry.getKey();
          long ts = StatsIterator.getTs(key, fmt);
          // convert to start time for this bucket
          ts -= ts % duration;
          Map<String,Long> byCF = stats.get(ts);
          if (byCF == null) {
            stats.put(ts, byCF = new TreeMap<>());
          }
          // Add values, by name given a string: "NAME:VALUE,NAME2:VALUE2"
          String value = entry.getValue().toString();
          if (!value.isEmpty()) {
            String nameCounts[] = value.split(",");
            for (String nameCount : nameCounts) {
              String parts[] = nameCount.split(":");
              Long current = byCF.get(parts[0]);
              if (current == null) {
                current = Long.decode(parts[1]);
              } else {
                current = Long.decode(parts[1]) + current.longValue();
              }
              byCF.put(parts[0], current);
            }
          }
        }
        if (stats.isEmpty()) return;
        // Use the range of the data, or a user specified range, if provided
        long start = stats.firstKey();
        long end = stats.lastKey();
        if (opts.start != null) {
          start = opts.start - (opts.start % duration);
        }
        if (opts.end != null) {
          end = opts.end - (opts.end % duration);
        }
        // Print a line for each bucket, even if there's no data
        for (long time = start; time <= end; time += duration) {
          Map<String,Long> byCF = stats.get(time);
          List<String> byCFList = new ArrayList<>();
          if (byCF != null) {
            for (Entry<String,Long> entry : byCF.entrySet()) {
              byCFList.add(String.format("%s: %d", entry.getKey(), entry.getValue()));
            }
          }
          printer.println(String.format("%s\t%s", fmt.format(new Date(time)), Joiner.on(", ").join(byCFList)));
        }
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
