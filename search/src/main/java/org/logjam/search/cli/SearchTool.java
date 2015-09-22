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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.logjam.client.LogEntry;
import org.logjam.schema.Schema;
import org.logjam.search.iterators.CountingIterator;
import org.logjam.search.iterators.GrepValueFilter;
import org.logjam.search.iterators.HostAndApplicationFilter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Joiner;

public class SearchTool {
  public static class TimeConverter implements IStringConverter<Long> {
    private static final String SHORT_FORMAT = "yyyy-MM-dd hh:mm:ss";
    private static final String DATE_ONLY = "yyyy-MM-dd";

    @Override
    public Long convert(String value) {
      for (String format : new String[] {LogEntry.DATE_FORMAT, SHORT_FORMAT, DATE_ONLY}) {
        try {
          return new SimpleDateFormat(format).parse(value).getTime();
        } catch (ParseException e) {}
      }
      throw new RuntimeException("Unable to parse date/time");
    }
  }

  public static class SliceConverter implements IStringConverter<Slice> {
    @Override
    public Slice convert(String value) {
      String[] parts = value.split(":");
      if (parts.length != 2) {
        throw new RuntimeException("Unable to parse \"" + value + "\" as a slice");
      }
      int start = -1, end = -1;
      if (parts[0].length() > 0) {
        start = Integer.parseInt(parts[0]);
      }
      if (parts[1].length() > 0) {
        end = Integer.parseInt(parts[1]);
      }
      return new Slice(start, end);
    }
  }

  static class Options {
    @Parameter(names = {"--start", "-s"}, converter = TimeConverter.class)
    long start = 0;

    @Parameter(names = {"--end", "-e"}, converter = TimeConverter.class)
    long end = 0;

    @Parameter(names = {"--count"})
    boolean count = false;

    @Parameter(names = {"--sort"}, description = "sort entries by time")
    boolean sort = false;

    @Parameter(names = {"--reverse", "-r"}, description = "sort, but in reverse time order")
    boolean reverse = false;

    @Parameter(names = {"--host", "-h"}, description = "host name(s)")
    List<String> hosts = new ArrayList<>();

    @Parameter(names = {"--app", "-a"}, description = "application name(s)")
    List<String> applications = new ArrayList<>();

    @Parameter(names = {"--regexp"}, description = "The search terms are regular expressions")
    boolean regexp = false;

    @Parameter(description = "search terms")
    List<String> terms = new ArrayList<>();

    @Parameter(names = {"--user", "-u"})
    String user = "root";

    @Parameter(names = {"--password", "-p"})
    String password = "secret";

    @Parameter(names = {"--table"})
    String table = "logs";
  }

  public static void main(String[] args) throws Exception {
    Options opts = new Options();
    JCommander parser = new JCommander(opts);
    try {
      parser.setProgramName(SearchTool.class.getSimpleName());
      parser.parse(args);
    } catch (ParameterException e) {
      parser.usage();
      System.exit(1);
    }
    ClientConfiguration config = ClientConfiguration.loadDefault();
    Instance instance = new ZooKeeperInstance(config);
    Connector conn = instance.getConnector(opts.user, new PasswordToken(opts.password.getBytes(UTF_8)));
    query(conn, opts, System.out);
  }

  private static void query(Connector conn, Options opts, PrintStream printer) throws Exception {
    BatchScanner bs = conn.createBatchScanner(opts.table, Authorizations.EMPTY, 8);
    try {
      SimpleDateFormat sdf = new SimpleDateFormat(LogEntry.DATE_FORMAT);
      String startDate = "";
      if (opts.start > 0) {
        startDate = sdf.format(new Date(opts.start));
      }
      String endDate = "9999";
      if (opts.end > 0) {
        endDate = sdf.format(new Date(opts.end));
      }
      List<Range> ranges = new ArrayList<>(Schema.SHARDS);
      for (int i = 0; i < Schema.SHARDS; i++) {
        Range r = new Range(String.format("%04x %s", i, startDate), String.format("%04x %s", i, endDate));
        ranges.add(r);
      }
      bs.setRanges(ranges);
      if (!opts.hosts.isEmpty() || !opts.applications.isEmpty()) {
        IteratorSetting is = new IteratorSetting(100, HostAndApplicationFilter.class);
        HostAndApplicationFilter.setApps(is, opts.applications);
        HostAndApplicationFilter.setHosts(is, opts.hosts);
        bs.addScanIterator(is);
      }
      if (!opts.terms.isEmpty()) {
        if (opts.regexp) {
          Joiner joiner = Joiner.on("|");
          IteratorSetting is = new IteratorSetting(100, RegExFilter.class);
          String anyTerm = joiner.join(opts.terms);
          RegExFilter.setRegexs(is, null, null, null, anyTerm, false);
          bs.addScanIterator(is);
        } else {
          for (int i = 0; i < opts.terms.size(); i++) {
            IteratorSetting is = new IteratorSetting(100 + i, "name" + i, GrepValueFilter.class);
            GrepValueFilter.setTerm(is, opts.terms.get(i));
            bs.addScanIterator(is);
          }
        }
      }
      if (opts.count) {
        IteratorSetting is = new IteratorSetting(200, CountingIterator.class);
        bs.addScanIterator(is);
        long total = 0;
        for (Entry<Key,Value> entry : bs) {
          total += Long.parseLong(entry.getValue().toString());
        }
        printer.println(total);
        return;
      }

      ArrayList<Entry<Key,Value>> results = new ArrayList<Entry<Key,Value>>();
      for (Entry<Key,Value> entry : bs) {
        results.add(entry);
      }
      if (opts.sort) {
        final int order = opts.reverse ? -1 : 1;
        Collections.sort(results, new Comparator<Entry<Key,Value>>() {
          @Override
          public int compare(Entry<Key,Value> o1, Entry<Key,Value> o2) {
            Text row = o1.getKey().getRow();
            Text row2 = o2.getKey().getRow();
            return order * BytesWritable.Comparator.compareBytes(row.getBytes(), 5, row.getLength() - 5, row2.getBytes(), 5, row2.getLength() - 5);
          }
        });
      }
      for (Entry<Key,Value> entry : results) {
        String cq = entry.getKey().getColumnQualifier().toString();
        String parts[] = cq.split("\0");
        String row = entry.getKey().getRow().toString();
        String value = entry.getValue().toString();
        printer.println(String.format("%s\t%s\t%s\t%s", row.substring(5), parts[0], parts[1], value));
      }
    } finally {
      bs.close();
    }
  }

}
