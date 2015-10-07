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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.loggo.client.LogEntry;
import org.loggo.schema.Schema;
import org.loggo.search.cli.options.LoginOptions;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class Tail implements KeywordExecutable {

  public static LogEntry toLogEntry(Key key, Value value, SimpleDateFormat fmt) throws ParseException {
    LogEntry result = new LogEntry();
    Date date = fmt.parse(key.getRow().toString().substring(Schema.SHARD_LENGTH));
    result.timestamp = date.getTime();
    String[] parts = key.getColumnQualifier().toString().split(Schema.APP_HOST_SEPARATOR);
    result.app = parts[0];
    result.host = parts[1];
    result.message = value.toString();
    return result;
  }

  @Override
  public String keyword() {
    return "loggo-tail";
  }

  @Override
  public void execute(String[] args) throws Exception {
    LoginOptions opts = new LoginOptions();
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
    SimpleDateFormat fmt = new SimpleDateFormat(LogEntry.DATE_FORMAT);
    String start = fmt.format(new Date(System.currentTimeMillis()));
    while (true) {
      BatchScanner scanner = conn.createBatchScanner(opts.table, Authorizations.EMPTY, 8);
      List<Range> ranges = new ArrayList<>(Schema.SHARDS);
      for (int i = 0; i < Schema.SHARDS; i++) {
        Range shardRange = new Range(String.format(Schema.ROW_FORMAT, i, start), false, String.format(Schema.SHARD_FORMAT, i + 1), true);
        ranges.add(shardRange);
      }
      scanner.setRanges(ranges);
      Map<Long,LogEntry> map = new TreeMap<>();
      for (Entry<Key,Value> entry : scanner) {
        LogEntry logEntry = toLogEntry(entry.getKey(), entry.getValue(), fmt);
        map.put(logEntry.timestamp, logEntry);
      }
      scanner.close();
      for (LogEntry entry : map.values()) {
        String dateTime = fmt.format(new Date(entry.timestamp));
        start = dateTime;
        System.out.println(String.format("%s\t%s\t%s\t%s", dateTime, entry.app, entry.host, entry.message));
      }
      sleepUninterruptibly(5, TimeUnit.SECONDS);
    }
  }
}
