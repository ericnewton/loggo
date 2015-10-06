/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.loggo.server;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.configuration.Configuration;
import org.loggo.client.LogEntry;
import org.loggo.schema.Defaults;
import org.loggo.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read LogEntries from a queue, and put them in an accumulo table.
 */
public class Writer {

  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
  private static final LogEntry TERMINATE = new LogEntry();

  private final SimpleDateFormat dateFormat = new SimpleDateFormat(LogEntry.DATE_FORMAT);
  private final ExecutorService threadPool;
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final Connector connector;
  private final LinkedBlockingDeque<LogEntry> source;
  private final Configuration conf;

  public Writer(LinkedBlockingDeque<LogEntry> source, ClientConfiguration clientConf, Configuration conf) {
    this.source = source;
    this.conf = conf;
    this.threadPool = Executors.newFixedThreadPool(1);
    Instance instance = getInstance(clientConf);
    String user = conf.getString("user", Defaults.USER);
    String passwd = conf.getString("password", Defaults.PASSWORD);
    try {
      this.connector = instance.getConnector(user, new PasswordToken(passwd.getBytes(UTF_8)));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  protected Instance getInstance(ClientConfiguration clientConf) {
    LOG.info("About to talk to zookeeper, if this hangs, check zookeeper");
    return new ZooKeeperInstance(clientConf);
  }

  static final String ROW_FORMAT = Schema.SHARD_FORMAT + " %s";

  public static Mutation logEntryToMutation(LogEntry entry, SimpleDateFormat formatter) {
    String family = Schema.DEFAULT_FAMILY;
    for (String substr : Schema.FAMILIES) {
      if (entry.message.indexOf(substr) >= 0) {
        family = substr;
        break;
      }
    }
    long hashCode = Math.abs(entry.message.hashCode() + entry.host.hashCode()) % Schema.SHARDS;
    Mutation m = new Mutation(String.format(ROW_FORMAT, hashCode, formatter.format(new Date(entry.timestamp))));
    m.put(family, entry.app + Schema.APP_HOST_SEPARATOR + entry.host, entry.message);
    return m;
  }

  public void start() {
    final String table = conf.getString("table", Defaults.TABLE);
    LOG.info("starting writer");
    threadPool.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        while (!stop.get()) {
          try {
            BatchWriterConfig bwConfig = new BatchWriterConfig();
            String latency = conf.getString("maxLatency");
            if (latency != null) {
              bwConfig.setMaxLatency(AccumuloConfiguration.getTimeInMillis(latency), TimeUnit.MILLISECONDS);
            }
            String maxMemory = conf.getString("maxMemory");
            if (maxMemory != null) {
              bwConfig.setMaxMemory(AccumuloConfiguration.getMemoryInBytes(maxMemory));
            }
            String maxThreads = conf.getString("maxWriteThreads");
            if (maxThreads != null) {
              bwConfig.setMaxWriteThreads(Integer.parseInt(maxThreads));
            }
            String timeout = conf.getString("timeout");
            if (timeout != null) {
              bwConfig.setTimeout(AccumuloConfiguration.getTimeInMillis(timeout), TimeUnit.MILLISECONDS);
            }
            BatchWriter bw = connector.createBatchWriter(table, bwConfig);
            while (!stop.get()) {
              LogEntry entry = source.take();
              if (entry == TERMINATE) {
                bw.close();
                return 0;
              }
              LOG.info(entry.message);
              bw.addMutation(logEntryToMutation(entry, dateFormat));
            }
          } catch (TableNotFoundException ex) {
            LOG.warn("table " + table + " does not exist");
            sleepUninterruptibly(1, TimeUnit.SECONDS);
          } catch (MutationsRejectedException e) {
            LOG.warn("failed to store log entry: {}", e.toString());
            return 1;
          }
        }
        return 0;
      }
    });
  }

  public void stop() {
    source.offer(TERMINATE);
    threadPool.shutdown();
    try {
      threadPool.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
