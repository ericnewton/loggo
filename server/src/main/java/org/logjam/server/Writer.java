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
package org.logjam.server;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.logjam.client.LogEntry;
import org.logjam.server.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Writer {

  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
  private static final LogEntry TERMINATE = new LogEntry();
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat(" " + LogEntry.DATE_FORMAT);
  private static final BatchWriterOpts BW_OPTS = new BatchWriterOpts();

  private final ExecutorService threadPool;
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final Connector connector;
  private final Options options;
  private final LinkedBlockingDeque<LogEntry> source;

  static {
    BW_OPTS.batchLatency = 5 * 1000L;
  }

  public Writer(LinkedBlockingDeque<LogEntry> source, ClientConfiguration clientConf, Options options) {
    this.source = source;
    this.options = options;
    this.threadPool = Executors.newFixedThreadPool(1);
    Instance instance = getInstance(clientConf);
    try {
      this.connector = instance.getConnector(options.user, new PasswordToken(options.password.value));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  protected Instance getInstance(ClientConfiguration clientConf) {
    LOG.info("About to talk to zookeeper, if this hangs, check zookeeper");
    return new ZooKeeperInstance(clientConf);
  }

  public void start() {
    threadPool.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        while (!stop.get()) {
          try {
            BatchWriter bw = connector.createBatchWriter(options.table, null);
            while (!stop.get()) {
              LogEntry entry = source.take();
              if (entry == TERMINATE) {
                bw.close();
                return 0;
              }
              Mutation m = new Mutation(entry.host + dateFormat.format(new Date(entry.timestamp)));
              m.put("", entry.app, entry.message);
              bw.addMutation(m);
            }
          } catch (TableNotFoundException ex) {
            LOG.warn("table " + options.table + " does not exist");
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
