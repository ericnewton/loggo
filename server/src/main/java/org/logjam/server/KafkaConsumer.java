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
package org.logjam.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.logjam.client.LogEntry;
import org.logjam.schema.Defaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

public class KafkaConsumer {
  static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

  static class LogEntryDecoder implements Decoder<LogEntry> {
    SimpleDateFormat formatter = new SimpleDateFormat(LogEntry.DATE_FORMAT);

    @Override
    public LogEntry fromBytes(byte[] data) {
      String msg = new String(data, UTF_8);
      try {
        return LogEntry.parseEntry(msg, formatter);
      } catch (ParseException e) {
        LOG.info("Unable to parse message: {} ({})", e.toString(), msg);
        return null;
      }
    }
  }

  private final ExecutorService threadPool;
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private ConsumerConfig consumerConfig;
  private String topic;
  private LinkedBlockingDeque<LogEntry> queue;

  public KafkaConsumer() {
    threadPool = Executors.newFixedThreadPool(1);
  }

  private static Properties asProperties(Configuration conf) {
    Properties result = new Properties();
    @SuppressWarnings("rawtypes")
    Iterator keys = conf.getKeys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      result.setProperty(key, conf.getProperty(key).toString());
    }
    return result;
  }

  public void initialize(HierarchicalINIConfiguration props, LinkedBlockingDeque<LogEntry> queue) {
    SubnodeConfiguration kafkaConsumer = props.getSection("KafkaConsumer");
    this.consumerConfig = new ConsumerConfig(asProperties(kafkaConsumer));
    SubnodeConfiguration kafka = props.getSection("kafka");
    topic = kafka.getString("topic", Defaults.TOPIC);
    this.queue = queue;
  }

  public void start() {
    threadPool.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        ConsumerConnector connector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String,List<KafkaStream<String,LogEntry>>> topMessageStreams;
        topMessageStreams = connector.createMessageStreams(ImmutableMap.of(topic, 1), new StringDecoder(null), new LogEntryDecoder());
        for (List<KafkaStream<String,LogEntry>> streams : topMessageStreams.values()) {
          for (KafkaStream<String,LogEntry> stream : streams) {
            ConsumerIterator<String,LogEntry> iterator = stream.iterator();
            while (iterator.hasNext()) {
              MessageAndMetadata<String,LogEntry> entry = iterator.next();
              LogEntry message = entry.message();
              if (message != null) {
                if (!queue.offer(message)) {
                  LOG.warn("Dropping {}" + message);
                }
              }
              if (stop.get()) {
                return 0;
              }
            }
          }
        }
        return 0;
      }
    });
  }

  public void stop() {
    stop.set(true);
    threadPool.shutdownNow();
    try {
      threadPool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // don't care
    }
  }
}
