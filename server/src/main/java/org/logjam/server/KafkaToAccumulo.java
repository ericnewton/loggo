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

import java.io.File;
import java.io.FileInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.logjam.client.LogEntry;
import org.logjam.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableMap;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

/*
 * Consumes messages from Kafka of the form
 * <p>
 * hostname application %d{ISO8601} any message here you like
 * <p>
 * and stores the result in a table of the form [row][cf][cq][value]:
 * [shard date][log] [application\0host][message]
 *
 */
public class KafkaToAccumulo {

  static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

  public static class Options {

    @Parameter(names = {"--config"}, required = true)
    String config;
  }

  static class LogEntryDecoder implements Decoder<LogEntry> {
    SimpleDateFormat formatter = new SimpleDateFormat(LogEntry.DATE_FORMAT);

    @Override
    public LogEntry fromBytes(byte[] data) {
      try {
        return LogEntry.parseEntry(new String(data, UTF_8), formatter);
      } catch (ParseException e) {
        LOG.info("Unable to parse message: {}", e.toString());
        return null;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    JCommander commander = new JCommander();
    Options opts = new Options();
    commander.addObject(opts);
    try {
      commander.parse(args);
    } catch (ParameterException ex) {
      commander.usage();
      exitWithError(ex.getMessage(), 1);
    }
    KafkaToAccumulo server = new KafkaToAccumulo();
    server.initialize(opts);
    server.exit(server.run());
  }

  private ConsumerConfig consumerConfig;
  private String topic = "logs";
  private String table = "logs";
  private Connector connector;

  private void exit(int status) {
    System.exit(status);
  }

  private int run() throws Exception {
    SimpleDateFormat formatter = new SimpleDateFormat(LogEntry.DATE_FORMAT);
    ConsumerConnector cconnector = Consumer.createJavaConsumerConnector(this.consumerConfig);
    Map<String,List<KafkaStream<String,LogEntry>>> topMessageStreams = cconnector.createMessageStreams(ImmutableMap.of(topic, 1), new StringDecoder(null),
        new LogEntryDecoder());
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxLatency(5, TimeUnit.SECONDS);
    BatchWriter bw = connector.createBatchWriter(table, config);
    for (List<KafkaStream<String,LogEntry>> streams : topMessageStreams.values()) {
      for (KafkaStream<String,LogEntry> stream : streams) {
        ConsumerIterator<String,LogEntry> iterator = stream.iterator();
        while (iterator.hasNext()) {
          MessageAndMetadata<String,LogEntry> entry = iterator.next();
          LogEntry message = entry.message();
          if (message != null) {
            bw.addMutation(Writer.logEntryToMutation(message, formatter));
          }
        }
        bw.flush();
      }
    }
    bw.close();
    return 0;
  }

  private static void exitWithError(String message, int status) {
    System.out.println(message);
    System.exit(status);
  }

  public void initialize(Options opts) throws Exception {
    Properties props = new Properties();
    File configFile = new File(opts.config);
    props.load(new FileInputStream(configFile));
    this.consumerConfig = new ConsumerConfig(props);
    ClientConfiguration clientConfiguration = ClientConfiguration.loadDefault();
    topic = props.getProperty("topic", topic);
    table = props.getProperty("table", table);
    String username = props.getProperty("accumulo.user", "root");
    String password = props.getProperty("accumulo.password", "secret");
    Instance instance = new ZooKeeperInstance(clientConfiguration);
    connector = instance.getConnector(username, new PasswordToken(password.getBytes()));
    if (!connector.tableOperations().exists(table)) {
      try {
        connector.tableOperations().create(table);
        TreeSet<Text> splits = new TreeSet<Text>();
        int splitSize = Schema.SHARDS / 10;
        for (int i = splitSize; i < Schema.SHARDS - splitSize; i += splitSize) {
          splits.add(new Text(String.format(Schema.SHARD_FORMAT, i)));
        }
        connector.tableOperations().addSplits(table, splits);
        IteratorSetting is = new IteratorSetting(100, AgeOffFilter.class);
        AgeOffFilter.setTTL(is, 31 * 24 * 60 * 60 * 1000L);
        connector.tableOperations().attachIterator(table, is);
      } catch (TableExistsException ex) {
        // perhaps a peer server created it
      }
    }
  }
}
