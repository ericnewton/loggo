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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.server.master.balancer.RegexGroupBalancer;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.loggo.client.LogEntry;
import org.loggo.schema.Defaults;
import org.loggo.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.auto.service.AutoService;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import kafka.admin.AdminUtils;

@AutoService(KeywordExecutable.class)
public class Server implements KeywordExecutable {
  private static final Logger LOG = LoggerFactory.getLogger(Server.class);

  public static class Options {
    @Parameter(names = {"--config"}, required = true)
    String config;
  }

  public static void main(String[] args) throws Exception {
    Server server = new Server();
    server.execute(args);
  }

  @Override
  public String keyword() {
    return "loggo-server";
  }

  @Override
  public void execute(String[] args) throws Exception {
    JCommander commander = new JCommander();
    Options opts = new Options();
    commander.addObject(opts);
    try {
      commander.parse(args);
    } catch (ParameterException ex) {
      commander.usage();
      exitWithError(ex.getMessage(), 1);
    }
    ServerConfiguration config = new ServerConfiguration(opts.config);
    initialize(config);
    exit(run());
  }

  public void exit(int status) {
    System.exit(status);
  }

  public void exitWithError(String message, int status) {
    System.err.println(message);
    exit(status);
  }

  private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final EventLoopGroup dgramGroup = new NioEventLoopGroup();
  private Writer writer;
  private Channel channel;
  private KafkaConsumer kafkaConsumer;

  int run() {
    try {
      writer.start();
      channel.closeFuture().sync();
      return 0;
    } catch (InterruptedException e) {
      LOG.info("Interrupted, quitting");
      return 1;
    } finally {
      stop();
    }
  }

  void initialize(HierarchicalINIConfiguration config) throws ConfigurationException, AccumuloException, AccumuloSecurityException {
    Configuration kafkaConsumerSection = config.getSection("KafkaConsumer");
    Configuration serverSection = config.getSection("server");
    Configuration accumuloSection = config.getSection("accumulo");
    Configuration batchSection = config.getSection("batchwriter");
    Configuration kafkaSection = config.getSection("kafka");
    ClientConfiguration clientConfig = new ClientConfiguration(accumuloSection);

    // connect to accumulo, check on the table
    String username = batchSection.getString("user", Defaults.USER);
    String password = batchSection.getString("password", Defaults.PASSWORD);
    String table = batchSection.getString("table", Defaults.TABLE);
    Instance instance = new ZooKeeperInstance(clientConfig);
    Connector connector = instance.getConnector(username, new PasswordToken(password.getBytes()));
    if (!connector.tableOperations().exists(table)) {
      createTable(connector, table);
    }
    createTopic(kafkaConsumerSection.getString("zookeeper.connect"), kafkaSection);

    LinkedBlockingDeque<LogEntry> queue = new LinkedBlockingDeque<LogEntry>(config.getInt("queue.size", Defaults.QUEUE_SIZE));
    this.writer = new Writer(queue, clientConfig, batchSection);

    ServerBootstrap b = new ServerBootstrap();
    // @formatter:off

    // tcp
    b.group(bossGroup, workerGroup)
    .channel(NioServerSocketChannel.class)
    .handler(new LoggingHandler(LogLevel.INFO))
    .childHandler(new LoggerReaderInitializer(queue));

    // udp
    Bootstrap bb = new Bootstrap();
    bb.group(dgramGroup).channel(NioDatagramChannel.class).handler(new DgramHandler(queue));

    // @formatter:on
    String host = serverSection.getString("host", Defaults.HOST);
    serverSection.setProperty("host", host);
    if (host.equals(Defaults.HOST)) {
      try {
        serverSection.setProperty("host", InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException ex) {
        throw new RuntimeException("Unable to determine local hostname: " + ex.toString());
      }
    }
    try {
      int tcpPort = serverSection.getInteger("tcp.port", Defaults.PORT);
      channel = b.bind(host, tcpPort).sync().channel();
      tcpPort = ((InetSocketAddress) channel.localAddress()).getPort();
      serverSection.setProperty("tcp.port", tcpPort);

      int udpPort = serverSection.getInteger("udp.port", Defaults.PORT);
      Channel channel2 = bb.bind(host, udpPort).sync().channel();
      udpPort = ((InetSocketAddress) channel2.localAddress()).getPort();
      serverSection.setProperty("udp.port", udpPort);

      registerInZookeeper(serverSection);
    } catch (IOException | KeeperException | InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    String zookeeperConnect = kafkaConsumerSection.getString("zookeeper.connect");
    if (zookeeperConnect != null) {
      kafkaConsumer = new KafkaConsumer();
      kafkaConsumer.initialize(config, queue);
      kafkaConsumer.start();
    }
  }

  protected void createTopic(String zookeepers, Configuration kafkaSection) {
    ZkClient zk = new ZkClient(zookeepers);
    try {
      String topic = kafkaSection.getString("topic", Defaults.TOPIC);
      if (AdminUtils.topicExists(zk, topic)) {
        return;
      }
      int partitions = kafkaSection.getInt("topic.partitions", Defaults.PARTITIONS);
      int replication = kafkaSection.getInt("topic.replication", Defaults.REPLICATIONS);
      Properties properties = new Properties();
      String propString = kafkaSection.getString("topic.properties", "");
      for (String keyValue : propString.split(",")) {
        String parts[] = keyValue.split("=");
        if (parts.length == 2) {
          properties.setProperty(parts[0], parts[1]);
        }
      }
      AdminUtils.createTopic(zk, topic, partitions, replication, properties);
    } finally {
      zk.close();
    }
  }

  protected void createTable(Connector connector, String table) throws AccumuloException, AccumuloSecurityException {
    try {
      // Create a table with 10 initial splits
      connector.tableOperations().create(table);
      sleepUninterruptibly(30, TimeUnit.SECONDS);
      TreeSet<Text> splits = new TreeSet<Text>();
      int splitSize = Schema.SHARDS / 10;
      for (int i = splitSize; i < Schema.SHARDS - splitSize; i += splitSize) {
        splits.add(new Text(String.format(Schema.SHARD_FORMAT, i)));
      }
      connector.tableOperations().addSplits(table, splits);

      // Add a 31 day age off
      IteratorSetting is = new IteratorSetting(100, AgeOffFilter.class);
      AgeOffFilter.setTTL(is, 31 * 24 * 60 * 60 * 1000L);
      connector.tableOperations().attachIterator(table, is);

      // Put DEBUG messages into their own locality group
      Map<String,Set<Text>> groups = Collections.singletonMap("debug", Collections.singleton(new Text("DEBUG")));
      connector.tableOperations().setLocalityGroups(table, groups);
      // Spread the shards out over different hosts
      connector.tableOperations().setProperty(table, RegexGroupBalancer.REGEX_PROPERTY, "(\\d\\d\\d\\d).*");
      connector.tableOperations().setProperty(table, RegexGroupBalancer.DEFAUT_GROUP_PROPERTY, "0000");
      connector.tableOperations().setProperty(table, RegexGroupBalancer.WAIT_TIME_PROPERTY, "50ms");
      connector.tableOperations().setProperty(table, Property.TABLE_LOAD_BALANCER.getKey(), RegexGroupBalancer.class.getName());
    } catch (TableExistsException ex) {
      // perhaps a peer server created it
    } catch (TableNotFoundException ex) {
    }
  }

  private void registerInZookeeper(Configuration config) throws IOException, KeeperException, InterruptedException {
    String zookeepers = config.getString("zookeepers");
    if (zookeepers != null) {
      final CountDownLatch latch = new CountDownLatch(1);
      ZooKeeper zookeeper = new ZooKeeper(zookeepers, 30 * 1000, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            latch.countDown();
          }
        });
      try {
        latch.await();
        try {
          zookeeper.create("/udp", new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ex) {
          // expected
        }
        String host = config.getString("host");
        int port = config.getInt("udp.port");
        zookeeper.create("/udp/logger-", (host + ":" + port).getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        try {
          zookeeper.create("/tcp", new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ex) {
          // expected
        }
        port = config.getInt("tcp.port");
        zookeeper.create("/tcp/logger-", (host + ":" + port).getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      } finally {
        zookeeper.close();
      }
    }
  }

  public void stop() {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    dgramGroup.shutdownGracefully();
    if (kafkaConsumer != null) {
      kafkaConsumer.stop();
    }
    writer.stop();
  }
}
