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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.StringReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import kafka.Kafka;

public class SimpleServerIT {

  //@formatter:off
  private static final String LOGGER_CONFIG =
      "[KafkaConsumer]\n" +
      "zookeeper.connect = %ZOOKEEPER%/kafka\n" +
      "group.id = logger\n" +
      "\n" +
      "[kafka]\n" +
      "topic = logs\n" +
      "\n" +
      "[server]\n" +
      "zookeepers = %ZOOKEEPER%/loggers\n" +
      "tcp.port = %PORT%\n" +
      "udp.port = %PORT%\n" +
      "\n" +
      "[accumulo]\n" +
      "instance.name = %INSTANCE%\n" +
      "instance.zookeeper.host = %ZOOKEEPER%\n" +
      "\n" +
      "[batchwriter]\n" +
      "maxLatency = 1s\n"
      ;
  private static final String KAFKA_CONFIG =
      "broker.id = 0\n" +
      "port = %PORT%\n" +
      "log.dirs = %LOGS%\n" +
      "zookeeper.connect = %ZOOKEEPER%/kafka\n"
      ;
  private static final Logger LOG = LoggerFactory.getLogger(SimpleServerIT.class);
  //@formatter:on
  static Connector conn;
  static MiniAccumuloClusterImpl cluster;
  static Server server;
  static ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private static Process kafka;
  private static int loggerPort;
  private static int kafkaPort;

  @BeforeClass
  public static void setup() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/" + SimpleServerIT.class.getName());
    FileUtils.deleteDirectory(baseDir);
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    String passwd = "secret";
    cluster = new MiniAccumuloClusterImpl(new MiniAccumuloConfigImpl(baseDir, passwd));
    cluster.start();
    conn = cluster.getConnector("root", new PasswordToken(passwd));
    File kafkaConfig = new File(baseDir, "kafkaConfig");
    File kafkaLogs = new File(baseDir, "kafkaLogs");
    loggerPort = PortUtils.getRandomFreePort();
    String configString = KAFKA_CONFIG;
    configString = configString.replace("%ZOOKEEPER%", cluster.getZooKeepers());
    configString = configString.replace("%PORT%", "" + loggerPort);
    configString = configString.replace("%LOGS%", kafkaLogs.toString());
    FileUtils.write(kafkaConfig, configString);
    kafka = cluster.exec(Kafka.class, kafkaConfig.toString());
    final CountDownLatch latch = new CountDownLatch(1);
    ZooKeeper zookeeper = new ZooKeeper(cluster.getZooKeepers(), 10 * 1000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        LOG.info("Got a zookeeper event: " + event);
        latch.countDown();
      }
    });
    latch.await();
    LOG.info("Assuming zookeeper connected");
    assertEquals(ZooKeeper.States.CONNECTED, zookeeper.getState());
    zookeeper.create("/loggers", new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zookeeper.close();
    ServerConfiguration config = new ServerConfiguration();
    kafkaPort = PortUtils.getRandomFreePort();
    configString = LOGGER_CONFIG;
    configString = configString.replace("%ZOOKEEPER%", cluster.getZooKeepers());
    configString = configString.replace("%INSTANCE%", cluster.getInstanceName());
    configString = configString.replace("%PORT%", "" + kafkaPort);
    System.out.println(configString);
    config.load(new StringReader(configString));
    server = new Server();
    server.initialize(config);
    // start a log service
    threadPool.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return server.run();
      }
    });
    // wait for it to start
    sleepUninterruptibly(1, TimeUnit.SECONDS);
  }

  @AfterClass
  public static void teardown() throws Exception {
    // tear everything down
    kafka.destroy();
    server.stop();
    threadPool.shutdown();
    threadPool.awaitTermination(1, TimeUnit.MINUTES);
    cluster.stop();
  }

  @Test(timeout = 60 * 1000)
  public void sunnyDay() throws Exception {
    // no log files exist
    assertEquals(0, Iterators.size(conn.createScanner("logs", Authorizations.EMPTY).iterator()));
    // send a tcp message
    Socket s = new Socket("localhost", loggerPort);
    assertTrue(s.isConnected());
    s.getOutputStream().write("localhost tester 2014-01-01 01:01:01,123 This is a test message\n\n".getBytes(UTF_8));
    s.close();
    // send a udp message
    DatagramSocket ds = new DatagramSocket();
    String otherMessage = "localhost test2 2014-01-01 01:01:01,345 [INFO] This is a 2nd message";
    byte[] otherMessageBytes = otherMessage.getBytes(UTF_8);
    InetSocketAddress dest = new InetSocketAddress("localhost", loggerPort);
    ds.send(new DatagramPacket(otherMessageBytes, otherMessageBytes.length, dest));
    ds.close();
    // wait for a flush
    sleepUninterruptibly(8, TimeUnit.SECONDS);
    // verify the messages are stored
    Scanner scanner = conn.createScanner("logs", Authorizations.EMPTY);
    assertEquals(2, Iterators.size(scanner.iterator()));
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    Entry<Key,Value> next = iter.next();
    assertEquals(next.getValue().toString(), "This is a test message");
    assertEquals(next.getKey().getColumnQualifier().toString(), "tester\0localhost");
    assertEquals(next.getKey().getColumnFamily().toString(), "UNKNOWN");
    assertTrue(next.getKey().getRow().toString().endsWith("2014-01-01 01:01:01,123"));
    next = iter.next();
    assertEquals(next.getValue().toString(), "[INFO] This is a 2nd message");
    assertEquals(next.getKey().getColumnQualifier().toString(), "test2\0localhost");
    assertTrue(next.getKey().getRow().toString().endsWith("2014-01-01 01:01:01,123"));
    assertFalse(iter.hasNext());
    sleepUninterruptibly(30, TimeUnit.SECONDS);
    conn.tableOperations().deleteRows("logs", null, null);
  }

  public static class LogSomething {

    public static void main(String args[]) {
      Logger LOG = LoggerFactory.getLogger(LogSomething.class);
      LOG.error("Ohai, I am a log message");
    }
  }

  @Test(timeout = 60 * 1000)
  public void testAppender() throws Exception {
    Path temp = new Path(cluster.getConfig().getDir().toString(), "temp");
    FileSystem fs = cluster.getFileSystem();
    fs.mkdirs(temp);
    Path log4j = new Path(temp, "log4j.properties");
    FSDataOutputStream writer = fs.create(log4j);
    writer.writeBytes("log4j.rootLogger=ERROR,A1\n");
    writer.writeBytes("log4j.appender.A1=" + org.logjam.client.UDPAppender.class.getName() + "\n");
    writer.writeBytes("log4j.appender.A1.layout=org.apache.log4j.EnhancedPatternLayout\n");
    writer.writeBytes("log4j.appender.A1.layout.ConversionPattern=localhost someapp %d{ISO8601} [%c] %p: %m%n\n");
    writer.writeBytes("log4j.appender.A1.RemoteHost=localhost\n");
    writer.close();
    assertEquals(0, cluster.exec(LogSomething.class, Arrays.asList("-Dlog4j.configuration=file://" + log4j.toString(), "-Dlog4j.debug=true")).waitFor());
    sleepUninterruptibly(8, TimeUnit.SECONDS);
    verify();
  }

  @Test(timeout = 60 * 1000)
  public void testZooUDPAppender() throws Exception {
    Path temp = new Path(cluster.getConfig().getDir().toString(), "temp");
    FileSystem fs = cluster.getFileSystem();
    fs.mkdirs(temp);
    Path log4j = new Path(temp, "log4j.properties");
    FSDataOutputStream writer = fs.create(log4j);
    writer.writeBytes("log4j.rootLogger=ERROR,A1\n");
    writer.writeBytes("log4j.appender.A1=" + org.logjam.client.ZooUDPAppender.class.getName() + "\n");
    writer.writeBytes("log4j.appender.A1.layout=org.apache.log4j.EnhancedPatternLayout\n");
    writer.writeBytes("log4j.appender.A1.layout.ConversionPattern=localhost someapp %d{ISO8601} [%c] %p: %m%n\n");
    // writer.writeBytes("log4j.appender.A1.zookeepers=" + options.zookeepers + "/udp\n");
    writer.close();
    assertEquals(0, cluster.exec(LogSomething.class, Arrays.asList("-Dlog4j.configuration=file://" + log4j.toString(), "-Dlog4j.debug=true")).waitFor());
    sleepUninterruptibly(8, TimeUnit.SECONDS);

    verify();
  }

  private void verify() throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    Scanner scanner = conn.createScanner("logs", Authorizations.EMPTY);
    assertEquals(1, Iterators.size(scanner.iterator()));
    Entry<Key,Value> next = scanner.iterator().next();
    assertEquals(next.getKey().getColumnQualifier().toString(), "someapp");
    assertTrue(next.getKey().getRow().toString().startsWith("localhost"));
    assertTrue(next.getValue().toString().contains("Ohai, I am a log message"));
    // conn.tableOperations().deleteRows(options.table, null, null);
  }

  @Test(timeout = 60 * 1000)
  public void testZooSocketAppender() throws Exception {
    Path temp = new Path(cluster.getConfig().getDir().toString(), "temp");
    FileSystem fs = cluster.getFileSystem();
    fs.mkdirs(temp);
    Path log4j = new Path(temp, "log4j.properties");
    FSDataOutputStream writer = fs.create(log4j);
    writer.writeBytes("log4j.rootLogger=ERROR,A1\n");
    writer.writeBytes("log4j.appender.A1=" + org.logjam.client.ZooSocketAppender.class.getName() + "\n");
    writer.writeBytes("log4j.appender.A1.layout=org.apache.log4j.EnhancedPatternLayout\n");
    writer.writeBytes("log4j.appender.A1.layout.ConversionPattern=localhost someapp %d{ISO8601} [%c] %p: %m%n%n\n");
    // writer.writeBytes("log4j.appender.A1.zookeepers=" + options.zookeepers + "/tcp\n");
    writer.close();
    assertEquals(0, cluster.exec(LogSomething.class, Arrays.asList("-Dlog4j.configuration=file://" + log4j.toString(), "-Dlog4j.debug=true")).waitFor());
    sleepUninterruptibly(8, TimeUnit.SECONDS);
    verify();
  }
}
