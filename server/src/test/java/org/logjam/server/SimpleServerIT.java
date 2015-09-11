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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.logjam.server.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class SimpleServerIT {

  static Connector conn;
  static MiniAccumuloClusterImpl cluster;
  static Options options = new Options();
  static Server server;
  static ExecutorService threadPool = Executors.newFixedThreadPool(1);

  @BeforeClass
  public static void setup() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/" + SimpleServerIT.class.getName());
    FileUtils.deleteDirectory(baseDir);
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    String passwd = "secret";
    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(baseDir, passwd);
    cluster = new MiniAccumuloClusterImpl(config);
    cluster.start();
    ClientConfiguration clientConfig = cluster.getClientConfig();
    File ccFile = new File(baseDir, "clientConfig");
    FileUtils.write(ccFile, clientConfig.serialize());
    conn = cluster.getConnector("root", new PasswordToken(passwd));
    conn.tableOperations().create(options.table);
    server = new Server();
    options.clientConfigurationFile = ccFile.getAbsolutePath();
    options.zookeepers = conn.getInstance().getZooKeepers() + "/loggers";
    ZooReaderWriter zookeeper = new ZooReaderWriter(conn.getInstance().getZooKeepers(), 30 * 1000, "");
    zookeeper.mkdirs("/loggers");
    server.initialize(options);
    // start a log service
    threadPool.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return server.run();
      }
    });
    // wait for it to start
    sleepUninterruptibly(5, TimeUnit.SECONDS);
  }

  @AfterClass
  public static void teardown() throws Exception {
    // tear everything down
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
    Socket s = new Socket(options.host, options.port);
    s.getOutputStream().write("localhost tester 2014-01-01 01:01:01,123 This is a test message\n\n".getBytes(UTF_8));
    s.close();
    // send a udp message
    DatagramSocket ds = new DatagramSocket();
    String otherMessage = "localhost test2 2014-01-01 01:01:01,345 This is a 2nd message";
    byte[] otherMessageBytes = otherMessage.getBytes(UTF_8);
    InetSocketAddress dest = new InetSocketAddress(options.host, options.port);
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
    assertEquals(next.getKey().getColumnQualifier().toString(), "tester");
    assertEquals(next.getKey().getRow().toString(), "localhost 2014-01-01 01:01:01,123");
    next = iter.next();
    assertEquals(next.getValue().toString(), "This is a 2nd message");
    assertEquals(next.getKey().getColumnQualifier().toString(), "test2");
    assertEquals(next.getKey().getRow().toString(), "localhost 2014-01-01 01:01:01,345");
    assertFalse(iter.hasNext());
    conn.tableOperations().deleteRows(options.table, null, null);
  }

  public static class LogSomething {

    public static void main(String args[]) {
      Logger LOG = LoggerFactory.getLogger(LogSomething.class);
      LOG.error("Ohai, I am a log message");
    }
  }

  @Test(timeout = 60 * 1000)
  public void testAppender() throws Exception {
    Path temp = cluster.getTemporaryPath();
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
    Scanner scanner = conn.createScanner("logs", Authorizations.EMPTY);
    assertEquals(1, Iterators.size(scanner.iterator()));
    Entry<Key,Value> next = scanner.iterator().next();
    assertEquals(next.getKey().getColumnQualifier().toString(), "someapp");
    assertTrue(next.getKey().getRow().toString().startsWith("localhost"));
    assertTrue(next.getValue().toString().contains("Ohai, I am a log message"));
    conn.tableOperations().deleteRows(options.table, null, null);
  }

  @Test(timeout = 60 * 1000)
  public void testZooUDPAppender() throws Exception {
    Path temp = cluster.getTemporaryPath();
    FileSystem fs = cluster.getFileSystem();
    fs.mkdirs(temp);
    Path log4j = new Path(temp, "log4j.properties");
    FSDataOutputStream writer = fs.create(log4j);
    writer.writeBytes("log4j.rootLogger=ERROR,A1\n");
    writer.writeBytes("log4j.appender.A1=" + org.logjam.client.ZooUDPAppender.class.getName() + "\n");
    writer.writeBytes("log4j.appender.A1.layout=org.apache.log4j.EnhancedPatternLayout\n");
    writer.writeBytes("log4j.appender.A1.layout.ConversionPattern=localhost someapp %d{ISO8601} [%c] %p: %m%n\n");
    writer.writeBytes("log4j.appender.A1.zookeepers=" + options.zookeepers + "/udp\n");
    writer.close();
    assertEquals(0, cluster.exec(LogSomething.class, Arrays.asList("-Dlog4j.configuration=file://" + log4j.toString(), "-Dlog4j.debug=true")).waitFor());
    sleepUninterruptibly(8, TimeUnit.SECONDS);
    Scanner scanner = conn.createScanner("logs", Authorizations.EMPTY);
    assertEquals(1, Iterators.size(scanner.iterator()));
    Entry<Key,Value> next = scanner.iterator().next();
    assertEquals(next.getKey().getColumnQualifier().toString(), "someapp");
    assertTrue(next.getKey().getRow().toString().startsWith("localhost"));
    assertTrue(next.getValue().toString().contains("Ohai, I am a log message"));
    conn.tableOperations().deleteRows(options.table, null, null);
  }
}
