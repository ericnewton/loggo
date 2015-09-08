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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.logjam.server.options.Options;

import com.google.common.collect.Iterators;

public class SimpleServerIT {

  @Test(timeout = 60 * 1000)
  public void test() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/" + getClass().getName());
    FileUtils.deleteDirectory(baseDir);
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    String passwd = "secret";
    MiniAccumuloConfig config = new MiniAccumuloConfig(baseDir, passwd);
    MiniAccumuloCluster c = new MiniAccumuloCluster(config);
    c.start();
    ClientConfiguration clientConfig = c.getClientConfig();
    File ccFile = new File(baseDir, "clientConfig");
    FileUtils.write(ccFile, clientConfig.serialize());
    Connector conn = c.getConnector("root", passwd);
    conn.tableOperations().create("logs");
    assertEquals(0, Iterators.size(conn.createScanner("logs", Authorizations.EMPTY).iterator()));
    final Server server = new Server();
    Options options = new Options();
    options.clientConfigurationFile = ccFile.getAbsolutePath();
    server.initialize(options);
    ExecutorService t = Executors.newFixedThreadPool(1);
    t.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return server.run();
      }
    });
    sleepUninterruptibly(5, TimeUnit.SECONDS);
    Socket s = new Socket(options.host, options.port);
    s.getOutputStream().write("localhost tester 2014-01-01 01:01:01,123 This is a test message\n\n".getBytes(UTF_8));
    s.close();
    DatagramSocket ds = new DatagramSocket();
    String otherMessage = "localhost test2 2014-01-01 01:01:01,345 This is a 2nd message";
    byte[] otherMessageBytes = otherMessage.getBytes(UTF_8);
    InetSocketAddress dest = new InetSocketAddress(options.host, options.port);
    ds.send(new DatagramPacket(otherMessageBytes, otherMessageBytes.length, dest));
    ds.close();
    sleepUninterruptibly(8, TimeUnit.SECONDS);
    Scanner scanner = conn.createScanner("logs", Authorizations.EMPTY);
    assertEquals(2, Iterators.size(scanner.iterator()));
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    Entry<Key,Value> next = iter.next();
    assertEquals(next.getValue().toString(), "This is a test message");
    next = iter.next();
    assertEquals(next.getValue().toString(), "This is a 2nd message");
    assertFalse(iter.hasNext());
    server.stop();
    t.shutdown();
    t.awaitTermination(1, TimeUnit.MINUTES);
    c.stop();
  }

}
