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
package org.logjam.client;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/*
 * SocketAppender that uses the simplified data format, and finds the destination host using entries registered in zookeeper.
 * Borrows heavily from SocketAppender
 */
public class ZooSocketAppender extends AppenderSkeleton implements Watcher {

  static final int DEFAULT_RECONNECTION_DELAY = 30 * 1000;

  private ZooKeeper zookeeper;
  private Socket socket;

  private String zookeepers;
  private int zookeeperTimeout = 30 * 1000;
  private String hostname;
  private String application;
  private int reconnectionDelay = DEFAULT_RECONNECTION_DELAY;

  private Connector connector;

  @Override
  public void activateOptions() {
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException uhe) {
      try {
        hostname = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException uhe2) {
        hostname = "unknown";
      }
    }

    // allow system property of application to be primary
    if (application == null) {
      application = System.getProperty(ZooUDPAppender.APPLICATION_KEY);
    } else {
      if (System.getProperty(ZooUDPAppender.APPLICATION_KEY) != null) {
        application = application + "-" + System.getProperty(ZooUDPAppender.APPLICATION_KEY);
      }
    }

    if (zookeepers != null) {
      try {
        this.zookeeper = new ZooKeeper(this.zookeepers, this.zookeeperTimeout, this);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to use zookeeper setting: " + this.zookeepers);
      }
    } else {
      String err = "The Zookeepers property is required for ZooSocketAppender named " + name;
      LogLog.error(err);
      throw new IllegalStateException(err);
    }
  }

  public ZooSocketAppender() {}

  @Override
  public void close() {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException e) {
        LogLog.debug("error closing socket: " + e);
      }
      socket = null;
    }
    if (connector != null) {
      connector.interrupted.set(true);
      connector.interrupt();
      connector = null;
    }
    if (zookeeper != null) {
      try {
        zookeeper.close();
      } catch (InterruptedException e) {
        LogLog.debug("Error closing zookeeper: " + e);
      }
    }
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }

  @Override
  protected void append(LoggingEvent event) {
    if (zookeeper == null) {
      return;
    }
    if (socket == null) {
      return;
    }
    event.setProperty(ZooUDPAppender.HOSTNAME_KEY, hostname);
    if (application != null) {
      event.setProperty(ZooUDPAppender.APPLICATION_KEY, application);
    }

    try {
      StringBuffer buf = new StringBuffer(layout.format(event));
      byte[] payload;
      payload = buf.toString().getBytes(UTF_8);
      socket.getOutputStream().write(payload);
      socket.getOutputStream().flush();
    } catch (IOException e) {
      LogLog.warn("Detected problem with connection: " + e);
      close();
      connect();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    switch (event.getState()) {
      case SyncConnected:
        LogLog.debug("Connected to zookeeper");
        connect();
        break;
      default:
        LogLog.debug("Unexpected event: " + event);
    }
  }

  private void connect() {
    if (socket != null) {
      return;
    }
    if (connector != null) {
      return;
    }
    if (zookeeper == null) {
      return;
    }
    try {
      LogLog.debug("Reading registered loggers from " + zookeepers);
      List<String> children = zookeeper.getChildren("/", new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          LogLog.debug("Detected a change in loggers, reconnecting");
          close();
          connect();
        }
      });
      if (children.isEmpty()) {
        LogLog.debug("No loggers found");
        return;
      }
      LogLog.debug("Found " + children.size() + " loggers registered at " + zookeepers);
      Random random = new Random();
      int choice = random.nextInt(children.size());
      String path = "/" + children.get(choice);
      byte[] data = zookeeper.getData(path, false, null);
      String address = new String(data, UTF_8);
      LogLog.debug("Selected logger at " + address);
      String parts[] = address.split(":", 2);
      if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
        LogLog.debug("Unable to parse address " + address + " found at " + path);
      }
      int port = Integer.decode(parts[1]);
      connector = new Connector(parts[0], port);
      connector.start();
    } catch (KeeperException | InterruptedException e) {
      LogLog.debug("Exception reading from zookeeper: " + e, e);
    } catch (Exception ex) {
      LogLog.debug("Error " + ex.toString(), ex);
    }
  }

  private class Connector extends Thread {
    final String host;
    final int port;
    AtomicBoolean interrupted = new AtomicBoolean(false);

    public Connector(String host, int port) {
      this.host = host;
      this.port = port;
      setDaemon(true);
      setPriority(Thread.MIN_PRIORITY);
    }

    @Override
    public void run() {
      while (!interrupted.get()) {
        try {
          InetAddress address = InetAddress.getByName(host);
          LogLog.debug("Attempting to connect to " + host + ":" + port);
          Socket sock = new Socket(address, port);
          LogLog.debug("Connected to " + host + ":" + port);
          socket = sock;
          connector = null;
          return;
        } catch (Exception ex) {
          LogLog.debug("Error connecting to " + host + ":" + port + ", retrying");
        }
        try {
          sleep(reconnectionDelay);
        } catch (InterruptedException e) {}
      }
    }
  }

  public String getZookeepers() {
    return zookeepers;
  }

  public void setZookeepers(String zookeepers) {
    this.zookeepers = zookeepers;
  }

  public int getZookeeperTimeout() {
    return zookeeperTimeout;
  }

  public void setZookeeperTimeout(int zookeeperTimeout) {
    this.zookeeperTimeout = zookeeperTimeout;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getApplication() {
    return application;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  public int getReconnectionDelay() {
    return reconnectionDelay;
  }

  public void setReconnectionDelay(int reconnectionDelay) {
    this.reconnectionDelay = reconnectionDelay;
  }

}
