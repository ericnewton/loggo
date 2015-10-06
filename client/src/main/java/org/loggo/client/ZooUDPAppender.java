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
package org.loggo.client;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.xml.XMLLayout;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Sends log information as a UDP datagrams.
 *
 * <p>
 * The UDPAppender is meant to be used as a diagnostic logging tool so that logging can be monitored by a simple UDP client.
 *
 * <p>
 * Messages are not sent as LoggingEvent objects but as text after applying the designated Layout.
 *
 * <p>
 * The port and remoteHost properties can be set in configuration properties. By setting the remoteHost to a broadcast address any number of clients can listen
 * for log messages.
 *
 * <p>
 * This was inspired and really extended/copied from {@link org.apache.log4j.net.SocketAppender}. Please see the docs for the proper credit to the authors of
 * that class.
 *
 * @author <a href="mailto:kbrown@versatilesolutions.com">Kevin Brown</a>
 * @author Scott Deboy &lt;sdeboy@apache.org&gt;
 */
/*
 * Trivially backported to log4j 1.2
 */
public class ZooUDPAppender extends AppenderSkeleton implements Watcher {

  public static final String APPLICATION_KEY = "application";
  public static final String HOSTNAME_KEY = "hostname";
  /**
   * We remember host name as String in addition to the resolved InetAddress so that it can be returned via getOption().
   */
  String hostname;
  String application;
  String zookeepers;
  int zookeeperTimeout = 30 * 1000;
  InetAddress address;
  DatagramSocket outSocket;

  // if there is something irrecoverably wrong with the settings, there is no
  // point in sending out packets.
  boolean inError = false;
  private ZooKeeper zookeeper;
  private Integer port;

  public ZooUDPAppender() {
    super(false);
  }

  public ZooUDPAppender(String zookeepers) {
    super(false);
    this.zookeepers = zookeepers;
    activateOptions();
  }

  /**
   * Open up zookeeper and find the location of the log writers.
   */
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
      application = System.getProperty(APPLICATION_KEY);
    } else {
      if (System.getProperty(APPLICATION_KEY) != null) {
        application = application + "-" + System.getProperty(APPLICATION_KEY);
      }
    }

    if (zookeepers != null) {
      try {
        this.zookeeper = new ZooKeeper(this.zookeepers, this.zookeeperTimeout, this);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to use zookeeper setting: " + this.zookeepers);
      }
    } else {
      String err = "The Zookeepers property is required for ZooUDPAppender named " + name;
      LogLog.error(err);
      throw new IllegalStateException(err);
    }

    if (layout == null) {
      layout = new XMLLayout();
    }

    super.activateOptions();
  }

  /**
   * Close this appender.
   * <p>
   * This will mark the appender as closed and call then {@link #cleanUp} method.
   */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }

    this.closed = true;
    cleanUp();
  }

  /**
   * Close the UDP Socket and release the underlying connector thread if it has been created
   */
  public void cleanUp() {
    if (outSocket != null) {
      try {
        outSocket.close();
      } catch (Exception e) {
        LogLog.error("Could not close outSocket.", e);
      }

      outSocket = null;
    }
  }

  void connect(InetAddress address, int port) {
    if (this.address == null) {
      return;
    }

    try {
      // First, close the previous connection if any.
      cleanUp();
      outSocket = new DatagramSocket();
      outSocket.connect(address, port);
    } catch (IOException e) {
      LogLog.error("Could not open UDP Socket for sending.", e);
      inError = true;
    }
  }

  @Override
  public void append(LoggingEvent event) {
    if (inError) {
      return;
    }

    if (event == null) {
      return;
    }

    if (address == null) {
      return;
    }

    if (outSocket == null) {
      return;
    }
    event.setProperty(HOSTNAME_KEY, hostname);
    if (application != null) {
      event.setProperty(APPLICATION_KEY, application);
    }

    try {
      StringBuffer buf = new StringBuffer(layout.format(event));

      byte[] payload;
      payload = buf.toString().getBytes(UTF_8);
      DatagramPacket dp = new DatagramPacket(payload, payload.length, address, port);
      outSocket.send(dp);
    } catch (IOException e) {
      outSocket = null;
      LogLog.warn("Detected problem with UDP connection: " + e);
    }
  }

  public boolean isActive() {
    return !inError;
  }

  InetAddress getAddressByName(String host) {
    try {
      return InetAddress.getByName(host);
    } catch (Exception e) {
      LogLog.error("Could not find address of [" + host + "].", e);
      return null;
    }
  }

  /**
   * The UDPAppender uses layouts. Hence, this method returns <code>true</code>.
   */
  @Override
  public boolean requiresLayout() {
    return true;
  }

  public void setZookeepers(String zookeepers) {
    this.zookeepers = zookeepers;
  }

  public String getZookeepers() {
    return zookeepers;
  }

  /**
   * The <b>App</b> option takes a string value which should be the name of the application getting logged. If property was already set (via system property),
   * don't set here.
   */
  public void setApplication(String app) {
    this.application = app;
  }

  /**
   * Returns value of the <b>App</b> option.
   */
  public String getApplication() {
    return application;
  }

  @SuppressWarnings("deprecation")
  @Override
  public void process(WatchedEvent event) {
    switch (event.getState()) {
      case AuthFailed:
        LogLog.error("Unable to connect to zookeeper");
        break;
      case ConnectedReadOnly:
        break;
      case Disconnected:
      case Expired:
        LogLog.error("Zookeeper disconnected: " + event.getState());
        break;
      case SaslAuthenticated:
        LogLog.debug("Zookeeper event " + event.getState());
        break;
      case SyncConnected:
        LogLog.debug("Connected: fetching address");
        reconfigure();
        break;
      case NoSyncConnected:
      case Unknown:
      default:
        LogLog.error("Unexpected zookeeper event " + event.getState());
        break;
    }
  }

  private void reconfigure() {
    List<String> children;
    try {
      children = zookeeper.getChildren("/", new Watcher() {
        @Override
        public void process(WatchedEvent arg0) {
          reconfigure();
        }
      });
      if (children.isEmpty()) {
        LogLog.debug("Zookeeper list of loggers is empty");
        return;
      }
      Random random = new Random();
      int choice = random.nextInt(children.size());
      byte[] data = zookeeper.getData("/" + children.get(choice), false, null);
      String dataString = new String(data, UTF_8);
      String parts[] = dataString.split(":");
      address = InetAddress.getByName(parts[0]);
      port = Integer.decode(parts[1]);
      connect(address, port);
    } catch (UnknownHostException | KeeperException | InterruptedException e) {
      LogLog.debug("Exception " + e);
    }

  }

  public int getZookeeperTimeout() {
    return zookeeperTimeout;
  }

  public void setZookeeperTimeout(int zookeeperTimeout) {
    this.zookeeperTimeout = zookeeperTimeout;
  }
}
