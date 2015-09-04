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

import java.io.File;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.logjam.client.LogEntry;
import org.logjam.server.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class Server {
  private static final Logger LOG = LoggerFactory.getLogger(Server.class);

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
    Server server = new Server();
    server.initialize(opts);
    exit(server.run());
  }

  public static void exit(int status) {
    System.exit(status);
  }

  public static void exitWithError(String message, int status) {
    System.err.println(message);
    exit(status);
  }

  private EventLoopGroup bossGroup = new NioEventLoopGroup(1);
  private EventLoopGroup workerGroup = new NioEventLoopGroup();
  private EventLoopGroup dgramGroup = new NioEventLoopGroup();
  private Writer writer;
  private Channel channel;

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

  void initialize(Options options) throws ConfigurationException {
    ClientConfiguration config;
    if (options.clientConfigurationFile != null) {
      config = new ClientConfiguration(new File(options.clientConfigurationFile));
    } else {
      config = ClientConfiguration.loadDefault();
    }
    LinkedBlockingDeque<LogEntry> queue = new LinkedBlockingDeque<LogEntry>(options.queueSize);
    this.writer = new Writer(queue, config, options);
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
    try {
      channel = b.bind(options.host, options.port).sync().channel();
      bb.bind(options.host, options.port).sync();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void stop() {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    dgramGroup.shutdownGracefully();
    writer.stop();
  }

}
