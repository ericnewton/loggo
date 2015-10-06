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

import java.util.concurrent.LinkedBlockingDeque;

import org.loggo.client.LogEntry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;

public class LoggerReaderInitializer extends ChannelInitializer<SocketChannel> {

  private static final StringDecoder DECODER = new StringDecoder();
  private final LinkedBlockingDeque<LogEntry> queue;

  //@formatter:off
  private static final ByteBuf[] DOUBLE_NEWLINE = {
      Unpooled.wrappedBuffer(new byte[] {'\r', '\n', '\r', '\n'}),
      Unpooled.wrappedBuffer(new byte[] {'\n', '\n'}),
      };
  //@formatter:on

  public LoggerReaderInitializer(LinkedBlockingDeque<LogEntry> queue) {
    this.queue = queue;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast(new DelimiterBasedFrameDecoder(10 * 1000, DOUBLE_NEWLINE));
    pipeline.addLast(DECODER);
    pipeline.addLast(new LogHandler(queue));
  }

}
