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
package org.apache.accumulo.logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class LogHandler extends SimpleChannelInboundHandler<String> {

  private static final Logger LOG = LoggerFactory.getLogger(LogHandler.class);

  private final SimpleDateFormat dateFormat = new SimpleDateFormat(LogEntry.DATE_FORMAT);
  private LinkedBlockingDeque<LogEntry> queue;

  public LogHandler(LinkedBlockingDeque<LogEntry> queue) {
    this.queue = queue;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
    try {
      if (!queue.offer(LogEntry.parseEntry(msg, dateFormat))) {
        LOG.trace("Dropping msg {}", msg);
      }
    } catch (ParseException er) {
      LOG.info("Error parsing message {}", msg);
    }
  }

}
