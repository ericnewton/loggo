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
package org.apache.accumulo.logger;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reader {
  private static final Logger LOG = LoggerFactory.getLogger(Reader.class);
  private SocketChannel socket;
  private ByteBuffer more = ByteBuffer.allocate(10 * 1000);
  private SimpleDateFormat parser = new SimpleDateFormat(LogEntry.DATE_FORMAT);

  public Reader(SocketChannel c) {
    socket = c;
  }

  public boolean readLogs(LinkedBlockingDeque<LogEntry> dest) {
    try {
      while (socket.read(more) > 0) {
        String message = new String(more.array(), 0, more.position(), UTF_8);
        int start = 0;
        while (start < message.length()) {
          int nl = message.indexOf('\n', start);
          if (nl < 0) {
            break;
          }
          try {
            dest.put(LogEntry.parseEntry(message.substring(start, nl), parser));
          } catch (Exception ex) {
            LOG.info("Unable to parse or process message: {}", message.trim());
          }
          start = nl + 1;
        }
        more.clear();
        more.put(message.substring(start).getBytes(UTF_8));
        if (more.position() == more.limit()) {
          LOG.info("log message exceeds size: {} dropping data: ", more.capacity(), message.substring(0, 100) + "...");
          more.clear();
        }
      }
    } catch (IOException e) {
      LOG.info("IOException: {}", e, e);
      return false;
    }
    return false;
  }

}
