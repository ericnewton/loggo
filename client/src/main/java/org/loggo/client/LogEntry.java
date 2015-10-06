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
package org.loggo.client;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

public class LogEntry {
  public String host;
  public String app;
  public long timestamp;
  public String message;

  public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss,SSS";
  // private static final Logger LOG = LoggerFactory.getLogger(LogEntry.class);

  public static LogEntry parseEntry(String msg, SimpleDateFormat dateFormat) throws ParseException {
    LogEntry result = new LogEntry();
    String[] parts = msg.trim().split(" ", 5);
    if (parts.length != 5) {
      throw new ParseException(msg, msg.length());
    }
    Iterator<String> i = Arrays.asList(parts).iterator();
    result.host = i.next();
    result.app = i.next();
    result.timestamp = dateFormat.parse(i.next() + " " + i.next()).getTime();
    result.message = i.next();
    return result;
  }

  @Override
  public String toString() {
    return String.format("%s %s %s %s", host, app, new SimpleDateFormat(DATE_FORMAT).format(new Date(timestamp)), message);
  }
}
