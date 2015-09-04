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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;

public class LoggerTest {
  private static String TEST_MESSAGE_1 = "localhost foo 2015-09-03 13:01:00,123 ohai, i am a message\n";
  private static SimpleDateFormat fmt = new SimpleDateFormat(LogEntry.DATE_FORMAT);

  @Test
  public void parseTest() throws Exception {
    LogEntry parseEntry = LogEntry.parseEntry(TEST_MESSAGE_1, fmt);
    assertEquals(parseEntry.app, "foo");
    assertEquals(parseEntry.host, "localhost");
    assertEquals(parseEntry.message, "ohai, i am a message");
    assertEquals(parseEntry.timestamp, 1441299660123L);
    try {
      parseEntry = LogEntry.parseEntry("", fmt);
      fail("should throw error");
    } catch (ParseException ex) {}
  }
}
