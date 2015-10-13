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
package org.loggo.search.iterators;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Test;
import org.loggo.schema.Schema;

public class StatsIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = Collections.emptyList();

  @Test
  public void test() throws Exception {
    SortedMap<Key,Value> source = new TreeMap<>();
    StatsIterator ai = new StatsIterator();
    ai.init(new SortedMapIterator(source), Collections.singletonMap("stats.ms", "" + 5 * 60 * 1000L), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);
    assertFalse(ai.hasTop());

    source.put(key("000 2015-10-10 12:01:01,000", "WARN", "app", "host1"), value("this is a warning message"));
    source.put(key("000 2015-10-10 12:01:02,000", "WARN", "app", "host1"), value("this is a warning message"));
    source.put(key("000 2015-10-10 12:01:03,000", "WARN", "app", "host1"), value("this is a warning message"));
    source.put(key("000 2015-10-10 12:05:01,000", "WARN", "app", "host1"), value("this is a warning message"));
    source.put(key("000 2015-10-10 12:06:01,000", "ERROR", "app", "host1"), value("this is an error message"));
    source.put(key("000 2015-10-10 12:07:01,000", "WARN", "app", "host1"), value("this is a warning message"));

    ai = new StatsIterator();
    ai.init(new SortedMapIterator(source), Collections.singletonMap("stats.ms", "" + 5 * 60 * 1000L), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);
    assertTrue(ai.hasTop());
    assertEquals(key("000 2015-10-10 12:01:03,000", "WARN", "app", "host1"), ai.getTopKey());
    assertEquals(value("WARN:3"), ai.getTopValue());
    ai.next();
    assertTrue(ai.hasTop());
    assertEquals(key("000 2015-10-10 12:07:01,000", "WARN", "app", "host1"), ai.getTopKey());
    assertEquals(value("ERROR:1,WARN:2"), ai.getTopValue());
    ai.next();
    assertFalse(ai.hasTop());
  }

  private Key key(String row, String cf, String app, String host) {
    return new Key(row, cf, app + Schema.APP_HOST_SEPARATOR + host);
  }

  private Value value(String msg) {
    return new Value(msg.getBytes(UTF_8));
  }
}
