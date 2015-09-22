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
package org.logjam.search.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Test;

public class HostAndApplicationFilterTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = Collections.emptyList();

  @Test
  public void test() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that do not aggregate
    nkv(tm1, "row1", "log", "application1\0some.host.name", 1, "value1");
    nkv(tm1, "row2", "cf", "application2\0other.host.name", 2, "value2");

    HostAndApplicationFilter ai = new HostAndApplicationFilter();

    Map<String,String> options = Collections.singletonMap("apps", "application2");
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);
    assertTrue(ai.hasTop());
    assertEquals("row2", ai.getTopKey().getRow().toString());
    ai.next();
    assertFalse(ai.hasTop());

    Map<String,String> emptyMap = Collections.emptyMap();
    ai.init(new SortedMapIterator(new TreeMap<Key,Value>()), emptyMap, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);
    assertFalse(ai.hasTop());

    options = Collections.singletonMap("hosts", "some.host.name");
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);
    assertTrue(ai.hasTop());
    assertEquals("row1", ai.getTopKey().getRow().toString());

    options = Collections.singletonMap("hosts", "some.host.name,other.host.name");
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);
    assertTrue(ai.hasTop());
    assertEquals("row1", ai.getTopKey().getRow().toString());
    ai.next();
    assertTrue(ai.hasTop());
    assertEquals("row2", ai.getTopKey().getRow().toString());
  }

  private void nkv(TreeMap<Key,Value> tm1, String row, String cf, String cq, long ts, String value) {
    tm1.put(new Key(row, cf, cq, ts), new Value(value.getBytes()));
  }

}
