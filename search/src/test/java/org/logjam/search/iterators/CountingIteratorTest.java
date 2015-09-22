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

public class CountingIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = Collections.emptyList();

  @Test
  public void test() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that do not aggregate
    nkv(tm1, "row1", "cf", "cq", 1, "value");
    nkv(tm1, "row2", "cf", "cq", 2, "value");
    nkv(tm1, "row3", "cf", "cq", 3, "value");

    CountingIterator ai = new CountingIterator();

    Map<String,String> emptyMap = Collections.emptyMap();
    ai.init(new SortedMapIterator(tm1), emptyMap, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);
    assertTrue(ai.hasTop());
    assertEquals("row3", ai.getTopKey().getRow().toString());
    assertEquals("3", ai.getTopValue().toString());
    ai.next();
    assertFalse(ai.hasTop());

    ai.init(new SortedMapIterator(new TreeMap<Key,Value>()), emptyMap, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);
    assertFalse(ai.hasTop());
  }

  private void nkv(TreeMap<Key,Value> tm1, String row, String cf, String cq, long ts, String value) {
    tm1.put(new Key(row, cf, cq, ts), new Value(value.getBytes()));
  }

}
