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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class GrepValueFilter extends Filter {

  private byte term[];
  private int right[] = new int[256];

  @Override
  public boolean accept(Key k, Value v) {
    // return Bytes.indexOf(v.get(), term) >= 0;
    return search(v.get()) >= 0;
  }

  private final int search(final byte[] value) {
    final int M = term.length;
    final int N = value.length;
    int skip;
    for (int i = 0; i <= N - M; i += skip) {
      skip = 0;
      for (int j = M - 1; j >= 0; j--) {
        if (term[j] != value[i + j]) {
          skip = Math.max(1, j - right[value[i + j] & 0xff]);
        }
      }
      if (skip == 0) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    GrepValueFilter copy = (GrepValueFilter) super.deepCopy(env);
    copy.term = Arrays.copyOf(term, term.length);
    copy.right = Arrays.copyOf(right, right.length);
    return copy;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    term = options.get("term").getBytes(UTF_8);
    for (int i = 0; i < right.length; i++) {
      right[i] = -1;
    }
    for (int i = 0; i < term.length; i++) {
      right[term[i] & 0xff] = i;
    }
  }

  public static void setTerm(IteratorSetting cfg, String term) {
    cfg.addOption("term", term);
  }
}
