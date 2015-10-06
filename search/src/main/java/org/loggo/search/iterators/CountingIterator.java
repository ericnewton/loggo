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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class CountingIterator extends WrappingIterator implements OptionDescriber {

  private long count = 0;
  private Key last = null;

  @Override
  public Key getTopKey() {
    return last;
  }

  @Override
  public Value getTopValue() {
    return new Value(Long.toString(count).getBytes());
  }

  @Override
  public boolean hasTop() {
    return last != null;
  }

  @Override
  public void next() throws IOException {
    last = null;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
    count = 0;
    SortedKeyValueIterator<Key,Value> source = getSource();
    while (source.hasTop()) {
      last = source.getTopKey();
      source.next();
      count++;
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions(getClass().getSimpleName(), "Counts keys in the range", new HashMap<String,String>(), new ArrayList<String>());
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    return true;
  }
}
