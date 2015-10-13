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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.loggo.client.LogEntry;
import org.loggo.schema.Schema;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/*
 * Count the number of entries, per column family, for an interval.
 * For example, count the different log entries for every 5 minutes, or every day.
 */
public class StatsIterator extends WrappingIterator {

  private static final String TIME_KEY = "stats.ms";

  private final SimpleDateFormat fmt = new SimpleDateFormat(LogEntry.DATE_FORMAT);
  private final Map<String,Long> counts = new HashMap<>();
  private long bucketStart = -1;
  private long bucketEnd = -1;
  private long period = -1;
  private Key last = null;
  private Range range;
  private Collection<ByteSequence> columnFamilies;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    period = Long.valueOf(options.get(TIME_KEY));
  }

  @Override
  public Key getTopKey() {
    return last;
  }

  @Override
  public Value getTopValue() {
    final Joiner comma = Joiner.on(",");
    final Joiner colon = Joiner.on(":");
    final List<String> parts = new ArrayList<>(counts.size());
    for (Entry<String,Long> entry : counts.entrySet()) {
      parts.add(colon.join(entry.getKey(), entry.getValue()));
    }
    return new Value(comma.join(parts).getBytes(UTF_8));
  }

  @Override
  public boolean hasTop() {
    return last != null;
  }

  @Override
  public void next() throws IOException {
    seek(new Range(last, false, range.getEndKey(), range.isEndKeyInclusive()), columnFamilies, false);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
    this.last = null;
    this.range = range;
    this.columnFamilies = columnFamilies;
    this.counts.clear();
    SortedKeyValueIterator<Key,Value> source = super.getSource();
    if (source.hasTop()) {
      last = source.getTopKey();
      long ts = getTs(last, fmt);
      bucketStart = ts - (ts % period);
      bucketEnd = bucketStart + period;
      while (source.hasTop()) {
        ts = getTs(source.getTopKey(), fmt);
        if (ts < bucketStart || ts >= bucketEnd) {
          return;
        }
        last = source.getTopKey();
        String cf = last.getColumnFamily().toString();
        Long count = counts.get(cf);
        if (count == null) {
          counts.put(cf, 1L);
        } else {
          counts.put(cf, count.longValue() + 1);
        }
        source.next();
      }
    }
  }

  public static long getTs(Key key, SimpleDateFormat fmt) {
    try {
      return fmt.parse(key.getRow().toString().substring(Schema.SHARD_LENGTH)).getTime();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static void duration(IteratorSetting cfg, long duration, TimeUnit unit) {
    Preconditions.checkNotNull(cfg);
    Preconditions.checkArgument(duration > 0, "Duration must be > 0");
    Preconditions.checkNotNull(unit);
    cfg.addOption(TIME_KEY, "" + unit.toMillis(duration));
  }
}