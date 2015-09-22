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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class HostAndApplicationFilter extends Filter {

  static final Logger log = LoggerFactory.getLogger(HostAndApplicationFilter.class);
  static final String HOSTS_OPTION = "hosts";
  static final String APPLICATIONS_OPTION = "apps";
  static final BytesWritable.Comparator comparator = new BytesWritable.Comparator();
  byte[][] hosts;
  byte[][] apps;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.hosts = fillArrays(options.get(HOSTS_OPTION));
    this.apps = fillArrays(options.get(APPLICATIONS_OPTION));
  }

  private byte[][] fillArrays(String entriesString) {
    if (entriesString == null || entriesString.isEmpty()) {
      return new byte[0][];
    }
    String[] entries = entriesString.split(",");
    byte[][] result = new byte[entries.length][];
    for (int i = 0; i < entries.length; i++) {
      result[i] = entries[i].getBytes(UTF_8);
    }
    return result;
  }

  @Override
  public boolean accept(Key k, Value v) {
    ByteSequence bs = k.getColumnQualifierData();
    byte[] bytes = bs.getBackingArray();
    int i;
    for (i = 0; i < bs.length(); i++) {
      if (bytes[i] == 0) {
        break;
      }
    }
    if (i == bs.length()) {
      return false;
    }
    boolean matchesApps = apps.length == 0;
    for (byte[] app : apps) {
      if (comparator.compare(app, 0, app.length, bytes, 0, i) == 0) {
        matchesApps = true;
        break;
      }
    }

    boolean matchesHost = hosts.length == 0;
    for (byte[] host : hosts) {
      if (comparator.compare(host, 0, host.length, bytes, i + 1, bs.length() - i - 1) == 0) {
        matchesHost = true;
        break;
      }
    }
    return matchesApps && matchesHost;
  }

  public static void setHosts(IteratorSetting is, Collection<String> hosts) {
    Joiner joiner = Joiner.on(",");
    is.addOption(HOSTS_OPTION, joiner.join(hosts));
  }

  public static void setApps(IteratorSetting is, Collection<String> apps) {
    Joiner joiner = Joiner.on(",");
    is.addOption(APPLICATIONS_OPTION, joiner.join(apps));
  }

}
