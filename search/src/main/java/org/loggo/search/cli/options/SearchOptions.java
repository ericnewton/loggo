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
package org.loggo.search.cli.options;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;

public class SearchOptions extends LoginOptions {
  @Parameter(names = {"--start", "-s"}, converter = TimeConverter.class,
      description = "Start time to search. Format can be one of \"YYYY-mm-dd HH:mm:ss,SSS\", \"YYYY-mm-dd HH:mm:ss\", \"YYYY-mm-dd\" or \"today\"")
  public Long start = null;

  @Parameter(names = {"--end", "-e"}, converter = TimeConverter.class, description = "Limit search to stop at this time. Can use the same format as --start")
  public Long end = null;

  @Parameter(names = {"--count"}, description = "Do not print log messages, just print the count.")
  public boolean count = false;

  @Parameter(names = {"--sort"}, description = "Sort logs by time. This requires all the logs to fit in memory.")
  public boolean sort = false;

  @Parameter(names = {"--reverse", "-r"}, description = "Sort, but in reverse time order.")
  public boolean reverse = false;

  @Parameter(names = {"--host", "-h"}, description = "Select only those logs from the given hostname(s)")
  public List<String> hosts = new ArrayList<>();

  @Parameter(names = {"--app", "-a"}, description = "Select only thos logs with the given application name(s)")
  public List<String> applications = new ArrayList<>();

  @Parameter(names = {"--regexp"}, description = "The search terms are regular expressions. Much match the entire message.")
  public boolean regexp = false;

  @Parameter(description = "search terms")
  public List<String> terms = new ArrayList<>();
}
