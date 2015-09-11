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
package org.logjam.server.options;

import com.beust.jcommander.Parameter;

public class Options {

  @Parameter(names = {"--clientConfig", "-c"})
  public String clientConfigurationFile;

  @Parameter(names = {"-p", "--port"})
  public int port = 9991;

  @Parameter(names = {"-h", "--host"})
  public String host = "localhost";

  @Parameter(names = {"-q", "--queueSize"})
  public int queueSize = 1000;

  @Parameter(names = {"-t", "--table"})
  public String table = "logs";

  @Parameter(names = {"-u", "--user"})
  public String user = "root";

  @Parameter(names = {"--password"}, converter = PasswordConverter.class)
  public Password password = new Password("secret");

  @Parameter(names = {"-zk", "--zookeepers"})
  public String zookeepers = "localhost:2181/loggers";
}
