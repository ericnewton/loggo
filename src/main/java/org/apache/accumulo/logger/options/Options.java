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
package org.apache.accumulo.logger.options;

import com.beust.jcommander.Parameter;

public class Options {
  @Parameter(names = {"-p", "--port"}, description = "port number to use, use 0 for a dynamic port")
  public int port = 9991;

  @Parameter(names = {"-l", "--listen"}, description = "the interface to listen on")
  public String host = "0.0.0.0";

  @Parameter(names = "-t", description = "the table to use")
  public String table = "logs";

  @Parameter(names = {"-c"}, description = "the accumulo client configuration file")
  public String clientConfigurationFile = null;

  @Parameter(names = {"-u"}, description = "the user to write to the log files")
  public String user = "root";

  @Parameter(names = {"--password"}, description = "the password to use, which will be decoded like the password used for the accumulo shell")
  public Password password = new Password("secret");

  @Parameter(names = {"-h", "-?", "-help", "--help"}, help = true)
  public boolean help = false;

  @Parameter(names = {"--queueSize", "-q"})
  public int queueSize = Integer.MAX_VALUE;
}
