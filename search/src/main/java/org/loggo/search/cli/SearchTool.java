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
package org.loggo.search.cli;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.loggo.search.cli.options.Options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class SearchTool implements KeywordExecutable {

  @Override
  public String keyword() {
    return "loggo-search";
  }

  @Override
  public void execute(String[] args) throws Exception {
    Options opts = new Options();
    JCommander parser = new JCommander(opts);
    try {
      parser.setProgramName(SearchTool.class.getSimpleName());
      parser.parse(args);
    } catch (ParameterException e) {
      parser.usage();
      System.exit(1);
    }
    ClientConfiguration config = ClientConfiguration.loadDefault();
    Instance instance = new ZooKeeperInstance(config);
    Connector conn = instance.getConnector(opts.user, new PasswordToken(opts.password.getBytes(UTF_8)));
    Search search = new Search(conn, opts, System.out);
    search.query();
  }

  public static void main(String args[]) throws Exception {
    SearchTool tool = new SearchTool();
    tool.execute(args);
  }
}
