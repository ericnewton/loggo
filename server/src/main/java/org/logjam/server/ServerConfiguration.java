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
package org.logjam.server;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.tree.DefaultExpressionEngine;
import org.apache.commons.configuration.tree.ExpressionEngine;

public class ServerConfiguration extends HierarchicalINIConfiguration {
  private static final long serialVersionUID = 1L;

  // HierarchicalINIConfiguration separates sections with '.' and double-dots properties: section.foo..bar
  // This changes the behavior to section:foo.bar
  private final DefaultExpressionEngine engine = new DefaultExpressionEngine();

  {
    engine.setEscapedDelimiter("::");
    engine.setPropertyDelimiter(":");
  }

  public ServerConfiguration(String filename) throws ConfigurationException {
    super(filename);
    this.setDelimiterParsingDisabled(true);
  }

  public ServerConfiguration() throws ConfigurationException {
    this.setDelimiterParsingDisabled(true);
  }

  @Override
  public ExpressionEngine getExpressionEngine() {
    return engine;
  }

}
