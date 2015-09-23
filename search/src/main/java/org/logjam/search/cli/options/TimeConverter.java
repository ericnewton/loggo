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
package org.logjam.search.cli.options;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.logjam.search.cli.Search;

import com.beust.jcommander.IStringConverter;

public class TimeConverter implements IStringConverter<Long> {

  @Override
  public Long convert(String value) {
    for (String format : Search.FORMATS) {
      try {
        return new SimpleDateFormat(format).parse(value).getTime();
      } catch (ParseException e) {}
    }
    if (value.equals("today")) {
      SimpleDateFormat dateOnly = new SimpleDateFormat(Search.DATE_ONLY);
      try {
        return dateOnly.parse(dateOnly.format(new Date())).getTime();
      } catch (ParseException e) {
        throw new RuntimeException(e); // unlikely
      }
    }
    throw new RuntimeException("Unable to parse date/time");
  }
}
