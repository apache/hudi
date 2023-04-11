/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.console.base.config;

import org.apache.hudi.console.base.util.CommonUtils;

import org.apache.commons.lang3.StringUtils;

import com.p6spy.engine.spy.appender.MessageFormattingStrategy;

import java.time.LocalDateTime;

public class P6spySqlFormatConfig implements MessageFormattingStrategy {

  @Override
  public String formatMessage(
      int connectionId,
      String now,
      long elapsed,
      String category,
      String prepared,
      String sql,
      String url) {

    return StringUtils.isBlank(sql)
        ? ""
        : String.format(
            "%s  | consume %d ms | SQL statement：\n %s;",
            CommonUtils.formatFullTime(LocalDateTime.now()),
            elapsed,
            sql.replaceAll("\\s+", StringUtils.SPACE));
  }
}
