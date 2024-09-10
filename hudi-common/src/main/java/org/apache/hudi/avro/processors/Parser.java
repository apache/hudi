/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro.processors;

import org.apache.hudi.common.util.collection.Pair;

public abstract class Parser {
  abstract Pair<Boolean, Object> handleNumberValue(Number value);

  abstract Pair<Boolean, Object> handleStringNumber(String value);

  abstract Pair<Boolean, Object> handleStringValue(String value);

  public static class IntParser extends Parser {
    @Override
    public Pair<Boolean, Object> handleNumberValue(Number value) {
      return Pair.of(true, value.intValue());
    }

    @Override
    public Pair<Boolean, Object> handleStringNumber(String value) {
      return Pair.of(true, Integer.parseInt(value));
    }

    @Override
    public Pair<Boolean, Object> handleStringValue(String value) {
      return Pair.of(true, Integer.valueOf(value));
    }
  }

  public static class DateParser extends Parser {

    private static long MILLI_SECONDS_PER_DAY = 86400000;

    @Override
    public Pair<Boolean, Object> handleNumberValue(Number value) {
      return Pair.of(true, new java.sql.Date(value.intValue() * MILLI_SECONDS_PER_DAY));
    }

    @Override
    public Pair<Boolean, Object> handleStringNumber(String value) {
      return Pair.of(true, new java.sql.Date(Integer.parseInt(value) * MILLI_SECONDS_PER_DAY));
    }

    @Override
    public Pair<Boolean, Object> handleStringValue(String value) {
      return Pair.of(true, java.sql.Date.valueOf(value));
    }
  }

  public static class LongParser extends Parser {
    @Override
    public Pair<Boolean, Object> handleNumberValue(Number value) {
      return Pair.of(true, value.longValue());
    }

    @Override
    public Pair<Boolean, Object> handleStringNumber(String value) {
      return Pair.of(true, Long.parseLong(value));
    }

    @Override
    public Pair<Boolean, Object> handleStringValue(String value) {
      return Pair.of(true, Long.valueOf(value));
    }
  }
}
