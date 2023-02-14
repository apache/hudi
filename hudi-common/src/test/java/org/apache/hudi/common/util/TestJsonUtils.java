/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJsonUtils {

  @Test
  void testDateTimeSerialization() {
    List<Object> dateTimeObjs = Arrays.asList(
        java.time.Instant.ofEpochSecond(1),
        java.time.LocalDate.of(1970, 1, 1),
        java.time.LocalTime.of(0, 0, 1),
        java.time.LocalDateTime.of(1970, 1, 1, 0, 0, 1, 0),
        java.sql.Date.valueOf("1970-01-01"),
        java.sql.Time.valueOf("00:00:01"),
        java.sql.Timestamp.from(Instant.ofEpochSecond(1)),
        java.util.Date.from(Instant.ofEpochSecond(1))
    );

    List<String> actual = dateTimeObjs.stream().map(JsonUtils::toString).collect(Collectors.toList());
    List<String> expected = Arrays.asList(
        "\"1970-01-01T00:00:01Z\"",
        "\"1970-01-01\"",
        "\"00:00:01\"",
        "\"1970-01-01T00:00:01\"",
        "\"1970-01-01\"",
        "\"00:00:01\"",
        "\"1970-01-01T00:00:01.000+0000\"",
        "\"1970-01-01T00:00:01.000+0000\""
    );

    assertEquals(expected, actual);
  }
}