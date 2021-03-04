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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestDateTimeUtils {

  @ParameterizedTest
  @ValueSource(strings = {"0", "1612542030000", "2020-01-01T01:01:00Z", "1970-01-01T00:00:00.123456Z"})
  public void testParseStringIntoInstant(String s) {
    assertDoesNotThrow(() -> {
      DateTimeUtils.parseDateTime(s);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"#", "0L", ""})
  public void testParseDateTimeThrowsException(String s) {
    assertThrows(DateTimeParseException.class, () -> {
      DateTimeUtils.parseDateTime(s);
    });
  }

  @Test
  public void testParseDateTimeWithNull() {
    assertThrows(IllegalArgumentException.class, () -> {
      DateTimeUtils.parseDateTime(null);
    });
  }
}
