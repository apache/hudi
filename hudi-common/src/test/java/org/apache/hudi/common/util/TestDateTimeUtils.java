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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link DateTimeUtils}.
 */
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

  @Test
  public void testParseDateTimeParsesEpochMillisAsMillis() {
    // A numeric string is treated as epoch millis, not ISO-8601.
    assertEquals(Instant.ofEpochMilli(1612542030000L), DateTimeUtils.parseDateTime("1612542030000"));
  }

  @Test
  public void testMicrosInstantRoundTripForPositiveEpoch() {
    Instant instant = Instant.ofEpochSecond(1, 2000);
    assertEquals(1_000_002L, DateTimeUtils.instantToMicros(instant));
    assertEquals(instant, DateTimeUtils.microsToInstant(1_000_002L));
  }

  @Test
  public void testMicrosInstantForNegativeEpochWithNanos() {
    // Before the epoch with a sub-second nano component exercises the negative-seconds branch.
    Instant instant = Instant.ofEpochSecond(-2, 500_000_000);
    long micros = DateTimeUtils.instantToMicros(instant);
    assertEquals(-1_500_000L, micros);
    assertEquals(instant, DateTimeUtils.microsToInstant(micros));
  }

  @Test
  public void testNanosInstantRoundTripForPositiveEpoch() {
    Instant instant = Instant.ofEpochSecond(3, 456);
    assertEquals(3_000_000_456L, DateTimeUtils.instantToNanos(instant));
    assertEquals(instant, DateTimeUtils.nanosToInstant(3_000_000_456L));
  }

  @Test
  public void testNanosInstantForNegativeEpochWithNanos() {
    Instant instant = Instant.ofEpochSecond(-2, 500_000_000);
    long nanos = DateTimeUtils.instantToNanos(instant);
    assertEquals(-1_500_000_000L, nanos);
    assertEquals(instant, DateTimeUtils.nanosToInstant(nanos));
  }

  @Test
  public void testMicrosMillisConversion() {
    assertEquals(1234L, DateTimeUtils.microsToMillis(1_234_567L));
    // floorDiv rounds towards negative infinity for negative micros.
    assertEquals(-158L, DateTimeUtils.microsToMillis(-157_500L));
    assertEquals(1_234_000L, DateTimeUtils.millisToMicros(1234L));
  }

  @ParameterizedTest
  @CsvSource({
      "123, 123",
      "'123ms', 123",
      "'321 s', 321000",
      "'2 min', 120000",
      "'1 day', 86400000"
  })
  public void testParseDurationValidLabels(String text, long expectedMillis) {
    assertEquals(Duration.of(expectedMillis, ChronoUnit.MILLIS), DateTimeUtils.parseDuration(text));
  }

  @Test
  public void testParseDurationDefaultsToMillisWhenUnitOmitted() {
    assertEquals(Duration.of(500, ChronoUnit.MILLIS), DateTimeUtils.parseDuration("500"));
  }

  @Test
  public void testParseDurationRejectsUnknownUnit() {
    assertThrows(IllegalArgumentException.class, () -> DateTimeUtils.parseDuration("10 fortnights"));
  }

  @Test
  public void testParseDurationRejectsMissingNumber() {
    assertThrows(NumberFormatException.class, () -> DateTimeUtils.parseDuration("ms"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  public void testParseDurationRejectsBlank(String text) {
    assertThrows(IllegalArgumentException.class, () -> DateTimeUtils.parseDuration(text));
  }

  @Test
  public void testParseDurationRejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> DateTimeUtils.parseDuration(null));
  }

  @Test
  public void testFormatUnixTimestamp() {
    // Uses the system default zone, so validate by re-parsing the formatted output rather
    // than hardcoding a zone-dependent string.
    long unixTimestamp = 1_612_542_030L;
    String pattern = "yyyy-MM-dd HH:mm:ss";
    String formatted = DateTimeUtils.formatUnixTimestamp(unixTimestamp, pattern);
    LocalDateTime parsed = LocalDateTime.parse(formatted, DateTimeFormatter.ofPattern(pattern));
    assertEquals(unixTimestamp, parsed.atZone(ZoneId.systemDefault()).toEpochSecond());
    assertDoesNotThrow(() -> DateTimeUtils.formatUnixTimestamp(unixTimestamp, "yyyy"));
  }

  @Test
  public void testFormatUnixTimestampRejectsEmptyFormat() {
    assertThrows(IllegalArgumentException.class, () -> DateTimeUtils.formatUnixTimestamp(0L, ""));
  }
}
