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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.model.HoodieTimelineTimeZone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestHoodieInstantTimeGenerator {

  private TimeZone originalDefaultZone;

  @BeforeEach
  void saveDefaultZone() {
    // LOCAL keeps the default zone from when the enum loads, so load it before a test changes that zone
    HoodieTimelineTimeZone.values();
    originalDefaultZone = TimeZone.getDefault();
  }

  @AfterEach
  void restoreDefaultZone() {
    TimeZone.setDefault(originalDefaultZone);
  }

  @ParameterizedTest
  @CsvSource({
      "Asia/Shanghai,   UTC,              20261111100000000, 2026-11-11T10:00:00Z",
      "America/New_York, UTC,             20261111100000000, 2026-11-11T10:00:00Z",
      "UTC,             Asia/Shanghai,    20261111180000000, 2026-11-11T10:00:00Z",
      "Asia/Shanghai,   America/New_York, 20261111050000000, 2026-11-11T10:00:00Z"
  })
  void parseDateFromInstantTimeUsesTheGivenZone(String jvmZone, String instantZone, String instantTime, String expected) throws ParseException {
    TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));

    Date parsed = HoodieInstantTimeGenerator.parseDateFromInstantTime(instantTime, ZoneId.of(instantZone));

    assertEquals(Instant.parse(expected), parsed.toInstant());
  }

  @ParameterizedTest
  @CsvSource({
      "20261111100000,        2026-11-11T10:00:00.999Z",
      "20261111100000123,     2026-11-11T10:00:00.123Z",
      "20261111100000000010,  2026-11-11T10:00:00Z"
  })
  void parseDateFromInstantTimeWithZoneKeepsCompatibilityHandling(String instantTime, String expected) throws ParseException {
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));

    Date parsed = HoodieInstantTimeGenerator.parseDateFromInstantTime(instantTime, ZoneOffset.UTC);

    assertEquals(Instant.parse(expected), parsed.toInstant());
  }

  @Test
  void parseDateFromInstantTimeWithZoneRejectsInvalidInstants() {
    assertThrows(ParseException.class, () -> HoodieInstantTimeGenerator.parseDateFromInstantTime("not-an-instant", ZoneOffset.UTC));
  }

  @Test
  void parseDateFromInstantTimeWithoutZoneUsesTheJvmDefaultZone() throws ParseException {
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));

    Date parsed = HoodieInstantTimeGenerator.parseDateFromInstantTime("20261111180000000");

    assertEquals(Instant.parse("2026-11-11T10:00:00Z"), parsed.toInstant());
  }

  @Test
  void formatDateBasedOnTimeZone() {
    long timestampMillis = 1750205942000L;
    String expectedFormattedTimeUTC = "20250618001902000";
    // Test with UTC timezone
    HoodieInstantTimeGenerator.setCommitTimeZone(HoodieTimelineTimeZone.UTC);
    String formattedTimeUTC = HoodieInstantTimeGenerator.formatDateBasedOnTimeZone(new Date(timestampMillis));
    assertEquals(expectedFormattedTimeUTC, formattedTimeUTC);
  }
}