/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.ttl.strategy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeepByEventTimeStrategy}'s pure parsing logic. Exercises the static helpers
 * directly so we don't need to spin up a HoodieTable / write client.
 */
public class TestKeepByEventTimeStrategy {

  @Test
  public void segmentCountCountsSlashes() {
    assertEquals(1, KeepByEventTimeStrategy.segmentCount("yyyy-MM-dd"));
    assertEquals(1, KeepByEventTimeStrategy.segmentCount("yyyyMMdd"));
    assertEquals(2, KeepByEventTimeStrategy.segmentCount("yyyy-MM-dd/HH"));
    assertEquals(2, KeepByEventTimeStrategy.segmentCount("yyyyMMdd/HH"));
  }

  // path, format, startIdx, hiveStyle, expectedExpired
  //
  // Full matrix mirroring the class-level JavaDoc: 4 formats x 4 position shapes
  // (time only / prefix+time / time+suffix / prefix+time+suffix) x hive vs non-hive.
  // Cutoff is "2026-04-30 00:00 UTC" so any 2026-04-22 path expires; the two
  // "not expired" rows pin down the strict-less-than boundary.
  @ParameterizedTest
  @CsvSource({
      // -------- Day, format=yyyy-MM-dd --------
      // time only
      "'dt=2026-04-22',                                'yyyy-MM-dd',     0, true,  true",
      "'dt=2026-04-30',                                'yyyy-MM-dd',     0, true,  false",
      "'2026-04-22',                                   'yyyy-MM-dd',     0, false, true",
      "'2026-04-30',                                   'yyyy-MM-dd',     0, false, false",
      // prefix + time
      "'region=us/dt=2026-04-22',                      'yyyy-MM-dd',     1, true,  true",
      "'eventType=login/dt=2026-04-30',                'yyyy-MM-dd',     1, true,  false",
      "'region=us/2026-04-22',                         'yyyy-MM-dd',     1, false, true",
      // time + suffix
      "'dt=2026-04-22/source=app',                     'yyyy-MM-dd',     0, true,  true",
      "'2026-04-22/source=app',                        'yyyy-MM-dd',     0, false, true",
      // prefix + time + suffix
      "'region=us/dt=2026-04-22/source=app',           'yyyy-MM-dd',     1, true,  true",
      "'region=us/2026-04-22/source=app',              'yyyy-MM-dd',     1, false, true",

      // -------- Day, format=yyyyMMdd --------
      // time only
      "'dt=20260422',                                  'yyyyMMdd',       0, true,  true",
      "'dt=20260430',                                  'yyyyMMdd',       0, true,  false",
      "'20260422',                                     'yyyyMMdd',       0, false, true",
      // prefix + time
      "'region=us/dt=20260422',                        'yyyyMMdd',       1, true,  true",
      "'region=us/20260422',                           'yyyyMMdd',       1, false, true",
      // time + suffix
      "'dt=20260422/source=app',                       'yyyyMMdd',       0, true,  true",
      "'20260422/source=app',                          'yyyyMMdd',       0, false, true",
      // prefix + time + suffix
      "'region=us/dt=20260422/source=app',             'yyyyMMdd',       1, true,  true",
      "'region=us/20260422/source=app',                'yyyyMMdd',       1, false, true",

      // -------- Hour, format=yyyy-MM-dd/HH --------
      // time only
      "'dt=2026-04-22/hh=06',                          'yyyy-MM-dd/HH',  0, true,  true",
      "'2026-04-22/06',                                'yyyy-MM-dd/HH',  0, false, true",
      // prefix + time
      "'region=us/dt=2026-04-22/hh=06',                'yyyy-MM-dd/HH',  1, true,  true",
      "'region=us/2026-04-22/06',                      'yyyy-MM-dd/HH',  1, false, true",
      // time + suffix
      "'dt=2026-04-22/hh=06/source=app',               'yyyy-MM-dd/HH',  0, true,  true",
      "'2026-04-22/06/source=app',                     'yyyy-MM-dd/HH',  0, false, true",
      // prefix + time + suffix
      "'region=us/dt=2026-04-22/hh=06/source=app',     'yyyy-MM-dd/HH',  1, true,  true",
      "'region=us/2026-04-22/06/source=app',           'yyyy-MM-dd/HH',  1, false, true",

      // -------- Hour, format=yyyyMMdd/HH --------
      // time only
      "'dt=20260422/hh=06',                            'yyyyMMdd/HH',    0, true,  true",
      "'20260422/06',                                  'yyyyMMdd/HH',    0, false, true",
      // prefix + time
      "'eventType=login/dt=20260422/hh=06',            'yyyyMMdd/HH',    1, true,  true",
      "'login/20260422/06',                            'yyyyMMdd/HH',    1, false, true",
      // time + suffix
      "'dt=20260422/hh=06/eventType=login',            'yyyyMMdd/HH',    0, true,  true",
      "'20260422/06/source=app',                       'yyyyMMdd/HH',    0, false, true",
      // prefix + time + suffix
      "'region=us/dt=20260422/hh=06/source=app',       'yyyyMMdd/HH',    1, true,  true",
      "'region=us/20260422/06/source=app',             'yyyyMMdd/HH',    1, false, true",
  })
  public void parsesPartitionAndCompares(String path,
                                         String format,
                                         int startIdx,
                                         boolean hiveStyle,
                                         boolean expectedExpired) {
    // Cutoff is "2026-04-30 00:00 UTC" so 2026-04-22 is expired (strictly before) while 2026-04-30 is not.
    long cutoff = LocalDate.of(2026, 4, 30).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format).withZone(ZoneOffset.UTC);
    int segCount = KeepByEventTimeStrategy.segmentCount(format);

    boolean expired = KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
        path, formatter, startIdx, segCount, cutoff, false, hiveStyle);

    assertEquals(expectedExpired, expired, "path=" + path + " format=" + format);
  }

  @Test
  public void parseFailureThrows() {
    // A partition whose time segment can't be parsed has no defined lifetime under event-time
    // semantics, so the strategy fails fast and asks the user to switch to commit-time TTL.
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    assertThrows(IllegalArgumentException.class, () ->
        KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
            "dt=not-a-date", f, 0, 1, Long.MAX_VALUE, false, true));
  }

  @Test
  public void shorterPathThanFormatThrows() {
    // Partition layout is fixed per table — a mismatch is a configuration error, not a per-row skip.
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH").withZone(ZoneOffset.UTC);
    assertThrows(IllegalArgumentException.class, () ->
        KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
            "dt=2026-04-22", f, 0, 2, Long.MAX_VALUE, false, true));
  }

  @Test
  public void startIndexOutOfRangeThrows() {
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    assertThrows(IllegalArgumentException.class, () ->
        KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
            "dt=2026-04-22", f, 5, 1, Long.MAX_VALUE, false, true));
  }

  @Test
  public void hiveStyleSegmentWithoutEqualsThrows() {
    // Table is hive-style but the segment lacks 'field=' prefix -> misconfiguration, fail fast.
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    assertThrows(IllegalArgumentException.class, () ->
        KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
            "2026-04-22", f, 0, 1, Long.MAX_VALUE, false, /*hiveStyle*/ true));
  }

  @Test
  public void nonHiveStyleSegmentWithEqualsThrows() {
    // hiveStyle=false: segment is taken verbatim, so '=' becomes part of the time string and
    // fails parsing -> hard error, same as any other unparseable partition.
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    assertThrows(IllegalArgumentException.class, () ->
        KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
            "dt=2026-04-22", f, 0, 1, Long.MAX_VALUE, false, /*hiveStyle*/ false));
  }

  @Test
  public void hiveDefaultPartitionSkippedWhenSwitchOff() {
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    boolean expired = KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
        "dt=__HIVE_DEFAULT_PARTITION__", f, 0, 1, Long.MAX_VALUE, false, true);
    assertFalse(expired);
  }

  @Test
  public void hiveDefaultPartitionDeletedWhenSwitchOn() {
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    boolean expired = KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
        "dt=__HIVE_DEFAULT_PARTITION__", f, 0, 1, /*cutoff*/ 0, /*deleteHiveDefault*/ true, /*hiveStyle*/ true);
    assertTrue(expired);
  }

  @Test
  public void wholeTimeBlockDefaultPartitionDeletedWhenSwitchOn() {
    // Multi-segment format where the entire time block is the default marker -> treated as
    // "the default partition" and respects the delete switch (just like the single-segment case).
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH").withZone(ZoneOffset.UTC);
    boolean expired = KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
        "dt=__HIVE_DEFAULT_PARTITION__/hh=__HIVE_DEFAULT_PARTITION__",
        f, 0, 2, /*cutoff*/ 0, /*deleteHiveDefault*/ true, /*hiveStyle*/ true);
    assertTrue(expired);
  }

  @Test
  public void partialDefaultPartitionTimeBlockFollowsSwitch() {
    // dt=2026-06-28/hh=__HIVE_DEFAULT_PARTITION__: the date column was recorded but the hour
    // column was NULL, so the event time has no place on the hour axis. We treat this exactly
    // like a fully-null time block — the explicit delete switch decides. The cutoff is a recent
    // date precisely to nail down that this is NOT a "the date is fresh, keep it" path.
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH").withZone(ZoneOffset.UTC);
    long cutoff = LocalDate.of(2026, 4, 30).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

    // switch on -> expire (even though dt looks fresh, event time is undefined)
    assertTrue(KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
        "dt=2026-06-28/hh=__HIVE_DEFAULT_PARTITION__",
        f, 0, 2, cutoff, /*deleteHiveDefault*/ true, /*hiveStyle*/ true));
    // switch off -> skip with WARN (user retains explicit control)
    assertFalse(KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
        "dt=2026-06-28/hh=__HIVE_DEFAULT_PARTITION__",
        f, 0, 2, cutoff, /*deleteHiveDefault*/ false, /*hiveStyle*/ true));
  }

  @Test
  public void resolveCutoffMillisInterpretsInstantInUtc() {
    // The instant string '20260430120000000' must be read as 2026-04-30T12:00:00Z regardless of the
    // JVM default zone. With ttl=0 the cutoff equals that exact instant; this pins down the contract
    // that resolveCutoffMillis and the partition formatter both speak UTC.
    long expected = LocalDateTime.of(2026, 4, 30, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    assertEquals(expected, KeepByEventTimeStrategy.resolveCutoffMillis("20260430120000000", 0));

    // ttl=1d shifts back exactly 24h, again with no dependence on the JVM zone.
    long oneDay = TimeUnit.DAYS.toMillis(1);
    assertEquals(expected - oneDay,
        KeepByEventTimeStrategy.resolveCutoffMillis("20260430120000000", oneDay));
  }

  @Test
  public void hourBoundaryRespected() {
    // Cutoff is 2026-04-22 12:00 UTC. 11:00 expired, 12:00 not expired.
    long cutoff = LocalDateTime.of(2026, 4, 22, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH").withZone(ZoneOffset.UTC);

    assertTrue(KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
        "2026-04-22/11", f, 0, 2, cutoff, false, /*hiveStyle*/ false));
    assertFalse(KeepByEventTimeStrategy.isPartitionExpiredByEventTime(
        "2026-04-22/12", f, 0, 2, cutoff, false, /*hiveStyle*/ false));
  }
}
