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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestCleanerUtils {
  private final Functions.Function0<Boolean> rollbackFunction = mock(Functions.Function0.class);

  @Test
  void rollbackFailedWrites_CleanWithEagerPolicy() {
    assertFalse(CleanerUtils.rollbackFailedWrites(HoodieFailedWritesCleaningPolicy.EAGER, HoodieActiveTimeline.CLEAN_ACTION, rollbackFunction));
    verify(rollbackFunction, never()).apply();
  }

  @Test
  void rollbackFailedWrites_CleanWithLazyPolicy() {
    when(rollbackFunction.apply()).thenReturn(true);
    assertTrue(CleanerUtils.rollbackFailedWrites(HoodieFailedWritesCleaningPolicy.LAZY, HoodieActiveTimeline.CLEAN_ACTION, rollbackFunction));
  }

  @Test
  void rollbackFailedWrites_CommitWithEagerPolicy() {
    when(rollbackFunction.apply()).thenReturn(true);
    assertTrue(CleanerUtils.rollbackFailedWrites(HoodieFailedWritesCleaningPolicy.EAGER, HoodieActiveTimeline.COMMIT_ACTION, rollbackFunction));
  }

  @Test
  void rollbackFailedWrites_CommitWithLazyPolicy() {
    assertFalse(CleanerUtils.rollbackFailedWrites(HoodieFailedWritesCleaningPolicy.LAZY, HoodieActiveTimeline.COMMIT_ACTION, rollbackFunction));
    verify(rollbackFunction, never()).apply();
  }

  @Test
  void testGetEarliestCommitToRetain_WithMaxCommitsToClean_NoCapping() {
    // Test scenario: 20 commits, retain 12, clean 8 commits, maxCommitsToClean=50
    // Expected: No capping needed, should return the originally calculated earliest commit
    HoodieTimeline timeline = createMockTimeline(20);

    Option<HoodieInstant> result = CleanerUtils.getEarliestCommitToRetain(
        timeline,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
        12, // commits to retain
        Instant.now(),
        24, // hours retained
        HoodieTimelineTimeZone.UTC,
        Option.of("20000000000005"), // previous clean's earliest commit to retain (commit 5)
        50L // maxCommitsToClean
    );

    assertTrue(result.isPresent());
    // With 20 commits and retain 12, earliest commit to retain should be commit at index 8 (20-12=8)
    assertEquals("20000000000008", result.get().requestedTime());
  }

  @Test
  void testGetEarliestCommitToRetain_WithMaxCommitsToClean_WithCapping() {
    // Test scenario: 1000 commits, retain 12, would clean 988 commits, maxCommitsToClean=50
    // Expected: Should cap to clean only 50 commits
    HoodieTimeline timeline = createMockTimeline(1000);

    Option<HoodieInstant> result = CleanerUtils.getEarliestCommitToRetain(
        timeline,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
        12, // commits to retain
        Instant.now(),
        24, // hours retained
        HoodieTimelineTimeZone.UTC,
        Option.of("20000000000000"), // previous clean's earliest commit to retain (commit 0)
        50L // maxCommitsToClean
    );

    assertTrue(result.isPresent());
    // Should cap at 50 commits from commit 0, so earliest commit to retain should be commit 50
    // (Clean commits 0-49, retain from 50 onwards)
    assertEquals("20000000000050", result.get().requestedTime());

    result = CleanerUtils.getEarliestCommitToRetain(
        timeline,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
        12, // commits to retain
        Instant.now(),
        24, // hours retained
        HoodieTimelineTimeZone.UTC,
        Option.of("20000000000050"), // previous clean's earliest commit to retain (commit 50)
        50L // maxCommitsToClean
    );

    assertTrue(result.isPresent());
    // Should cap at 50 commits from commit 50, so earliest commit to retain should be commit 100
    // (Clean commits 50-99, retain from 100 onwards)
    assertEquals("20000000000100", result.get().requestedTime());
  }

  @Test
  void testGetEarliestCommitToRetain_WithMaxCommitsToClean_ExactBoundary() {
    // Test scenario: Clean exactly maxCommitsToClean commits
    HoodieTimeline timeline = createMockTimeline(100);

    Option<HoodieInstant> result = CleanerUtils.getEarliestCommitToRetain(
        timeline,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
        12, // commits to retain
        Instant.now(),
        24, // hours retained
        HoodieTimelineTimeZone.UTC,
        Option.of("20000000000000"), // previous clean at commit 0
        88L // maxCommitsToClean (exactly the number of commits eligible: 88)
    );

    assertTrue(result.isPresent());
    // With 100 commits and retain 12, earliest would be commit 88
    // With previous clean at 0 and max 88 to clean, we can clean commits 0-87, so earliest to retain is 88
    assertEquals("20000000000088", result.get().requestedTime());
  }

  @Test
  void testGetEarliestCommitToRetain_WithMaxCommitsToClean_NoPreviousClean() {
    // Test scenario: No previous clean metadata available
    HoodieTimeline timeline = createMockTimeline(100);

    Option<HoodieInstant> result = CleanerUtils.getEarliestCommitToRetain(
        timeline,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
        12, // commits to retain
        Instant.now(),
        24, // hours retained
        HoodieTimelineTimeZone.UTC,
        Option.empty(), // no previous clean
        50L // maxCommitsToClean
    );

    assertTrue(result.isPresent());
    // Without previous clean, capping should not apply
    // With 100 commits and retain 12, earliest commit to retain should be commit 88
    assertEquals("20000000000088", result.get().requestedTime());
  }

  @Test
  void testGetEarliestCommitToRetain_WithMaxCommitsToClean_DefaultValue() {
    // Test scenario: maxCommitsToClean is set to default Long.MAX_VALUE (no capping)
    HoodieTimeline timeline = createMockTimeline(1000);

    Option<HoodieInstant> result = CleanerUtils.getEarliestCommitToRetain(
        timeline,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
        12, // commits to retain
        Instant.now(),
        24, // hours retained
        HoodieTimelineTimeZone.UTC,
        Option.of("20000000000000"), // previous clean at commit 0
        Long.MAX_VALUE // no capping
    );

    assertTrue(result.isPresent());
    // With no capping and 1000 commits retain 12, earliest should be commit 988
    assertEquals("20000000000988", result.get().requestedTime());
  }

  @Test
  void testGetEarliestCommitToRetain_WithMaxCommitsToClean_KeepLatestByHours() {
    // Test scenario: 100 commits spanning 10 hours, retain last 2 hours, maxCommitsToClean=10
    // Commits are spaced ~6 min apart (100 commits over 10 hours).
    // With hoursRetained=2, the BY_HOURS policy calculates an earliest commit to retain ~80% through the timeline.
    // The previous clean was at commit 0, so all commits before the calculated earliest are eligible for cleaning.
    // With maxCommitsToClean=10, capping should kick in and only allow cleaning 10 commits from commit 0.
    Instant now = Instant.now();
    ZonedDateTime nowUtc = ZonedDateTime.ofInstant(now, ZoneId.of("UTC"));
    int numCommits = 100;
    int totalHoursSpan = 10;

    // Generate timestamps: commit 0 is 10 hours ago, commit 99 is now
    List<String> timestamps = new ArrayList<>();
    for (int i = 0; i < numCommits; i++) {
      ZonedDateTime commitTime = nowUtc.minusHours(totalHoursSpan).plusMinutes((long) i * totalHoursSpan * 60 / numCommits);
      String ts = org.apache.hudi.common.table.timeline.TimelineUtils.formatDate(Date.from(commitTime.toInstant()));
      timestamps.add(ts);
    }

    HoodieTimeline timeline = createMockTimelineWithTimestamps(timestamps);
    String previousCleanEarliestCommit = timestamps.get(0);

    Option<HoodieInstant> result = CleanerUtils.getEarliestCommitToRetain(
        timeline,
        HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS,
        12, // commits to retain (not used for BY_HOURS)
        now,
        2, // retain last 2 hours
        HoodieTimelineTimeZone.UTC,
        Option.of(previousCleanEarliestCommit),
        10L // maxCommitsToClean
    );

    assertTrue(result.isPresent());
    // The BY_HOURS policy would calculate an earliest commit to retain ~80 commits in (retaining ~last 20).
    // With maxCommitsToClean=10, capping adjusts to only clean 10 commits from the previous clean point.
    // So the result should be commit at index 10.
    assertEquals(timestamps.get(10), result.get().requestedTime());
  }

  /**
   * Helper method to create a mock timeline with specified number of commits.
   * Commits are named as "20000000000000", "20000000000001", etc.
   */
  private HoodieTimeline createMockTimeline(int numCommits) {
    List<String> timestamps = new ArrayList<>();
    for (int i = 0; i < numCommits; i++) {
      timestamps.add(String.format("200000000%05d", i));
    }
    return createMockTimelineWithTimestamps(timestamps);
  }

  /**
   * Helper method to create a mock timeline with specified timestamps.
   */
  private HoodieTimeline createMockTimelineWithTimestamps(List<String> timestamps) {
    int numCommits = timestamps.size();
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);

    List<HoodieInstant> instants = new ArrayList<>();
    for (String timestamp : timestamps) {
      HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED,
          HoodieTimeline.COMMIT_ACTION, timestamp, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);
      instants.add(instant);
    }

    when(timeline.filterCompletedInstants()).thenReturn(completedTimeline);
    when(completedTimeline.countInstants()).thenReturn(numCommits);
    when(completedTimeline.getInstantsAsStream()).thenAnswer(invocation -> instants.stream());

    // Mock nthInstant to return the nth instant from the list
    for (int i = 0; i < numCommits; i++) {
      int index = i;
      when(completedTimeline.nthInstant(i)).thenReturn(Option.of(instants.get(index)));
    }

    // Mock findInstantsBefore to return all instants before a given timestamp
    when(completedTimeline.findInstantsBefore(org.mockito.ArgumentMatchers.anyString()))
        .thenAnswer(invocation -> {
          String timestamp = invocation.getArgument(0);
          HoodieTimeline beforeTimeline = mock(HoodieTimeline.class);
          List<HoodieInstant> beforeInstants = instants.stream()
              .filter(i -> i.requestedTime().compareTo(timestamp) < 0)
              .collect(Collectors.toList());
          when(beforeTimeline.getInstantsAsStream()).thenAnswer(inv -> beforeInstants.stream());
          return beforeTimeline;
        });

    // Mock filter for pending commits (empty for this test)
    HoodieTimeline emptyTimeline = mock(HoodieTimeline.class);
    when(emptyTimeline.firstInstant()).thenReturn(Option.empty());
    when(timeline.filter(org.mockito.ArgumentMatchers.any())).thenReturn(emptyTimeline);

    return timeline;
  }
}
