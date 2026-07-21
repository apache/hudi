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

package org.apache.hudi.common.table.timeline.versioning.v1;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link CompletionTimeQueryViewV1#getInstantTimes}.
 *
 * <p>A LAYOUT_VERSION_1 timeline (table versions 5 ~ 7) does not persist a separate completion
 * time; an instant's completion time is, backward-compatibly, its instant (request) time. The range
 * query therefore filters by requested time. These tests assert the candidate instant selection
 * matches the streaming/incremental read semantics (resume excludes the last issued offset, the
 * first read includes the start commit, etc.), the same behavior as the V2 view keyed on completion
 * time.
 *
 * <p>The timeline is mocked rather than materialized on disk: {@code getInstantTimes} consumes only
 * the supplied {@link HoodieTimeline}, and {@code hudi-common} test scope has no hadoop storage
 * implementation to build a real table from.
 */
public class TestCompletionTimeQueryViewV1 {

  // Use datetime-format instant times so they are not short-circuited by the length < 10 branch in
  // getCompletionTime, and behave like real V5 ~ V7 instants.
  private static final String T1 = "20240101010001000";
  private static final String T2 = "20240101010002000";
  private static final String T3 = "20240101010003000";
  private static final String T4 = "20240101010004000";
  private static final String T5 = "20240101010005000";

  private CompletionTimeQueryViewV1 view;
  private HoodieTimeline timeline;

  @BeforeEach
  void setUp() {
    List<HoodieInstant> instants = Arrays.asList(
        completedCommit(T1), completedCommit(T2), completedCommit(T3), completedCommit(T4), completedCommit(T5));

    // A timeline whose filterCompletedInstants() returns itself and replays a fresh stream each call.
    timeline = mock(HoodieTimeline.class);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(timeline.getInstantsAsStream()).thenAnswer(invocation -> instants.stream());
    when(timeline.lastInstant()).thenAnswer(invocation -> Option.of(instants.get(instants.size() - 1)));

    // Minimal meta client wiring so the V1 view can be constructed (load()/cursor/firstNonSavepoint).
    // Build the active timeline mock fully before stubbing getActiveTimeline(), otherwise Mockito
    // sees nested stubbing (a when() started inside another when()'s argument) and fails.
    HoodieActiveTimeline activeTimeline = activeTimelineMock(instants);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    view = new CompletionTimeQueryViewV1(metaClient);
  }

  private static HoodieInstant completedCommit(String instantTime) {
    // V1 instants are ordered/compared by requested(instant) time; completion time mirrors it.
    return new HoodieInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, instantTime, instantTime,
        InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  /**
   * Builds a mock active timeline used only for constructing the view (load(), cursor and
   * firstNonSavepointCommit). It is independent of the timeline passed to getInstantTimes.
   */
  private static HoodieActiveTimeline activeTimelineMock(List<HoodieInstant> instants) {
    HoodieActiveTimeline active = mock(HoodieActiveTimeline.class);
    when(active.filterCompletedInstants()).thenReturn(active);
    when(active.getWriteTimeline()).thenReturn(active);
    when(active.getInstantsAsStream()).thenAnswer(invocation -> instants.stream());
    when(active.firstInstant()).thenAnswer(invocation -> Option.of(instants.get(0)));
    when(active.getFirstNonSavepointCommit()).thenAnswer(invocation -> Option.of(instants.get(0)));
    return active;
  }

  private List<String> query(Option<String> start, Option<String> end, InstantRange.RangeType rangeType) {
    return view.getInstantTimes(timeline, start, end, rangeType);
  }

  @Test
  void testClosedClosedRange() {
    // [T2, T4]: first-time consume with explicit start/end commit, both bounds inclusive.
    assertEquals(Arrays.asList(T2, T3, T4), query(Option.of(T2), Option.of(T4), InstantRange.RangeType.CLOSED_CLOSED));

    // [T3, _): from the start commit to the latest, open end.
    assertEquals(Arrays.asList(T3, T4, T5), query(Option.of(T3), Option.empty(), InstantRange.RangeType.CLOSED_CLOSED));

    // [T1, T5]: the full range, also verifies the result is sorted ascending.
    assertEquals(Arrays.asList(T1, T2, T3, T4, T5), query(Option.of(T1), Option.of(T5), InstantRange.RangeType.CLOSED_CLOSED));
  }

  @Test
  void testOpenClosedResumeRange() {
    // (T2, T4]: streaming resume from issued offset T2, the start point must be excluded.
    assertEquals(Arrays.asList(T3, T4), query(Option.of(T2), Option.of(T4), InstantRange.RangeType.OPEN_CLOSED));

    // (T4, _]: resume from T4 with open end, only T5 is new.
    assertEquals(Collections.singletonList(T5), query(Option.of(T4), Option.empty(), InstantRange.RangeType.OPEN_CLOSED));

    // (T5, _]: resume from the latest issued offset T5, nothing new, must not re-read T5.
    assertEquals(Collections.emptyList(), query(Option.of(T5), Option.empty(), InstantRange.RangeType.OPEN_CLOSED));
  }

  @Test
  void testEarliestStart() {
    // ['earliest', T3]: 'earliest' degenerates to consuming from the first instant.
    assertEquals(Arrays.asList(T1, T2, T3),
        query(Option.of("earliest"), Option.of(T3), InstantRange.RangeType.CLOSED_CLOSED));
  }

  @Test
  void testEndOnlyReturnsLastAtOrBeforeEnd() {
    // (_, T3]: no start commit, returns the single latest instant at or before T3.
    assertEquals(Collections.singletonList(T3), query(Option.empty(), Option.of(T3), InstantRange.RangeType.CLOSED_CLOSED));

    // (_, T5]: end at the latest, returns T5.
    assertEquals(Collections.singletonList(T5), query(Option.empty(), Option.of(T5), InstantRange.RangeType.CLOSED_CLOSED));
  }

  @Test
  void testNoBoundsReadsLatestSnapshot() {
    // (_, _): no range at all, reads the latest snapshot instant.
    assertEquals(Collections.singletonList(T5), query(Option.empty(), Option.empty(), InstantRange.RangeType.CLOSED_CLOSED));
  }
}
