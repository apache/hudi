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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestInstantStateIndex {

  @Test
  void reportsTheRecordedStateForAnInstantOnTheActiveTimeline() {
    HoodieTimeline timeline = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "100"),
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "200"),
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "300"));
    InstantStateIndex index = InstantStateIndex.from(timeline);

    assertEquals(InstantState.COMPLETED, index.resolveAtPrev("100").getState());
    assertEquals(InstantState.INFLIGHT, index.resolveAtPrev("200").getState());
    assertEquals(InstantState.REQUESTED, index.resolveAtPrev("300").getState());
    assertTrue(index.resolveAtPrev("100").isResolved());
  }

  @Test
  void anInstantAbsentFromTheActiveTimelineIsUnresolvedRatherThanNull() {
    HoodieTimeline timeline = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "100"));
    InstantStateIndex index = InstantStateIndex.from(timeline);

    // Only the archive can tell COMPLETED-and-archived from NULL. The index must not guess.
    assertFalse(index.resolveAtPrev("050").isResolved());
    assertFalse(index.isOnActiveTimeline("050"));
  }

  @Test
  void indexesNonCommitActionsThatGetCommitsTimelineWouldHide() {
    HoodieTimeline timeline = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "400"),
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "500"));
    InstantStateIndex index = InstantStateIndex.from(timeline);

    assertEquals(InstantState.COMPLETED, index.resolveAtPrev("400").getState());
    assertEquals(InstantState.INFLIGHT, index.resolveAtPrev("500").getState());
  }

  @Test
  void whenSeveralStatesOfOneWriteActionCoexistTheMostAdvancedWins() {
    // revertCompleteToInflight creates .requested and .inflight BEFORE deleting .completed
    // (ActiveTimelineV2:607-641), so a mid-revert read sees all three. Reading the least advanced
    // would present an ordinary rollback as a regression.
    HoodieTimeline timeline = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "600"),
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "600"),
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "600"));
    InstantStateIndex index = InstantStateIndex.from(timeline);

    assertEquals(InstantState.COMPLETED, index.resolveAtPrev("600").getState());
  }

  @Test
  void ignoresNonWriteActionsSharingAnInstantTimeWithAWriteAction() {
    // A savepoint is named with the instant it savepoints, so creating one mid-run would otherwise
    // present as (COMPLETED, INFLIGHT) — a pair the switch treats as impossible.
    HoodieTimeline timeline = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "700"),
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION, "700"));
    InstantStateIndex index = InstantStateIndex.from(timeline);

    assertEquals(InstantState.COMPLETED, index.resolveAtPrev("700").getState());
  }

  @Test
  void exposesTheOrderedInstantListAndTheOldestCompletedCommit() {
    HoodieTimeline timeline = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "300"),
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "100"),
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "200"));
    InstantStateIndex index = InstantStateIndex.from(timeline);

    List<String> ordered = index.getOrderedInstants();
    assertEquals(Arrays.asList("100", "200", "300"), ordered);
    assertEquals("100", index.getOldestActiveCommit());
  }
}
