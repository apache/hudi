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
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDuplicateInstantWindow {

  @Test
  void anInstantAbsentFromBothReadsIsNotSanctioned() {
    // The worked example this class exists for: a slice under an instant with no requested,
    // inflight, completed or archived file at all, at either read.
    HoodieTimeline tPrevD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "100"));
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "100"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertFalse(window.sanctions("999"));
    assertEquals(Option.empty(), window.sanctionReasonFor("999"));
  }

  @Test
  void requestedAtPrevIsSanctionedAsPrevInflight() {
    HoodieTimeline tPrevD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "200"));
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "200"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertTrue(window.sanctions("200"));
    assertEquals(DuplicateInstantWindow.Reason.PREV_INFLIGHT, window.sanctionReasonFor("200").get());
  }

  @Test
  void inflightAtPrevIsSanctionedAsPrevInflight() {
    HoodieTimeline tPrevD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "300"));
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "300"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertTrue(window.sanctions("300"));
    assertEquals(DuplicateInstantWindow.Reason.PREV_INFLIGHT, window.sanctionReasonFor("300").get());
  }

  @Test
  void completedAtPrevIsNotSanctionedAsPrevInflight() {
    // A completed instant was never "inflight". It is also on tAftD (only archival removes a
    // completed instant, which this class does not need to special-case), so it survives.
    HoodieTimeline tPrevD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "400"));
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "400"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertFalse(window.sanctions("400"));
  }

  @Test
  void newAtAftInRequestedStateIsSanctionedAsAftExclusive() {
    HoodieTimeline tPrevD = TestTimelines.of();
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "500"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertTrue(window.sanctions("500"));
    assertEquals(DuplicateInstantWindow.Reason.AFT_EXCLUSIVE, window.sanctionReasonFor("500").get());
  }

  @Test
  void newAtAftInInflightStateIsSanctionedAsAftExclusive() {
    HoodieTimeline tPrevD = TestTimelines.of();
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "600"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertTrue(window.sanctions("600"));
    assertEquals(DuplicateInstantWindow.Reason.AFT_EXCLUSIVE, window.sanctionReasonFor("600").get());
  }

  @Test
  void newAtAftInCompletedStateIsSanctionedAsAftExclusive() {
    // Started and finished entirely inside the window: absent at T_prevD, completed by T_aftD.
    HoodieTimeline tPrevD = TestTimelines.of();
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "700"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertTrue(window.sanctions("700"));
    assertEquals(DuplicateInstantWindow.Reason.AFT_EXCLUSIVE, window.sanctionReasonFor("700").get());
  }

  @Test
  void presentAtPrevAndUnchangedAtAftMatchesPrevsOwnResolution() {
    // Inflight at prev, still inflight at aft: still PREV_INFLIGHT, regardless of aft's own state.
    HoodieTimeline tPrevD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "800"));
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "800"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertTrue(window.sanctions("800"));
    assertEquals(DuplicateInstantWindow.Reason.PREV_INFLIGHT, window.sanctionReasonFor("800").get());
  }

  @Test
  void nullInstantTimeNeverSanctions() {
    HoodieTimeline tPrevD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "900"));
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "900"));
    DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);

    assertFalse(window.sanctions(null));
    assertEquals(Option.empty(), window.sanctionReasonFor(null));
  }
}
