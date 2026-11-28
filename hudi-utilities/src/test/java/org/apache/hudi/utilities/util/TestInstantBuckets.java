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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestInstantBuckets {

  @Test
  void instantBelowTheEarliestPokesTheArchive() {
    assertTrue(InstantBuckets.pokesArchive("001", "005"));
  }

  @Test
  void earliestItselfDoesNotPokeTheArchive() {
    assertFalse(InstantBuckets.pokesArchive("005", "005"));
  }

  @Test
  void instantAboveTheEarliestDoesNotPokeTheArchive() {
    assertFalse(InstantBuckets.pokesArchive("009", "005"));
  }

  @Test
  void emptyActiveTimelineSendsEverythingToTheArchive() {
    assertTrue(InstantBuckets.pokesArchive("001", null));
  }

  @Test
  void nullWritingInstantIsACallerBug() {
    assertThrows(IllegalArgumentException.class, () -> InstantBuckets.pokesArchive(null, "005"));
  }

  @Test
  void earliestOfAnEmptyTimelineIsNull() {
    HoodieTimeline emptyTimeline = TestTimelines.of();
    assertNull(InstantBuckets.earliestCompletedWriteCommit(emptyTimeline));
  }

  @Test
  void earliestIsTheMinimumNotTheFirstIterated() {
    // Build a timeline with instants NOT in ascending order to verify we find the minimum
    HoodieTimeline timeline = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "009"),
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "005"),
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "007"));

    assertEquals("005", InstantBuckets.earliestCompletedWriteCommit(timeline));
  }
}
