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

import org.apache.hudi.common.table.timeline.versioning.common.InstantComparators;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestInstantComparators {
  @Test
  void testCompletionTimeOrdering() {
    HoodieInstant instant1 = createCompletedHoodieInstant("001", "002");
    HoodieInstant instant2 = createCompletedHoodieInstant("003", "005");
    HoodieInstant instant3 = createCompletedHoodieInstant("002", "004");
    HoodieInstant instant4 = createInflightHoodieInstant("004");
    HoodieInstant instant5 = createInflightHoodieInstant("009");

    Comparator<HoodieInstant> comparator = new InstantComparators.CompletionTimeBasedComparator(Collections.singletonMap(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.COMMIT_ACTION));
    List<HoodieInstant> instants = Arrays.asList(instant5, instant3, instant1, instant4, instant2);
    instants.sort(comparator);
    assertEquals(Arrays.asList(instant1, instant3, instant2, instant4, instant5), instants);
  }

  private static HoodieInstant createCompletedHoodieInstant(String requestedTime, String completionTime) {
    return new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, requestedTime, completionTime, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);
  }

  private static HoodieInstant createInflightHoodieInstant(String requestedTime) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, requestedTime, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }
}
