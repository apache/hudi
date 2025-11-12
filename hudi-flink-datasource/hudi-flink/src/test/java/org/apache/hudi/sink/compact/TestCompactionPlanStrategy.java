/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategies;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test case for every {@link CompactionPlanStrategy} implements
 */
public class TestCompactionPlanStrategy {
  private HoodieTimeline timeline;
  private HoodieTimeline emptyTimeline;
  private HoodieTimeline allCompleteTimeline;

  private static final HoodieInstant INSTANT_001 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001");
  private static final HoodieInstant INSTANT_002 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "002");
  private static final HoodieInstant INSTANT_003 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "003");
  private static final HoodieInstant INSTANT_004 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "004");
  private static final HoodieInstant INSTANT_005 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMPACTION_ACTION, "005");
  private static final HoodieInstant INSTANT_006 = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "006");

  @BeforeEach
  public void beforeEach() {
    timeline = new MockHoodieActiveTimeline(INSTANT_001, INSTANT_002, INSTANT_003, INSTANT_004, INSTANT_005, INSTANT_006);
    emptyTimeline = new MockHoodieActiveTimeline();
    allCompleteTimeline = new MockHoodieActiveTimeline(INSTANT_001, INSTANT_005);
  }

  @Test
  void testSingleCompactionPlanSelectStrategy() {
    HoodieTimeline pendingCompactionTimeline = this.timeline.filterPendingCompactionTimeline();
    FlinkCompactionConfig compactionConfig = new FlinkCompactionConfig();
    CompactionPlanStrategy strategy = CompactionPlanStrategies.getStrategy(compactionConfig);

    assertHoodieInstantsEquals(new HoodieInstant[] {INSTANT_002}, strategy.select(pendingCompactionTimeline));

    compactionConfig.compactionSeq = FlinkCompactionConfig.SEQ_LIFO;
    assertHoodieInstantsEquals(new HoodieInstant[] {INSTANT_006}, strategy.select(pendingCompactionTimeline));

    HoodieTimeline emptyPendingCompactionTimeline = emptyTimeline.filterPendingCompactionTimeline();
    assertHoodieInstantsEquals(new HoodieInstant[] {}, strategy.select(emptyPendingCompactionTimeline));

    HoodieTimeline allCompleteCompactionTimeline = allCompleteTimeline.filterPendingCompactionTimeline();
    assertHoodieInstantsEquals(new HoodieInstant[] {}, strategy.select(allCompleteCompactionTimeline));
  }

  @Test
  void testMultiCompactionPlanSelectStrategy() {
    HoodieTimeline pendingCompactionTimeline = this.timeline.filterPendingCompactionTimeline();
    FlinkCompactionConfig compactionConfig = new FlinkCompactionConfig();
    compactionConfig.maxNumCompactionPlans = 2;

    CompactionPlanStrategy strategy = CompactionPlanStrategies.getStrategy(compactionConfig);
    assertHoodieInstantsEquals(new HoodieInstant[] {INSTANT_002, INSTANT_003}, strategy.select(pendingCompactionTimeline));

    compactionConfig.compactionSeq = FlinkCompactionConfig.SEQ_LIFO;
    assertHoodieInstantsEquals(new HoodieInstant[] {INSTANT_006, INSTANT_004}, strategy.select(pendingCompactionTimeline));

    HoodieTimeline emptyPendingCompactionTimeline = emptyTimeline.filterPendingCompactionTimeline();
    assertHoodieInstantsEquals(new HoodieInstant[] {}, strategy.select(emptyPendingCompactionTimeline));

    HoodieTimeline allCompleteCompactionTimeline = allCompleteTimeline.filterPendingCompactionTimeline();
    assertHoodieInstantsEquals(new HoodieInstant[] {}, strategy.select(allCompleteCompactionTimeline));
  }

  @Test
  void testAllPendingCompactionPlanSelectStrategy() {
    HoodieTimeline pendingCompactionTimeline = this.timeline.filterPendingCompactionTimeline();
    FlinkCompactionConfig compactionConfig = new FlinkCompactionConfig();
    compactionConfig.compactionPlanSelectStrategy = CompactionPlanStrategy.ALL;
    CompactionPlanStrategy strategy = CompactionPlanStrategies.getStrategy(compactionConfig);

    assertHoodieInstantsEquals(new HoodieInstant[] {INSTANT_002, INSTANT_003, INSTANT_004, INSTANT_006},
        strategy.select(pendingCompactionTimeline));

    HoodieTimeline emptyPendingCompactionTimeline = emptyTimeline.filterPendingCompactionTimeline();
    assertHoodieInstantsEquals(new HoodieInstant[] {}, strategy.select(emptyPendingCompactionTimeline));

    HoodieTimeline allCompleteCompactionTimeline = allCompleteTimeline.filterPendingCompactionTimeline();
    assertHoodieInstantsEquals(new HoodieInstant[] {}, strategy.select(allCompleteCompactionTimeline));
  }

  @Test
  void testInstantCompactionPlanSelectStrategy() {
    HoodieTimeline pendingCompactionTimeline = this.timeline.filterPendingCompactionTimeline();
    FlinkCompactionConfig compactionConfig = new FlinkCompactionConfig();

    compactionConfig.compactionPlanSelectStrategy = CompactionPlanStrategy.INSTANTS;
    CompactionPlanStrategy strategy = CompactionPlanStrategies.getStrategy(compactionConfig);
    compactionConfig.compactionPlanInstant = "004";

    assertHoodieInstantsEquals(new HoodieInstant[] {INSTANT_004}, strategy.select(pendingCompactionTimeline));

    compactionConfig.compactionPlanInstant = "002,003";
    assertHoodieInstantsEquals(new HoodieInstant[] {INSTANT_002, INSTANT_003}, strategy.select(pendingCompactionTimeline));

    compactionConfig.compactionPlanInstant = "002,005";
    assertHoodieInstantsEquals(new HoodieInstant[] {INSTANT_002}, strategy.select(pendingCompactionTimeline));

    compactionConfig.compactionPlanInstant = "005";
    assertHoodieInstantsEquals(new HoodieInstant[] {}, strategy.select(pendingCompactionTimeline));
  }

  private void assertHoodieInstantsEquals(HoodieInstant[] expected, List<HoodieInstant> actual) {
    assertEquals(expected.length, actual.size());
    for (int index = 0; index < expected.length; index++) {
      assertHoodieInstantEquals(expected[index], actual.get(index));
    }
  }

  private void assertHoodieInstantEquals(HoodieInstant expected, HoodieInstant actual) {
    assertEquals(expected.getState(), actual.getState());
    assertEquals(expected.getAction(), actual.getAction());
    assertEquals(expected.requestedTime(), actual.requestedTime());
  }

  private static final class MockHoodieActiveTimeline extends ActiveTimelineV2 {
    public MockHoodieActiveTimeline(HoodieInstant... instants) {
      super();
      setInstants(Arrays.asList(instants));
    }
  }
}
