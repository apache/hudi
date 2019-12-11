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

package org.apache.hudi.common.table.string;

import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link HoodieActiveTimeline}.
 */
public class TestHoodieActiveTimeline extends HoodieCommonTestHarness {

  private HoodieActiveTimeline timeline;
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    initMetaClient();
  }

  @Test
  public void testLoadingInstantsFromFiles() throws IOException {
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "3");
    HoodieInstant instant3 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "5");
    HoodieInstant instant4 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "8");
    HoodieInstant instant1Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant instant2Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "3");
    HoodieInstant instant3Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "5");
    HoodieInstant instant4Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "8");

    HoodieInstant instant5 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "9");

    timeline = new HoodieActiveTimeline(metaClient);
    timeline.saveAsComplete(instant1, Option.empty());
    timeline.saveAsComplete(instant2, Option.empty());
    timeline.saveAsComplete(instant3, Option.empty());
    timeline.saveAsComplete(instant4, Option.empty());
    timeline.createInflight(instant5);
    timeline = timeline.reload();

    assertEquals("Total instants should be 5", 5, timeline.countInstants());
    HoodieTestUtils.assertStreamEquals("Check the instants stream",
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete, instant5),
        timeline.getInstants());
    HoodieTestUtils.assertStreamEquals("Check the instants stream",
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete, instant5),
        timeline.getCommitTimeline().getInstants());
    HoodieTestUtils.assertStreamEquals("Check the instants stream",
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete),
        timeline.getCommitTimeline().filterCompletedInstants().getInstants());
    HoodieTestUtils.assertStreamEquals("Check the instants stream", Stream.of(instant5),
        timeline.getCommitTimeline().filterInflightsExcludingCompaction().getInstants());
  }

  @Test
  public void testTimelineOperationsBasic() throws Exception {
    timeline = new HoodieActiveTimeline(metaClient);
    assertTrue(timeline.empty());
    assertEquals("", 0, timeline.countInstants());
    assertEquals("", Option.empty(), timeline.firstInstant());
    assertEquals("", Option.empty(), timeline.nthInstant(5));
    assertEquals("", Option.empty(), timeline.nthInstant(-1));
    assertEquals("", Option.empty(), timeline.lastInstant());
    assertFalse("", timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "01")));
  }

  @Test
  public void testTimelineOperations() throws Exception {
    timeline = new MockHoodieTimeline(Stream.of("01", "03", "05", "07", "09", "11", "13", "15", "17", "19"),
        Stream.of("21", "23"));
    HoodieTestUtils.assertStreamEquals("", Stream.of("05", "07", "09", "11"), timeline.getCommitTimeline()
        .filterCompletedInstants().findInstantsInRange("04", "11").getInstants().map(HoodieInstant::getTimestamp));
    HoodieTestUtils.assertStreamEquals("", Stream.of("09", "11"), timeline.getCommitTimeline().filterCompletedInstants()
        .findInstantsAfter("07", 2).getInstants().map(HoodieInstant::getTimestamp));
    assertFalse(timeline.empty());
    assertFalse(timeline.getCommitTimeline().filterInflightsExcludingCompaction().empty());
    assertEquals("", 12, timeline.countInstants());
    HoodieTimeline activeCommitTimeline = timeline.getCommitTimeline().filterCompletedInstants();
    assertEquals("", 10, activeCommitTimeline.countInstants());

    assertEquals("", "01", activeCommitTimeline.firstInstant().get().getTimestamp());
    assertEquals("", "11", activeCommitTimeline.nthInstant(5).get().getTimestamp());
    assertEquals("", "19", activeCommitTimeline.lastInstant().get().getTimestamp());
    assertEquals("", "09", activeCommitTimeline.nthFromLastInstant(5).get().getTimestamp());
    assertTrue("", activeCommitTimeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "09")));
    assertFalse("", activeCommitTimeline.isBeforeTimelineStarts("02"));
    assertTrue("", activeCommitTimeline.isBeforeTimelineStarts("00"));
  }
}
