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

package org.apache.hudi.common.table;

import org.apache.hudi.common.model.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestTimelineLayout  {

  @Test
  public void testTimelineLayoutFilter() {
    List<HoodieInstant> rawInstants = Arrays.asList(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "001"),
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "001"),
        new HoodieInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "001"),
        new HoodieInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "002"),
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "002"),
        new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "002"),
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "003"),
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "003"),
        new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "003"),
        new HoodieInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "004"),
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "004"),
        new HoodieInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "005"),
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "005"),
        new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "005"),
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "006"),
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "006"),
        new HoodieInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "007"),
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "007"));

    List<HoodieInstant> layout0Instants = TimelineLayout.getLayout(new TimelineLayoutVersion(0))
        .filterHoodieInstants(rawInstants.stream()).collect(Collectors.toList());
    Assert.assertEquals(rawInstants, layout0Instants);
    List<HoodieInstant> layout1Instants = TimelineLayout.getLayout(TimelineLayoutVersion.CURR_LAYOUT_VERSION)
        .filterHoodieInstants(rawInstants.stream()).collect(Collectors.toList());
    Assert.assertEquals(7, layout1Instants.size());
    Assert.assertTrue(layout1Instants.contains(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "007")));
    Assert.assertTrue(layout1Instants.contains(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "006")));
    Assert.assertTrue(layout1Instants.contains(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "005")));
    Assert.assertTrue(layout1Instants.contains(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "004")));
    Assert.assertTrue(layout1Instants.contains(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "003")));
    Assert.assertTrue(layout1Instants.contains(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "002")));
    Assert.assertTrue(layout1Instants.contains(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "001")));
  }
}
