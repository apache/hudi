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

package org.apache.hudi.source;

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * Test cases for {@link IncrementalInputSplits}.
 */
public class TestIncrementalInputSplits extends HoodieCommonTestHarness {

  @BeforeEach
  private void init() throws IOException {
    initPath();
    initMetaClient();
  }

  @Test
  void testFilterInstantsWithRange() {
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient, true);
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    IncrementalInputSplits iis = IncrementalInputSplits.builder()
        .conf(conf)
        .path(new Path(basePath))
        .rowType(TestConfigurations.ROW_TYPE)
        .build();

    HoodieInstant commit1 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "1");
    HoodieInstant commit2 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "2");
    HoodieInstant commit3 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "3");
    timeline.createNewInstant(commit1);
    timeline.createNewInstant(commit2);
    timeline.createNewInstant(commit3);
    timeline = timeline.reload();

    // previous read iteration read till instant time "1", next read iteration should return ["2", "3"]
    List<HoodieInstant> instantRange2 = iis.filterInstantsWithRange(timeline, "1");
    assertEquals(2, instantRange2.size());
    assertIterableEquals(Arrays.asList(commit2, commit3), instantRange2);

    // simulate first iteration cycle with read from LATEST commit
    List<HoodieInstant> instantRange1 = iis.filterInstantsWithRange(timeline, null);
    assertEquals(1, instantRange1.size());
    assertIterableEquals(Collections.singletonList(commit3), instantRange1);

    // specifying a start and end commit
    conf.set(FlinkOptions.READ_START_COMMIT, "1");
    conf.set(FlinkOptions.READ_END_COMMIT, "3");
    List<HoodieInstant> instantRange3 = iis.filterInstantsWithRange(timeline, null);
    assertEquals(3, instantRange3.size());
    assertIterableEquals(Arrays.asList(commit1, commit2, commit3), instantRange3);

    // add an inflight instant which should be excluded
    HoodieInstant commit4 = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "4");
    timeline.createNewInstant(commit4);
    timeline = timeline.reload();
    assertEquals(4, timeline.getInstants().size());
    List<HoodieInstant> instantRange4 = iis.filterInstantsWithRange(timeline, null);
    assertEquals(3, instantRange4.size());
  }

}
