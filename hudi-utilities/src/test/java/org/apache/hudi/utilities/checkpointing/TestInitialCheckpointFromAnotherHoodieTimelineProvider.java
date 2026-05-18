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

package org.apache.hudi.utilities.checkpointing;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link InitialCheckpointFromAnotherHoodieTimelineProvider}.
 */
class TestInitialCheckpointFromAnotherHoodieTimelineProvider extends HoodieCommonTestHarness {

  @BeforeEach
  void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanMetaClient();
  }

  private InitialCheckpointFromAnotherHoodieTimelineProvider createProvider() {
    TypedProperties props = new TypedProperties();
    props.put("hoodie.streamer.checkpoint.provider.path", basePath);
    InitialCheckpointFromAnotherHoodieTimelineProvider provider =
        new InitialCheckpointFromAnotherHoodieTimelineProvider(props);
    provider.init(HoodieTestUtils.getDefaultStorageConf().unwrap());
    return provider;
  }

  @Test
  void testGetCheckpointFromCommitWithValidCheckpoint() throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant = new HoodieInstant(INFLIGHT, COMMIT_ACTION, "1",
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant);
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "topic:100");
    activeTimeline.saveAsComplete(instant, getCommitMetadata(basePath, "p1", "1", 2, extraMetadata));

    assertEquals("topic:100", createProvider().getCheckpoint());
  }

  @Test
  void testGetCheckpointSkipsCommitsWithoutCheckpoint() throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant1 = new HoodieInstant(INFLIGHT, COMMIT_ACTION, "1",
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant1);
    Map<String, String> metadata1 = new HashMap<>();
    metadata1.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "topic:50");
    activeTimeline.saveAsComplete(instant1, getCommitMetadata(basePath, "p1", "1", 2, metadata1));

    HoodieInstant instant2 = new HoodieInstant(INFLIGHT, COMMIT_ACTION, "2",
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant2);
    activeTimeline.saveAsComplete(instant2, getCommitMetadata(basePath, "p1", "2", 2, Collections.emptyMap()));

    assertEquals("topic:50", createProvider().getCheckpoint());
  }

  @Test
  void testGetCheckpointSkipsEmptyCheckpointStrings() throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant1 = new HoodieInstant(INFLIGHT, COMMIT_ACTION, "1",
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant1);
    Map<String, String> metadata1 = new HashMap<>();
    metadata1.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "topic:50");
    activeTimeline.saveAsComplete(instant1, getCommitMetadata(basePath, "p1", "1", 2, metadata1));

    // Newer commit has a reset key but empty checkpoint key — getCheckpointKey() returns ""
    HoodieInstant instant2 = new HoodieInstant(INFLIGHT, COMMIT_ACTION, "2",
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant2);
    Map<String, String> metadata2 = new HashMap<>();
    metadata2.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "");
    metadata2.put(StreamerCheckpointV1.STREAMER_CHECKPOINT_RESET_KEY_V1, "reset-value");
    activeTimeline.saveAsComplete(instant2, getCommitMetadata(basePath, "p1", "2", 2, metadata2));

    assertEquals("topic:50", createProvider().getCheckpoint());
  }

  @Test
  void testGetCheckpointThrowsWhenNoCheckpointExists() throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant = new HoodieInstant(INFLIGHT, COMMIT_ACTION, "1",
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, getCommitMetadata(basePath, "p1", "1", 2, Collections.emptyMap()));

    assertThrows(HoodieException.class, () -> createProvider().getCheckpoint());
  }
}
