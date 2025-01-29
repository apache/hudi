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

package org.apache.hudi.common.table.checkpoint;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.BLOCK;
import static org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.FAIL;
import static org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.USE_TRANSITION_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CheckpointUtils}.
 */
public class TestCheckpointUtils {

  private HoodieTableMetaClient metaClient;
  private HoodieActiveTimeline activeTimeline;

  private static final String CHECKPOINT_TO_RESUME = "20240101000000";
  private static final String GENERAL_SOURCE = "org.apache.hudi.utilities.sources.GeneralSource";

  @BeforeEach
  public void setUp() {
    metaClient = mock(HoodieTableMetaClient.class);
    activeTimeline = mock(HoodieActiveTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
  }

  @Test
  public void testGetCheckpointWithV1Metadata() {
    HoodieCommitMetadata commitMetadata = mock(HoodieCommitMetadata.class);
    when(commitMetadata.getMetadata("deltastreamer.checkpoint.key")).thenReturn("v1_key");

    Checkpoint checkpoint = CheckpointUtils.getCheckpoint(commitMetadata);
    assertTrue(checkpoint instanceof StreamerCheckpointV1);
    assertEquals("v1_key", checkpoint.getCheckpointKey());
  }

  @Test
  public void testGetCheckpointWithV2Metadata() {
    HoodieCommitMetadata commitMetadata = mock(HoodieCommitMetadata.class);
    when(commitMetadata.getMetadata("streamer.checkpoint.key.v2")).thenReturn("v2_key");

    Checkpoint checkpoint = CheckpointUtils.getCheckpoint(commitMetadata);
    assertTrue(checkpoint instanceof StreamerCheckpointV2);
    assertEquals("v2_key", checkpoint.getCheckpointKey());
  }

  @Test
  public void testGetCheckpointThrowsExceptionForMissingCheckpoint() {
    HoodieCommitMetadata commitMetadata = mock(HoodieCommitMetadata.class);
    when(commitMetadata.getMetadata(anyString())).thenReturn(null);

    Exception exception = assertThrows(HoodieException.class,
        () -> CheckpointUtils.getCheckpoint(commitMetadata));
    assertTrue(exception.getMessage().contains("Checkpoint is not found"));
  }

  @Test
  public void testConvertToCheckpointV2ForCommitTime() {
    String instantTime = "20231127010101";
    String completionTime = "20231127020102";

    // Mock active timeline
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", instantTime, completionTime, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    when(activeTimeline.getInstantsAsStream()).thenReturn(Stream.of(instant));

    Checkpoint checkpoint = new StreamerCheckpointV1(instantTime);
    StreamerCheckpointV2 translatedCheckpoint = CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient, FAIL);

    assertEquals(completionTime, translatedCheckpoint.getCheckpointKey());
  }

  @Test
  public void testConvertToCheckpointV1ForCommitTime() {
    String completionTime = "20231127020102";
    String instantTime = "20231127010101";

    // Mock active timeline
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", instantTime, completionTime, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);
    when(activeTimeline.getInstantsAsStream()).thenReturn(Stream.of(instant));

    Checkpoint checkpoint = new StreamerCheckpointV2(completionTime);
    StreamerCheckpointV1 translatedCheckpoint = CheckpointUtils.convertToCheckpointV1ForCommitTime(checkpoint, metaClient);

    assertEquals(instantTime, translatedCheckpoint.getCheckpointKey());
  }

  @Test
  public void testConvertToCheckpointV2ThrowsExceptionForMissingCompletionTime() {
    String instantTime = "20231127010101";

    // Mock active timeline without completion time
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "deltacommit", instantTime, null, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    when(activeTimeline.getInstantsAsStream()).thenReturn(Stream.of(instant));

    Checkpoint checkpoint = new StreamerCheckpointV1(instantTime);

    Exception exception = assertThrows(UnsupportedOperationException.class,
        () -> CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient, BLOCK));
    assertTrue(exception.getMessage().contains("Unable to find completion time"));
  }

  @Test
  public void testConvertToCheckpointV1ThrowsExceptionForMissingRequestedTime() {
    String completionTime = "20231127020101";

    // Mock active timeline without requested time
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", null, completionTime, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);
    when(activeTimeline.getInstantsAsStream()).thenReturn(Stream.of(instant));

    Checkpoint checkpoint = new StreamerCheckpointV2(completionTime);

    Exception exception = assertThrows(UnsupportedOperationException.class,
        () -> CheckpointUtils.convertToCheckpointV1ForCommitTime(checkpoint, metaClient));
    assertTrue(exception.getMessage().contains("Unable to find requested time"));
  }

  @Test
  public void testConvertToCheckpointV2ForCommitTimeEmptyTimeline() {
    String instantTime = "20231127010101";
    String completionTime = "20231127020102";

    // Mock active timeline
    when(activeTimeline.getInstantsAsStream()).thenReturn(Stream.empty());

    Checkpoint checkpoint = new StreamerCheckpointV1(instantTime);
    Exception exception = assertThrows(UnsupportedOperationException.class,
        () -> CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient, FAIL));
    assertTrue(exception.getMessage().contains("Unable to find completion time"));
  }

  @Test
  public void testConvertCheckpointWithInitTimestamp() {
    String instantTime = HoodieTimeline.INIT_INSTANT_TS;

    Checkpoint checkpoint = new StreamerCheckpointV1(instantTime);
    Checkpoint translated = CheckpointUtils.convertToCheckpointV1ForCommitTime(checkpoint, metaClient);
    assertEquals(HoodieTimeline.INIT_INSTANT_TS, translated.getCheckpointKey());

    checkpoint = new StreamerCheckpointV2(instantTime);
    translated = CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient, BLOCK);
    assertEquals(HoodieTimeline.INIT_INSTANT_TS, translated.getCheckpointKey());
  }

  @Test
  public void testConvertCheckpointWithUseTransitionTime() {
    String instantTime = "20231127010101";
    String completionTime = "20231127020102";

    // Mock active timeline
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", instantTime, completionTime, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    when(activeTimeline.getInstantsAsStream()).thenReturn(Stream.of(instant));

    Checkpoint checkpoint = new StreamerCheckpointV1(completionTime);
    StreamerCheckpointV2 translatedCheckpoint = CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient, USE_TRANSITION_TIME);

    assertEquals(completionTime, translatedCheckpoint.getCheckpointKey());
  }

  @ParameterizedTest
  @CsvSource({
      // version, sourceClassName, expectedResult
      // Version >= 8 with allowed sources should return true
      "8, org.apache.hudi.utilities.sources.TestSource, true",
      "9, org.apache.hudi.utilities.sources.AnotherSource, true",
      // Version < 8 should return false regardless of source
      "7, org.apache.hudi.utilities.sources.TestSource, false",
      "6, org.apache.hudi.utilities.sources.AnotherSource, false",
      // Disallowed sources should return false even with version >= 8
      "8, org.apache.hudi.utilities.sources.S3EventsHoodieIncrSource, false",
      "8, org.apache.hudi.utilities.sources.GcsEventsHoodieIncrSource, false",
      "8, org.apache.hudi.utilities.sources.MockS3EventsHoodieIncrSource, false",
      "8, org.apache.hudi.utilities.sources.MockGcsEventsHoodieIncrSource, false"
  })
  public void testTargetCheckpointV2(int version, String sourceClassName, boolean isV2Checkpoint) {
    assertEquals(isV2Checkpoint, CheckpointUtils.buildCheckpointFromGeneralSource(sourceClassName, version, "ignored") instanceof StreamerCheckpointV2);
  }

  @Test
  public void testBuildCheckpointFromGeneralSource() {
    // Test V2 checkpoint creation (newer table version + general source)
    Checkpoint checkpoint1 = CheckpointUtils.buildCheckpointFromGeneralSource(
        GENERAL_SOURCE,
        HoodieTableVersion.EIGHT.versionCode(),
        CHECKPOINT_TO_RESUME
    );
    assertInstanceOf(StreamerCheckpointV2.class, checkpoint1);
    assertEquals(CHECKPOINT_TO_RESUME, checkpoint1.getCheckpointKey());

    // Test V1 checkpoint creation (older table version)
    Checkpoint checkpoint2 = CheckpointUtils.buildCheckpointFromGeneralSource(
        GENERAL_SOURCE,
        HoodieTableVersion.SEVEN.versionCode(),
        CHECKPOINT_TO_RESUME
    );
    assertInstanceOf(StreamerCheckpointV1.class, checkpoint2);
    assertEquals(CHECKPOINT_TO_RESUME, checkpoint2.getCheckpointKey());
  }

  @Test
  public void testBuildCheckpointFromConfigOverride() {
    // Test checkpoint from config creation (newer table version + general source)
    Checkpoint checkpoint1 = CheckpointUtils.buildCheckpointFromConfigOverride(
        GENERAL_SOURCE,
        HoodieTableVersion.EIGHT.versionCode(),
        CHECKPOINT_TO_RESUME
    );
    assertInstanceOf(UnresolvedStreamerCheckpointBasedOnCfg.class, checkpoint1);
    assertEquals(CHECKPOINT_TO_RESUME, checkpoint1.getCheckpointKey());

    // Test V1 checkpoint creation (older table version)
    Checkpoint checkpoint2 = CheckpointUtils.buildCheckpointFromConfigOverride(
        GENERAL_SOURCE,
        HoodieTableVersion.SEVEN.versionCode(),
        CHECKPOINT_TO_RESUME
    );
    assertInstanceOf(StreamerCheckpointV1.class, checkpoint2);
    assertEquals(CHECKPOINT_TO_RESUME, checkpoint2.getCheckpointKey());
  }
}
