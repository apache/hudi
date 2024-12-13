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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    StreamerCheckpointV2 translatedCheckpoint = CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient);

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
        () -> CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient));
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
        () -> CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient));
    assertTrue(exception.getMessage().contains("Unable to find completion time"));
  }

  @Test
  public void testConvertCheckpointWithInitTimestamp() {
    String instantTime = HoodieTimeline.INIT_INSTANT_TS;

    Checkpoint checkpoint = new StreamerCheckpointV1(instantTime);
    Checkpoint translated = CheckpointUtils.convertToCheckpointV1ForCommitTime(checkpoint, metaClient);
    assertEquals(HoodieTimeline.INIT_INSTANT_TS, translated.getCheckpointKey());

    checkpoint = new StreamerCheckpointV2(instantTime);
    translated = CheckpointUtils.convertToCheckpointV2ForCommitTime(checkpoint, metaClient);
    assertEquals(HoodieTimeline.INIT_INSTANT_TS, translated.getCheckpointKey());
  }
}
