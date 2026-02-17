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

package org.apache.hudi.common.util;

import org.apache.hudi.common.util.CheckpointUtils.CheckpointFormat;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for CheckpointUtils - Phase 1 core functionality.
 */
public class TestCheckpointUtils {

  @Test
  public void testParseDeltaStreamerKafkaCheckpoint() {
    String checkpoint = "test_topic,0:100,1:200,2:150";
    Map<Integer, Long> offsets = CheckpointUtils.parseCheckpoint(
        CheckpointFormat.DELTASTREAMER_KAFKA, checkpoint);

    assertEquals(3, offsets.size());
    assertEquals(100L, offsets.get(0));
    assertEquals(200L, offsets.get(1));
    assertEquals(150L, offsets.get(2));
  }

  @Test
  public void testParseSinglePartition() {
    String checkpoint = "my_topic,0:1000";
    Map<Integer, Long> offsets = CheckpointUtils.parseCheckpoint(
        CheckpointFormat.DELTASTREAMER_KAFKA, checkpoint);

    assertEquals(1, offsets.size());
    assertEquals(1000L, offsets.get(0));
  }

  @Test
  public void testParseInvalidFormat() {
    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.DELTASTREAMER_KAFKA, "invalid"));

    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.DELTASTREAMER_KAFKA, "topic"));

    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.DELTASTREAMER_KAFKA, ""));

    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.DELTASTREAMER_KAFKA, null));
  }

  @Test
  public void testParseInvalidPartitionOffset() {
    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.DELTASTREAMER_KAFKA, "topic,0:abc"));

    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.DELTASTREAMER_KAFKA, "topic,abc:100"));

    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.DELTASTREAMER_KAFKA, "topic,0-100"));
  }

  @Test
  public void testExtractTopicName() {
    assertEquals("test_topic",
        CheckpointUtils.extractTopicName("test_topic,0:100,1:200"));
    assertEquals("my.topic.name",
        CheckpointUtils.extractTopicName("my.topic.name,0:1000"));
  }

  @Test
  public void testExtractTopicNameInvalid() {
    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.extractTopicName(""));

    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.extractTopicName(null));

    // Topic-only without partition data should be invalid
    assertThrows(IllegalArgumentException.class, () ->
        CheckpointUtils.extractTopicName("just_topic"));
  }

  @Test
  public void testCalculateOffsetDifferenceBasic() {
    String previous = "topic,0:100,1:200";
    String current = "topic,0:150,1:300";

    // (150-100) + (300-200) = 50 + 100 = 150
    long diff = CheckpointUtils.calculateOffsetDifference(
        CheckpointFormat.DELTASTREAMER_KAFKA, previous, current);
    assertEquals(150L, diff);
  }

  @Test
  public void testCalculateOffsetDifferenceNoChange() {
    String previous = "topic,0:100,1:200";
    String current = "topic,0:100,1:200";

    long diff = CheckpointUtils.calculateOffsetDifference(
        CheckpointFormat.DELTASTREAMER_KAFKA, previous, current);
    assertEquals(0L, diff);
  }

  @Test
  public void testCalculateOffsetDifferenceNewPartition() {
    String previous = "topic,0:100";
    String current = "topic,0:150,1:200";

    // Partition 0: 150-100 = 50
    // Partition 1: new partition, count from 0 to 200 = 200
    // Total: 50 + 200 = 250
    long diff = CheckpointUtils.calculateOffsetDifference(
        CheckpointFormat.DELTASTREAMER_KAFKA, previous, current);
    assertEquals(250L, diff);
  }

  @Test
  public void testCalculateOffsetDifferenceRemovedPartition() {
    String previous = "topic,0:100,1:200";
    String current = "topic,0:150";

    // Only partition 0 exists in both: 150-100 = 50
    // Partition 1 ignored (not in current)
    long diff = CheckpointUtils.calculateOffsetDifference(
        CheckpointFormat.DELTASTREAMER_KAFKA, previous, current);
    assertEquals(50L, diff);
  }

  @Test
  public void testCalculateOffsetDifferenceNegativeOffset() {
    // Simulate topic reset where offset goes back to 0
    String previous = "topic,0:1000";
    String current = "topic,0:100";

    // When current < previous, we use current offset as the diff
    // This handles the case where topic was reset
    long diff = CheckpointUtils.calculateOffsetDifference(
        CheckpointFormat.DELTASTREAMER_KAFKA, previous, current);
    assertEquals(100L, diff);
  }

  @Test
  public void testCalculateOffsetDifferenceMultiplePartitionsWithReset() {
    String previous = "topic,0:1000,1:2000";
    String current = "topic,0:100,1:2500";

    // Partition 0: reset, use current offset = 100
    // Partition 1: normal increment = 2500-2000 = 500
    // Total: 100 + 500 = 600
    long diff = CheckpointUtils.calculateOffsetDifference(
        CheckpointFormat.DELTASTREAMER_KAFKA, previous, current);
    assertEquals(600L, diff);
  }

  @Test
  public void testIsValidCheckpointFormat() {
    assertTrue(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.DELTASTREAMER_KAFKA, "topic,0:100"));
    assertTrue(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.DELTASTREAMER_KAFKA, "topic,0:100,1:200,2:300"));
    assertTrue(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.DELTASTREAMER_KAFKA, "my.topic.name,0:1000"));

    assertFalse(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.DELTASTREAMER_KAFKA, ""));
    assertFalse(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.DELTASTREAMER_KAFKA, null));
    assertFalse(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.DELTASTREAMER_KAFKA, "just_topic"));
    assertFalse(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.DELTASTREAMER_KAFKA, "topic,invalid"));
    assertFalse(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.DELTASTREAMER_KAFKA, "topic,0:abc"));
  }

  @Test
  public void testUnsupportedFormats() {
    // Flink format not yet implemented (Phase 2)
    assertThrows(UnsupportedOperationException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.FLINK_KAFKA, "anystring"));

    // Pulsar format not yet implemented (Phase 4)
    assertThrows(UnsupportedOperationException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.PULSAR, "anystring"));

    // Kinesis format not yet implemented (Phase 4)
    assertThrows(UnsupportedOperationException.class, () ->
        CheckpointUtils.parseCheckpoint(CheckpointFormat.KINESIS, "anystring"));
  }
}
