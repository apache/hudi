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

package org.apache.hudi.sink.validator;

import org.apache.hudi.common.util.CheckpointUtils;
import org.apache.hudi.common.util.CheckpointUtils.CheckpointFormat;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Flink Kafka checkpoint parsing in {@link CheckpointUtils}.
 */
public class TestFlinkKafkaCheckpointParsing {

  @Test
  public void testParseSinglePartition() {
    String checkpoint = "kafka_metadata%3Aevents%3A0:1000";
    Map<Integer, Long> offsets = CheckpointUtils.parseCheckpoint(CheckpointFormat.FLINK_KAFKA, checkpoint);
    assertEquals(1, offsets.size());
    assertEquals(1000L, offsets.get(0));
  }

  @Test
  public void testParseMultiplePartitions() {
    String checkpoint = "kafka_metadata%3Aevents%3A0:100;kafka_metadata%3Aevents%3A1:200;kafka_metadata%3Aevents%3A2:300";
    Map<Integer, Long> offsets = CheckpointUtils.parseCheckpoint(CheckpointFormat.FLINK_KAFKA, checkpoint);
    assertEquals(3, offsets.size());
    assertEquals(100L, offsets.get(0));
    assertEquals(200L, offsets.get(1));
    assertEquals(300L, offsets.get(2));
  }

  @Test
  public void testParseWithClusterMetadata() {
    // Cluster metadata entry should be skipped (not a partition offset)
    String checkpoint = "kafka_metadata%3Aevents%3A0:100;kafka_metadata%3Aevents%3A1:200"
        + ";kafka_metadata%3Akafka_cluster%3Aevents%3A:my-cluster";
    Map<Integer, Long> offsets = CheckpointUtils.parseCheckpoint(CheckpointFormat.FLINK_KAFKA, checkpoint);
    assertEquals(2, offsets.size());
    assertEquals(100L, offsets.get(0));
    assertEquals(200L, offsets.get(1));
  }

  @Test
  public void testCalculateOffsetDifference() {
    String prev = "kafka_metadata%3Aevents%3A0:100;kafka_metadata%3Aevents%3A1:200";
    String curr = "kafka_metadata%3Aevents%3A0:300;kafka_metadata%3Aevents%3A1:500";
    // Diff = (300-100) + (500-200) = 200 + 300 = 500
    long diff = CheckpointUtils.calculateOffsetDifference(CheckpointFormat.FLINK_KAFKA, prev, curr);
    assertEquals(500L, diff);
  }

  @Test
  public void testCalculateOffsetDifferenceWithClusterMetadata() {
    String prev = "kafka_metadata%3Aevents%3A0:100;kafka_metadata%3Akafka_cluster%3Aevents%3A:cluster1";
    String curr = "kafka_metadata%3Aevents%3A0:200;kafka_metadata%3Akafka_cluster%3Aevents%3A:cluster1";
    long diff = CheckpointUtils.calculateOffsetDifference(CheckpointFormat.FLINK_KAFKA, prev, curr);
    assertEquals(100L, diff);
  }

  @Test
  public void testIsValidFormat() {
    assertTrue(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.FLINK_KAFKA, "kafka_metadata%3Aevents%3A0:100"));
    assertFalse(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.FLINK_KAFKA, ""));
    assertFalse(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.FLINK_KAFKA, null));
    // "invalid-format" is parsed without error (entries silently skipped), so it's "valid" format
    assertTrue(CheckpointUtils.isValidCheckpointFormat(
        CheckpointFormat.FLINK_KAFKA, "invalid-format"));
  }

  @Test
  public void testParseWithNoOffsets() {
    // Only cluster metadata, no partition offsets
    String checkpoint = "kafka_metadata%3Akafka_cluster%3Aevents%3A:my-cluster";
    Map<Integer, Long> offsets = CheckpointUtils.parseCheckpoint(CheckpointFormat.FLINK_KAFKA, checkpoint);
    assertEquals(0, offsets.size());
  }

  @Test
  public void testOffsetResetSkippedInDiff() {
    // Partition 0 offset goes backward (reset). Should be skipped, only partition 1 counted.
    String prev = "kafka_metadata%3Aevents%3A0:500;kafka_metadata%3Aevents%3A1:100";
    String curr = "kafka_metadata%3Aevents%3A0:200;kafka_metadata%3Aevents%3A1:300";
    // Partition 0: 200-500 = -300 (skipped). Partition 1: 300-100 = 200.
    long diff = CheckpointUtils.calculateOffsetDifference(CheckpointFormat.FLINK_KAFKA, prev, curr);
    assertEquals(200L, diff);
  }

  @Test
  public void testParseInvalidFormatReturnsEmpty() {
    // Missing %3A separators — entry is silently skipped (consistent with production parser)
    Map<Integer, Long> offsets = CheckpointUtils.parseCheckpoint(
        CheckpointFormat.FLINK_KAFKA, "invalid-no-separators");
    assertEquals(0, offsets.size());
  }

  @Test
  public void testParseNullThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> CheckpointUtils.parseCheckpoint(CheckpointFormat.FLINK_KAFKA, null));
  }

  @Test
  public void testParseEmptyStringThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> CheckpointUtils.parseCheckpoint(CheckpointFormat.FLINK_KAFKA, ""));
  }

  @Test
  public void testParseWithEmptySemicolonEntries() {
    // Extra semicolons should be handled gracefully
    String checkpoint = ";kafka_metadata%3Aevents%3A0:100;;kafka_metadata%3Aevents%3A1:200;";
    Map<Integer, Long> offsets = CheckpointUtils.parseCheckpoint(CheckpointFormat.FLINK_KAFKA, checkpoint);
    assertEquals(2, offsets.size());
    assertEquals(100L, offsets.get(0));
    assertEquals(200L, offsets.get(1));
  }

  @Test
  public void testZeroOffsetDifference() {
    String checkpoint = "kafka_metadata%3Aevents%3A0:100;kafka_metadata%3Aevents%3A1:200";
    long diff = CheckpointUtils.calculateOffsetDifference(CheckpointFormat.FLINK_KAFKA, checkpoint, checkpoint);
    assertEquals(0L, diff);
  }

  @Test
  public void testLargeOffsets() {
    // Test with large offset values to ensure no overflow
    String prev = "kafka_metadata%3Aevents%3A0:" + Long.MAX_VALUE / 2;
    String curr = "kafka_metadata%3Aevents%3A0:" + (Long.MAX_VALUE / 2 + 1000);
    long diff = CheckpointUtils.calculateOffsetDifference(CheckpointFormat.FLINK_KAFKA, prev, curr);
    assertEquals(1000L, diff);
  }

  @Test
  public void testNewPartitionSkippedInDiff() {
    // Previous has partition 0, current has 0 and 1 (new partition)
    String prev = "kafka_metadata%3Aevents%3A0:100";
    String curr = "kafka_metadata%3Aevents%3A0:200;kafka_metadata%3Aevents%3A1:50";
    // Only partition 0 diff counted: 200-100 = 100. Partition 1 skipped (new).
    long diff = CheckpointUtils.calculateOffsetDifference(CheckpointFormat.FLINK_KAFKA, prev, curr);
    assertEquals(100L, diff);
  }
}
