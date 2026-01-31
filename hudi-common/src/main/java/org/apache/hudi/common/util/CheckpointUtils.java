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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for parsing and working with streaming checkpoints.
 * Supports multiple checkpoint formats used by different engines and sources.
 *
 * Checkpoint formats:
 * - DELTASTREAMER_KAFKA: "topic,partition:offset,partition:offset,..."
 *   Example: "events,0:1000,1:2000,2:1500"
 *   Used by: DeltaStreamer (Spark)
 *
 * - FLINK_KAFKA: Base64-encoded serialized Map (TopicPartition → Long)
 *   Example: "eyJ0b3BpY..." (base64)
 *   Used by: Flink streaming connector
 *   Note: Actual implementation requires Flink checkpoint deserialization (Phase 2)
 *
 * - PULSAR: "partition:ledgerId:entryId,partition:ledgerId:entryId,..."
 *   Example: "0:123:45,1:234:56"
 *   Used by: Pulsar sources
 *   Note: To be implemented in Phase 4
 *
 * - KINESIS: "shardId:sequenceNumber,shardId:sequenceNumber,..."
 *   Example: "shardId-000000000000:49590338271490256608559692538361571095921575989136588898"
 *   Used by: Kinesis sources
 *   Note: To be implemented in Phase 4
 */
public class CheckpointUtils {

  /**
   * Supported checkpoint formats across engines and sources.
   */
  public enum CheckpointFormat {
    /** DeltaStreamer (Spark) Kafka format: "topic,0:1000,1:2000" */
    DELTASTREAMER_KAFKA,

    /** Flink Kafka format: base64-encoded Map&lt;TopicPartition, Long&gt; */
    FLINK_KAFKA,

    /** Pulsar format: "0:123:45,1:234:56" (ledgerId:entryId) */
    PULSAR,

    /** Kinesis format: "shard-0:12345,shard-1:67890" */
    KINESIS,

    /** Custom user-defined format */
    CUSTOM
  }

  /**
   * Parse checkpoint string into partition → offset mapping.
   *
   * @param format Checkpoint format
   * @param checkpointStr Checkpoint string
   * @return Map from partition number to offset/sequence number
   * @throws IllegalArgumentException if format is invalid
   */
  public static Map<Integer, Long> parseCheckpoint(CheckpointFormat format, String checkpointStr) {
    switch (format) {
      case DELTASTREAMER_KAFKA:
        return parseDeltaStreamerKafkaCheckpoint(checkpointStr);
      case FLINK_KAFKA:
        throw new UnsupportedOperationException(
            "Flink Kafka checkpoint parsing not yet implemented. "
                + "This will be added in Phase 2 with Flink checkpoint deserialization support.");
      case PULSAR:
        throw new UnsupportedOperationException(
            "Pulsar checkpoint parsing not yet implemented. Planned for Phase 4.");
      case KINESIS:
        throw new UnsupportedOperationException(
            "Kinesis checkpoint parsing not yet implemented. Planned for Phase 4.");
      default:
        throw new IllegalArgumentException("Unsupported checkpoint format: " + format);
    }
  }

  /**
   * Calculate offset difference between two checkpoints.
   * Handles partition additions, removals, and resets.
   *
   * Algorithm:
   * 1. For each partition in current checkpoint:
   *    - If partition exists in previous: diff = current - previous
   *    - If partition is new: diff = current (count from 0)
   *    - If diff is negative (reset): use current offset
   * 2. Sum all partition diffs
   *
   * @param format Checkpoint format
   * @param previousCheckpoint Previous checkpoint string
   * @param currentCheckpoint Current checkpoint string
   * @return Total offset difference across all partitions
   */
  public static long calculateOffsetDifference(CheckpointFormat format,
                                                String previousCheckpoint,
                                                String currentCheckpoint) {
    Map<Integer, Long> previousOffsets = parseCheckpoint(format, previousCheckpoint);
    Map<Integer, Long> currentOffsets = parseCheckpoint(format, currentCheckpoint);

    long totalDiff = 0;

    for (Map.Entry<Integer, Long> entry : currentOffsets.entrySet()) {
      int partition = entry.getKey();
      long currentOffset = entry.getValue();
      Long previousOffset = previousOffsets.get(partition);

      if (previousOffset != null) {
        // Partition exists in both checkpoints
        long diff = currentOffset - previousOffset;

        // Handle offset reset (negative diff) - topic/partition recreated
        if (diff < 0) {
          // Use current offset as diff (count from 0 to current)
          totalDiff += currentOffset;
        } else {
          totalDiff += diff;
        }
      } else {
        // New partition - count from 0 to current
        totalDiff += currentOffset;
      }
    }

    return totalDiff;
  }

  /**
   * Validate checkpoint format.
   *
   * @param format Expected checkpoint format
   * @param checkpointStr Checkpoint string to validate
   * @return true if valid format
   */
  public static boolean isValidCheckpointFormat(CheckpointFormat format, String checkpointStr) {
    if (checkpointStr == null || checkpointStr.trim().isEmpty()) {
      return false;
    }

    try {
      parseCheckpoint(format, checkpointStr);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Extract topic name from DeltaStreamer Kafka checkpoint.
   * Format: "topic,partition:offset,..."
   *
   * @param checkpointStr DeltaStreamer Kafka checkpoint
   * @return Topic name
   * @throws IllegalArgumentException if invalid format
   */
  public static String extractTopicName(String checkpointStr) {
    if (checkpointStr == null || checkpointStr.trim().isEmpty()) {
      throw new IllegalArgumentException("Checkpoint string cannot be null or empty");
    }

    String[] splits = checkpointStr.split(",");
    if (splits.length < 1) {
      throw new IllegalArgumentException(
          "Invalid checkpoint format. Expected: topic,partition:offset,... Got: " + checkpointStr);
    }

    return splits[0];
  }

  // ========== Format-Specific Parsers ==========

  /**
   * Parse DeltaStreamer (Spark) Kafka checkpoint.
   * Format: "topic,partition:offset,partition:offset,..."
   * Example: "events,0:1000,1:2000,2:1500"
   *
   * @param checkpointStr Checkpoint string
   * @return Map of partition → offset
   * @throws IllegalArgumentException if format is invalid
   */
  private static Map<Integer, Long> parseDeltaStreamerKafkaCheckpoint(String checkpointStr) {
    if (checkpointStr == null || checkpointStr.trim().isEmpty()) {
      throw new IllegalArgumentException("Checkpoint string cannot be null or empty");
    }

    Map<Integer, Long> offsetMap = new HashMap<>();
    String[] splits = checkpointStr.split(",");

    if (splits.length < 2) {
      throw new IllegalArgumentException(
          "Invalid DeltaStreamer Kafka checkpoint. Expected: topic,partition:offset,... Got: " + checkpointStr);
    }

    // First element is topic name, skip it
    for (int i = 1; i < splits.length; i++) {
      String[] partitionOffset = splits[i].split(":");
      if (partitionOffset.length != 2) {
        throw new IllegalArgumentException(
            "Invalid partition:offset format in checkpoint: " + splits[i]);
      }

      try {
        int partition = Integer.parseInt(partitionOffset[0]);
        long offset = Long.parseLong(partitionOffset[1]);
        offsetMap.put(partition, offset);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid number format in checkpoint: " + splits[i], e);
      }
    }

    return offsetMap;
  }
}
