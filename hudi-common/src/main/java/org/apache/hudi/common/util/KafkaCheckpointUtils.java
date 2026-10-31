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

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utility methods for parsing and working with streaming checkpoints.
 * Supports multiple checkpoint formats used by different engines and sources.
 *
 * Checkpoint formats:
 * - SPARK_KAFKA: "topic,partition:offset,partition:offset,..."
 *   Example: "events,0:1000,1:2000,2:1500"
 *   Used by: HoodieStreamer (Spark)
 *   Note: Both offset-based and timestamp-based Kafka sources produce this same string
 *   format (see {@code KafkaOffsetGen}). The offsets may originate from either
 *   {@code consumer.endOffsets()} or {@code consumer.offsetsForTimes()}, but the
 *   checkpoint string format is identical.
 *
 * - FLINK_KAFKA: URL-encoded format with topic and partition offsets
 *   Example: "kafka_metadata%3Aevents%3A0:100;kafka_metadata%3Aevents%3A1:200"
 *   Format: "kafka_metadata%3A{topic}%3A{partition}:{offset}" separated by ";"
 *   Used by: Flink streaming connector (stored in HoodieMetadataKey extraMetadata)
 *   Note: Cluster metadata entries (kafka_metadata%3Akafka_cluster%3A...) are skipped
 *
 * - PULSAR: "partition:ledgerId:entryId,partition:ledgerId:entryId,..."
 *   Example: "0:123:45,1:234:56"
 *   Used by: Pulsar sources (engine-agnostic)
 *   Note: To be implemented in Phase 4
 *
 * - KINESIS: "shardId:sequenceNumber,shardId:sequenceNumber,..."
 *   Example: "shardId-000000000000:49590338271490256608559692538361571095921575989136588898"
 *   Used by: Kinesis sources (engine-agnostic)
 *   Note: To be implemented in Phase 4
 */
@Slf4j
public class KafkaCheckpointUtils {

  /**
   * Result of comparing offsets from two checkpoints.
   *
   * <p>The known offset difference only includes partitions whose offsets can be compared
   * exactly. A comparison is incomplete when partitions were added or removed, or when an
   * offset reset is detected, since the checkpoints do not contain the start offset used for
   * those partitions.</p>
   */
  public static final class OffsetComparisonResult {
    private final long knownOffsetDifference;
    private final Set<Integer> addedPartitions;
    private final Set<Integer> resetPartitions;
    private final Set<Integer> missingPartitions;

    private OffsetComparisonResult(long knownOffsetDifference,
                                   Set<Integer> addedPartitions,
                                   Set<Integer> resetPartitions,
                                   Set<Integer> missingPartitions) {
      this.knownOffsetDifference = knownOffsetDifference;
      this.addedPartitions = Collections.unmodifiableSet(new TreeSet<>(addedPartitions));
      this.resetPartitions = Collections.unmodifiableSet(new TreeSet<>(resetPartitions));
      this.missingPartitions = Collections.unmodifiableSet(new TreeSet<>(missingPartitions));
    }

    public long getKnownOffsetDifference() {
      return knownOffsetDifference;
    }

    public Set<Integer> getAddedPartitions() {
      return addedPartitions;
    }

    public Set<Integer> getResetPartitions() {
      return resetPartitions;
    }

    public Set<Integer> getMissingPartitions() {
      return missingPartitions;
    }

    /**
     * Returns whether the offset difference covers all partitions in both checkpoints.
     */
    public boolean isExact() {
      return addedPartitions.isEmpty() && resetPartitions.isEmpty() && missingPartitions.isEmpty();
    }
  }

  /**
   * Supported checkpoint formats across engines and sources.
   *
   * <p>Engine-specific formats (Kafka) are prefixed with the engine name because different
   * engines use different checkpoint serialization for the same source. Source-specific formats
   * (Pulsar, Kinesis) are not engine-prefixed because they use the same format across engines.</p>
   */
  public enum CheckpointFormat {
    /** HoodieStreamer (Spark) Kafka format: "topic,0:1000,1:2000" */
    SPARK_KAFKA,

    /**
     * Flink Kafka format: URL-encoded "kafka_metadata%3Atopic%3Apartition:offset" separated by ";".
     * Cluster metadata entries (containing "kafka_cluster") are skipped during parsing.
     */
    FLINK_KAFKA,

    /** Pulsar format: "0:123:45,1:234:56" (ledgerId:entryId). Engine-agnostic. */
    PULSAR,

    /** Kinesis format: "shard-0:12345,shard-1:67890". Engine-agnostic. */
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
      case SPARK_KAFKA:
        return parseSparkKafkaCheckpoint(checkpointStr);
      case FLINK_KAFKA:
        return parseFlinkKafkaCheckpoint(checkpointStr);
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
   * Calculate the known offset difference between two checkpoints.
   *
   * Algorithm:
   * 1. For each partition in current checkpoint:
   *    - If partition exists in previous: diff = current - previous
   *    - If partition is new: skip (start offset unknown, would overcount)
   *    - If diff is negative (reset): skip (start offset unknown, would overcount)
   * 2. Sum all comparable partition diffs
   *
   * <p>If partitions are added or removed, or an offset reset is detected, the returned value
   * is only a partial difference. Callers that require an exact difference should use
   * {@link #compareOffsets(CheckpointFormat, String, String)} and check
   * {@link OffsetComparisonResult#isExact()}.</p>
   *
   * @param format Checkpoint format
   * @param previousCheckpoint Previous checkpoint string
   * @param currentCheckpoint Current checkpoint string
   * @return Known offset difference across comparable partitions
   */
  public static long calculateOffsetDifference(CheckpointFormat format,
                                                String previousCheckpoint,
                                                String currentCheckpoint) {
    OffsetComparisonResult result = compareOffsets(format, previousCheckpoint, currentCheckpoint);
    if (!result.isExact()) {
      log.warn("Offset difference is incomplete. Known offset difference: {}, added partitions: {}, "
              + "reset partitions: {}, missing partitions: {}.",
          result.getKnownOffsetDifference(), result.getAddedPartitions(),
          result.getResetPartitions(), result.getMissingPartitions());
    }
    return result.getKnownOffsetDifference();
  }

  /**
   * Compare two checkpoints while preserving whether the resulting offset difference is exact.
   *
   * <p>Offsets for partitions present in both checkpoints are included when they move forward or
   * remain unchanged. Partitions with an unknown start offset are reported separately and excluded
   * from the known difference.</p>
   *
   * @param format Checkpoint format
   * @param previousCheckpoint Previous checkpoint string
   * @param currentCheckpoint Current checkpoint string
   * @return Offset comparison result containing the known difference and incomparable partitions
   */
  public static OffsetComparisonResult compareOffsets(CheckpointFormat format,
                                                       String previousCheckpoint,
                                                       String currentCheckpoint) {
    Map<Integer, Long> previousOffsets = parseCheckpoint(format, previousCheckpoint);
    Map<Integer, Long> currentOffsets = parseCheckpoint(format, currentCheckpoint);

    long totalDiff = 0;
    Set<Integer> addedPartitions = new TreeSet<>();
    Set<Integer> resetPartitions = new TreeSet<>();
    Set<Integer> missingPartitions = new TreeSet<>(previousOffsets.keySet());
    missingPartitions.removeAll(currentOffsets.keySet());

    for (Map.Entry<Integer, Long> entry : currentOffsets.entrySet()) {
      int partition = entry.getKey();
      long currentOffset = entry.getValue();
      Long previousOffset = previousOffsets.get(partition);

      if (previousOffset != null) {
        // Partition exists in both checkpoints
        long diff = currentOffset - previousOffset;

        if (diff < 0) {
          // The checkpoint does not record the start offset used after a reset, so this
          // partition cannot contribute to an exact offset difference.
          resetPartitions.add(partition);
        } else {
          totalDiff += diff;
        }
      } else {
        // A new partition's start offset is not available in the previous checkpoint.
        addedPartitions.add(partition);
      }
    }

    return new OffsetComparisonResult(totalDiff, addedPartitions, resetPartitions, missingPartitions);
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
   * Extract topic name from Spark Kafka checkpoint.
   * Format: "topic,partition:offset,..."
   *
   * @param checkpointStr Spark Kafka checkpoint
   * @return Topic name
   * @throws IllegalArgumentException if invalid format
   */
  static String extractTopicName(String checkpointStr) {
    if (checkpointStr == null || checkpointStr.trim().isEmpty()) {
      throw new IllegalArgumentException("Checkpoint string cannot be null or empty");
    }

    String[] splits = checkpointStr.split(",");
    if (splits.length < 2) {
      throw new IllegalArgumentException(
          "Invalid checkpoint format. Expected: topic,partition:offset,... Got: " + checkpointStr);
    }

    return splits[0];
  }

  // ========== Format-Specific Parsers ==========

  /**
   * Parse HoodieStreamer (Spark) Kafka checkpoint.
   * Format: "topic,partition:offset,partition:offset,..."
   * Example: "events,0:1000,1:2000,2:1500"
   *
   * <p>Note: {@code KafkaOffsetGen.CheckpointUtils#strToOffsets} in hudi-utilities
   * parses the same format but returns {@code Map<TopicPartition, Long>} (Kafka-specific).
   * This method returns {@code Map<Integer, Long>} to avoid Kafka client dependencies
   * in hudi-common. TODO: consolidate with KafkaOffsetGen.CheckpointUtils once
   * https://github.com/apache/hudi/pull/18125 lands.</p>
   *
   * @param checkpointStr Checkpoint string
   * @return Map of partition → offset
   * @throws IllegalArgumentException if format is invalid
   */
  private static Map<Integer, Long> parseSparkKafkaCheckpoint(String checkpointStr) {
    if (checkpointStr == null || checkpointStr.trim().isEmpty()) {
      throw new IllegalArgumentException("Checkpoint string cannot be null or empty");
    }

    Map<Integer, Long> offsetMap = new HashMap<>();
    String[] splits = checkpointStr.split(",");

    if (splits.length < 2) {
      throw new IllegalArgumentException(
          "Invalid Spark Kafka checkpoint. Expected: topic,partition:offset,... Got: " + checkpointStr);
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

  /**
   * Parse Flink Kafka checkpoint.
   * Format: "kafka_metadata%3Atopic%3Apartition:offset" entries separated by ";".
   * Cluster metadata entries (containing "kafka_cluster") are skipped.
   *
   * <p>Example: "kafka_metadata%3Aevents%3A0:100;kafka_metadata%3Aevents%3A1:200
   * ;kafka_metadata%3Akafka_cluster%3Aevents%3A:my-cluster"</p>
   *
   * <p>The URL-encoded colons (%3A) separate the prefix, topic, and partition:offset.
   * The format is produced by {@code StreamerUtil.stringFy()} in hudi-flink.</p>
   *
   * <p>Parsing uses {@code lastIndexOf} to locate the partition and offset from the end
   * of each entry, consistent with the production parser in
   * {@code StreamerUtil.parseKafkaOffsets()}.</p>
   *
   * @param checkpointStr Checkpoint string
   * @return Map of partition → offset (cluster metadata entries excluded)
   * @throws IllegalArgumentException if format is invalid
   */
  private static Map<Integer, Long> parseFlinkKafkaCheckpoint(String checkpointStr) {
    if (checkpointStr == null || checkpointStr.trim().isEmpty()) {
      throw new IllegalArgumentException("Flink Kafka checkpoint string cannot be null or empty");
    }

    Map<Integer, Long> offsetMap = new HashMap<>();
    String[] entries = checkpointStr.split(";");

    for (String entry : entries) {
      entry = entry.trim();
      if (entry.isEmpty()) {
        continue;
      }

      // Skip cluster metadata entries (e.g., kafka_metadata%3Akafka_cluster%3Atopic%3A:cluster)
      if (!entry.contains(":") || entry.contains("kafka_cluster")) {
        continue;
      }

      // Entry format: kafka_metadata%3Atopic%3Apartition:offset
      // Find the last colon which separates partition:offset
      int lastColonIndex = entry.lastIndexOf(':');
      if (lastColonIndex == -1) {
        continue;
      }

      String offsetStr = entry.substring(lastColonIndex + 1);
      String beforeOffset = entry.substring(0, lastColonIndex);

      // Find the partition number (everything after the last %3A)
      int lastEncodedColonIndex = beforeOffset.lastIndexOf("%3A");
      if (lastEncodedColonIndex == -1) {
        continue;
      }

      String partitionStr = beforeOffset.substring(lastEncodedColonIndex + "%3A".length());

      try {
        int partition = Integer.parseInt(partitionStr);
        long offset = Long.parseLong(offsetStr);
        offsetMap.put(partition, offset);
      } catch (NumberFormatException e) {
        log.warn("Failed to parse partition ID or offset from entry: {}", entry, e);
      }
    }

    return offsetMap;
  }
}
