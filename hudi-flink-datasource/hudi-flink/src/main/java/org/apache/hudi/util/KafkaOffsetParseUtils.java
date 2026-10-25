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

package org.apache.hudi.util;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.exception.HoodieException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for parsing and comparing Kafka offset metadata stored in Hudi commits.
 */
@Slf4j
public class KafkaOffsetParseUtils {

  public static final String HOODIE_METADATA_KEY = "HoodieMetadataKey";
  private static final String URL_ENCODED_COLON = "%3A";
  private static final String PARTITION_SEPARATOR = ";";

  /**
   * Extracts Kafka offset metadata from a Hudi commit.
   *
   * @param commitInstant The commit instant to extract metadata from
   * @param timeline The timeline to read commit details from
   * @return The Kafka offset metadata string, or null if not found
   * @throws IOException if error reading commit metadata
   */
  public static String extractKafkaOffsetMetadata(final HoodieInstant commitInstant,
                                                  final HoodieTimeline timeline) throws IOException {
    HoodieCommitMetadata commitMetadata = TimelineUtils.getCommitMetadata(commitInstant, timeline);
    return commitMetadata.getExtraMetadata().get(HOODIE_METADATA_KEY);
  }

  /**
   * Parses Kafka offset string and restores partition offsets as Map.
   *
   * @param kafkaOffsetString URL-encoded Kafka offset string in format:
   *        "kafka_metadata%3Atopic%3Apartition:offset;kafka_metadata%3Atopic%3Apartition:offset"
   * @return Map of partition ID to offset, or empty map if input is null/empty
   * @throws HoodieException if parsing fails unexpectedly
   */
  public static Map<Integer, Long> parseKafkaOffsets(final String kafkaOffsetString) {
    Map<Integer, Long> partitionOffsets = new HashMap<>();

    if (kafkaOffsetString == null || kafkaOffsetString.isEmpty()) {
      return partitionOffsets;
    }

    try {
      // Split by semicolon to get individual partition entries
      String[] partitionEntries = kafkaOffsetString.split(PARTITION_SEPARATOR);

      for (String entry : partitionEntries) {
        entry = entry.trim();

        // Skip cluster metadata entries (they don't contain partition:offset format)
        if (!entry.contains(":") || entry.contains("kafka_cluster")) {
          continue;
        }

        // Entry format: kafka_metadata%3Atopic%3Apartition:offset
        // Find the last colon which separates partition and offset
        int lastColonIndex = entry.lastIndexOf(':');
        if (lastColonIndex == -1) {
          continue;
        }

        String offsetStr = entry.substring(lastColonIndex + 1);
        String beforeOffset = entry.substring(0, lastColonIndex);

        // Find the partition number (everything after the last %3A before the colon)
        int lastEncodedColonIndex = beforeOffset.lastIndexOf(URL_ENCODED_COLON);
        if (lastEncodedColonIndex == -1) {
          continue;
        }

        String partitionStr = beforeOffset.substring(lastEncodedColonIndex + URL_ENCODED_COLON.length());

        try {
          int partitionId = Integer.parseInt(partitionStr);
          long offset = Long.parseLong(offsetStr);
          partitionOffsets.put(partitionId, offset);
        } catch (NumberFormatException e) {
          log.warn("Failed to parse partition ID or offset from entry: {}", entry, e);
        }
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to parse Kafka offset string: " + kafkaOffsetString, e);
    }

    return partitionOffsets;
  }

  /**
   * Calculates the total difference count between Kafka offsets of two Hudi commits.
   * The difference is: current commit's kafka offset minus previous commit's kafka offset
   * for each partition. All partition differences are summed together.
   *
   * <p>Both commit instants must be present in the provided timeline. If a commit has been
   * archived and is no longer in the active timeline, this method will fail.</p>
   *
   * @param currentCommitInstant The current (newer) commit instant
   * @param previousCommitInstant The previous (older) commit instant
   * @param timeline The timeline to read commit details from (must contain both instants)
   * @return The total count of offset differences across all partitions
   * @throws IllegalArgumentException if any parameter is null
   * @throws HoodieException if metadata extraction or parsing fails
   */
  public static long calculateKafkaOffsetDifference(
      final HoodieInstant currentCommitInstant,
      final HoodieInstant previousCommitInstant,
      final HoodieTimeline timeline) throws IOException {

    if (currentCommitInstant == null || previousCommitInstant == null || timeline == null) {
      throw new IllegalArgumentException(
          "currentCommitInstant, previousCommitInstant, and timeline must not be null");
    }

    // Extract Kafka offset metadata from both commits
    String currentOffsetString = extractKafkaOffsetMetadata(currentCommitInstant, timeline);
    String previousOffsetString = extractKafkaOffsetMetadata(previousCommitInstant, timeline);

    if (currentOffsetString == null || previousOffsetString == null) {
      throw new HoodieException(String.format(
          "Kafka offset metadata not found in one or both commits: current=%s, previous=%s",
          currentCommitInstant.requestedTime(), previousCommitInstant.requestedTime()));
    }

    // Parse offset strings into partition -> offset maps
    Map<Integer, Long> currentOffsets = parseKafkaOffsets(currentOffsetString);
    Map<Integer, Long> previousOffsets = parseKafkaOffsets(previousOffsetString);

    if (currentOffsets.isEmpty() || previousOffsets.isEmpty()) {
      throw new HoodieException(String.format(
          "Parsed Kafka offsets are empty for commits: current=%s, previous=%s",
          currentCommitInstant.requestedTime(), previousCommitInstant.requestedTime()));
    }

    // Warn about partitions present in previous but missing from current.
    // A disappearing partition contributes 0 to the diff (no records consumed),
    // but may indicate topic reconfiguration or consumer rebalance issues.
    for (Integer partitionId : previousOffsets.keySet()) {
      if (!currentOffsets.containsKey(partitionId)) {
        log.warn("Partition {} exists in previous commit but not in current commit. "
            + "This may indicate a topic reconfiguration or consumer rebalance. "
            + "Previous offset for this partition was: {}", partitionId, previousOffsets.get(partitionId));
      }
    }

    // Calculate total difference across all partitions
    long totalDifference = 0L;

    for (Map.Entry<Integer, Long> currentEntry : currentOffsets.entrySet()) {
      Integer partitionId = currentEntry.getKey();
      Long currentOffset = currentEntry.getValue();
      Long previousOffset = previousOffsets.get(partitionId);

      // If partition doesn't exist in previous commit (new partition), use 0 as base
      if (previousOffset == null) {
        previousOffset = 0L;
        log.debug("New partition {} detected, using 0 as previous offset baseline",
            partitionId);
      }

      long difference = currentOffset - previousOffset;
      totalDifference += difference;

      log.debug("Partition {} offset difference: {} - {} = {}",
          partitionId, currentOffset, previousOffset, difference);
    }

    log.info("Total Kafka offset difference between commits {} and {}: {}",
        currentCommitInstant.requestedTime(), previousCommitInstant.requestedTime(),
        totalDifference);

    return totalDifference;
  }
}
