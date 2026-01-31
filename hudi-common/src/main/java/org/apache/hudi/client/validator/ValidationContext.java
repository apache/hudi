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

package org.apache.hudi.client.validator;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import java.util.List;
import java.util.Map;

/**
 * Provides validators with access to commit information.
 * Engine-specific implementations (Spark, Flink, Java) provide concrete implementations.
 *
 * This interface abstracts away engine-specific details while providing consistent
 * access to validation data across all write engines.
 *
 * Example implementations:
 * - SparkValidationContext (Phase 3): Accesses Spark RDD write metadata
 * - FlinkValidationContext (Phase 2): Accesses Flink checkpoint state
 * - JavaValidationContext (Future): Accesses Java client write metadata
 */
public interface ValidationContext {

  /**
   * Get the current commit instant time being validated.
   *
   * @return Instant time string (format: yyyyMMddHHmmss)
   */
  String getInstantTime();

  /**
   * Get commit metadata for the current commit being validated.
   * Contains extraMetadata (checkpoints, custom metadata), operation type, write stats, etc.
   *
   * @return Optional commit metadata
   */
  Option<HoodieCommitMetadata> getCommitMetadata();

  /**
   * Get write statistics for the current commit.
   * Contains record counts, partition info, file info, bytes written, etc.
   *
   * @return Optional list of write statistics per partition
   */
  Option<List<HoodieWriteStat>> getWriteStats();

  /**
   * Get all extra metadata from the current commit.
   * This includes:
   * - Streaming checkpoints (Kafka offsets, Pulsar message IDs, etc.)
   * - Custom metadata added by users
   * - Schema information
   *
   * Common keys:
   * - "deltastreamer.checkpoint.key" (DeltaStreamer Kafka checkpoint)
   * - "flink.kafka.checkpoint" (Flink Kafka checkpoint)
   * - "schema" (Table schema)
   *
   * @return Map of metadata key to value
   */
  Map<String, String> getExtraMetadata();

  /**
   * Get a specific extra metadata value by key.
   *
   * @param key Metadata key
   * @return Optional metadata value
   */
  Option<String> getExtraMetadata(String key);

  /**
   * Get the active timeline for accessing previous commits.
   * Used to navigate commit history and extract previous checkpoints.
   *
   * @return Active timeline
   */
  HoodieActiveTimeline getActiveTimeline();

  /**
   * Get the previous completed commit instant.
   * Used to access previous checkpoint for delta validation.
   *
   * @return Optional previous instant
   */
  Option<HoodieInstant> getPreviousCommitInstant();

  /**
   * Get commit metadata for the previous commit.
   * Used to extract previous checkpoint for comparison.
   *
   * @return Optional previous commit metadata
   */
  Option<HoodieCommitMetadata> getPreviousCommitMetadata();

  /**
   * Calculate total records written in the current commit.
   * Sum of inserts, updates, and deletes across all partitions.
   *
   * Formula: sum(writeStats.numWrites) for all partitions
   *
   * @return Total record count
   */
  long getTotalRecordsWritten();

  /**
   * Calculate total insert records written.
   *
   * Formula: sum(writeStats.numInserts) for all partitions
   *
   * @return Total insert count
   */
  long getTotalInsertRecordsWritten();

  /**
   * Calculate total update records written.
   *
   * Formula: sum(writeStats.numUpdateWrites) for all partitions
   *
   * @return Total update count
   */
  long getTotalUpdateRecordsWritten();

  /**
   * Check if this is the first commit (no previous commits exist).
   * Validators should skip validation for first commit since there's
   * no previous checkpoint to compare against.
   *
   * @return true if first commit
   */
  boolean isFirstCommit();
}
