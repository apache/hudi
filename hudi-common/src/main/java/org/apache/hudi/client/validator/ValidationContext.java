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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Provides validators with access to commit information.
 * Engine-specific implementations (Spark, Flink, Java) provide concrete implementations
 * for the core methods; computed convenience methods are provided as defaults.
 *
 * <p>This interface abstracts away engine-specific details while providing consistent
 * access to validation data across all write engines.</p>
 *
 * <p>Example implementations:</p>
 * <ul>
 *   <li>SparkValidationContext (Phase 3): Accesses Spark RDD write metadata</li>
 *   <li>FlinkValidationContext (Phase 2): Accesses Flink checkpoint state</li>
 *   <li>JavaValidationContext (Future): Accesses Java client write metadata</li>
 * </ul>
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface ValidationContext {

  /**
   * Get the current commit instant time being validated.
   *
   * @return Instant time string (format: yyyyMMddHHmmss)
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  String getInstantTime();

  /**
   * Get commit metadata for the current commit being validated.
   * Contains extraMetadata (checkpoints, custom metadata), operation type, write stats, etc.
   *
   * @return Optional commit metadata
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Option<HoodieCommitMetadata> getCommitMetadata();

  /**
   * Get write statistics for the current commit.
   * Contains record counts, partition info, file info, bytes written, etc.
   *
   * @return Optional list of write statistics per partition
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Option<List<HoodieWriteStat>> getWriteStats();

  /**
   * Get the active timeline for accessing previous commits.
   * Used to navigate commit history and extract previous checkpoints.
   *
   * @return Active timeline
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  HoodieActiveTimeline getActiveTimeline();

  /**
   * Get the previous completed commit instant.
   * Used to access previous checkpoint for delta validation.
   *
   * @return Optional previous instant
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Option<HoodieInstant> getPreviousCommitInstant();

  /**
   * Get commit metadata for the previous commit.
   * Used to extract previous checkpoint for comparison.
   *
   * @return Optional previous commit metadata
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Option<HoodieCommitMetadata> getPreviousCommitMetadata();

  // ========== Default convenience methods derived from core methods ==========

  /**
   * Get all extra metadata from the current commit.
   * Derived from {@link #getCommitMetadata()}.
   *
   * @return Map of metadata key to value, or empty map if no metadata
   */
  default Map<String, String> getExtraMetadata() {
    return getCommitMetadata()
        .map(HoodieCommitMetadata::getExtraMetadata)
        .orElse(Collections.emptyMap());
  }

  /**
   * Get a specific extra metadata value by key.
   * Derived from {@link #getCommitMetadata()}.
   *
   * @param key Metadata key
   * @return Optional metadata value
   */
  default Option<String> getExtraMetadata(String key) {
    return getCommitMetadata()
        .flatMap(metadata -> Option.ofNullable(metadata.getMetadata(key)));
  }

  /**
   * Calculate total records written in the current commit.
   * Derived from {@link #getWriteStats()} by summing {@code numWrites} across all partitions.
   *
   * @return Total record count
   */
  default long getTotalRecordsWritten() {
    return getWriteStats()
        .map(stats -> stats.stream().mapToLong(HoodieWriteStat::getNumWrites).sum())
        .orElse(0L);
  }

  /**
   * Calculate total insert records written.
   * Derived from {@link #getWriteStats()} by summing {@code numInserts} across all partitions.
   *
   * @return Total insert count
   */
  default long getTotalInsertRecordsWritten() {
    return getWriteStats()
        .map(stats -> stats.stream().mapToLong(HoodieWriteStat::getNumInserts).sum())
        .orElse(0L);
  }

  /**
   * Calculate total update records written.
   * Derived from {@link #getWriteStats()} by summing {@code numUpdateWrites} across all partitions.
   *
   * @return Total update count
   */
  default long getTotalUpdateRecordsWritten() {
    return getWriteStats()
        .map(stats -> stats.stream().mapToLong(HoodieWriteStat::getNumUpdateWrites).sum())
        .orElse(0L);
  }

  /**
   * Check if this is the first commit (no previous commits exist).
   * Derived from {@link #getPreviousCommitInstant()}.
   * Validators should skip validation for first commit since there's
   * no previous checkpoint to compare against.
   *
   * @return true if first commit
   */
  default boolean isFirstCommit() {
    return !getPreviousCommitInstant().isPresent();
  }
}
