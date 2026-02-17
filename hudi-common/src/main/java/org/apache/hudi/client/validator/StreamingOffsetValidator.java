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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.CheckpointUtils;
import org.apache.hudi.common.util.CheckpointUtils.CheckpointFormat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for streaming offset validators.
 * Handles common offset validation logic across all streaming sources (Kafka, Pulsar, Kinesis).
 *
 * This validator compares source offset differences with actual record counts written to detect data loss.
 *
 * <p><b>Note:</b> This validator is primarily intended for append-only ingestion scenarios.
 * For upsert workloads with deduplication or event-time ordering, the deviation between
 * source offsets and records written can be legitimately large (e.g., duplicate keys are
 * deduplicated, late-arriving events are dropped). In such cases, configure an appropriate
 * tolerance percentage or use warn-only mode.</p>
 *
 * Algorithm:
 * 1. Extract current and previous checkpoints from commit metadata
 * 2. Calculate offset difference using source-specific format
 * 3. Get actual record count from write statistics
 * 4. Calculate deviation percentage: |offsetDiff - recordCount| / offsetDiff * 100
 * 5. If deviation &gt; tolerance: fail (or warn if warn-only mode)
 *
 * Subclasses specify:
 * - Checkpoint format (DELTASTREAMER_KAFKA, FLINK_KAFKA, etc.)
 * - Checkpoint metadata key
 * - Source-specific parsing logic (if needed)
 *
 * Configuration:
 * - hoodie.precommit.validators.streaming.offset.tolerance.percentage (default: 0.0)
 * - hoodie.precommit.validators.warn.only (default: false)
 */
public abstract class StreamingOffsetValidator extends BasePreCommitValidator {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingOffsetValidator.class);

  // Configuration keys - these mirror the ConfigProperty definitions in
  // HoodiePreCommitValidatorConfig (hudi-client-common) for documentation surfacing.
  protected static final String TOLERANCE_PERCENTAGE_KEY = "hoodie.precommit.validators.streaming.offset.tolerance.percentage";
  protected static final String WARN_ONLY_MODE_KEY = "hoodie.precommit.validators.warn.only";

  // Default values
  protected static final double DEFAULT_TOLERANCE_PERCENTAGE = 0.0;
  protected static final boolean DEFAULT_WARN_ONLY_MODE = false;

  protected final String checkpointKey;
  protected final double tolerancePercentage;
  protected final boolean warnOnlyMode;
  protected final CheckpointFormat checkpointFormat;

  /**
   * Create a streaming offset validator.
   *
   * @param config Validator configuration
   * @param checkpointKey Key to extract checkpoint from extraMetadata
   * @param checkpointFormat Format of the checkpoint string
   */
  protected StreamingOffsetValidator(TypedProperties config,
                                      String checkpointKey,
                                      CheckpointFormat checkpointFormat) {
    super(config);
    this.checkpointKey = checkpointKey;
    this.checkpointFormat = checkpointFormat;
    this.tolerancePercentage = config.getDouble(TOLERANCE_PERCENTAGE_KEY, DEFAULT_TOLERANCE_PERCENTAGE);
    this.warnOnlyMode = config.getBoolean(WARN_ONLY_MODE_KEY, DEFAULT_WARN_ONLY_MODE);
  }

  @Override
  protected void validateWithMetadata(ValidationContext context) throws HoodieValidationException {
    // Skip validation for first commit (no previous checkpoint)
    if (context.isFirstCommit()) {
      LOG.info("Skipping offset validation for first commit");
      return;
    }

    // Extract current checkpoint
    Option<String> currentCheckpointOpt = context.getExtraMetadata(checkpointKey);
    if (!currentCheckpointOpt.isPresent()) {
      LOG.warn("Current checkpoint not found with key: {}. Skipping validation.", checkpointKey);
      return;
    }
    String currentCheckpoint = currentCheckpointOpt.get();

    // Validate current checkpoint format
    if (!CheckpointUtils.isValidCheckpointFormat(checkpointFormat, currentCheckpoint)) {
      LOG.warn("Current checkpoint has invalid format: {}. Skipping validation.", currentCheckpoint);
      return;
    }

    // Extract previous checkpoint
    Option<String> previousCheckpointOpt = context.getPreviousCommitMetadata()
        .flatMap(metadata -> Option.ofNullable(metadata.getMetadata(checkpointKey)));

    if (!previousCheckpointOpt.isPresent()) {
      LOG.info("Previous checkpoint not found. May be first streaming commit. Skipping validation.");
      return;
    }
    String previousCheckpoint = previousCheckpointOpt.get();

    // Validate previous checkpoint format
    if (!CheckpointUtils.isValidCheckpointFormat(checkpointFormat, previousCheckpoint)) {
      LOG.warn("Previous checkpoint has invalid format: {}. Skipping validation.", previousCheckpoint);
      return;
    }

    // Calculate offset difference using format-specific logic
    long offsetDifference = CheckpointUtils.calculateOffsetDifference(
        checkpointFormat, previousCheckpoint, currentCheckpoint);

    // Handle negative offset (source reset)
    if (offsetDifference < 0) {
      LOG.warn("Negative offset difference detected ({}). May indicate source reset. Skipping validation.",
          offsetDifference);
      return;
    }

    // Get actual record count from write stats
    long recordsWritten = context.getTotalRecordsWritten();

    // Validate offset vs record consistency
    validateOffsetConsistency(offsetDifference, recordsWritten,
        currentCheckpoint, previousCheckpoint);
  }

  /**
   * Validate that offset difference matches record count within tolerance.
   *
   * @param offsetDiff Expected records based on offset difference
   * @param recordsWritten Actual records written
   * @param currentCheckpoint Current checkpoint string (for error messages)
   * @param previousCheckpoint Previous checkpoint string (for error messages)
   * @throws HoodieValidationException if validation fails (and not warn-only mode)
   */
  protected void validateOffsetConsistency(long offsetDiff, long recordsWritten,
                                            String currentCheckpoint, String previousCheckpoint)
      throws HoodieValidationException {

    double deviation = calculateDeviation(offsetDiff, recordsWritten);

    if (deviation > tolerancePercentage) {
      String errorMsg = String.format(
          "Streaming offset validation failed. "
              + "Offset difference: %d, Records written: %d, Deviation: %.2f%%, Tolerance: %.2f%%. "
              + "This may indicate data loss or filtering. "
              + "Previous checkpoint: %s, Current checkpoint: %s",
          offsetDiff, recordsWritten, deviation, tolerancePercentage,
          previousCheckpoint, currentCheckpoint);

      if (warnOnlyMode) {
        LOG.warn(errorMsg + " (warn-only mode enabled, commit will proceed)");
      } else {
        throw new HoodieValidationException(errorMsg);
      }
    } else {
      LOG.info("Offset validation passed. Offset diff: {}, Records: {}, Deviation: {}% (within {}%)",
          offsetDiff, recordsWritten, String.format("%.2f", deviation), tolerancePercentage);
    }
  }

  /**
   * Calculate percentage deviation between expected (offset diff) and actual (record count).
   *
   * Formula:
   * - If both are zero: 0% (perfect match, no data)
   * - If one is zero: 100% (complete mismatch)
   * - Otherwise: |offsetDiff - recordsWritten| / offsetDiff * 100
   *
   * @param offsetDiff Expected records from offset difference
   * @param recordsWritten Actual records written
   * @return Deviation percentage (0.0 = exact match, 100.0 = complete mismatch)
   */
  private double calculateDeviation(long offsetDiff, long recordsWritten) {
    // Handle edge cases
    if (offsetDiff == 0 && recordsWritten == 0) {
      return 0.0;  // Both zero - perfect match (no data processed)
    }
    if (offsetDiff == 0 || recordsWritten == 0) {
      return 100.0;  // One is zero - complete mismatch
    }

    long difference = Math.abs(offsetDiff - recordsWritten);
    return (100.0 * difference) / offsetDiff;
  }
}
