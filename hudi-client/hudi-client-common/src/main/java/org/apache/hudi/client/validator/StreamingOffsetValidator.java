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
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig.ValidationFailurePolicy;
import org.apache.hudi.exception.HoodieValidationException;

import lombok.extern.slf4j.Slf4j;

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
 * tolerance percentage or use WARN_LOG failure policy.</p>
 *
 * Algorithm:
 * 1. Extract current and previous checkpoints from commit metadata
 * 2. Calculate offset difference using source-specific format
 * 3. Get actual record count from write statistics
 * 4. Calculate deviation percentage: |offsetDiff - recordCount| / offsetDiff * 100
 * 5. If deviation &gt; tolerance: fail or warn based on failure policy
 *
 * Subclasses specify:
 * - Checkpoint format (SPARK_KAFKA, FLINK_KAFKA, etc.)
 * - Checkpoint metadata key
 * - Source-specific parsing logic (if needed)
 *
 * Configuration:
 * - hoodie.precommit.validators.streaming.offset.tolerance.percentage (default: 0.0)
 * - hoodie.precommit.validators.failure.policy (default: FAIL)
 */
@Slf4j
public abstract class StreamingOffsetValidator extends BasePreCommitValidator {

  protected final String checkpointKey;
  protected final double tolerancePercentage;
  protected final ValidationFailurePolicy failurePolicy;
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
    this.tolerancePercentage = Double.parseDouble(
        config.getString(HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.key(),
            HoodiePreCommitValidatorConfig.STREAMING_OFFSET_TOLERANCE_PERCENTAGE.defaultValue()));
    String policyStr = config.getString(
        HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(),
        HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.defaultValue());
    this.failurePolicy = ValidationFailurePolicy.valueOf(policyStr);
  }

  @Override
  public void validateWithMetadata(ValidationContext context) throws HoodieValidationException {
    // Skip validation for first commit (no previous checkpoint)
    if (context.isFirstCommit()) {
      log.info("Skipping offset validation for first commit");
      return;
    }

    // Extract current checkpoint
    Option<String> currentCheckpointOpt = context.getExtraMetadata(checkpointKey);
    if (!currentCheckpointOpt.isPresent()) {
      log.warn("Current checkpoint not found with key: {}. Skipping validation.", checkpointKey);
      return;
    }
    String currentCheckpoint = currentCheckpointOpt.get();

    // Validate current checkpoint format
    if (!CheckpointUtils.isValidCheckpointFormat(checkpointFormat, currentCheckpoint)) {
      log.warn("Current checkpoint has invalid format: {}. Skipping validation.", currentCheckpoint);
      return;
    }

    // Extract previous checkpoint
    Option<String> previousCheckpointOpt = context.getPreviousCommitMetadata()
        .flatMap(metadata -> Option.ofNullable(metadata.getMetadata(checkpointKey)));

    if (!previousCheckpointOpt.isPresent()) {
      log.info("Previous checkpoint not found. May be first streaming commit. Skipping validation.");
      return;
    }
    String previousCheckpoint = previousCheckpointOpt.get();

    // Validate previous checkpoint format
    if (!CheckpointUtils.isValidCheckpointFormat(checkpointFormat, previousCheckpoint)) {
      log.warn("Previous checkpoint has invalid format: {}. Skipping validation.", previousCheckpoint);
      return;
    }

    // Calculate offset difference using format-specific logic.
    // Note: calculateOffsetDifference always returns >= 0 because negative per-partition
    // diffs (offset resets) are handled internally by using the current offset.
    long offsetDifference = CheckpointUtils.calculateOffsetDifference(
        checkpointFormat, previousCheckpoint, currentCheckpoint);

    // Get actual new record count from write stats.
    // Use numInserts + numUpdateWrites instead of numWrites to avoid counting records
    // re-written due to small file handling. With INSERT operation and small file handling,
    // numWrites includes all records in the merged file (existing + new), which would
    // inflate the count and mask real data loss.
    long recordsWritten = context.getTotalInsertRecordsWritten()
        + context.getTotalUpdateRecordsWritten();

    // Track write errors so callers can distinguish write-failure deviation (write errors > 0)
    // from silent data loss (write errors == 0) when the validator fires.
    long writeErrors = context.getTotalWriteErrors();

    // For empty commits (e.g., no new data from source), both offsetDiff and recordsWritten
    // can be zero. This is a valid scenario — skip validation to avoid false positives.
    if (offsetDifference == 0 && recordsWritten == 0) {
      log.info("Empty commit detected (no offset change, no records written). Skipping offset validation.");
      return;
    }

    // Validate offset vs record consistency
    validateOffsetConsistency(offsetDifference, recordsWritten, writeErrors,
        currentCheckpoint, previousCheckpoint);
  }

  /**
   * Validate that offset difference matches record count within tolerance.
   *
   * @param offsetDiff Expected records based on offset difference
   * @param recordsWritten Actual records written (inserts + updates)
   * @param writeErrors Records that failed to write (tracked in write status errors)
   * @param currentCheckpoint Current checkpoint string (for error messages)
   * @param previousCheckpoint Previous checkpoint string (for error messages)
   * @throws HoodieValidationException if validation fails and policy is FAIL
   */
  protected void validateOffsetConsistency(long offsetDiff, long recordsWritten, long writeErrors,
                                            String currentCheckpoint, String previousCheckpoint)
      throws HoodieValidationException {

    double deviation = calculateDeviation(offsetDiff, recordsWritten);

    if (deviation > tolerancePercentage) {
      String errorMsg = String.format(
          "Streaming offset validation failed. "
              + "Offset difference: %d, Records written: %d, Write errors: %d, Deviation: %.2f%%, Tolerance: %.2f%%. "
              + "%s"
              + "Previous checkpoint: %s, Current checkpoint: %s",
          offsetDiff, recordsWritten, writeErrors, deviation, tolerancePercentage,
          writeErrors > 0
              ? "Non-zero write errors suggest records failed to write rather than silent data loss. "
              : "This may indicate data loss or filtering. ",
          previousCheckpoint, currentCheckpoint);

      if (failurePolicy == ValidationFailurePolicy.WARN_LOG) {
        log.warn(errorMsg + " (failure policy is WARN_LOG, commit will proceed)");
      } else {
        throw new HoodieValidationException(errorMsg);
      }
    } else {
      log.info("Offset validation passed. Offset diff: {}, Records: {}, Write errors: {}, Deviation: {}% (within {}%)",
          offsetDiff, recordsWritten, writeErrors, String.format("%.2f", deviation), tolerancePercentage);
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
