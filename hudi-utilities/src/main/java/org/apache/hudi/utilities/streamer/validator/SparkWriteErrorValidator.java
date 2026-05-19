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

package org.apache.hudi.utilities.streamer.validator;

import org.apache.hudi.client.validator.BasePreCommitValidator;
import org.apache.hudi.client.validator.ValidationContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig.ValidationFailurePolicy;
import org.apache.hudi.exception.HoodieValidationException;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

/**
 * Pre-commit validator that fails the commit when records failed to write.
 *
 * <p>Equivalent of the legacy {@code HoodieStreamerWriteStatusValidator}'s boolean error check
 * ({@code hasErrorRecords = totalErroredRecords > 0}), wired through the pre-commit validator
 * framework (issue #18750). Pure validation: no side effects (no error-table commit, no
 * top-100 error logging, no instant rollback). Those side effects are handled separately by
 * {@code StreamSync}'s pre-commit orchestration.</p>
 *
 * <p><b>Relationship with the inline write-error gate in {@code StreamSync}:</b> the default
 * commit path in {@code StreamSync} already applies an equivalent error check via the
 * {@code commitOnErrors} flag. This validator exists so that users running multiple validators
 * (e.g. write-error + offset checks) can express a unified pass/fail story through a single
 * {@code failure.policy} knob. Enabling this validator while leaving {@code commitOnErrors=false}
 * means both checks run and either can block the commit — they are intentionally not mutually
 * exclusive.</p>
 *
 * <p>Behavior mapping from the legacy HSWSV:</p>
 * <ul>
 *   <li>{@code commitOnErrors = false} (HSWSV default) ↔ {@code failure.policy = FAIL}</li>
 *   <li>{@code commitOnErrors = true} ↔ {@code failure.policy = WARN_LOG}</li>
 * </ul>
 *
 * <p>Configuration:</p>
 * <ul>
 *   <li>{@code hoodie.precommit.validators}: Include
 *       {@code org.apache.hudi.utilities.streamer.validator.SparkWriteErrorValidator}</li>
 *   <li>{@code hoodie.precommit.validators.failure.policy}: FAIL (default) or WARN_LOG</li>
 * </ul>
 *
 * <p>Like {@link SparkKafkaOffsetValidator}, this class extends {@link BasePreCommitValidator}
 * and must be invoked via {@link SparkStreamerValidatorUtils} — not {@code SparkValidatorUtils},
 * which expects a different constructor signature.</p>
 */
@Slf4j
public class SparkWriteErrorValidator extends BasePreCommitValidator {

  private final ValidationFailurePolicy failurePolicy;

  public SparkWriteErrorValidator(TypedProperties config) {
    super(config);
    String policyStr = config.getString(
        HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(),
        HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.defaultValue());
    try {
      this.failurePolicy = ValidationFailurePolicy.valueOf(policyStr);
    } catch (IllegalArgumentException e) {
      throw new HoodieValidationException(String.format(
          "Invalid value '%s' for %s. Allowed values: %s.",
          policyStr,
          HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key(),
          Arrays.toString(ValidationFailurePolicy.values())), e);
    }
  }

  @Override
  public void validateWithMetadata(ValidationContext context) throws HoodieValidationException {
    long totalErrors = context.getTotalWriteErrors();
    long totalRecordsWritten = context.getTotalRecordsWritten();
    // Total considered for the commit = successfully-written + failed. HSWSV computed this from
    // the raw WriteStatus RDD; we derive the equivalent from HoodieWriteStat fields exposed by
    // ValidationContext.
    long totalRecords = totalRecordsWritten + totalErrors;

    if (totalRecords == 0) {
      // Empty commit (mirrors HSWSV "No new data, perform empty commit.").
      log.info("Empty commit (no records written, no errors). Skipping write-error validation "
          + "for instant {}.", context.getInstantTime());
      return;
    }

    if (totalErrors == 0) {
      log.info("Write-error validation passed for instant {}: 0 errors out of {} records.",
          context.getInstantTime(), totalRecords);
      return;
    }

    String errorMsg = String.format(
        "Write-error validation failed for instant %s. "
            + "Errors: %d, Total: %d. "
            + "To allow the commit to proceed despite write errors, set %s=WARN_LOG, "
            + "or run HoodieStreamer with --commit-on-errors.",
        context.getInstantTime(), totalErrors, totalRecords,
        HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key());

    if (failurePolicy == ValidationFailurePolicy.WARN_LOG) {
      log.warn("{} (failure policy is WARN_LOG, commit will proceed)", errorMsg);
    } else {
      throw new HoodieValidationException(errorMsg);
    }
  }
}
