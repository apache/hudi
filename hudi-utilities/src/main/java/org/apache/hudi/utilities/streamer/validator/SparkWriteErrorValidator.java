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

/**
 * Pure pre-commit validator that fails the commit when any records failed to write.
 *
 * <p>This is Phase 1 of migrating {@code HoodieStreamerWriteStatusValidator} (HSWSV) into the
 * pre-commit validator framework (issue #18750). It mirrors the boolean error check from
 * HSWSV ({@code hasErrorRecords = totalErroredRecords &gt; 0}) without any of HSWSV's
 * side effects — no error-table commit, no top-100 error logging, no instant rollback.
 * Those concerns are extracted in subsequent phases.</p>
 *
 * <p>Behavior mapping from HSWSV:</p>
 * <ul>
 *   <li>{@code cfg.commitOnErrors = false} (default) ↔ {@code failure.policy = FAIL}</li>
 *   <li>{@code cfg.commitOnErrors = true} ↔ {@code failure.policy = WARN_LOG}</li>
 * </ul>
 *
 * <p>Configuration:</p>
 * <ul>
 *   <li>{@code hoodie.precommit.validators}: Include
 *       {@code org.apache.hudi.utilities.streamer.validator.SparkWriteErrorValidator}</li>
 *   <li>{@code hoodie.precommit.validators.failure.policy}: FAIL (default) or WARN_LOG</li>
 * </ul>
 *
 * <p><b>Important:</b> In Phase 1 this validator is opt-in and runs <i>alongside</i> HSWSV,
 * not in place of it. HSWSV remains the canonical path that commits the error table and
 * rolls back the instant on failure. Enabling this validator gives an additional pre-commit
 * guard but does not yet allow HSWSV to be disabled.</p>
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
    this.failurePolicy = ValidationFailurePolicy.valueOf(policyStr);
  }

  @Override
  public void validateWithMetadata(ValidationContext context) throws HoodieValidationException {
    long totalErrors = context.getTotalWriteErrors();
    long totalRecordsWritten = context.getTotalRecordsWritten();
    // Total records considered for the commit includes both successfully written records
    // and records that failed to write. HSWSV computes this from the raw WriteStatus RDD;
    // here we derive the equivalent from HoodieWriteStat fields exposed by ValidationContext.
    long totalRecords = totalRecordsWritten + totalErrors;

    if (totalRecords == 0) {
      // Mirrors HSWSV: "No new data, perform empty commit." — nothing to validate.
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
            + "To allow the commit to proceed despite write errors, set "
            + "%s=WARN_LOG (mirrors hoodie.streamer.commit.on.errors=true).",
        context.getInstantTime(), totalErrors, totalRecords,
        HoodiePreCommitValidatorConfig.VALIDATION_FAILURE_POLICY.key());

    if (failurePolicy == ValidationFailurePolicy.WARN_LOG) {
      log.warn("{} (failure policy is WARN_LOG, commit will proceed)", errorMsg);
    } else {
      throw new HoodieValidationException(errorMsg);
    }
  }
}
