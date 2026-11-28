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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Where one exhaustive validation run wrote its reports, and whether it reached a conclusion.
 *
 * <p>Storage cannot tell a clean run from one that failed to conclude, so a consumer asks
 * {@link #isUsable()}: false for {@link #refused} (re-run the pass on the raw reports) and for
 * {@link #skipped} (nothing was listed, wait for the next run).
 *
 * <p>Each of the two dimensions writes a raw and an adjudicated document: FS-MDT diff in
 * {@link #getMismatchDir()} and {@link #getDiffAdjudicatedDir()}, duplicate base files in
 * {@link #getSuspectsDir()} and {@link #getAdjudicatedDir()}. They conclude independently, so no
 * single absent directory means the table is clean.
 *
 * <p>A usable handle can still carry only the duplicate-base-file dimension's findings, when the
 * FS-MDT comparison did not run; {@link #isFsMdtDiffAssessed()} says which.
 */
public final class ValidationRunReports implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String runId;
  private final Option<String> mismatchDir;
  private final Option<String> diffAdjudicatedDir;
  private final Option<String> suspectsDir;
  private final Option<String> adjudicatedDir;
  private final boolean reconciliationRefused;
  private final boolean skipped;
  private final boolean fsMdtDiffAssessed;

  private ValidationRunReports(String runId, Option<String> mismatchDir,
                               Option<String> diffAdjudicatedDir, Option<String> suspectsDir,
                               Option<String> adjudicatedDir, boolean reconciliationRefused,
                               boolean skipped, boolean fsMdtDiffAssessed) {
    this.runId = runId;
    this.mismatchDir = mismatchDir;
    this.diffAdjudicatedDir = diffAdjudicatedDir;
    this.suspectsDir = suspectsDir;
    this.adjudicatedDir = adjudicatedDir;
    this.reconciliationRefused = reconciliationRefused;
    this.skipped = skipped;
    this.fsMdtDiffAssessed = fsMdtDiffAssessed;
  }

  /** The three-directory form, for a caller that writes no FS-MDT adjudicated document. */
  public static ValidationRunReports of(String runId, Option<String> mismatchDir,
                                        Option<String> suspectsDir,
                                        Option<String> adjudicatedDir) {
    return of(runId, mismatchDir, Option.empty(), suspectsDir, adjudicatedDir);
  }

  /** Every directory a run wrote, one slot each. */
  public static ValidationRunReports of(String runId, Option<String> mismatchDir,
                                        Option<String> diffAdjudicatedDir,
                                        Option<String> suspectsDir,
                                        Option<String> adjudicatedDir) {
    return new ValidationRunReports(
        runId, mismatchDir, diffAdjudicatedDir, suspectsDir, adjudicatedDir, false, false, true);
  }

  /**
   * A run whose FS-MDT comparison did not run (skipped, or switched off) and whose
   * duplicate-base-file dimension concluded. Usable for that dimension's findings, but
   * {@link #isFsMdtDiffAssessed()} is false: it says nothing about the metadata table.
   */
  public static ValidationRunReports withoutFsMdtDiff(String runId, Option<String> suspectsDir,
                                                      Option<String> adjudicatedDir) {
    return new ValidationRunReports(
        runId, Option.empty(), Option.empty(), suspectsDir, adjudicatedDir, false, false, false);
  }

  /** The run declined to reconcile or threw trying, so nothing may be acted on. */
  public static ValidationRunReports refused(String runId) {
    return new ValidationRunReports(
        runId, Option.empty(), Option.empty(), Option.empty(), Option.empty(), true, false, false);
  }

  /**
   * The run validated nothing, and {@link #isUsable()} is false. A skipped fail-fast run still returns
   * {@code true} (a busy table must not fail every run), so this flag is the only thing separating it
   * from a clean run that also wrote no reports.
   */
  public static ValidationRunReports skipped(String runId) {
    return new ValidationRunReports(
        runId, Option.empty(), Option.empty(), Option.empty(), Option.empty(), false, true, false);
  }

  public String getRunId() {
    return runId;
  }

  /** The raw FS-MDT slice-level document, {@code fs_mdt_diff/diff/}. */
  public Option<String> getMismatchDir() {
    return mismatchDir;
  }

  /** The unexplained FS-MDT diff records; absent when none remain after {@code SANCTIONED} ones. */
  public Option<String> getDiffAdjudicatedDir() {
    return diffAdjudicatedDir;
  }

  public Option<String> getSuspectsDir() {
    return suspectsDir;
  }

  /** The duplicate-base-file dimension's conclusion; its absence says nothing about the run. */
  public Option<String> getAdjudicatedDir() {
    return adjudicatedDir;
  }

  /**
   * Whether the run validated nothing a consumer may act on (logged as
   * {@code MDT_VALIDATOR_PINNING_SKIPPED} or {@code MDT_VALIDATOR_ARCHIVAL_SKIPPED}). A run whose
   * duplicate-base-file dimension found something publishes it and is not skipped, even though its
   * FS-MDT comparison did not run; check {@link #isFsMdtDiffAssessed()} for that.
   */
  public boolean wasSkipped() {
    return skipped;
  }

  /**
   * Whether the FS-MDT comparison reached a verdict on this run. False for a usable handle from
   * {@link #withoutFsMdtDiff}, whose empty FS-MDT slots mean nothing looked rather than nothing
   * was found, and for every unusable handle.
   */
  public boolean isFsMdtDiffAssessed() {
    return fsMdtDiffAssessed;
  }

  /** Whether a consumer may act on this run's findings. */
  public boolean isUsable() {
    return !reconciliationRefused && !skipped;
  }

  @Override
  public String toString() {
    return "ValidationRunReports{runId=" + runId + ", mismatchDir=" + mismatchDir
        + ", diffAdjudicatedDir=" + diffAdjudicatedDir + ", suspectsDir=" + suspectsDir
        + ", adjudicatedDir=" + adjudicatedDir + ", reconciliationRefused=" + reconciliationRefused
        + ", skipped=" + skipped + ", fsMdtDiffAssessed=" + fsMdtDiffAssessed + '}';
  }
}
