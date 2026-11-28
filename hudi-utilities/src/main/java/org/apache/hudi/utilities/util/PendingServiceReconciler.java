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

/**
 * Answers "is there a pending or completed table service that explains this file?" for a file the
 * metadata table no longer lists. A file a clean or rollback was planning to (or already did) delete
 * is sanctioned rather than a fault; this class is the sole place that combines what changed during
 * the run ({@link ServiceActivityDelta}) with what was already pending before it ({@link PendingServiceState})
 * to answer that question. Neither input alone is enough: {@code dlt} only reports services that
 * *changed* between {@code T_prev} and {@code T_aft}, while a clean or rollback that was already
 * inflight at {@code T_prev} and is still inflight at {@code T_aft} never appears in that diff at all,
 * despite having already deleted files the metadata table still names.
 *
 * <p><b>Read failures never collapse into "nothing explains this file".</b> {@link
 * Outcome#UNRESOLVED} is a distinct answer from {@link Outcome#NA}: a partition whose clean or
 * rollback plan could not be read must never be reported as clean, because that reads downstream as
 * proof of a fault (DANGLING, or a fatal OUTBOUND_DELETION) when the truth is simply "could not tell".
 */
public final class PendingServiceReconciler {

  private PendingServiceReconciler() {
  }

  /** Whether a pending or completed table service explains a file the metadata table no longer lists. */
  public enum Outcome {
    /**
     * A clean or rollback -- pending at either read, or completed during the run -- explains the
     * file. Also reached when the only unreadable plan found was still {@code .requested}-only: such
     * a plan may simply be mid-write, so the mismatch it might explain is sanctioned rather than left
     * {@link #UNRESOLVED}.
     */
    SANCTIONED,
    /** Nothing pending or observed explains the file, and every relevant plan was readable. */
    NA,
    /**
     * A plan that had already reached {@code .inflight} (or further) could not be read; this is not
     * proof of anything. A {@code .requested}-only unreadable plan is weaker evidence and resolves to
     * {@link #SANCTIONED} instead -- see {@link PendingServiceState#hasRequestedOnlyPlanReadFailure()}.
     */
    UNRESOLVED
  }

  /**
   * @param partition      the file's partition path
   * @param fileName       the file's bare name; {@code null} is always {@link Outcome#UNRESOLVED}
   * @param writingInstant the instant that wrote the file, if known
   * @param dlt            the clean and rollback activity between {@code T_prev} and {@code T_aft};
   *                       this method is the only thing that consults it, and it is consulted for
   *                       nothing else
   * @param pending        what a clean or rollback pending at EITHER of the run's two timeline
   *                       reads had planned to delete; see {@link PendingServiceState#across}
   */
  public static Outcome reconcile(String partition, String fileName, String writingInstant,
                                  ServiceActivityDelta dlt, PendingServiceState pending) {
    if (fileName == null) {
      BranchBeacon.hit(Branch.PSR_FILE_NAME_ABSENT);
      return Outcome.UNRESOLVED;
    }
    if (dlt.fileNamesFreedByClean(partition).contains(fileName)
        || dlt.fileNamesFreedByRollback(partition).contains(fileName)
        || pending.cleanPlannedFiles(partition).contains(fileName)
        || pending.rollbackPlannedFiles(partition).contains(fileName)) {
      BranchBeacon.hit(Branch.PSR_FILE_CLAIMED);
      return Outcome.SANCTIONED;
    }
    if (writingInstant != null
        && (dlt.rolledBackInstants().contains(writingInstant)
            || pending.plannedRollbackInstants().contains(writingInstant))) {
      BranchBeacon.hit(Branch.PSR_WRITING_INSTANT_ROLLED_BACK);
      return Outcome.SANCTIONED;
    }
    // Only now. A read failure that could not have covered this file must not blank it: an
    // unscoped UNRESOLVED lets one corrupt plan make the whole run NOT_SETTLED, and NOT_SETTLED
    // is non-fatal, so the run would pass a table nobody actually checked.
    if (dlt.readFailedFor(partition) || pending.readFailedFor(partition)) {
      BranchBeacon.hit(Branch.PSR_PLAN_UNREADABLE);
      return Outcome.UNRESOLVED;
    }
    // A requested-only plan that failed to read is weaker evidence than the hard failure above --
    // checked after it, never instead of it -- because a plan that has not yet reached .inflight may
    // simply not be fully flushed. Loosened to SANCTIONED rather than left UNRESOLVED: the mismatch
    // it might explain is exactly the shape a write still in progress leaves behind.
    if (dlt.hasRequestedOnlyPlanReadFailure() || pending.hasRequestedOnlyPlanReadFailure()) {
      BranchBeacon.hit(Branch.PSR_REQUESTED_ONLY_PLAN_UNREADABLE_SANCTIONED);
      return Outcome.SANCTIONED;
    }
    BranchBeacon.hit(Branch.PSR_NOTHING_ACCOUNTS_FOR_IT);
    return Outcome.NA;
  }
}
