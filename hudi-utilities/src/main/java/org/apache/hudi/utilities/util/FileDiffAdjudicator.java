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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Decides what one {@link FileDiffRecord} means, from the state of its writing instant before the
 * run and after it.
 *
 * <p>Global arms 2, 3 and 4 run first (numbered to keep the {@code FDA_ARM*} branch names stable),
 * then rows selected by {@link DiffPattern}, then a total fallback. There is no raise arm: a
 * mid-revert read ({@code ActiveTimelineV2.revertCompleteToInflight:607-641}) observes pairs that
 * look impossible, so an unnamed pair is counted, logged loudly and reported {@code NOT_SETTLED}.
 *
 * <p>A failed read or an unresolvable state never reaches a fatal verdict ({@link Verdict#DANGLING},
 * {@link Verdict#MDT_CORRUPTION}, {@link Verdict#OUTBOUND_DELETION}, {@link Verdict#CODE_BUG}); it
 * ends at {@link Verdict#NOT_SETTLED}. Two deliberate exceptions reach a fatal verdict without a
 * direct positive answer; do not "fix" them:
 *
 * <ul>
 *   <li>Five rows go fatal when {@code r()} answers {@link PendingServiceReconciler.Outcome#NA}:
 *       MDT-exclusive {@code (COMPLETED, COMPLETED)}, unattributable and catch-all, and FS-exclusive
 *       {@code (pending, NULL)} and {@code (NULL, NULL)}. The diff itself is the observation, and
 *       {@code r()} is asked first so only a file no clean or rollback touches gets there;
 *       {@code UNRESOLVED} still routes to {@code NOT_SETTLED}. This is why
 *       {@link ServiceActivityDelta} absorbs a clean's {@code successDeleteFiles}: on the
 *       MDT-exclusive side it is the only evidence that withdraws the finding.
 *   <li>The two {@code (COMPLETED, pending-or-gone)} rows reach {@code CODE_BUG} without {@code r()}:
 *       a run must never observe a completed instant in any other state. A rollback or restore of a
 *       completed instant that overlaps the run fails it; that is the accepted cost, since it cannot
 *       be told apart from a timeline edited outside Hudi. Failed-write cleanup does not reach these
 *       rows ({@code getInstantsToRollback} draws only from the inflight timeline).
 * </ul>
 *
 * <p>The two FS-exclusive {@code (·, COMPLETED)} rows differ by the prev state alone: completed at
 * {@code T_prev} means the metadata table lost a committed file; completed during the run means it
 * is legitimately behind. Do not merge them.
 *
 * <p>{@code m()} ({@link InstantMembershipResolver}) and {@code r()} ({@link PendingServiceReconciler})
 * are consulted only inside the rows that name them: a pending instant has no readable commit
 * metadata, so hoisting {@code m()} would turn an ordinary in-flight write into {@code NOT_SETTLED}.
 */
public class FileDiffAdjudicator {

  private static final Logger LOG = LoggerFactory.getLogger(FileDiffAdjudicator.class);

  /**
   * What the archive says about an instant the active timeline does not hold: the only way to tell
   * a completed-and-archived instant from one that never existed.
   *
   * <p>Must return unresolved on a failed or incomplete read, and {@code NULL} only on a proven
   * absence ({@code InstantSearchState.NOT_FOUND}); {@code INDETERMINATE} is a refusal. An in-window
   * absence must not be trusted on the premise that the active timeline is contiguous back to the
   * archiving cutoff, since savepoint/archival interplay refutes it ({@code BaseHoodieTimeline:608}).
   * Fail-fast passes {@link #neverReadsTheArchive()}, which returns unresolved for everything.
   */
  @FunctionalInterface
  public interface ArchivedStateLookup extends Serializable {
    InstantStateIndex.Resolution resolve(String partitionPath, String fileGroupId, String instantTime);

    /**
     * Whether the archive holds a completed record of {@code instantTime} that recorded write
     * stats. Never a membership answer: it only establishes that the instant completed.
     */
    default boolean isCompletedWithWriteStats(String instantTime) {
      return false;
    }

    static ArchivedStateLookup neverReadsTheArchive() {
      return (p, g, t) -> InstantStateIndex.Resolution.unresolved();
    }
  }

  private final InstantStateIndex prevIndex;
  /** The state of every instant on the full active timeline at {@code T_aft}. */
  private final InstantStateIndex aftIndex;

  /** Clean and rollback activity during the run, and nothing else; consulted only via {@link #reconcile}. */
  private final ServiceActivityDelta dlt;
  private final PendingServiceState pendingServices;
  private final InstantMembershipResolver membership;
  private final ArchivedStateLookup archive;
  private final boolean failFast;

  /** Records that fell through every row; atomic because one adjudicator may serve several threads. */
  private final AtomicLong unmatchedStateCount = new AtomicLong();

  public FileDiffAdjudicator(InstantStateIndex prevIndex, InstantStateIndex aftIndex,
                             ServiceActivityDelta dlt,
                             PendingServiceState pendingServices, InstantMembershipResolver membership,
                             ArchivedStateLookup archive, boolean failFast) {
    this.prevIndex = prevIndex;
    this.aftIndex = aftIndex;
    this.dlt = dlt;
    this.pendingServices = pendingServices;
    this.membership = membership;
    this.archive = archive;
    this.failFast = failFast;
  }

  public FileDiffRecord adjudicate(FileDiffRecord record) {
    String instant = record.getWritingInstant();

    InstantStateIndex.Resolution prev = instant == null
        ? InstantStateIndex.Resolution.unresolved() : resolvePrev(record, instant);
    InstantStateIndex.Resolution aft = instant == null
        ? InstantStateIndex.Resolution.unresolved() : resolveAft(instant, prev);

    // A storage-only file whose archived completed commit recorded write stats but no name for its
    // group: the instant completed, and membership reads that absence as the active path does.
    if (instant != null && !failFast && !prev.isResolved() && !aft.isResolved()
        && record.getDiffPattern() == DiffPattern.FS_EXCLUSIVE
        && archive.isCompletedWithWriteStats(instant)) {
      BranchBeacon.hit(Branch.FDA_ARM23_ARCHIVED_COMPLETED_FS_EXCLUSIVE);
      InstantStateIndex.Resolution completed =
          InstantStateIndex.Resolution.of(InstantState.COMPLETED);
      return adjudicateFsExclusive(record, completed, completed);
    }

    // --- global arms 2 and 3, only where an instant was expected at all.
    if (instant != null && (!prev.isResolved() || !aft.isResolved())) {
      return failFast
          ? verdict(record, Branch.FDA_ARM23_UNRESOLVED_FAIL_FAST, Verdict.SANCTIONED,
              "the instant is not on the active timeline and fail-fast does not read the archive")
          : verdict(record, Branch.FDA_ARM23_UNRESOLVED_EXHAUSTIVE, Verdict.NOT_SETTLED,
              "the archive could not establish whether the instant ever completed");
    }

    // --- global arm 4: pattern-independent.
    if (instant != null && aft.getState() == InstantState.REQUESTED
        && (prev.getState() == InstantState.NULL || prev.getState() == InstantState.REQUESTED)
        && !isRevertedTableService(record, prev.getState(), aft.getState())) {
      // r() before the fatal branch: this may be the tail of a rollback deleting the instant
      // (.inflight goes before .requested), and a rollback that names it is positive evidence.
      switch (reconcile(record)) {
        case SANCTIONED:
          return verdict(record, Branch.FDA_ARM4_REQUESTED_SANCTIONED, Verdict.SANCTIONED,
              "a rollback or restore observed during the run explains this file");
        case UNRESOLVED:
          return verdict(record, Branch.FDA_ARM4_REQUESTED_UNRESOLVED, Verdict.NOT_SETTLED,
              "a requested instant never writes a data file, but a plan that could have explained "
                  + "this one could not be read");
        default:
          return verdict(record, Branch.FDA_ARM4_REQUESTED_NEVER_WRITES, Verdict.CODE_BUG,
              "a requested instant never writes a data file");
      }
    }

    if (record.getDiffPattern() == null) {
      // Emitters always set a pattern; guarded because switching on null throws and kills the task.
      return unmatched(record, Branch.FDA_PATTERN_ABSENT, prev, aft);
    }
    switch (record.getDiffPattern()) {
      case FS_EXCLUSIVE:
        return adjudicateFsExclusive(record, prev, aft);
      case MDT_EXCLUSIVE:
        return adjudicateMdtExclusive(record, instant, prev, aft);
      case BOTH_DISAGREE:
        return verdict(record, Branch.FDA_BOTH_DISAGREE, Verdict.MDT_CORRUPTION,
            "both sides hold the file and disagree about it");
      default:
        return unmatched(record, Branch.FDA_PATTERN_UNKNOWN, prev, aft);
    }
  }

  /** How many records no row named. Zero on a healthy run; anything else is a design gap. */
  public long getUnmatchedStateCount() {
    return unmatchedStateCount.get();
  }

  /** Storage holds the file, the metadata table does not. */
  private FileDiffRecord adjudicateFsExclusive(FileDiffRecord record,
                                               InstantStateIndex.Resolution prevRes,
                                               InstantStateIndex.Resolution aftRes) {
    InstantState prev = prevRes.getState();
    InstantState aft = aftRes.getState();

    if (prev == InstantState.COMPLETED && aft == InstantState.COMPLETED) {
      // The commit was already finished when the metadata table was read, so its answer is
      // authoritative in both directions.
      switch (lookUpMembership(record)) {
        case COMMITTED:
          return verdict(record, Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED, Verdict.MDT_CORRUPTION,
              "the commit completed before T_prev and names this file, so the metadata table lost a "
                  + "committed file");
        case DANGLING:
          return verdict(record, Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING, Verdict.DANGLING,
              "the commit completed before T_prev, was read, and does not name this file");
        default:
          return verdict(record, Branch.FDA_FS_PREV_DONE_AFT_DONE_UNKNOWN, Verdict.NOT_SETTLED,
              "the commit could not be read, so whether it names this file is unknown");
      }
    }
    if (prev == InstantState.COMPLETED && isPendingOrGone(aft)) {
      // The transition itself is the finding, so r() is not consulted (see the class javadoc).
      return verdict(record, Branch.FDA_FS_PREV_DONE_AFT_GONE, Verdict.CODE_BUG,
          "the instant left completed during the run; a completed instant is never observed in any "
              + "other state, so this is a rollback or restore that overlapped the run, a defect in "
              + "the validator's own reads, or a timeline edited outside Hudi's write paths");
    }
    if (isNullOrPending(prev) && aft == InstantState.COMPLETED) {
      // Unlike the (COMPLETED, COMPLETED) row, the metadata table was loaded before this instant
      // completed and is legitimately behind, so m() is not consulted on any answer. The sanction
      // is deliberately blanket: a commit naming a sibling file of the group, one claiming nothing
      // for this file group, and one claiming nothing for this partition are all retry shapes,
      // because bulk insert mints the file id per task attempt and the commit records only the
      // winning attempt. Cost: an orphan whose instant completes inside the two reads goes
      // unreported for that one run; the (COMPLETED, COMPLETED) row catches it on the next run.
      return verdict(record, Branch.FDA_FS_PREV_OPEN_AFT_DONE_SANCTIONED, Verdict.SANCTIONED,
          "the instant was open at T_prev and completed during the run, so this file is its own "
              + "output and the metadata table is legitimately behind");
    }
    if (isNullOrPending(prev) && aft == InstantState.INFLIGHT) {
      return verdict(record, Branch.FDA_FS_PREV_OPEN_AFT_INFLIGHT, Verdict.SANCTIONED, "an in-flight write");
    }
    if (prev == InstantState.INFLIGHT && aft == InstantState.REQUESTED) {
      return verdict(record, Branch.FDA_FS_PREV_INFLIGHT_AFT_REQUESTED, Verdict.NOT_SETTLED, "the instant was reverted to requested during the run");
    }
    if (prev == InstantState.REQUESTED && aft == InstantState.REQUESTED
        && record.getFileType() == MismatchRecord.FileType.BASE) {
      // Named cell for arm 4's exemption (isRevertedTableService), so it must not count as unmatched.
      // The file-type test is repeated so that narrowing arm 4 cannot silently widen this row.
      return verdict(record, Branch.FDA_FS_PREV_REQUESTED_AFT_REQUESTED_BASE, Verdict.NOT_SETTLED,
          "the instant was requested at both reads; a compaction or clustering instant reverted from "
              + "inflight to requested keeps its files (HoodieTable:712-733)");
    }
    if (prev == InstantState.NULL && aft == InstantState.NULL) {
      // Absent at both reads, so nothing references the file. NULL is a positive answer here:
      // resolvePrev already asked the archive, which yields NULL only on NOT_FOUND (fully searched);
      // an absence it could not vouch for stays unresolved and ends at arm 3.
      switch (reconcile(record)) {
        case SANCTIONED:
          return verdict(record, Branch.FDA_FS_PREV_NULL_AFT_NULL_SANCTIONED, Verdict.SANCTIONED,
              "a clean or rollback explains this file");
        case UNRESOLVED:
          return verdict(record, Branch.FDA_FS_PREV_NULL_AFT_NULL_UNRESOLVED, Verdict.NOT_SETTLED,
              "a clean or rollback plan that could have explained this file could not be read");
        default:
          return verdict(record, Branch.FDA_FS_PREV_NULL_AFT_NULL_DANGLING, Verdict.DANGLING,
              "no instant on the timeline at either read wrote this file, the archive held nothing "
                  + "that could have named one, and no clean or rollback explains it");
      }
    }
    if (isNullOrPending(prev) && aft == InstantState.NULL) {
      // Pending at the start of the run, gone at the end: the run watched a rollback of a failed write.
      switch (reconcile(record)) {
        case SANCTIONED:
          return verdict(record, Branch.FDA_FS_PREV_PENDING_AFT_NULL_SANCTIONED, Verdict.SANCTIONED,
              "a clean or rollback explains this file");
        case UNRESOLVED:
          return verdict(record, Branch.FDA_FS_PREV_PENDING_AFT_NULL_UNRESOLVED, Verdict.NOT_SETTLED,
              "a clean or rollback plan that could have explained this file could not be read");
        default:
          // Base and log files alike: everything that could tell them apart is excluded upstream.
          return verdict(record, Branch.FDA_FS_PREV_PENDING_AFT_NULL_DANGLING, Verdict.DANGLING,
              "the writing instant was pending at the start of the run and gone by the end of it, "
                  + "and no clean or rollback explains this file");
      }
    }
    return unmatched(record, Branch.FDA_FS_UNMATCHED, prevRes, aftRes);
  }

  /**
   * The metadata table holds the file, storage does not. An MDT-exclusive log file is unattributable
   * (it is not on storage to open, and below table version 8 its name carries the slice's base
   * commit), so it takes the clean-only rule.
   */
  private FileDiffRecord adjudicateMdtExclusive(FileDiffRecord record, String instant,
                                                InstantStateIndex.Resolution prevRes,
                                                InstantStateIndex.Resolution aftRes) {
    if (instant == null) {
      switch (reconcile(record)) {
        case SANCTIONED:
          return verdict(record, Branch.FDA_MDT_NO_INSTANT_SANCTIONED, Verdict.SANCTIONED,
              "a clean or rollback explains this file");
        case UNRESOLVED:
          return verdict(record, Branch.FDA_MDT_NO_INSTANT_UNRESOLVED, Verdict.NOT_SETTLED,
              "a clean or rollback plan that could have explained this file could not be read");
        default:
          return verdict(record, Branch.FDA_MDT_NO_INSTANT_CORRUPTION, Verdict.MDT_CORRUPTION,
              "the metadata table names a file storage does not hold and no clean or rollback "
                  + "explains it");
      }
    }

    InstantState prev = prevRes.getState();
    InstantState aft = aftRes.getState();
    if (prev == InstantState.COMPLETED && aft == InstantState.COMPLETED) {
      switch (reconcile(record)) {
        case SANCTIONED:
          return verdict(record, Branch.FDA_MDT_PREV_DONE_AFT_DONE_SANCTIONED, Verdict.SANCTIONED,
              "a clean or rollback deleted this file");
        case UNRESOLVED:
          return verdict(record, Branch.FDA_MDT_PREV_DONE_AFT_DONE_UNRESOLVED, Verdict.NOT_SETTLED,
              "a clean or rollback plan that could have explained this file could not be read");
        default:
          return verdict(record, Branch.FDA_MDT_PREV_DONE_AFT_DONE_OUTBOUND_DELETION, Verdict.OUTBOUND_DELETION,
              "a completed instant's file is gone from storage and no clean or rollback explains it");
      }
    }
    if (prev == InstantState.COMPLETED && isPendingOrGone(aft)) {
      // As the FS-exclusive twin: the transition alone decides, and r() is not consulted.
      return verdict(record, Branch.FDA_MDT_PREV_DONE_AFT_GONE, Verdict.CODE_BUG,
          "the instant left completed during the run; a completed instant is never observed in any "
              + "other state, so this is a rollback or restore that overlapped the run, a defect in "
              + "the validator's own reads, or a timeline edited outside Hudi's write paths");
    }
    // Every remaining pair. r() runs first: a rollback during the run can free exactly this file,
    // which is what a mid-revert (INFLIGHT, NULL) read looks like.
    switch (reconcile(record)) {
      case SANCTIONED:
        return verdict(record, Branch.FDA_MDT_CATCHALL_SANCTIONED, Verdict.SANCTIONED,
            "a clean or rollback explains this file");
      case UNRESOLVED:
        return verdict(record, Branch.FDA_MDT_CATCHALL_UNRESOLVED, Verdict.NOT_SETTLED,
            "a clean or rollback plan that could have explained this file could not be read");
      default:
        // Does not say "never completed": aft == COMPLETED lands here too, so the pair is reported.
        return verdict(record, Branch.FDA_MDT_CATCHALL_CORRUPTION, Verdict.MDT_CORRUPTION,
            "the metadata table names a file storage does not hold and no clean or rollback "
                + "explains it; its writing instant was " + describe(prevRes) + " at T_prev and "
                + describe(aftRes) + " at T_aft");
    }
  }

  /** The state at {@code T_prev}: the active timeline, else the archive (which may refuse). */
  private InstantStateIndex.Resolution resolvePrev(FileDiffRecord record, String instant) {
    InstantStateIndex.Resolution onActive = prevIndex.resolveAtPrev(instant);
    if (onActive.isResolved()) {
      return onActive;
    }
    return archive.resolve(record.getPartition(), record.getFileGroupId(), instant);
  }

  /**
   * The state at {@code T_aft}: the aft active timeline when it holds the instant; {@code NULL} when
   * the instant left the active timeline during the run (only a rollback can do that, since
   * {@link ArchivalProbe} established the archiver did not run); otherwise the archived answer taken
   * for {@code T_prev}, reused because the archive is constant across the run. An unresolved
   * {@code prev} stays unresolved, so arm 3 reports {@code NOT_SETTLED}.
   */
  private InstantStateIndex.Resolution resolveAft(String instant, InstantStateIndex.Resolution prev) {
    InstantStateIndex.Resolution onAftActive = aftIndex.resolveAtPrev(instant);
    if (onAftActive.isResolved()) {
      return onAftActive;
    }
    // isOnActiveTimeline on both sides: a savepoint borrows its commit's instant time and owns no
    // state, so asking the resolution would invent a departure for an instant that never left.
    if (prevIndex.isOnActiveTimeline(instant) && !aftIndex.isOnActiveTimeline(instant)) {
      return InstantStateIndex.Resolution.of(InstantState.NULL);
    }
    return prev;
  }

  /**
   * Arm 4's exemption for a compaction or clustering instant reverted to requested with its files
   * kept ({@code HoodieTable:712-733}), so it presents as requested at both reads beside a real base
   * file. The action is not available here, so the key is the pair and file type, which also exempts
   * an ordinary write at {@code (REQUESTED, REQUESTED)}: on FS-exclusive that only softens
   * {@code CODE_BUG} to {@code NOT_SETTLED}, and on MDT-exclusive it only changes the diagnosis.
   */
  private static boolean isRevertedTableService(FileDiffRecord record, InstantState prev, InstantState aft) {
    return prev == InstantState.REQUESTED && aft == InstantState.REQUESTED
        && record.getFileType() == MismatchRecord.FileType.BASE;
  }

  /** {@code NULL} and the two pending states, the left-hand side of every "not yet completed" row. */
  private static boolean isNullOrPending(InstantState state) {
    return state == InstantState.NULL || state == InstantState.REQUESTED || state == InstantState.INFLIGHT;
  }

  /** The right-hand side of the rollback-of-a-completed-instant rows (its three aft states). */
  private static boolean isPendingOrGone(InstantState state) {
    return state == InstantState.REQUESTED || state == InstantState.INFLIGHT || state == InstantState.NULL;
  }

  private InstantMembershipResolver.Membership lookUpMembership(FileDiffRecord record) {
    return membership.lookup(record.getPartition(), record.getFileGroupId(), record.getFileName(),
        record.getWritingInstant());
  }

  private PendingServiceReconciler.Outcome reconcile(FileDiffRecord record) {
    return PendingServiceReconciler.reconcile(record.getPartition(), record.getFileName(),
        record.getWritingInstant(), dlt, pendingServices);
  }

  /**
   * Attaches a verdict and records the deciding row via {@link BranchBeacon}; several rows share a
   * verdict, so this is the only way a test or operator can tell them apart.
   */
  private static FileDiffRecord verdict(FileDiffRecord record, Branch branch, Verdict verdict,
                                        String detail) {
    BranchBeacon.hit(branch);
    return record.withVerdict(verdict, detail);
  }

  /**
   * The total fallback. A pair no row names is a gap in this table, not a finding about the table
   * being validated: it is counted and logged loudly, and the record is reported non-fatally.
   */
  private FileDiffRecord unmatched(FileDiffRecord record, Branch branch,
                                   InstantStateIndex.Resolution prev,
                                   InstantStateIndex.Resolution aft) {
    BranchBeacon.hit(branch);
    unmatchedStateCount.incrementAndGet();
    LOG.error("No adjudication row names the state pair ({}, {}) for {} in partition {} "
            + "(diff pattern {}, writing instant {}); reporting NOT_SETTLED",
        describe(prev), describe(aft), record.getFileName(), record.getPartition(),
        record.getDiffPattern(), record.getWritingInstant());
    return record.withVerdict(Verdict.NOT_SETTLED,
        "no adjudication row names the state pair (" + describe(prev) + ", " + describe(aft) + ")");
  }

  private static String describe(InstantStateIndex.Resolution resolution) {
    return resolution.isResolved() ? resolution.getState().name() : "UNRESOLVED";
  }
}
