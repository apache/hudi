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
 * Every decision site of the v2 validator's adjudication surface, as one id per site.
 *
 * <p>The names are a contract: {@link BranchBeacon} logs them and the functional suites grep for
 * them, so a rename is a test break. A new decision site needs a constant here and a
 * {@link BranchBeacon#hit} call at the site. The id names the row, not the verdict, since several
 * rows share a verdict.
 *
 * <p>Run-level pass/fail, skip paths, report writing and failure classification in
 * {@code HoodieMetadataTableValidatorV2} are not enumerated here; they have their own tests and log
 * markers. Constants are grouped by the owning class, in the order that class states its rows.
 */
public enum Branch {

  // ------------------------------------------------------------------ FileDiffAdjudicator: arms

  /** Arms 2-3, fail-fast: a state neither timeline resolved, and fail-fast reads no archive. → {@code SANCTIONED}. */
  FDA_ARM23_UNRESOLVED_FAIL_FAST,
  /** Arms 2-3, exhaustive: the archive could not establish whether the instant ever completed. → {@code NOT_SETTLED}. */
  FDA_ARM23_UNRESOLVED_EXHAUSTIVE,
  /** Arms 2-3, exhaustive, FS-exclusive: the archived completed commit has write stats but no name for this group. → the {@code (COMPLETED, COMPLETED)} row. */
  FDA_ARM23_ARCHIVED_COMPLETED_FS_EXCLUSIVE,
  /** Arm 4: requested at {@code T_aft}, absent-or-requested at {@code T_prev}, a rollback or restore observed during the run explains it. → {@code SANCTIONED}. */
  FDA_ARM4_REQUESTED_SANCTIONED,
  /** Arm 4: requested at {@code T_aft}, absent-or-requested at {@code T_prev}, a plan that could have explained it could not be read. → {@code NOT_SETTLED}. */
  FDA_ARM4_REQUESTED_UNRESOLVED,
  /** Arm 4: requested at {@code T_aft}, absent-or-requested at {@code T_prev}, not a reverted service, nothing explains it. → {@code CODE_BUG}. */
  FDA_ARM4_REQUESTED_NEVER_WRITES,
  /** A record phase 1 emitted with no diff pattern. Defensive; no phase-1 emitter produces one. → {@code NOT_SETTLED}. */
  FDA_PATTERN_ABSENT,
  /** Both sides hold the file and disagree about it, on the shuffled path. → {@code MDT_CORRUPTION}. */
  FDA_BOTH_DISAGREE,
  /** A {@code DiffPattern} constant the switch has no case for. Unreachable while the switch is exhaustive. → {@code NOT_SETTLED}. */
  FDA_PATTERN_UNKNOWN,

  // -------------------------------------------- FileDiffAdjudicator: FS_EXCLUSIVE rows

  /** FS-exclusive, {@code (COMPLETED, COMPLETED)}, commit names the file. The metadata table lost a committed file. → {@code MDT_CORRUPTION}. */
  FDA_FS_PREV_DONE_AFT_DONE_COMMITTED,
  /** FS-exclusive, {@code (COMPLETED, COMPLETED)}, commit read and does not name the file. → {@code DANGLING}. */
  FDA_FS_PREV_DONE_AFT_DONE_DANGLING,
  /** FS-exclusive, {@code (COMPLETED, COMPLETED)}, commit unreadable. → {@code NOT_SETTLED}. */
  FDA_FS_PREV_DONE_AFT_DONE_UNKNOWN,
  /** FS-exclusive, completed at {@code T_prev}, pending-or-gone at {@code T_aft}. → {@code CODE_BUG}. */
  FDA_FS_PREV_DONE_AFT_GONE,
  /** FS-exclusive, open at {@code T_prev}, completed mid-run: the MDT is behind. → {@code SANCTIONED}. */
  FDA_FS_PREV_OPEN_AFT_DONE_SANCTIONED,
  /** FS-exclusive, in flight at {@code T_aft}: an ordinary in-flight write. → {@code SANCTIONED}. */
  FDA_FS_PREV_OPEN_AFT_INFLIGHT,
  /** FS-exclusive, inflight at {@code T_prev} and requested at {@code T_aft}: reverted mid-run. → {@code NOT_SETTLED}. */
  FDA_FS_PREV_INFLIGHT_AFT_REQUESTED,
  /** FS-exclusive base file, requested at both reads: a service reverted with its files kept. → {@code NOT_SETTLED}. */
  FDA_FS_PREV_REQUESTED_AFT_REQUESTED_BASE,
  /** FS-exclusive, absent at both reads, a clean or rollback claims it. → {@code SANCTIONED}. */
  FDA_FS_PREV_NULL_AFT_NULL_SANCTIONED,
  /** FS-exclusive, absent at both reads, a plan that could have explained it was unreadable. → {@code NOT_SETTLED}. */
  FDA_FS_PREV_NULL_AFT_NULL_UNRESOLVED,
  /** FS-exclusive, absent at both reads and nothing explains it: the dangling-file definition. → {@code DANGLING}. */
  FDA_FS_PREV_NULL_AFT_NULL_DANGLING,
  /** FS-exclusive, pending at {@code T_prev} and gone at {@code T_aft}, a clean or rollback claims it. → {@code SANCTIONED}. */
  FDA_FS_PREV_PENDING_AFT_NULL_SANCTIONED,
  /** FS-exclusive, pending then gone, a plan that could have explained it was unreadable. → {@code NOT_SETTLED}. */
  FDA_FS_PREV_PENDING_AFT_NULL_UNRESOLVED,
  /** FS-exclusive, pending then gone with nothing explaining it: a failed write's leftovers. → {@code DANGLING}. */
  FDA_FS_PREV_PENDING_AFT_NULL_DANGLING,
  /** FS-exclusive state pair no row names. Raises the unmatched counter. → {@code NOT_SETTLED}. */
  FDA_FS_UNMATCHED,

  // ------------------------------------------- FileDiffAdjudicator: MDT_EXCLUSIVE rows

  /** MDT-exclusive with no attributable instant, a clean or rollback claims it. → {@code SANCTIONED}. */
  FDA_MDT_NO_INSTANT_SANCTIONED,
  /** MDT-exclusive with no attributable instant, a plan was unreadable. → {@code NOT_SETTLED}. */
  FDA_MDT_NO_INSTANT_UNRESOLVED,
  /** MDT-exclusive with no attributable instant and nothing explaining it. → {@code MDT_CORRUPTION}. */
  FDA_MDT_NO_INSTANT_CORRUPTION,
  /** MDT-exclusive, {@code (COMPLETED, COMPLETED)}, a clean or rollback deleted it. → {@code SANCTIONED}. */
  FDA_MDT_PREV_DONE_AFT_DONE_SANCTIONED,
  /** MDT-exclusive, {@code (COMPLETED, COMPLETED)}, a plan was unreadable. → {@code NOT_SETTLED}. */
  FDA_MDT_PREV_DONE_AFT_DONE_UNRESOLVED,
  /** MDT-exclusive, {@code (COMPLETED, COMPLETED)}, nothing explains the file's disappearance. → {@code OUTBOUND_DELETION}. */
  FDA_MDT_PREV_DONE_AFT_DONE_OUTBOUND_DELETION,
  /** MDT-exclusive twin of {@link #FDA_FS_PREV_DONE_AFT_GONE}. → {@code CODE_BUG}. */
  FDA_MDT_PREV_DONE_AFT_GONE,
  /** MDT-exclusive, every remaining state pair, a clean or rollback claims it. → {@code SANCTIONED}. */
  FDA_MDT_CATCHALL_SANCTIONED,
  /** MDT-exclusive, every remaining state pair, a plan was unreadable. → {@code NOT_SETTLED}. */
  FDA_MDT_CATCHALL_UNRESOLVED,
  /** MDT-exclusive, every remaining state pair, nothing explains it. → {@code MDT_CORRUPTION}. */
  FDA_MDT_CATCHALL_CORRUPTION,

  // ------------------------------- FsMdtDiffDimension.settleWithoutTheTimeline (no shuffle)

  /** Settled without the timeline: both sides hold the file and disagree. → {@code MDT_CORRUPTION}. */
  SWT_BOTH_DISAGREE,
  /** Settled without the timeline: MDT-exclusive log file, a clean or rollback claims it. → {@code SANCTIONED}. */
  SWT_MDT_LOG_SANCTIONED,
  /** Settled without the timeline: MDT-exclusive log file, a plan was unreadable. → {@code NOT_SETTLED}. */
  SWT_MDT_LOG_UNRESOLVED,
  /** Settled without the timeline: MDT-exclusive log file nothing explains. → {@code MDT_CORRUPTION}. */
  SWT_MDT_LOG_CORRUPTION,
  /** Settled without the timeline: a resolved instant whose (pattern, file type) pair takes no shuffle arm. → {@code NOT_SETTLED}. */
  SWT_SHAPE_TAKES_NO_ROW,
  /** Settled without the timeline: no writing instant could be attributed, so it was excluded from adjudication. → {@code NOT_SETTLED}. */
  SWT_NO_WRITING_INSTANT,

  // -------------------------------------------------------------------- DanglingAdjudicator

  /** Duplicate-base-file adjudication: a null partition, file group, name or instant. → {@code NOT_SETTLED}. */
  DAJ_INPUT_INCOMPLETE,
  /** Duplicate-base-file adjudication: the driver could not read the completed timeline at all. → {@code NOT_SETTLED}. */
  DAJ_TIMELINE_UNREADABLE,
  /** Duplicate-base-file adjudication: the instant is completed on the active timeline but its commit metadata would not read. → {@code NOT_SETTLED}. */
  DAJ_ACTIVE_METADATA_UNREADABLE,
  /** Duplicate-base-file adjudication: the commit recorded no path at all for the file group. → {@code NOT_SETTLED}. */
  DAJ_ACTIVE_NO_RECORDED_PATHS,
  /** Duplicate-base-file adjudication: the completed commit names this copy. → {@code REFERENCED}. */
  DAJ_ACTIVE_REFERENCED,
  /** Duplicate-base-file adjudication: the completed commit was read and does not name this copy. → {@code DANGLING_COMMIT_DOES_NOT_NAME_IT}. */
  DAJ_ACTIVE_NOT_NAMED,
  /** Duplicate-base-file adjudication: the archive produced paths and they name this copy. → {@code REFERENCED}. */
  DAJ_ARCHIVE_REFERENCED,
  /** Duplicate-base-file adjudication: the archive produced paths and they do not name this copy. → {@code DANGLING_COMMIT_DOES_NOT_NAME_IT}. */
  DAJ_ARCHIVE_NOT_NAMED,
  /** Duplicate-base-file adjudication: the instant is inside the active window and on no timeline, so it never completed. → {@code DANGLING_NO_COMPLETED_COMMIT}. */
  DAJ_NEVER_COMMITTED,
  /** Duplicate-base-file adjudication: the archived pass found the commit and it recorded no path at all for the file group. → {@code NOT_SETTLED}. */
  DAJ_ARCHIVE_NO_RECORDED_PATHS,
  /** Duplicate-base-file adjudication: the archived pass read everything that could have held the instant and it was not there. → {@code DANGLING_NO_COMPLETED_COMMIT}. */
  DAJ_ARCHIVE_ABSENT,
  /** Duplicate-base-file adjudication: no archived pass ran, or the one that ran could not cover the instant. → {@code NOT_SETTLED}. */
  DAJ_ARCHIVE_UNPROVEN,
  /** Duplicate-base-file adjudication threw. → {@code NOT_SETTLED}. */
  DAJ_THREW,

  // ---------------------------------------------- FsMdtDiffDimension.ArchiveStateLookup

  /** Archived state lookup: a null partition, file group or instant. → unresolved. */
  ASL_INPUT_INCOMPLETE,
  /** Archived state lookup: no working set (fail-fast, empty bucket, v2 layout). → unresolved, not {@code NULL}. */
  ASL_ARCHIVE_NOT_ALLOWED,
  /** Archived state lookup: the instant is not among this task's archived targets. → unresolved. */
  ASL_ARCHIVE_INSTANT_NOT_A_TARGET,
  /** Archived state lookup: the working set found the instant and recorded paths for this file group, so the commit completed. → {@code COMPLETED}. */
  ASL_FOUND_COMPLETED,
  /** Archived state lookup: the working set found the instant and its per-group projection has no entry for THIS file group. → unresolved, never {@code NULL}. */
  ASL_ARCHIVE_RECORDED_NOTHING,
  /** Archived state lookup: {@code NOT_FOUND}, all bytes that could hold the instant read cleanly. → {@code NULL}. */
  ASL_ABSENCE_TRUSTED_NULL,
  /** Archived state lookup: {@code INDETERMINATE}, the pass could not cover the instant. → unresolved. */
  ASL_UNRESOLVED,

  // ------------------------------------------ FsMdtDiffDimension: the run-level decision

  /** The archiver ran mid-dimension, so phase 2 is skipped and no record reaches a verdict. */
  FSM_ARCHIVAL_RAN_INCONCLUSIVE,
  /** The FS-MDT diff's phase-1 listing re-ran after the barrier; silences the whole dimension. */
  FSM_RECOMPUTED_INCONCLUSIVE,

  // ------------------------------------------------- DuplicateBaseFileDetector (phase 1)

  /** A file slice holding two or more base files, decided from the listing alone. */
  DUP_SLICE_CARDINALITY_FINDING,
  /** A file slice holding exactly one base file: the healthy shape, and not a finding. */
  DUP_SLICE_HOLDS_ONE_BASE_FILE,

  // ---------------------------------------- DuplicateBaseFileDimension: classify stage

  /** A duplicate suspect whose writing instant was pending at {@code T_prevD}: an ordinary write. → not reported. */
  DUP_SANCTIONED_PREV_INFLIGHT,
  /** The duplicate dimension's phase-1 listing re-ran after the barrier; silences the whole dimension. */
  DUP_RECOMPUTED_INCONCLUSIVE,
  /** A duplicate suspect whose writing instant first appeared during the run's window. → not reported. */
  DUP_SANCTIONED_AFT_EXCLUSIVE,
  /** A duplicate suspect whose writing instant was not transitioning in {@code [T_prevD, T_aftD]}. → reported. */
  DUP_SURVIVED_FILTER_SET,

  // ------------------------- HoodieMetadataTableValidatorV2: "nothing was validated"

  /** Snapshot unpinnable, fail-fast: success having validated nothing (also logs the pinning-skipped marker). */
  MTV_UNPINNED_FAIL_FAST_SKIPPED,
  /** The same, in exhaustive mode, which raises rather than reporting success. */
  MTV_UNPINNED_EXHAUSTIVE_FAILED,
  /** The archiver ran during the run and the mode is fail-fast: success, having validated nothing. */
  MTV_ARCHIVAL_FAIL_FAST_SKIPPED,
  /** The same, in exhaustive mode, which raises. */
  MTV_ARCHIVAL_EXHAUSTIVE_FAILED,
  /** A fail-fast run that phase 1 recomputed after its barrier: sanctioned, not failed. */
  MTV_RECOMPUTE_FAIL_FAST_SKIPPED,
  /** An exhaustive run that phase 1 recomputed after its barrier: refused. */
  MTV_RECOMPUTE_EXHAUSTIVE_FAILED,

  // ------------------------------------------------------- InstantMembershipResolver (m())

  /** Membership: a null partition, file group, name or instant. → {@code INSTANT_NOT_FOUND}. */
  IMR_INPUT_INCOMPLETE,
  /** Membership: off the active timeline with no archived working set. → {@code INSTANT_NOT_FOUND}. */
  IMR_ARCHIVE_NOT_ALLOWED,
  /** Membership: the completed commit's write stats would not read. → {@code INSTANT_NOT_FOUND}. */
  IMR_ACTIVE_STATS_UNREADABLE,
  /** Membership: the file group's write stats name the file. → {@code COMMITTED}. */
  IMR_ACTIVE_NAMED,
  /** Membership: the file group's write stats were read and do not name the file. → {@code DANGLING}. */
  IMR_ACTIVE_NOT_NAMED,
  /** Membership: the commit claims to have written nothing anywhere, so its silence is a refusal. → {@code INSTANT_NOT_FOUND}. */
  IMR_ACTIVE_NO_WRITE_STATS_AT_ALL,
  /** Membership: the commit records partitions and, under any spelling, not this one. → {@code DANGLING}. */
  IMR_ACTIVE_PARTITION_UNCLAIMED,
  /** Membership: the commit records this partition and no stat for this file group. → {@code DANGLING}. */
  IMR_ACTIVE_FILE_GROUP_UNCLAIMED,
  /** Membership: found under a re-spelled partition key, and the stats name the file. → {@code COMMITTED}. */
  IMR_ACTIVE_RESPELLED_NAMED,
  /** Membership: found under a re-spelled partition key, and the stats do not name the file. → {@code DANGLING}. */
  IMR_ACTIVE_RESPELLED_NOT_NAMED,
  /** Membership: the archived state is not {@code FOUND}, so it proves nothing. → {@code INSTANT_NOT_FOUND}. */
  IMR_ARCHIVE_INSTANT_UNRESOLVED,
  /** Membership: the instant was never a target of this working set (a routing bug). → {@code INSTANT_NOT_FOUND}. */
  IMR_ARCHIVE_INSTANT_NOT_A_TARGET,
  /** Membership: no name for THIS file group, and no completed record of write stats that could prove its absence. → {@code INSTANT_NOT_FOUND}. */
  IMR_ARCHIVE_RECORDED_NOTHING,
  /** Membership: the archived completed commit recorded write stats, none naming this file group. → {@code DANGLING}. */
  IMR_ARCHIVE_GROUP_UNCLAIMED,
  /** Membership: the archived commit's recorded names for this file group include the file. → {@code COMMITTED}. */
  IMR_ARCHIVE_NAMED,
  /** Membership: a CDC file missing from pre-1.0 archived names, which lack cdcStats. → {@code INSTANT_NOT_FOUND}. */
  IMR_ARCHIVE_CDC_REFUSED,
  /** Membership: the archived names for the file group exclude this base or log file. → {@code DANGLING}. */
  IMR_ARCHIVE_NOT_NAMED,
  /** Membership: inside the active window but not completed. → {@code INSTANT_NOT_FOUND}. */
  IMR_INSIDE_WINDOW_BUT_NOT_COMPLETED,
  /** Membership lookup threw. → {@code INSTANT_NOT_FOUND}. */
  IMR_THREW,

  // ------------------------------------------------------ PendingServiceReconciler (r())

  /** Reconciliation: a null file name, which nothing can be asked about. → {@code UNRESOLVED}. */
  PSR_FILE_NAME_ABSENT,
  /** Reconciliation: a completed-mid-run or pending clean or rollback names the file. → {@code SANCTIONED}. */
  PSR_FILE_CLAIMED,
  /** Reconciliation: a rollback observed or planned targets the file's writing instant. → {@code SANCTIONED}. */
  PSR_WRITING_INSTANT_ROLLED_BACK,
  /** Reconciliation: a plan already {@code .inflight} or further covering this partition could not be read. → {@code UNRESOLVED}. */
  PSR_PLAN_UNREADABLE,
  /** Reconciliation: only a {@code .requested} plan, possibly mid-write, was unreadable. → {@code SANCTIONED}. */
  PSR_REQUESTED_ONLY_PLAN_UNREADABLE_SANCTIONED,
  /** Reconciliation: every relevant plan was readable and none accounts for the file. → {@code NA}. */
  PSR_NOTHING_ACCOUNTS_FOR_IT,

  // --------------------------------------- V1ArchiveTimelineInstantBatchResolver

  /** V1 batch resolve: archive directory absent, or listable but empty. → {@code allNotFound}. */
  V1ABR_NO_ARCHIVE_DIRECTORY,
  /** V1 batch resolve: directory has entries, but the catalogue lists no archive file among them. → {@code allIndeterminate}. */
  V1ABR_NO_ARCHIVE_FILES,
  /** V1 batch resolve: storage or the catalogue threw while establishing the archive's shape. → {@code allIndeterminate}. */
  V1ABR_CATALOGUE_UNREADABLE,
  /** V1 batch resolve: the searcher itself threw. → {@code allIndeterminate}. */
  V1ABR_SEARCH_FAILED,
  /** V1 batch resolve: located and projected. → {@code found(instant, value)}. */
  V1ABR_FOUND,
  /** V1 batch resolve: located, but the projector threw or produced nothing for it. → {@code indeterminate}. */
  V1ABR_UNDECODABLE,
  /** V1 batch resolve: every byte that could have held the instant was read cleanly and it was not there. → {@code notFound}. */
  V1ABR_NOT_FOUND,
  /** V1 batch resolve: damage covered the instant, or it was missing from the searcher's map. → {@code indeterminate}. */
  V1ABR_INDETERMINATE;
}
