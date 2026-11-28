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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

/**
 * The reconciliation switch, exercised without Spark, a filesystem or a real table: every fixture
 * here is a hand-built {@link InstantStateIndex}, {@link ServiceActivityDelta}, {@link PendingServiceState}
 * and {@link InstantMembershipResolver}, so what is under test is the table of rows and nothing else.
 *
 * <p>Six method names differ from the brief's by the leading article ("aRolledBackWrite..." ->
 * "rolledBackWrite..."): the repo's checkstyle {@code MethodName} rule is
 * {@code ^[a-z][a-z0-9][a-zA-Z0-9_]*$}, which a one-letter article followed by a capital violates.
 */
class TestFileDiffAdjudicator {

  private static final String PARTITION = "2026/08/15";
  private static final String FILE_GROUP_ID = "fg1";
  private static final String DEFAULT_INSTANT = "100";
  /** A base file of {@link #FILE_GROUP_ID} that no fixture's commit ever names. */
  private static final String UNNAMED_BASE_FILE = "fg1_0-1-0_000.parquet";

  // ---- global arms -------------------------------------------------------

  @Test
  void anUnresolvedStateInFailFastIsSanctioned() {
    // arm 2: fail-fast never reads the archive, so it cannot tell COMPLETED-and-archived from
    // NULL. It declines to condemn rather than guessing.
    assertEquals(Verdict.SANCTIONED, verdictOf(fs("050"), failFast().withEmptyTimeline()));
  }

  @Test
  void anUnresolvedStateInExhaustiveIsNotSettled() {
    Decision decision = decide(fs("050"), exhaustive().withUnreadableArchive());

    assertEquals(Verdict.NOT_SETTLED, decision.verdict());
    // Pins arm 3: deleting it drops this record into the fallback on the same verdict.
    assertTrue(decision.detail().contains("the archive could not establish"), decision.detail());
    assertEquals(0L, decision.unmatched());
  }

  @Test
  void requestedAftStateIsACodeBugWhateverTheDiffPattern() {
    // arm 4 is pattern-independent: CODE_BUG says "this state should not exist", which is a
    // statement about the run, not about which side holds the file.
    assertEquals(Verdict.CODE_BUG, verdictOf(fs("100"), states(InstantState.NULL, InstantState.REQUESTED)));
    assertEquals(Verdict.CODE_BUG, verdictOf(mdt("100"), states(InstantState.NULL, InstantState.REQUESTED)));
  }

  @Test
  void requestedAftStateIsSanctionedWhenARollbackExplainsIt() {
    // The tail of the same rollback-deletion sequence the (COMPLETED, gone) row exists for: absent
    // or barely requested at T_prev, .requested only at T_aft, one delete away from not existing at
    // all (deleteInflightAndRequestedInstant removes .inflight first, .requested second). A
    // rollback naming the instant is positive evidence of exactly this shape, not a gap -- so arm 4
    // is consulted the same way every other fatal row is, before it condemns.
    assertEquals(Verdict.SANCTIONED,
        verdictOf(fs("100"), states(InstantState.NULL, InstantState.REQUESTED)
            .withReconciler(PendingServiceReconciler.Outcome.SANCTIONED)));
    // fsLog, not fs/fsBase: (REQUESTED, REQUESTED) on a base file is the reverted-table-service
    // exemption below, a different row entirely.
    assertEquals(Verdict.SANCTIONED,
        verdictOf(fsLog("100"), states(InstantState.REQUESTED, InstantState.REQUESTED)
            .withReconciler(PendingServiceReconciler.Outcome.SANCTIONED)));
  }

  @Test
  void requestedAftStateWithAnUnreadablePlanIsNotSettledRatherThanACodeBug() {
    // The guard that keeps arm 4 honest: reaching CODE_BUG because one rollback plan happened to be
    // unreadable would condemn a table nobody could actually check.
    Decision decision = decide(fs("100"), states(InstantState.NULL, InstantState.REQUESTED)
        .withReconciler(PendingServiceReconciler.Outcome.UNRESOLVED));

    assertEquals(Verdict.NOT_SETTLED, decision.verdict());
    assertTrue(decision.detail().contains("could not be read"), decision.detail());
  }

  @Test
  void requestedAtBothReadsIsNotACodeBugForARevertedTableService() {
    // The design's one evidence-based exception to arm 4: HoodieTable:712-733 reverts an inflight
    // compaction or clustering instant to requested with deleteInstants=false, deliberately keeping
    // its files, so a base file beside a requested instant is not proof of a bug.
    Decision decision = decide(fsBase("100"), states(InstantState.REQUESTED, InstantState.REQUESTED));

    assertEquals(Verdict.NOT_SETTLED, decision.verdict());
    // Pins the row rather than the fallback: this is a named cell, and a routine reverted
    // compaction must not raise the counter Task 14 asserts is zero.
    assertTrue(decision.detail().contains("requested at both reads"), decision.detail());
    assertEquals(0L, decision.unmatched());
  }

  // ---- FS_EXCLUSIVE ------------------------------------------------------

  @Test
  void anAlreadyCompletedCommitThatNamesTheFileIsMdtCorruption() {
    // THE case the validator exists to detect: MDT was loaded after the commit finished, so a
    // commit that names the file and an MDT that lacks it means MDT lost a committed file.
    assertEquals(Verdict.MDT_CORRUPTION,
        verdictOf(fs("100"), states(InstantState.COMPLETED, InstantState.COMPLETED)
            .withMembership(InstantMembershipResolver.Membership.COMMITTED)));
  }

  @Test
  void anAlreadyCompletedCommitThatDoesNotNameTheFileIsDangling() {
    assertEquals(Verdict.DANGLING,
        verdictOf(fs("100"), states(InstantState.COMPLETED, InstantState.COMPLETED)
            .withMembership(InstantMembershipResolver.Membership.DANGLING)));
  }

  /**
   * An instant open at {@code T_prev} and completed by {@code T_aft} sanctions the file it carries,
   * on every membership answer. The twin of the two cases above, and the reason they cannot be
   * merged: there the metadata table was loaded AFTER the commit finished, here BEFORE it, so here
   * it is legitimately behind.
   *
   * <p>Parameterised over all three membership answers because m() is not consulted on this row at
   * all: an instant still open when the run began may retry a task and leave a second attempt under
   * its own instant, and the commit records only the winner. A row that read that silence as a
   * denial would condemn a live writer's ordinary output.
   */
  @ParameterizedTest
  @EnumSource(InstantMembershipResolver.Membership.class)
  void instantThatCompletedDuringTheRunSanctionsItsFileWhateverTheCommitSays(
      InstantMembershipResolver.Membership membership) {
    Decision decision = decide(fs("100"), states(InstantState.INFLIGHT, InstantState.COMPLETED)
        .withMembership(membership));

    assertEquals(Verdict.SANCTIONED, decision.verdict(), "membership was " + membership);
    // The detail, not just the verdict: a verdict-only assertion here would still pass if this row
    // were deleted and some later SANCTIONED arm caught the record instead.
    assertTrue(decision.detail().contains("open at T_prev and completed during the run"),
        decision.detail());
  }

  /**
   * The same row reached from {@code NULL} rather than a pending state: an instant that did not
   * exist on the timeline at all at {@code T_prev} and had completed by {@code T_aft} is a commit
   * that landed entirely inside the run, and the metadata table is behind it for the same reason.
   */
  @ParameterizedTest
  @EnumSource(InstantMembershipResolver.Membership.class)
  void instantThatAppearedAndCompletedDuringTheRunSanctionsItsFile(
      InstantMembershipResolver.Membership membership) {
    assertEquals(Verdict.SANCTIONED,
        verdictOf(fs("100"), states(InstantState.NULL, InstantState.COMPLETED)
            .withMembership(membership)),
        "membership was " + membership);
  }

  @Test
  void instantNotFoundIsNotSettledNotDangling() {
    assertEquals(Verdict.NOT_SETTLED,
        verdictOf(fs("100"), states(InstantState.COMPLETED, InstantState.COMPLETED)
            .withMembership(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND)));
  }

  @Test
  void anInFlightWriteIsSanctionedEvenThoughItsCommitMetadataIsUnreadable() {
    // A pending instant has no readable commit metadata (getWriteStatsAddedForInstant returns
    // an empty list for a non-commit action), so membership says INSTANT_NOT_FOUND. That must
    // not preempt this row -- m() is a row-specific predicate, not a global arm.
    assertEquals(Verdict.SANCTIONED,
        verdictOf(fs("100"), states(InstantState.NULL, InstantState.INFLIGHT)
            .withMembership(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND)));
  }

  @Test
  void rolledBackWriteIsSanctioned() {
    assertEquals(Verdict.SANCTIONED,
        verdictOf(fs("100"), states(InstantState.INFLIGHT, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.SANCTIONED)));
  }

  /**
   * The new rule: an FS-exclusive file whose writing instant is on neither timeline read is
   * dangling. The archive has already been consulted -- resolvePrev falls back to it, and an
   * absence the archive could not vouch for is unresolved and caught by arm 3 -- so reaching NULL
   * at both reads means no instant this table knows about wrote the file.
   */
  @Test
  void fsExclusiveFileAbsentAtBothReadsIsDangling() {
    assertEquals(Verdict.DANGLING,
        verdictOf(fsBase("100"), states(InstantState.NULL, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.NA)));
    assertEquals(Verdict.DANGLING,
        verdictOf(fsLog("100"), states(InstantState.NULL, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.NA)));
  }

  /**
   * ...and r() is still asked first, so a clean or rollback that positively claims the file
   * withdraws the fatal verdict. This is the invariant the whole table rests on: every fatal row
   * consults the reconciler before it condemns.
   */
  @Test
  void fsExclusiveFileAbsentAtBothReadsIsSanctionedWhenACleanClaimsIt() {
    assertEquals(Verdict.SANCTIONED,
        verdictOf(fsBase("100"), states(InstantState.NULL, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.SANCTIONED)));
  }

  /**
   * A plan that could have explained the file but could not be read is never proof of anything.
   */
  @Test
  void fsExclusiveFileAbsentAtBothReadsIsNotSettledWhenAPlanCouldNotBeRead() {
    assertEquals(Verdict.NOT_SETTLED,
        verdictOf(fs("100"), states(InstantState.NULL, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.UNRESOLVED)));
  }

  /** A write the run watched fail: pending at T_prev, gone at T_aft, nothing explaining the file. */
  @Test
  void writeThatWentPendingToGoneWithNothingExplainingItIsDangling() {
    assertEquals(Verdict.DANGLING,
        verdictOf(fsBase("100"), states(InstantState.REQUESTED, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.NA)));
    assertEquals(Verdict.DANGLING,
        verdictOf(fsLog("100"), states(InstantState.INFLIGHT, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.NA)));
  }

  @Test
  void anUnreadablePlanInTheRolledBackArmIsNotSettled() {
    assertEquals(Verdict.NOT_SETTLED,
        verdictOf(fs("100"), states(InstantState.REQUESTED, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.UNRESOLVED)));
  }

  @Test
  void rollbackOfACompletedInstantIsACodeBugInEveryTransientStateAndForEveryReconcilerAnswer() {
    // revertCompleteToInflight creates .requested and .inflight before deleting .completed, then
    // the teardown deletes inflight then requested. T_aft can land in any of the three, and the row
    // folds all three into one verdict. r() is looped as well as aft: the row does not consult it,
    // and the only way to assert that from outside is to show every answer reaches the same place.
    for (InstantState aft : new InstantState[] {
        InstantState.INFLIGHT, InstantState.REQUESTED, InstantState.NULL}) {
      for (PendingServiceReconciler.Outcome outcome : PendingServiceReconciler.Outcome.values()) {
        Fixture fixture = states(InstantState.COMPLETED, aft).withReconciler(outcome);
        assertEquals(Verdict.CODE_BUG, verdictOf(fs("100"), fixture),
            "aft=" + aft + " r()=" + outcome);
        // Stronger than "the answer did not change the verdict": reconcile() beacons whichever arm
        // it takes, and one record is adjudicated per call, so a PSR_ id here could only have come
        // from this row. Its absence is the assertion that the row never asks.
        Set<Branch> taken = BranchBeacons.takenBy(
            () -> verdictOf(fs("100"), states(InstantState.COMPLETED, aft).withReconciler(outcome)));
        assertTrue(taken.stream().noneMatch(b -> b.name().startsWith("PSR_")),
            "the row must not consult the reconciler; aft=" + aft + " r()=" + outcome
                + " took " + taken);
      }
    }
  }

  @Test
  void revertedInstantIsNotSettled() {
    assertEquals(Verdict.NOT_SETTLED,
        verdictOf(fs("100"), states(InstantState.INFLIGHT, InstantState.REQUESTED)));
  }

  @Test
  void anArchivedCompletedInstantUnchangedByDltIsStillCompletedAtAft() {
    // Section 6.3's aftState rule: an instant dlt reports no change for holds its T_prev state at
    // T_aft. Dropping it would leave every archived instant unresolved at aft, which would turn
    // arm 3 into a blanket NOT_SETTLED and silently disable the whole archived-instant route.
    assertEquals(Verdict.DANGLING,
        verdictOf(fs("100"), exhaustive().withArchiveState(InstantState.COMPLETED)
            .withMembership(InstantMembershipResolver.Membership.DANGLING)));
  }

  // ---- MDT_EXCLUSIVE and BOTH_DISAGREE -----------------------------------

  @Test
  void anMdtExclusiveFileNoCleanExplainsIsOutboundDeletion() {
    assertEquals(Verdict.OUTBOUND_DELETION,
        verdictOf(mdt("100"), states(InstantState.COMPLETED, InstantState.COMPLETED)
            .withReconciler(PendingServiceReconciler.Outcome.NA)));
  }

  @Test
  void anMdtExclusiveFileACleanExplainsIsSanctioned() {
    assertEquals(Verdict.SANCTIONED,
        verdictOf(mdt("100"), states(InstantState.COMPLETED, InstantState.COMPLETED)
            .withReconciler(PendingServiceReconciler.Outcome.SANCTIONED)));
  }

  @Test
  void anMdtExclusiveFileACompletedCleanSuccessfullyDeletedIsSanctioned() {
    // An undamaged table that must pass. A clean completes during phase 1 and successfully
    // deletes a file that is in the latest file slice; the executor lists the partition afterwards,
    // so the file is MDT_EXCLUSIVE with its writing instant COMPLETED at both reads. PendingServiceState
    // cannot help (the clean was not pending at T_prev), so the completed clean's successDeleteFiles is
    // the ONLY evidence there is -- and a ServiceActivityDelta that absorbed only failedDeleteFiles
    // would make this row return fatal OUTBOUND_DELETION.
    //
    // Driven through the real ServiceActivityDelta.between avro path rather than the forTesting() facade,
    // because absorbing that set is precisely what is being pinned.
    FileDiffRecord record = mdt("100").build();
    ServiceActivityDelta dlt = deltaFromACompletedCleanDeleting(record.getFileName());
    FileDiffAdjudicator adjudicator = new FileDiffAdjudicator(
        TestTimelines.indexOf("100", InstantState.COMPLETED),
        TestTimelines.indexOf("100", InstantState.COMPLETED), dlt, PendingServiceState.empty(),
        membershipResolver(new Fixture(), "100", record.getFileName()),
        FileDiffAdjudicator.ArchivedStateLookup.neverReadsTheArchive(),
        false);

    FileDiffRecord decided = adjudicator.adjudicate(record);

    assertEquals(Verdict.SANCTIONED, decided.getVerdict(), decided.getVerdictDetail());
    assertTrue(decided.getVerdictDetail().contains("deleted this file"), decided.getVerdictDetail());
  }

  @Test
  void anMdtExclusiveFileUnderANeverCompletedInstantIsMdtCorruption() {
    assertEquals(Verdict.MDT_CORRUPTION,
        verdictOf(mdt("100"), states(InstantState.NULL, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.NA)));
  }

  @Test
  void anUnattributableMdtExclusiveLogFileTakesTheCleanOnlyRule() {
    assertEquals(Verdict.MDT_CORRUPTION,
        verdictOf(mdtLogNoInstant(), states(InstantState.NULL, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.NA)));
    assertEquals(Verdict.SANCTIONED,
        verdictOf(mdtLogNoInstant(), states(InstantState.NULL, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.SANCTIONED)));
  }

  @Test
  void anUnreadablePlanNeverBecomesOutboundDeletion() {
    // The UNRESOLVED arm of the MDT (COMPLETED, COMPLETED) row. Folding it into the default would
    // ship a fatal deletion authorisation off a plan nobody could read.
    assertEquals(Verdict.NOT_SETTLED,
        verdictOf(mdt("100"), states(InstantState.COMPLETED, InstantState.COMPLETED)
            .withReconciler(PendingServiceReconciler.Outcome.UNRESOLVED)));
  }

  @Test
  void anUnreadablePlanNeverCondemnsAnUnattributableMdtExclusiveLogFile() {
    // The UNRESOLVED arm of the clean-only rule, guarding a fatal MDT_CORRUPTION.
    assertEquals(Verdict.NOT_SETTLED,
        verdictOf(mdtLogNoInstant(), states(InstantState.NULL, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.UNRESOLVED)));
  }

  @Test
  void anMdtExclusiveFileWhoseInstantLeftTheTimelineWithNoExplanationIsACodeBug() {
    // The whole (COMPLETED, pending/gone) row with nothing explaining the departure. Distinct from
    // (COMPLETED, COMPLETED)'s OUTBOUND_DELETION, which is why ServiceActivityDelta must report a
    // departure rather than let the instant carry its T_prev state forward.
    assertEquals(Verdict.CODE_BUG,
        verdictOf(mdt("100"), states(InstantState.COMPLETED, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.NA)));
  }

  @Test
  void anMdtExclusiveFileWhoseCompletedInstantWasRolledBackDuringTheRunIsACodeBug() {
    // A rollback that freed exactly this file name is the strongest explanation the reconciler can
    // offer, and this row still does not take it: the departure from completed is the finding.
    assertEquals(Verdict.CODE_BUG,
        verdictOf(mdt("100"), states(InstantState.COMPLETED, InstantState.INFLIGHT)
            .withRollbackFreeingTheFile()));
  }

  @Test
  void anMdtExclusiveFileARollbackFreedIsSanctionedRatherThanCorruptByExclusion() {
    // A mid-revert read on a healthy table: the instant is INFLIGHT at T_prev and gone at T_aft,
    // and a rollback in dlt freed exactly this file name. Concluding corruption by exclusion --
    // without asking r() -- would fail the run on it.
    assertEquals(Verdict.SANCTIONED,
        verdictOf(mdt("100"), states(InstantState.INFLIGHT, InstantState.NULL)
            .withRollbackFreeingTheFile()));
  }

  @Test
  void anMdtExclusiveFileNothingExplainsStaysMdtCorruption() {
    Decision decision = decide(mdt("100"), states(InstantState.INFLIGHT, InstantState.NULL)
        .withReconciler(PendingServiceReconciler.Outcome.NA));

    assertEquals(Verdict.MDT_CORRUPTION, decision.verdict());
    // The pairs routed here include aft == COMPLETED, so the reason must not claim the instant
    // never completed. It reports the pair instead.
    assertTrue(decision.detail().contains("INFLIGHT at T_prev"), decision.detail());
  }

  @Test
  void anUnreadablePlanNeverCondemnsAnMdtExclusiveFileByExclusion() {
    assertEquals(Verdict.NOT_SETTLED,
        verdictOf(mdt("100"), states(InstantState.INFLIGHT, InstantState.NULL)
            .withReconciler(PendingServiceReconciler.Outcome.UNRESOLVED)));
  }

  @Test
  void bothDisagreeIsAlwaysMdtCorruption() {
    assertEquals(Verdict.MDT_CORRUPTION, verdictOf(both(), states(InstantState.COMPLETED, InstantState.COMPLETED)));
  }

  // ---- the fallback ------------------------------------------------------

  @Test
  void anUnnamedStatePairIsNotSettledAndIsCounted() {
    // The total fallback, reached here by a record carrying no diff pattern. Every foreseen shape
    // now has a row: the pattern tables are total over resolved state pairs and arms 2 and 3 catch
    // the unresolved ones. An FS-exclusive record with no attributable writing instant is NOT among
    // them -- phase 1 drops it into countDrop and takesTheAdjudicationShuffle keeps it out, so it
    // never reaches this class at all. What is left for the fallback is what this design did not
    // foresee -- reported, counted, logged, non-fatal.
    FileDiffAdjudicator adjudicator = adjudicatorFor(
        states(InstantState.COMPLETED, InstantState.COMPLETED).withMembership(null));
    FileDiffRecord decided = adjudicator.adjudicate(unmatchable());

    assertEquals(Verdict.NOT_SETTLED, decided.getVerdict());
    assertEquals(1L, adjudicator.getUnmatchedStateCount());
  }

  /**
   * A {@link ServiceActivityDelta} built the way a real run builds one: a clean absent from the timeline
   * at {@code T_prev} and completed at {@code T_aft}, recording {@code fileName} among its
   * {@code successDeleteFiles}.
   */
  private static ServiceActivityDelta deltaFromACompletedCleanDeleting(String fileName) {
    HoodieCleanPartitionMetadata cleanPartition = HoodieCleanPartitionMetadata.newBuilder()
        .setPartitionPath(PARTITION)
        .setPolicy("KEEP_LATEST_COMMITS")
        .setDeletePathPatterns(Collections.singletonList(fileName))
        .setSuccessDeleteFiles(Collections.singletonList("s3://bucket/tbl/" + PARTITION + "/" + fileName))
        .setFailedDeleteFiles(Collections.emptyList())
        .setIsPartitionDeleted(false)
        .build();
    HoodieCleanMetadata cleanMetadata = HoodieCleanMetadata.newBuilder()
        .setStartCleanTime("700")
        .setTimeTakenInMillis(5L)
        .setTotalFilesDeleted(1)
        .setEarliestCommitToRetain("100")
        .setLastCompletedCommitTimestamp("100")
        .setPartitionMetadata(Collections.singletonMap(PARTITION, cleanPartition))
        .setVersion(2)
        .build();
    HoodieInstant writingCommit =
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "100");
    HoodieInstant cleanInstant =
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "700");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(cleanInstant, TestTimelines.serialize(cleanMetadata, HoodieCleanMetadata.class)),
        writingCommit, cleanInstant);
    return ServiceActivityDelta.between(TestTimelines.indexOf("100", InstantState.COMPLETED), aft, bareMetaClient());
  }

  private static Rec fs(String instant) {
    return fsBase(instant);
  }

  private static Rec fsBase(String instant) {
    return new Rec(baseRecord(instant).withDiffPattern(DiffPattern.FS_EXCLUSIVE));
  }

  private static Rec fsLog(String instant) {
    return new Rec(FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileName(logFileOf(instant))
        .withFileGroupId(FILE_GROUP_ID)
        .withFileType(MismatchRecord.FileType.LOG)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withWritingInstant(instant, MismatchRecord.InstantSource.LOG_BLOCK_HEADER));
  }

  /** A base-file record the metadata table lists and storage does not hold. */
  private static Rec mdt(String instant) {
    return new Rec(baseRecord(instant).withDiffPattern(DiffPattern.MDT_EXCLUSIVE));
  }

  /**
   * An MDT-exclusive log file. Unattributable on every table version: it is not on storage to open,
   * and below version 8 its name carries the slice's base commit rather than the writing one.
   */
  private static Rec mdtLogNoInstant() {
    return new Rec(FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileName(logFileOf(DEFAULT_INSTANT))
        .withFileGroupId(FILE_GROUP_ID)
        .withFileType(MismatchRecord.FileType.LOG)
        .withDiffPattern(DiffPattern.MDT_EXCLUSIVE)
        .withUnresolvedInstant(MismatchRecord.InstantUnresolvedReason.NAME_NOT_PARSEABLE));
  }

  private static Rec both() {
    return new Rec(baseRecord(DEFAULT_INSTANT).withDiffPattern(DiffPattern.BOTH_DISAGREE));
  }

  /**
   * A record nothing in the table can name: it carries no {@link DiffPattern} at all, so no row
   * selects it whatever its states are. Every <em>foreseen</em> shape now has a row of its own --
   * the pattern rows are total over resolved pairs and arms 2 and 3 catch the unresolved ones --
   * which is the point: the fallback is reserved for shapes this design did not foresee.
   */
  private static FileDiffRecord unmatchable() {
    return baseRecord(DEFAULT_INSTANT).build();
  }

  private static FileDiffRecord.Builder baseRecord(String instant) {
    return FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileName(baseFileOf(instant))
        .withFileGroupId(FILE_GROUP_ID)
        .withFileType(MismatchRecord.FileType.BASE)
        .withWritingInstant(instant, MismatchRecord.InstantSource.FILE_NAME);
  }

  private static String baseFileOf(String instant) {
    return FILE_GROUP_ID + "_0-1-0_" + instant + ".parquet";
  }

  private static String logFileOf(String instant) {
    return "." + FILE_GROUP_ID + "_" + instant + ".log.1_0-1-0";
  }

  /** A record still being built, so the brief's {@code fs("100").build()} reads as written. */
  private static final class Rec {

    private final FileDiffRecord.Builder builder;

    private Rec(FileDiffRecord.Builder builder) {
      this.builder = builder;
    }

    FileDiffRecord build() {
      return builder.build();
    }
  }

  // ---------------------------------------------------------------------------------------------
  //  The adjudicator's inputs
  // ---------------------------------------------------------------------------------------------

  /** What the four broadcast inputs should say. Every field is "nothing configured" by default. */
  private static final class Fixture {

    private InstantState prev;
    private InstantState aft;
    private InstantState archiveState;
    private InstantMembershipResolver.Membership membership;
    private PendingServiceReconciler.Outcome reconciler = PendingServiceReconciler.Outcome.NA;
    private boolean failFast;
    private boolean archiveUnreadable;
    private boolean sanctionByRollback;

    Fixture withMembership(InstantMembershipResolver.Membership value) {
      this.membership = value;
      return this;
    }

    Fixture withReconciler(PendingServiceReconciler.Outcome value) {
      this.reconciler = value;
      return this;
    }

    /** Sanctioned by a rollback observed during the run rather than by a clean. */
    Fixture withRollbackFreeingTheFile() {
      this.reconciler = PendingServiceReconciler.Outcome.SANCTIONED;
      this.sanctionByRollback = true;
      return this;
    }

    /** Nothing on the active timeline at either read, so both states go to the archive. */
    Fixture withEmptyTimeline() {
      this.prev = null;
      this.aft = null;
      return this;
    }

    /** The archive could not answer -- a failed or incomplete read, never a guess. */
    Fixture withUnreadableArchive() {
      this.archiveUnreadable = true;
      return this;
    }

    /** The archive answered positively, for an instant the active timeline does not hold. */
    Fixture withArchiveState(InstantState value) {
      this.archiveState = value;
      return this;
    }

  }

  private static Fixture states(InstantState prev, InstantState aft) {
    Fixture fixture = new Fixture();
    fixture.prev = prev;
    fixture.aft = aft;
    return fixture;
  }

  private static Fixture failFast() {
    Fixture fixture = new Fixture();
    fixture.failFast = true;
    return fixture;
  }

  private static Fixture exhaustive() {
    return new Fixture();
  }

  private static Verdict verdictOf(Rec rec, Fixture fixture) {
    return decide(rec, fixture).verdict();
  }

  /**
   * The whole outcome, not just the verdict. Every non-fatal arm collapses to {@code NOT_SETTLED},
   * so a test that asserts the verdict alone passes even when the arm it is meant to pin has been
   * deleted and some later arm caught the record instead. The detail string and the unmatched
   * counter are what tell those arms apart.
   */
  private static final class Decision {

    private final FileDiffRecord decided;
    private final FileDiffAdjudicator adjudicator;

    private Decision(FileDiffRecord decided, FileDiffAdjudicator adjudicator) {
      this.decided = decided;
      this.adjudicator = adjudicator;
    }

    Verdict verdict() {
      return decided.getVerdict();
    }

    String detail() {
      return decided.getVerdictDetail();
    }

    long unmatched() {
      return adjudicator.getUnmatchedStateCount();
    }
  }

  private static Decision decide(Rec rec, Fixture fixture) {
    FileDiffRecord record = rec.build();
    FileDiffAdjudicator adjudicator = adjudicatorFor(fixture, record);
    return new Decision(adjudicator.adjudicate(record), adjudicator);
  }

  private static FileDiffAdjudicator adjudicatorFor(Fixture fixture) {
    return adjudicatorFor(fixture, null);
  }

  /**
   * An instant that left the active timeline during the run reads {@code NULL} at {@code T_aft} --
   * and the stake is a fatal verdict.
   *
   * <p>The delta holds no instant states; the aft timeline is asked directly, and a departure is
   * "on prev's active timeline, not on aft's". Only two
   * actors remove an instant -- a rollback and the archiver -- and {@code ArchivalProbe} has
   * already excluded the archiver by the time any record is adjudicated.
   *
   * <p>Getting this wrong is not cosmetic: both readings reach a fatal verdict, but not the same
   * one. An MDT-exclusive file whose instant departed must reach {@code (COMPLETED, gone)}, which
   * -- with nothing explaining the departure, as here -- is {@code CODE_BUG}: the defect is in the
   * validator or the timeline, not the table. If the aft resolution instead carried the prev state
   * forward, the same file would reach {@code (COMPLETED, COMPLETED)} and be condemned
   * {@code OUTBOUND_DELETION} on identical evidence -- telling an operator committed data vanished,
   * which is a different, wrong, and much louder diagnosis.
   */
  @Test
  void instantThatLeftTheActiveTimelineReadsAsGoneAtAft() {
    FileDiffRecord record = mdt("100").build();
    FileDiffAdjudicator adjudicator = new FileDiffAdjudicator(
        TestTimelines.indexOf("100", InstantState.COMPLETED),
        // T_aft holds a different instant entirely: 100 departed while the run was in progress.
        InstantStateIndex.from(TestTimelines.of(TestTimelines.instant(
            HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "200"))),
        ServiceActivityDelta.forTesting().build(), PendingServiceState.empty(),
        membershipResolver(new Fixture(), "100", record.getFileName()),
        FileDiffAdjudicator.ArchivedStateLookup.neverReadsTheArchive(),
        false);

    assertEquals(Verdict.CODE_BUG, adjudicator.adjudicate(record).getVerdict(),
        "a departed instant must not be read as still completed, which would condemn this file as "
            + "OUTBOUND_DELETION instead of CODE_BUG");
  }

  /**
   * A savepoint borrows the instant time of the commit it savepoints and owns no state of its own,
   * so it must not be mistaken for a departure.
   *
   * <p>The consequence if it were: the invented {@code NULL} at {@code T_aft} would override the
   * archive's answer for exactly the savepointed-and-archived instants the archived route exists to
   * adjudicate, and that route would go quietly dead.
   */
  @Test
  void savepointsBorrowedInstantTimeIsNotADeparture() {
    InstantStateIndex prev = InstantStateIndex.from(TestTimelines.of(TestTimelines.instant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "100")));
    assertTrue(prev.getOrderedInstants().contains("100"));
    assertFalse(prev.isOnActiveTimeline("100"),
        "fixture check: the index must own no state for a savepoint's borrowed time");

    FileDiffRecord record = mdt("100").build();
    FileDiffAdjudicator adjudicator = new FileDiffAdjudicator(prev,
        InstantStateIndex.from(TestTimelines.of()), ServiceActivityDelta.forTesting().build(),
        PendingServiceState.empty(),
        membershipResolver(new Fixture(), "100", record.getFileName()),
        // The archive is what actually places this instant, at BOTH ends: it is constant across the
        // run, so the answer taken for T_prev is reused for T_aft rather than re-derived.
        (partitionPath, fileGroupId, instantTime) ->
            InstantStateIndex.Resolution.of(InstantState.COMPLETED),
        false);

    assertEquals(Verdict.OUTBOUND_DELETION, adjudicator.adjudicate(record).getVerdict(),
        "the archived answer must survive to T_aft; an invented departure would hide this file");
  }

  /**
   * The archived answer is taken once and reused for both ends, which is what the archival
   * invariant buys: the archive cannot change while the run is in progress, so a second archived
   * pass would read the same bytes to reach the same conclusion.
   */
  @Test
  void theArchivedAnswerIsResolvedOnceAndReusedForBothEnds() {
    AtomicLong archiveReads = new AtomicLong();
    FileDiffRecord record = mdt("100").build();
    FileDiffAdjudicator adjudicator = new FileDiffAdjudicator(
        InstantStateIndex.from(TestTimelines.of()), InstantStateIndex.from(TestTimelines.of()),
        ServiceActivityDelta.forTesting().build(), PendingServiceState.empty(),
        membershipResolver(new Fixture(), "100", record.getFileName()),
        (partitionPath, fileGroupId, instantTime) -> {
          archiveReads.incrementAndGet();
          return InstantStateIndex.Resolution.of(InstantState.COMPLETED);
        },
        false);

    adjudicator.adjudicate(record);

    assertEquals(1L, archiveReads.get(),
        "the archive is constant across the run, so one lookup answers for T_prev and T_aft");
  }

  /**
   * A storage-only file whose archived completed commit recorded write stats but no name for its
   * group reaches the {@code (COMPLETED, COMPLETED)} row, where membership decides; the same archive
   * answer leaves the metadata-table side and a commit without write stats refusing.
   */
  @Test
  void archivedCompletedCommitLetsMembershipDecideAStorageOnlyFile() {
    FileDiffRecord record = fs("100").build();

    assertEquals(Verdict.DANGLING,
        archivedCompletedAdjudicator(record, true, InstantMembershipResolver.Membership.DANGLING)
            .adjudicate(record).getVerdict());
    BranchBeacons.assertTakes(Branch.FDA_ARM23_ARCHIVED_COMPLETED_FS_EXCLUSIVE,
        () -> archivedCompletedAdjudicator(record, true, InstantMembershipResolver.Membership.DANGLING)
            .adjudicate(record));
    assertEquals(Verdict.NOT_SETTLED,
        archivedCompletedAdjudicator(record, true, InstantMembershipResolver.Membership.INSTANT_NOT_FOUND)
            .adjudicate(record).getVerdict(), "membership that cannot answer still refuses");
    assertEquals(Verdict.NOT_SETTLED,
        archivedCompletedAdjudicator(record, false, InstantMembershipResolver.Membership.DANGLING)
            .adjudicate(record).getVerdict(), "no completed write stats in the archive: refuse");
  }

  @Test
  void archivedCompletedCommitDoesNotChangeTheMetadataTableSide() {
    FileDiffRecord record = mdt("100").build();

    assertEquals(Verdict.NOT_SETTLED,
        archivedCompletedAdjudicator(record, true, InstantMembershipResolver.Membership.DANGLING)
            .adjudicate(record).getVerdict());
  }

  private static FileDiffAdjudicator archivedCompletedAdjudicator(
      FileDiffRecord record, boolean completedWithWriteStats,
      InstantMembershipResolver.Membership membership) {
    FileDiffAdjudicator.ArchivedStateLookup archive = new FileDiffAdjudicator.ArchivedStateLookup() {
      @Override
      public InstantStateIndex.Resolution resolve(String partitionPath, String fileGroupId,
                                                  String instantTime) {
        return InstantStateIndex.Resolution.unresolved();
      }

      @Override
      public boolean isCompletedWithWriteStats(String instantTime) {
        return completedWithWriteStats;
      }
    };
    return new FileDiffAdjudicator(InstantStateIndex.from(TestTimelines.of()),
        InstantStateIndex.from(TestTimelines.of()), ServiceActivityDelta.forTesting().build(),
        PendingServiceState.empty(),
        membershipResolver(new Fixture().withMembership(membership), record.getWritingInstant(),
            record.getFileName()),
        archive, false);
  }

  private static FileDiffAdjudicator adjudicatorFor(Fixture fixture, FileDiffRecord record) {
    String instant = record == null || record.getWritingInstant() == null
        ? DEFAULT_INSTANT : record.getWritingInstant();
    String fileName = record == null ? baseFileOf(DEFAULT_INSTANT) : record.getFileName();

    InstantStateIndex prevIndex = fixture.prev == null || fixture.prev == InstantState.NULL
        ? InstantStateIndex.from(TestTimelines.of())
        : TestTimelines.indexOf(instant, fixture.prev);

    // The aft state is a timeline, not an entry in a state diff. An unset fixture.aft means
    // "nothing changed", which is the prev state carried forward.
    InstantState aftState = fixture.aft != null ? fixture.aft : fixture.prev;
    InstantStateIndex aftIndex = aftState == null || aftState == InstantState.NULL
        ? InstantStateIndex.from(TestTimelines.of())
        : TestTimelines.indexOf(instant, aftState);

    ServiceActivityDelta.Builder delta = ServiceActivityDelta.forTesting();
    if (fixture.reconciler == PendingServiceReconciler.Outcome.SANCTIONED) {
      if (fixture.sanctionByRollback) {
        delta.withRollbackFreedFile(PARTITION, fileName);
      } else {
        delta.withCleanFreedFile(PARTITION, fileName);
      }
    } else if (fixture.reconciler == PendingServiceReconciler.Outcome.UNRESOLVED) {
      delta.withReadFailure(PARTITION);
    }

    return new FileDiffAdjudicator(prevIndex, aftIndex, delta.build(), PendingServiceState.empty(),
        membershipResolver(fixture, instant, fileName), archiveLookup(fixture),
        fixture.failFast);
  }

  /**
   * Fail-fast never reads the archive at all, so a fail-fast fixture gets the lookup
   * production wires for it whatever else the fixture says. Keying on {@code archiveUnreadable}
   * alone would let a test pin an archive-derived verdict -- {@code DANGLING} among them -- that no
   * fail-fast run can produce.
   */
  private static FileDiffAdjudicator.ArchivedStateLookup archiveLookup(Fixture fixture) {
    if (fixture.failFast || fixture.archiveUnreadable) {
      return FileDiffAdjudicator.ArchivedStateLookup.neverReadsTheArchive();
    }
    final InstantState answer = fixture.archiveState != null ? fixture.archiveState
        : (fixture.prev == InstantState.NULL ? InstantState.NULL : null);
    if (answer == null) {
      return FileDiffAdjudicator.ArchivedStateLookup.neverReadsTheArchive();
    }
    return (partitionPath, fileGroupId, instantTime) -> InstantStateIndex.Resolution.of(answer);
  }

  /**
   * A real {@link InstantMembershipResolver} over a hand-built completed timeline, rather than a
   * stub: COMMITTED is a commit that names the file, DANGLING a commit that records this file group
   * and does not, and everything else a resolver that cannot place the instant at all.
   */
  private static InstantMembershipResolver membershipResolver(Fixture fixture, String instant,
                                                              String fileName) {
    if (fixture.membership == InstantMembershipResolver.Membership.COMMITTED) {
      return resolverNaming(instant, fileName);
    }
    if (fixture.membership == InstantMembershipResolver.Membership.DANGLING) {
      return resolverNaming(instant, UNNAMED_BASE_FILE);
    }
    return new InstantMembershipResolver(bareMetaClient(), TestTimelines.of(), null, null);
  }

  private static InstantMembershipResolver resolverNaming(String instant, String name) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(FILE_GROUP_ID);
    stat.setPath(PARTITION + "/" + name);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addWriteStat(PARTITION, stat);

    HoodieInstant completed = Mockito.mock(HoodieInstant.class);
    Mockito.when(completed.getAction()).thenReturn(HoodieTimeline.COMMIT_ACTION);
    Mockito.when(completed.requestedTime()).thenReturn(instant);
    HoodieTimeline timeline = Mockito.mock(HoodieTimeline.class);
    Mockito.when(timeline.getInstants()).thenReturn(Collections.singletonList(completed));
    try {
      Mockito.when(timeline.readCommitMetadata(any())).thenReturn(metadata);
    } catch (IOException e) {
      throw new HoodieIOException("Stubbing a mock cannot throw", e);
    }
    return new InstantMembershipResolver(bareMetaClient(), timeline, instant, null);
  }

  private static HoodieTableMetaClient bareMetaClient() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    return metaClient;
  }
}
