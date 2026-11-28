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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;
import org.apache.hudi.utilities.util.archive.InstantSearchState;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rows that <b>refuse</b> — and the archived rows a real table cannot reach.
 *
 * <p>Two populations, both of which have to be tested here rather than end to end, and for two
 * different reasons:
 *
 * <ul>
 *   <li><b>the defensive guards</b> ({@code …_INPUT_INCOMPLETE}, {@code …_THREW},
 *       {@code PSR_FILE_NAME_ABSENT}). These fire on a null field or an exception from a collaborator.
 *       A real run cannot produce them — phase 1 never emits a record without a file name, and the
 *       collaborators do not throw on a healthy table — which is exactly why they exist, and exactly
 *       why they need a test that can inject the condition. Left untested they are the rows most
 *       likely to be deleted as dead code and then be needed;
 *   <li><b>the archived membership rows</b> ({@code IMR_ARCHIVE_*}) and the archived state lookup's
 *       own rows ({@code ASL_*}). Reaching {@code ASL_FOUND_COMPLETED} or an {@code IMR_ARCHIVE_*}
 *       row that returns {@code COMMITTED} from a table needs an archived instant whose commit
 *       records the file group of a file that is <em>still</em> on storage and missing from the
 *       metadata table — and archival never advances ahead of the cleaner, so by the time an instant
 *       is archived the slices it wrote have been deleted. See the workspace inventory's sanction;
 *       {@code TestValidatorV2ArchiveBranches} covers what a real archive <em>can</em> reach. The rest
 *       of {@code ArchiveStateLookup}'s rows are ordinary working-set shapes ({@code NOT_FOUND},
 *       {@code INDETERMINATE}, a {@code FOUND} instant that recorded nothing for this group) that a
 *       real archived pass produces routinely, and are tested here only because they are most cheaply
 *       pinned by construction rather than by growing a real archive to the right shape.
 * </ul>
 *
 * <p>Every case asserts the beacon as well as the answer, through {@link BranchBeacons} — the same
 * mechanism the functional suites use, so a row cannot be "covered" by a test that reached a
 * different one.
 */
class TestAdjudicationRefusalBranches {

  private static final String PARTITION = "2026/08/17";
  private static final String FILE_GROUP_ID = "fg1";
  private static final String INSTANT_TIME = "1005";
  private static final String BASE_FILE = "fg1_0-1-0_1005.parquet";
  private static final String RECORDED_PATH = PARTITION + "/" + BASE_FILE;

  // ------------------------------------------------------------------ defensive guards

  /**
   * <b>A membership question with a null field is {@code INSTANT_NOT_FOUND}</b>, never an answer.
   *
   * <p>Each of the four fields is asked about separately: a guard written as a single
   * {@code &&} chain would let three of the four through, and the resulting answer would be drawn
   * from a null partition or a null instant rather than refused.
   */
  @Test
  void membershipWithAnyNullFieldRefuses() {
    InstantMembershipResolver resolver = new InstantMembershipResolver(bareMetaClient(),
        emptyCompletedTimeline(), null, null);

    // The BEACON per field, not just the answer. `INSTANT_NOT_FOUND` alone proves nothing here: with
    // the guard deleted outright, all four calls fall through to the archived lookup on an unstubbed
    // mock, whose blanket catch answers `INSTANT_NOT_FOUND` too — so four assertEquals would pass
    // while three of the four fields went unguarded. Review finding; that is exactly the shape a
    // single `&&` chain would leave behind, and on a real table a nameless record then reaches
    // `namesOfStats(...).contains(null)` and comes back DANGLING, i.e. a delete authorisation.
    BranchBeacons.assertTakes(Branch.IMR_INPUT_INCOMPLETE,
        () -> resolver.lookup(null, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_INPUT_INCOMPLETE,
        () -> resolver.lookup(PARTITION, null, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_INPUT_INCOMPLETE,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, null, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_INPUT_INCOMPLETE,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, null));

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(null, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME), "null partition");
    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, null, BASE_FILE, INSTANT_TIME), "null file group");
    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, null, INSTANT_TIME), "null file name");
    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, null), "null instant");
  }

  /**
   * <b>A duplicate-base-file adjudication whose name set cannot be built is {@code NOT_SETTLED}</b>,
   * and the exception does not escape.
   *
   * <p>Same contract as the membership lookup's, and it matters more here: this runs on the executors
   * inside a loop over a whole instant range's suspects, so an escaping exception fails the task
   * rather than refusing one file.
   *
   * <p>The throw is injected the way
   * {@link #membershipWhoseNameSetCannotBeBuiltRefusesRatherThanPropagating} injects it, and for the
   * same reason: every other call inside {@code adjudicate}'s try block handles its own failure, and
   * the name set does not. {@code StoragePath(String)} wraps a {@code URISyntaxException} in an
   * {@code IllegalArgumentException}, so a commit recording a path that is not a legal URI reaches
   * the outer handler with nothing in between. A foreign writer putting such a path in commit
   * metadata is not hypothetical.
   */
  @Test
  void danglingAdjudicationThatThrowsRefusesRatherThanPropagating() throws IOException {
    HoodieTimeline completed = timelineWhoseCommitRecordsPath(":/not-a-legal-uri.parquet");
    DanglingAdjudicator adjudicator = new DanglingAdjudicator(completed,
        InstantBuckets.earliestCompletedWriteCommit(completed), null);

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME),
        "a question that could not be asked is not an answer about the file");
    BranchBeacons.assertTakes(Branch.DAJ_THREW,
        () -> adjudicator.adjudicate(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>A membership lookup whose name-set construction throws is {@code INSTANT_NOT_FOUND}</b>, and the
   * exception does not escape.
   *
   * <p>This row was <b>sanctioned as unreachable</b> until a review found the path: the argument was
   * that every call inside {@code lookup}'s try block handles its own failure, and it missed the name
   * set. {@code namesOfStats} builds a {@link StoragePath} per recorded path, and
   * {@code StoragePath(String)} wraps a {@code URISyntaxException} in an
   * {@code IllegalArgumentException} — so a commit recording a path that is not a legal URI reaches the
   * outer handler with nothing in between. A write stat whose path is {@code ":/…"} is enough.
   *
   * <p>The class promises never to throw because it is called from inside a Spark task's adjudication
   * loop: an escaping exception kills the task rather than refusing one file. A malformed recorded path
   * is not hypothetical either — it is exactly the kind of thing a foreign writer puts in commit
   * metadata.
   */
  @Test
  void membershipWhoseNameSetCannotBeBuiltRefusesRatherThanPropagating() throws IOException {
    HoodieTimeline completed = timelineWhoseCommitRecordsPath(":/not-a-legal-uri.parquet");
    InstantMembershipResolver resolver =
        new InstantMembershipResolver(bareMetaClient(), completed, INSTANT_TIME, null);

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME),
        "a name set that could not be built is not an answer about the file");
    BranchBeacons.assertTakes(Branch.IMR_THREW,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>Found under a re-spelled partition key, and the stats do not name the file: {@code DANGLING}.</b>
   *
   * <p>The negative half of the re-spelling rule, and it needs both halves of a conjunction to hold at
   * once: the commit has to record this partition under a <em>different</em> spelling — an absolute
   * path, a leading slash, a scheme — <b>and</b> its stats for this file group have to name some other
   * file. A same-spelling commit with no stat for the group lands on
   * {@code IMR_ACTIVE_FILE_GROUP_UNCLAIMED} instead, which is a different row returning the same
   * answer.
   *
   * <p>Also sanctioned as unreachable until a review pointed out that the re-spelled commit the
   * positive case already builds needs one field changed to reach it. Both rows return
   * {@code DANGLING} and {@code DANGLING} drives a delete, so telling them apart is the difference
   * between "the commit declined to claim this file" and "the map lookup missed an entry that is
   * right there".
   */
  @Test
  void respelledCommitThatNamesAnotherFileOfTheGroupAnswersDangling() throws IOException {
    HoodieTimeline completed = timelineWhoseCommitRecords(
        "file:/tmp/table/" + PARTITION, FILE_GROUP_ID, PARTITION + "/fg1_9-9-9_1005.parquet");
    InstantMembershipResolver resolver =
        new InstantMembershipResolver(bareMetaClient(), completed, INSTANT_TIME, null);

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ACTIVE_RESPELLED_NOT_NAMED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>The three membership rows that all answer {@code DANGLING}, told apart.</b>
   *
   * <p>`DANGLING` drives the fatal verdict that authorises deleting a file, and three different rows
   * produce it: the commit records this partition and no stat for the group; the commit records
   * partitions and, under any spelling, not this one; and the ordinary "the stats were read and do not
   * name it". Until a review pointed it out, <b>no test anywhere asserted which of the three fired</b>
   * — and one existing test's name claimed a row it does not take, which is precisely the drift the
   * beacon exists to prevent.
   */
  @Test
  void theThreeDanglingMembershipRowsAreDistinguishable() throws IOException {
    // (a) the commit records this partition, and a stat for this file group naming another file.
    BranchBeacons.assertTakes(Branch.IMR_ACTIVE_NOT_NAMED,
        () -> lookUpAgainst(timelineWhoseCommitRecordsUnchecked(
            PARTITION, FILE_GROUP_ID, PARTITION + "/fg1_9-9-9_1005.parquet")));

    // (b) the commit records partitions, and not this one under any spelling.
    BranchBeacons.assertTakes(Branch.IMR_ACTIVE_PARTITION_UNCLAIMED,
        () -> lookUpAgainst(timelineWhoseCommitRecordsUnchecked(
            "some/other/partition", FILE_GROUP_ID, "some/other/partition/fg1_0-1-0_1005.parquet")));

    // (c) the commit records this partition and no stat for this file group.
    BranchBeacons.assertTakes(Branch.IMR_ACTIVE_FILE_GROUP_UNCLAIMED,
        () -> lookUpAgainst(timelineWhoseCommitRecordsUnchecked(
            PARTITION, "fg2", PARTITION + "/fg2_0-1-0_1005.parquet")));
  }

  /**
   * <b>A reconciliation with no file name is {@code UNRESOLVED}</b>, not {@code NA}.
   *
   * <p>The distinction is the only thing standing between "nothing accounts for this file" and "there
   * was no file to account for": {@code NA} is what five fatal rows fire on, and a nameless record
   * reaching it would condemn on the strength of a question that was never asked.
   */
  @Test
  void reconciliationWithNoFileNameRefuses() {
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting().build();
    assertEquals(PendingServiceReconciler.Outcome.UNRESOLVED,
        PendingServiceReconciler.reconcile(PARTITION, null, INSTANT_TIME, dlt,
            PendingServiceState.empty()),
        "and specifically not NA, which is the answer five fatal rows act on");
    BranchBeacons.assertTakes(Branch.PSR_FILE_NAME_ABSENT,
        () -> PendingServiceReconciler.reconcile(PARTITION, null, INSTANT_TIME, dlt,
            PendingServiceState.empty()));
  }

  // ------------------------------------------------- the archived rows a table cannot reach

  /**
   * <b>The archived commit records the file: {@code COMMITTED}.</b>
   *
   * <p>The only archived answer that can withdraw a dangling verdict, so the one whose absence would
   * be a silent data-loss bug rather than a missing test.
   */
  @Test
  void archivedCommitThatRecordsTheFileAnswersCommitted() throws IOException {
    InstantMembershipResolver resolver = resolverOverArchived(archivedNaming(RECORDED_PATH));

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_NAMED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>The archived commit records the file group and not this file: {@code DANGLING}.</b>
   *
   * <p>Positive proof rather than an absence — the commit spoke about the group and declined to claim
   * the file — which is what separates this from the two refusals below it.
   */
  @Test
  void archivedCommitThatRecordsAnotherFileAnswersDangling() throws IOException {
    InstantMembershipResolver resolver =
        resolverOverArchived(archivedNaming(PARTITION + "/fg1_9-9-9_1005.parquet"));

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_NOT_NAMED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>The working set found the instant and its per-group projection has no entry for THIS file
   * group: {@code INSTANT_NOT_FOUND}.</b>
   *
   * <p>Present-but-empty is not proof of absence — the commit may genuinely have written nothing for
   * this group, which {@link CommitPathProjector} records as an absent map entry — so it must not
   * become {@code DANGLING}.
   */
  @Test
  void archiveThatRecordedNothingForTheInstantRefuses() throws IOException {
    InstantMembershipResolver resolver = resolverOverArchived(
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
                Collections.singleton(INSTANT_TIME))
            .found(INSTANT_TIME, ArchivedCommitPaths.empty())
            .build());

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_RECORDED_NOTHING,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>The archived pass could not resolve the instant at all: {@code INSTANT_NOT_FOUND}.</b>
   *
   * <p>{@link InstantSearchState#INDETERMINATE} and {@link InstantSearchState#NOT_FOUND} both fall
   * short of {@code FOUND} through the same {@code stateOf} check, and neither is evidence about the
   * file — see {@code TestInstantMembershipResolver} for both states asserted individually.
   */
  @Test
  void archiveThatCouldNotAnswerRefuses() throws IOException {
    InstantMembershipResolver resolver = resolverOverArchived(
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
                Collections.singleton(INSTANT_TIME))
            .indeterminate(INSTANT_TIME)
            .build());

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_INSTANT_UNRESOLVED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>{@code ArchiveStateLookup} answers entirely from the working set it is constructed with.</b>
   *
   * <p>This is the row that proves the class does not read the archive on its own: no
   * {@code warmUp}, no {@code (partition, file group, instant)} cache, and no second archived pass
   * per file group. There is no meta client argument on this constructor for an implementation to
   * use for one; the only way this test can get an answer out of it is by handing it a working set
   * directly, which is exactly what {@code adjudicateBatch}'s single per-task pass does for it.
   */
  @Test
  void archiveStateLookupReadsTheSharedWorkingSetWithoutItsOwnPass() {
    FsMdtDiffDimension.ArchiveStateLookup lookup =
        new FsMdtDiffDimension.ArchiveStateLookup(archivedNaming(RECORDED_PATH));

    InstantStateIndex.Resolution resolution = lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME);
    assertTrue(resolution.isResolved(), "the working set recorded paths, so it placed the instant");
    assertEquals(InstantState.COMPLETED, resolution.getState(),
        "and paths recorded for the file group mean the commit completed");
    BranchBeacons.assertTakes(Branch.ASL_FOUND_COMPLETED,
        () -> lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME));
  }

  /** <b>A null partition, file group or instant is a refusal</b>, never an answer. */
  @Test
  void archiveStateLookupWithANullFieldRefuses() {
    FsMdtDiffDimension.ArchiveStateLookup lookup =
        new FsMdtDiffDimension.ArchiveStateLookup(archivedNaming(RECORDED_PATH));

    assertFalse(lookup.resolve(null, FILE_GROUP_ID, INSTANT_TIME).isResolved());
    assertFalse(lookup.resolve(PARTITION, null, INSTANT_TIME).isResolved());
    assertFalse(lookup.resolve(PARTITION, FILE_GROUP_ID, null).isResolved());
    BranchBeacons.assertTakes(Branch.ASL_INPUT_INCOMPLETE,
        () -> lookup.resolve(null, FILE_GROUP_ID, INSTANT_TIME));
  }

  /**
   * <b>No working set at all is a refusal, never {@code NULL}.</b>
   *
   * <p>Fail-fast, an archive bucket that ended up empty, and a v2-layout table all leave
   * {@code adjudicateBatch}'s shared working set {@code null}; none of them proves an instant never
   * existed, so answering {@code NULL} here would turn "nobody looked" into "looked and found
   * nothing".
   */
  @Test
  void archiveStateLookupWithNoWorkingSetRefuses() {
    FsMdtDiffDimension.ArchiveStateLookup lookup = new FsMdtDiffDimension.ArchiveStateLookup(null);

    InstantStateIndex.Resolution resolution = lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME);
    assertFalse(resolution.isResolved(), "no pass ran for this task, so this must refuse");
    BranchBeacons.assertTakes(Branch.ASL_ARCHIVE_NOT_ALLOWED,
        () -> lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME));
  }

  /**
   * <b>An instant outside this task's archived bucket is a routing bug, not an archive miss.</b>
   *
   * <p>{@code ArchivedInstantWorkingSet.stateOf} throws {@code IllegalArgumentException} for a
   * non-target, so this class must check {@code targets()} before ever calling it -- mirrors
   * {@code InstantMembershipResolver}'s {@code IMR_ARCHIVE_INSTANT_NOT_A_TARGET}.
   */
  @Test
  void archiveStateLookupAskedAboutANonTargetInstantRefuses() {
    ArchivedInstantWorkingSet<ArchivedCommitPaths> workingSet =
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
                Collections.singleton("9999999999999"))
            .notFound("9999999999999")
            .build();
    FsMdtDiffDimension.ArchiveStateLookup lookup = new FsMdtDiffDimension.ArchiveStateLookup(workingSet);

    assertFalse(lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME).isResolved());
    BranchBeacons.assertTakes(Branch.ASL_ARCHIVE_INSTANT_NOT_A_TARGET,
        () -> lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME));
  }

  /**
   * <b>A {@code FOUND} instant whose per-group projection has no entry for THIS file group is a
   * refusal, never {@code NULL}.</b> The commit completed -- {@code FOUND} says so -- but recorded
   * nothing for this particular group, which is not evidence either way; mirrors
   * {@code InstantMembershipResolver}'s {@code IMR_ARCHIVE_RECORDED_NOTHING}.
   */
  @Test
  void archiveStateLookupFoundInstantThatRecordedNothingForThisGroupRefuses() {
    ArchivedInstantWorkingSet<ArchivedCommitPaths> workingSet =
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
                Collections.singleton(INSTANT_TIME))
            .found(INSTANT_TIME, ArchivedCommitPaths.empty())
            .build();
    FsMdtDiffDimension.ArchiveStateLookup lookup = new FsMdtDiffDimension.ArchiveStateLookup(workingSet);

    InstantStateIndex.Resolution resolution = lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME);
    assertFalse(resolution.isResolved());
    assertFalse(resolution.getState() == InstantState.NULL);
    BranchBeacons.assertTakes(Branch.ASL_ARCHIVE_RECORDED_NOTHING,
        () -> lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME));
  }

  /**
   * <b>{@code InstantSearchState.NOT_FOUND}: every byte that could have held the instant was read
   * cleanly and it was not there.</b> The one working-set shape this class trusts as a proven
   * absence.
   */
  @Test
  void archiveStateLookupNotFoundInstantAnswersNull() {
    ArchivedInstantWorkingSet<ArchivedCommitPaths> workingSet =
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
                Collections.singleton(INSTANT_TIME))
            .notFound(INSTANT_TIME)
            .build();
    FsMdtDiffDimension.ArchiveStateLookup lookup = new FsMdtDiffDimension.ArchiveStateLookup(workingSet);

    InstantStateIndex.Resolution resolution = lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME);
    assertTrue(resolution.isResolved());
    assertEquals(InstantState.NULL, resolution.getState());
    BranchBeacons.assertTakes(Branch.ASL_ABSENCE_TRUSTED_NULL,
        () -> lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME));
  }

  /**
   * <b>{@code InstantSearchState.INDETERMINATE}: the pass could not cover what it needed to.</b>
   * Never {@code NULL} and never {@code COMPLETED}.
   */
  @Test
  void archiveStateLookupIndeterminateInstantRefuses() {
    ArchivedInstantWorkingSet<ArchivedCommitPaths> workingSet =
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
                Collections.singleton(INSTANT_TIME))
            .indeterminate(INSTANT_TIME)
            .build();
    FsMdtDiffDimension.ArchiveStateLookup lookup = new FsMdtDiffDimension.ArchiveStateLookup(workingSet);

    assertFalse(lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME).isResolved());
    BranchBeacons.assertTakes(Branch.ASL_UNRESOLVED,
        () -> lookup.resolve(PARTITION, FILE_GROUP_ID, INSTANT_TIME));
  }

  // ---------------------------------------------------------------- fixtures

  /**
   * A completed active timeline holding one commit at {@link #INSTANT_TIME} whose metadata records one
   * write stat, under a caller-chosen partition key, file id and path.
   *
   * <p>The partition key is a parameter because the re-spelling rows turn on it: Hudi metadata mixes
   * absolute and relative forms, and a raw-string comparison silently answers "not found" for
   * everything in the partition.
   */
  private static HoodieTimeline timelineWhoseCommitRecords(String partitionKey, String fileId,
                                                           String path) throws IOException {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(fileId);
    stat.setPath(path);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addWriteStat(partitionKey, stat);

    HoodieInstant instant = Mockito.mock(HoodieInstant.class);
    Mockito.when(instant.requestedTime()).thenReturn(INSTANT_TIME);
    Mockito.when(instant.getAction()).thenReturn(HoodieTimeline.COMMIT_ACTION);
    HoodieTimeline timeline = Mockito.mock(HoodieTimeline.class);
    Mockito.when(timeline.getInstants()).thenReturn(Collections.singletonList(instant));
    Mockito.when(timeline.readCommitMetadata(instant)).thenReturn(metadata);
    return timeline;
  }

  /** The same, recording a path under this partition and file group; for the throwing-path case. */
  private static HoodieTimeline timelineWhoseCommitRecordsPath(String path) throws IOException {
    return timelineWhoseCommitRecords(PARTITION, FILE_GROUP_ID, path);
  }

  /** {@link #timelineWhoseCommitRecords} for a lambda, which cannot throw a checked exception. */
  private static HoodieTimeline timelineWhoseCommitRecordsUnchecked(String partitionKey,
                                                                   String fileId, String path) {
    try {
      return timelineWhoseCommitRecords(partitionKey, fileId, path);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** One membership lookup for {@link #BASE_FILE} against {@code completed}, archive disallowed. */
  private static InstantMembershipResolver.Membership lookUpAgainst(HoodieTimeline completed) {
    return new InstantMembershipResolver(bareMetaClient(), completed, INSTANT_TIME, null)
        .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME);
  }

  private static HoodieTableMetaClient bareMetaClient() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    return metaClient;
  }

  private static HoodieTimeline emptyCompletedTimeline() {
    HoodieTimeline timeline = Mockito.mock(HoodieTimeline.class);
    Mockito.when(timeline.getInstants()).thenReturn(Collections.emptyList());
    Mockito.when(timeline.getInstantsAsStream())
        .thenReturn(Collections.<HoodieInstant>emptyList().stream());
    return timeline;
  }

  /**
   * A resolver whose active timeline holds nothing — so every question goes to the archived working
   * set — and whose archived working set is the one given.
   */
  private static InstantMembershipResolver resolverOverArchived(
      ArchivedInstantWorkingSet<ArchivedCommitPaths> archived) {
    return new InstantMembershipResolver(bareMetaClient(), emptyCompletedTimeline(), null, archived);
  }

  /**
   * A {@code FOUND} working set for {@link #INSTANT_TIME} whose sole file group,
   * ({@link #PARTITION}, {@link #FILE_GROUP_ID}), records exactly the bare name of each given path —
   * mirroring what {@link CommitPathProjector} would have projected from a real archived record.
   */
  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> archivedNaming(
      String... paths) {
    Set<String> names = new LinkedHashSet<>();
    for (String path : paths) {
      names.add(new StoragePath(path).getName());
    }
    Map<HoodieFileGroupId, Set<String>> content = new HashMap<>();
    content.put(new HoodieFileGroupId(PARTITION, FILE_GROUP_ID), names);
    return ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
            Collections.singleton(INSTANT_TIME))
        .found(INSTANT_TIME, ArchivedCommitPaths.namesOnly(content))
        .build();
  }

}
