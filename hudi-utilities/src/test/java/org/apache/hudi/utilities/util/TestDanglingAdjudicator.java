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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

/**
 * The last gate before a delete, held to the standard that a wrong answer here removes a live file.
 *
 * <p>One test per row of the adjudication table, and each asserts the {@link Branch} as well as the
 * {@link DanglingAdjudicator.Outcome}: several rows share an outcome, so an assertion on the outcome
 * alone cannot say which row produced it, and the rows that share {@code NOT_SETTLED} are precisely
 * the ones that would be quietly merged into a blanket refusal.
 *
 * <p><b>No archive I/O anywhere.</b> The adjudicator reads a materialized
 * {@link ArchivedInstantWorkingSet}, so every archived row is a value this class builds directly.
 * What one archived pass over real files produces is {@code TestV1ArchiveTimelineInstantBatchResolver}'s
 * subject, and which resolver a table gets is {@code TestArchiveTimelineInstantBatchResolvers}'.
 *
 * <p>The pairs matter more than the singletons:
 *
 * <ul>
 *   <li>{@link #emptyArchivedProjectionIsNotSettled} and
 *       {@link #archivedProjectionThatNamesOthersIsDangling} differ only in whether the projection
 *       is empty, so neither an implementation that always refuses nor one that always condemns can
 *       satisfy both;
 *   <li>{@link #archivedProjectionThatNamesOthersIsDangling} and
 *       {@link #archivedCommitNamingAnotherGroupOnlyIsNotSettled} hand it the same shape of evidence
 *       — a non-empty archived projection that does not name the file — and differ only in WHICH
 *       file group the projection recorded it under, which is the difference between positive proof
 *       and another group's business;
 *   <li>{@link #archivedInstantProvenAbsentIsDangling} and
 *       {@link #indeterminateArchivedInstantIsNotSettled} differ only in the search state, which is
 *       the whole safety property of the working set;
 *   <li>{@link #fileGroupWithSeveralRecordedPathsIsReferencedNotUnsettled} and
 *       {@link #fileAbsentFromSeveralRecordedPathsIsDangling} hand it the same commit and differ
 *       only in which file is asked about, so an implementation that answers from the path count
 *       rather than from membership fails one of them whichever way it leans.
 * </ul>
 */
class TestDanglingAdjudicator {

  private static final String PARTITION = "2026/08/12";
  private static final String FILE_ID = "fg1";
  private static final String INSTANT_TIME = "1005";
  private static final String BASE_FILE = "fg1_0-1-0_1005.parquet";
  private static final HoodieFileGroupId GROUP = new HoodieFileGroupId(PARTITION, FILE_ID);
  private static final HoodieFileGroupId OTHER_GROUP = new HoodieFileGroupId(PARTITION, "fg2");

  /** An active timeline whose oldest completed commit is newer than {@link #INSTANT_TIME}. */
  private static final List<String> ARCHIVE_BUCKET_WINDOW = Collections.singletonList("2000");

  /** An active timeline whose window contains {@link #INSTANT_TIME}. */
  private static final List<String> ACTIVE_BUCKET_WINDOW = Arrays.asList("1000", "2000");

  // ---------------------------------------------------------------------------------------------
  //  Rows that refuse before either bucket is chosen
  // ---------------------------------------------------------------------------------------------

  /**
   * Each of the four fields is asked about separately: a guard written as one {@code &&} chain
   * would let three of the four through, and the answer would then be drawn from a null partition
   * or an instant nobody ever established.
   */
  @Test
  void nullFieldInTheQuestionIsNotSettled() throws IOException {
    DanglingAdjudicator adjudicator =
        adjudicator(timelineWhoseCommitRecords(ACTIVE_BUCKET_WINDOW, BASE_FILE), null);

    for (String[] question : new String[][] {
        {null, FILE_ID, BASE_FILE, INSTANT_TIME},
        {PARTITION, null, BASE_FILE, INSTANT_TIME},
        {PARTITION, FILE_ID, null, INSTANT_TIME},
        {PARTITION, FILE_ID, BASE_FILE, null}}) {
      assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
          adjudicator.adjudicate(question[0], question[1], question[2], question[3]),
          Arrays.toString(question));
      BranchBeacons.assertTakes(Branch.DAJ_INPUT_INCOMPLETE,
          () -> adjudicator.adjudicate(question[0], question[1], question[2], question[3]));
    }
  }

  /**
   * The driver could not read the completed timeline at all, which arrives here as a {@code null}
   * argument. Without commit content there is nothing to condemn a file on — and specifically not
   * the archived working set, which is present and would otherwise answer.
   */
  @Test
  void unreadableDriverTimelineIsNotSettled() {
    DanglingAdjudicator adjudicator =
        new DanglingAdjudicator(null, "2000", allNotFound(INSTANT_TIME));

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_TIMELINE_UNREADABLE,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  // ---------------------------------------------------------------------------------------------
  //  The active bucket: the instant is at or after the cutoff, so this timeline answers for it
  // ---------------------------------------------------------------------------------------------

  @Test
  void completedCommitThatNamesTheFileIsReferenced() throws IOException {
    DanglingAdjudicator adjudicator = adjudicator(
        timelineWhoseCommitRecords(Collections.singletonList(INSTANT_TIME), BASE_FILE), null);

    assertEquals(DanglingAdjudicator.Outcome.REFERENCED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ACTIVE_REFERENCED,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void completedCommitThatDoesNotNameTheFileIsDangling() throws IOException {
    // The commit named a different write token for the same slice: the retry-duplicate case this
    // whole effort exists for.
    DanglingAdjudicator adjudicator = adjudicator(
        timelineWhoseCommitRecords(Collections.singletonList(INSTANT_TIME), BASE_FILE), null);

    assertEquals(DanglingAdjudicator.Outcome.DANGLING_COMMIT_DOES_NOT_NAME_IT,
        adjudicator.adjudicate(PARTITION, FILE_ID, "fg1_0-2-0_1005.parquet", INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ACTIVE_NOT_NAMED,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, "fg1_0-2-0_1005.parquet", INSTANT_TIME));
  }

  @Test
  void unreadableCommitMetadataIsNotSettled() throws IOException {
    // "I could not check" must never become "nothing references this".
    HoodieTimeline completed = completedTimelineOver(Collections.singletonList(INSTANT_TIME));
    Mockito.when(completed.readCommitMetadata(any()))
        .thenThrow(new IOException("commit file unreadable"));
    DanglingAdjudicator adjudicator = adjudicator(completed, null);

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ACTIVE_METADATA_UNREADABLE,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void zeroRecordedPathsForTheGroupIsNotSettled() throws IOException {
    // Logically this reads as "not referenced". It is refused anyway, because getWriteStats is
    // keyed on the partition-path STRING: one spelling mismatch returns zero for every file in the
    // partition and would condemn the lot in a single run.
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addWriteStat(PARTITION, writeStat("fg2", PARTITION + "/fg2_0-1-0_1005.parquet"));
    DanglingAdjudicator adjudicator = adjudicator(
        timelineWhoseCommitIs(Collections.singletonList(INSTANT_TIME), metadata), null);

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ACTIVE_NO_RECORDED_PATHS,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void partitionTheCommitNeverTouchedIsNotSettled() throws IOException {
    // The partition-path spelling hazard, reached the way it actually occurs.
    // HoodieCommitMetadata.getWriteStats(String) is a bare map lookup returning null for an absent
    // key, so without a null guard in the shared lookup this arrives as an NPE reported as "the
    // commit could not be read" -- the same verdict by accident, for the wrong reason, and the
    // zero-recorded-paths branch this hazard was documented against is never taken.
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addWriteStat("2026/08/13", writeStat(FILE_ID, "2026/08/13/" + BASE_FILE));
    DanglingAdjudicator adjudicator = adjudicator(
        timelineWhoseCommitIs(Collections.singletonList(INSTANT_TIME), metadata), null);

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ACTIVE_NO_RECORDED_PATHS,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void fileGroupWithSeveralRecordedPathsIsReferencedNotUnsettled() throws IOException {
    // The case that makes adjudication possible at all. A MOR delta commit records several
    // paths for one fileId, because write stats reference log files too -- so even a base-file-only
    // caller routinely faces a multi-path file group. Hudi's own keeper-identification rule gives up
    // at paths.size() != 1; reusing it here would mark every such group unsettled and nothing would
    // ever be adjudicated.
    DanglingAdjudicator adjudicator = adjudicator(
        timelineWhoseCommitRecords(Collections.singletonList(INSTANT_TIME),
            ".fg1_1005.log.1_0-1-0", ".fg1_1005.log.2_0-1-0", BASE_FILE), null);

    assertEquals(DanglingAdjudicator.Outcome.REFERENCED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ACTIVE_REFERENCED,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void fileAbsentFromSeveralRecordedPathsIsDangling() throws IOException {
    // Membership's other edge, on the identical commit: three paths recorded for this group and the
    // suspect is not one of them.
    DanglingAdjudicator adjudicator = adjudicator(
        timelineWhoseCommitRecords(Collections.singletonList(INSTANT_TIME),
            ".fg1_1005.log.1_0-1-0", ".fg1_1005.log.2_0-1-0", BASE_FILE), null);

    assertEquals(DanglingAdjudicator.Outcome.DANGLING_COMMIT_DOES_NOT_NAME_IT,
        adjudicator.adjudicate(PARTITION, FILE_ID, "fg1_0-9-0_1005.parquet", INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ACTIVE_NOT_NAMED,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, "fg1_0-9-0_1005.parquet", INSTANT_TIME));
  }

  /**
   * The instant is at or after the cutoff and is on no timeline, so it never completed: inside the
   * window the active timeline is contiguous back to the archival cutoff, and that is what makes
   * this absence proof rather than ignorance.
   *
   * <p>It is also guard 3, the order of questions. The commits the fixture's timeline does hold
   * record this very file name, so an implementation that asked the naming question first — reading
   * whichever commit metadata happened to be to hand — would answer {@code REFERENCED} and leave a
   * genuine orphan on storage forever.
   */
  @Test
  void instantInsideTheWindowOnNoTimelineNeverCommitted() throws IOException {
    DanglingAdjudicator adjudicator =
        adjudicator(timelineWhoseCommitRecords(ACTIVE_BUCKET_WINDOW, BASE_FILE), null);

    assertEquals(DanglingAdjudicator.Outcome.DANGLING_NO_COMPLETED_COMMIT,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_NEVER_COMMITTED,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>The cutoff is exclusive, and this is the case that pins it.</b>
   *
   * <p>{@code InstantBuckets.pokesArchive} returns false for an instant equal to the earliest
   * completed commit, because that instant is still <em>on</em> the active timeline. The working set
   * here says the archive proved the same instant absent — which on the archived path is
   * {@code DANGLING_NO_COMPLETED_COMMIT}, a delete authorisation — so an off-by-one that routed the
   * boundary to the archive would condemn a file the active timeline plainly names.
   */
  @Test
  void instantAtTheEarliestBoundaryUsesTheActiveTimeline() throws IOException {
    HoodieTimeline completed =
        timelineWhoseCommitRecords(Collections.singletonList(INSTANT_TIME), BASE_FILE);
    DanglingAdjudicator adjudicator = adjudicator(completed, allNotFound(INSTANT_TIME));

    assertEquals(INSTANT_TIME, InstantBuckets.earliestCompletedWriteCommit(completed),
        "the fixture only means anything if the suspect sits exactly on the cutoff");
    assertEquals(DanglingAdjudicator.Outcome.REFERENCED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ACTIVE_REFERENCED,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  // ---------------------------------------------------------------------------------------------
  //  The archive bucket: one materialized pass is the whole of the evidence
  // ---------------------------------------------------------------------------------------------

  @Test
  void archivedProjectionThatNamesTheFileIsReferenced() throws IOException {
    DanglingAdjudicator adjudicator = archiveBucketAdjudicator(found(BASE_FILE));

    assertEquals(DanglingAdjudicator.Outcome.REFERENCED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ARCHIVE_REFERENCED,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void archivedProjectionThatNamesOthersIsDangling() throws IOException {
    DanglingAdjudicator adjudicator =
        archiveBucketAdjudicator(found("fg1_0-2-0_1005.parquet", ".fg1_1005.log.1_0-1-0"));

    assertEquals(DanglingAdjudicator.Outcome.DANGLING_COMMIT_DOES_NOT_NAME_IT,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ARCHIVE_NOT_NAMED,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>Found, and the projection for this group is empty: refused, not condemned.</b>
   *
   * <p>It reads as "the commit exists and does not reference this file", which is the archived
   * path's only route to a wrong deletion. It is refused because an empty entry cannot be told apart
   * from a projection that matched no wanted group at all — only the first would be evidence — and
   * because "the commit recorded nothing here" is not a statement about this file. The active path
   * refuses the same shape; see {@link #zeroRecordedPathsForTheGroupIsNotSettled}.
   */
  @Test
  void emptyArchivedProjectionIsNotSettled() throws IOException {
    DanglingAdjudicator adjudicator = archiveBucketAdjudicator(found());

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ARCHIVE_NO_RECORDED_PATHS,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>A commit that recorded some OTHER group of the batch says nothing about this one.</b>
   *
   * <p>The archived projection covers every file group the shuffle partition happened to name. This
   * instant's commit wrote group {@code fg2} and nothing for {@code fg1}, so {@code fg1}'s entry is
   * absent — which must refuse, exactly as an empty entry does.
   *
   * <p>Flattened into one name set per instant, {@code fg1} would be
   * asked whether {@code fg2}'s names contain its file, get a non-empty set that does not, and come
   * back {@code DANGLING_COMMIT_DOES_NOT_NAME_IT} — a delete authorisation, and one that would have
   * been {@code NOT_SETTLED} had {@code fg2} landed in a different shuffle partition. No deletion
   * may depend on how the work was partitioned.
   */
  @Test
  void archivedCommitNamingAnotherGroupOnlyIsNotSettled() throws IOException {
    DanglingAdjudicator adjudicator =
        archiveBucketAdjudicator(foundFor(OTHER_GROUP, "fg2_0-1-0_1005.parquet"));

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ARCHIVE_NO_RECORDED_PATHS,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * The one archived absence that condemns: {@code NOT_FOUND} means the pass read every byte that
   * could have held the instant and it was not there.
   */
  @Test
  void archivedInstantProvenAbsentIsDangling() throws IOException {
    DanglingAdjudicator adjudicator = archiveBucketAdjudicator(allNotFound(INSTANT_TIME));

    assertEquals(DanglingAdjudicator.Outcome.DANGLING_NO_COMPLETED_COMMIT,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ARCHIVE_ABSENT,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * The counterweight to the row above, on evidence identical but for the search state: a pass that
   * could not cover what it needed to proves nothing, and the two states are one enum apart.
   */
  @Test
  void indeterminateArchivedInstantIsNotSettled() throws IOException {
    DanglingAdjudicator adjudicator = archiveBucketAdjudicator(
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>allIndeterminate(
            Collections.singleton(INSTANT_TIME)));

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ARCHIVE_UNPROVEN,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * <b>No pass ran at all, which is what a v2-layout table produces</b>: the LSM archive has no
   * batch resolver, so {@code DuplicateBaseFileDimension} leaves the working set null rather than
   * construct one. A null is a refusal and never an absence — reading it as "the archive holds
   * nothing" would condemn every aged file on every table version the fleet actually runs.
   */
  @Test
  void nullWorkingSetRefusesTheArchiveBucket() throws IOException {
    DanglingAdjudicator adjudicator = archiveBucketAdjudicator(null);

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_ARCHIVE_UNPROVEN,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * A working set that never targeted the instant is a caller bug, and the working set says so by
   * throwing. This class turns it into a refusal rather than letting it kill the Spark task that is
   * adjudicating a whole instant range.
   */
  @Test
  void instantTheArchivedPassNeverTargetedIsNotSettled() throws IOException {
    DanglingAdjudicator adjudicator = archiveBucketAdjudicator(allNotFound("9999"));

    assertEquals(DanglingAdjudicator.Outcome.NOT_SETTLED,
        adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.DAJ_THREW,
        () -> adjudicator.adjudicate(PARTITION, FILE_ID, BASE_FILE, INSTANT_TIME));
  }

  // ---------------------------------------------------------------------------------------------
  //  Fixtures
  // ---------------------------------------------------------------------------------------------

  /**
   * The production wiring: the cutoff is read off the very timeline whose commits answer for the
   * active bucket, exactly as {@code HoodieMetadataTableValidatorV2} reads it off {@code T_prevD}.
   */
  private static DanglingAdjudicator adjudicator(
      HoodieTimeline completedActive,
      ArchivedInstantWorkingSet<ArchivedCommitPaths> archived) {
    return new DanglingAdjudicator(completedActive,
        InstantBuckets.earliestCompletedWriteCommit(completedActive), archived);
  }

  /**
   * An adjudicator whose active window starts after {@link #INSTANT_TIME}, so the suspect lands in
   * the archive bucket and {@code archived} is the whole of the evidence. The active timeline still
   * holds a commit naming the suspect, so a routing bug that sent it to the active path would answer
   * {@code REFERENCED} and be visible rather than agreeing by accident.
   */
  private static DanglingAdjudicator archiveBucketAdjudicator(
      ArchivedInstantWorkingSet<ArchivedCommitPaths> archived) throws IOException {
    return adjudicator(timelineWhoseCommitRecords(ARCHIVE_BUCKET_WINDOW, BASE_FILE), archived);
  }

  /**
   * A pass that found {@link #INSTANT_TIME} and projected exactly these bare names for
   * {@link #GROUP} — the group every question in this class is asked about.
   */
  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> found(
      String... names) {
    return foundFor(GROUP, names);
  }

  /** The same, for a caller that needs the projection to name some OTHER group. */
  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> foundFor(
      HoodieFileGroupId group, String... names) {
    Map<HoodieFileGroupId, Set<String>> byGroup = new LinkedHashMap<>();
    byGroup.put(group, new LinkedHashSet<>(Arrays.asList(names)));
    return ArchivedInstantWorkingSet
        .<ArchivedCommitPaths>builder(Collections.singleton(INSTANT_TIME))
        .found(INSTANT_TIME, ArchivedCommitPaths.namesOnly(byGroup))
        .build();
  }

  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> allNotFound(
      String instantTime) {
    return ArchivedInstantWorkingSet.allNotFound(Collections.singleton(instantTime));
  }

  private static HoodieInstant instantAt(String time) {
    HoodieInstant instant = Mockito.mock(HoodieInstant.class);
    Mockito.when(instant.getAction()).thenReturn(HoodieTimeline.COMMIT_ACTION);
    Mockito.when(instant.requestedTime()).thenReturn(time);
    return instant;
  }

  private static HoodieWriteStat writeStat(String fileId, String path) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(fileId);
    stat.setPath(path);
    return stat;
  }

  private static HoodieTimeline completedTimelineOver(List<String> instantTimes) {
    List<HoodieInstant> instants = new ArrayList<>();
    for (String time : instantTimes) {
      instants.add(instantAt(time));
    }
    HoodieTimeline timeline = Mockito.mock(HoodieTimeline.class);
    Mockito.when(timeline.getInstants()).thenReturn(instants);
    return timeline;
  }

  /**
   * A completed timeline over {@code instantTimes} whose every commit records these names for
   * {@link #FILE_ID} in {@link #PARTITION}.
   */
  private static HoodieTimeline timelineWhoseCommitRecords(List<String> instantTimes,
                                                           String... names) throws IOException {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (String name : names) {
      metadata.addWriteStat(PARTITION, writeStat(FILE_ID, PARTITION + "/" + name));
    }
    return timelineWhoseCommitIs(instantTimes, metadata);
  }

  private static HoodieTimeline timelineWhoseCommitIs(List<String> instantTimes,
                                                      HoodieCommitMetadata metadata)
      throws IOException {
    HoodieTimeline completed = completedTimelineOver(instantTimes);
    Mockito.when(completed.readCommitMetadata(any())).thenReturn(metadata);
    return completed;
  }
}
