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
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

/**
 * Batched membership lookup that answers COMMITTED / DANGLING / INSTANT_NOT_FOUND for one
 * (file, writing-instant) pair. Fixtures are adapted from {@link TestDanglingAdjudicator}: the
 * constructor here takes the completed-active timeline directly rather than deriving it from the
 * meta client, so the active timeline and the (optional) archive are staged separately.
 */
class TestInstantMembershipResolver {

  private static final String PARTITION = "2026/08/12";
  private static final String FILE_GROUP_ID = "fg1";
  private static final String INSTANT_TIME = "1005";
  private static final String BASE_FILE = "fg1_0-1-0_1005.parquet";

  @Test
  void fileNamedByAStatPathIsCommitted() throws IOException {
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = activeTimelineNaming(BASE_FILE);

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void anAbsoluteStatPathStillMatchesTheBareSuspectName() throws IOException {
    // The single most dangerous hazard in this class: Hudi commit metadata mixes absolute and bare
    // path forms, and every comparison must be on the bare name. A mismatched form here would
    // silently return INSTANT_NOT_FOUND for every file in the partition, which reads as a clean
    // table while feeding a fatal verdict downstream.
    HoodieTableMetaClient metaClient = bareMetaClient();
    String absoluteStatPath = "file:/tmp/table/" + PARTITION + "/" + BASE_FILE;
    HoodieTimeline completedActive = activeTimelineWithStats(
        Collections.singletonList(INSTANT_TIME),
        metadataNaming(Collections.singletonList(writeStat(FILE_GROUP_ID, absoluteStatPath))));

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME),
        "the recorded path is absolute and the suspect name is bare; comparing raw strings would "
            + "wrongly report DANGLING here");
  }

  @Test
  void fileNamedOnlyInGetLogFilesIsCommitted() throws IOException {
    // getLogFiles() is a SUPERSET seeded from the pre-existing file slice; it can only refuse a
    // deletion, never cause one. Here .log.1 belongs to the slice but no stat's path names it.
    HoodieDeltaWriteStat stat = new HoodieDeltaWriteStat();
    stat.setFileId(FILE_GROUP_ID);
    stat.setPath(PARTITION + "/.fg1_1005.log.3_0-1-0");
    stat.setLogFiles(new ArrayList<>(Arrays.asList(
        ".fg1_1005.log.1_0-1-0", ".fg1_1005.log.2_0-1-0", ".fg1_1005.log.3_0-1-0")));
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = activeTimelineWithStats(
        Collections.singletonList(INSTANT_TIME), metadataNaming(Collections.singletonList(stat)));

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, ".fg1_1005.log.1_0-1-0", INSTANT_TIME),
        "the slice still carries this log file; ignoring getLogFiles() deletes it");
  }

  @Test
  void fileNamedOnlyInCdcStatsIsCommitted() throws Exception {
    // Mirrored from DanglingAdjudicator: CDC files are recorded ONLY in
    // getCdcStats(), nowhere else.
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(FILE_GROUP_ID);
    stat.setPath("2026/08/15/fg1_0-1-2_20260815120000.parquet");
    Map<String, Long> cdcStats = new HashMap<>();
    cdcStats.put("2026/08/15/.fg1_20260815120000.log.1_0-1-2.cdc", 128L);
    stat.setCdcStats(cdcStats);
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addWriteStat("2026/08/15", stat);
    HoodieTimeline completedActive =
        activeTimelineWithStats(Collections.singletonList("20260815120000"), metadata);

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        new InstantMembershipResolver(metaClient, completedActive, "20260815120000", null).lookup(
            "2026/08/15", FILE_GROUP_ID, ".fg1_20260815120000.log.1_0-1-2.cdc", "20260815120000"));
  }

  @Test
  void fileTheCommitDoesNotNameIsDangling() throws IOException {
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = activeTimelineNaming(BASE_FILE);

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, "fg1_0-2-0_1005.parquet", INSTANT_TIME));
  }

  @Test
  void zeroStatsWithThePartitionAbsentAfterNormalisationIsDangling() throws IOException {
    // A whole partition the commit never recorded -- the shape a partition present on storage and
    // absent from the metadata table takes, and among the most serious things this validator can
    // find. The commit was read, it records another partition, and it records nothing for this one
    // under any spelling, while the file's own name says this very instant wrote it.
    //
    // This is DANGLING only because the comparison is on NORMALISED partition keys
    // (partitionPathsAreNormalisedToOneComparableForm, and the two absolute-key cases below). On
    // raw strings the same answer would be returned for a partition the commit spells differently,
    // and that would condemn every file in it.
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieCommitMetadata elsewhere = new HoodieCommitMetadata();
    elsewhere.addWriteStat("2026/08/99", writeStat("fg2", "2026/08/99/fg2_0-1-0_1005.parquet"));
    HoodieTimeline completedActive =
        activeTimelineWithStats(Collections.singletonList(INSTANT_TIME), elsewhere);

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void commitThatRecordsNoPartitionAtAllIsInstantNotFound() throws IOException {
    // The boundary of the rule above. A commit whose partition map is empty has not declined to
    // claim this file -- it has said nothing about anything, which is what an unreadable read and a
    // no-write-stats action also amount to. Reading silence as proof is how one bad read condemns a
    // table.
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = activeTimelineWithStats(
        Collections.singletonList(INSTANT_TIME), new HoodieCommitMetadata());

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void zeroStatsWithThePartitionPresentInTheCommitIsDangling() throws IOException {
    // The other half of the same empty read, and it is not ambiguous: the commit recorded THIS
    // partition, was read, and its write stats do not name this file group. That is a commit
    // speaking about the partition and declining to claim the file -- the same evidence the
    // non-empty read rests on.
    //
    // Refusing here as well is what made an extra file group on storage, and a whole extra
    // partition, unadjudicable: every such record reached NOT_SETTLED and the run passed a table
    // the v1 validator fails.
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = activeTimelineWithStats(
        Collections.singletonList(INSTANT_TIME),
        metadataNaming(Collections.singletonList(
            writeStat("fg2", PARTITION + "/fg2_0-1-0_1005.parquet"))));

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void commitKeyedOnAnAbsolutePartitionPathStillMatchesTheRelativeOne() throws IOException {
    // The partition-level twin of the bare-name rule, and what lets the empty-stats branch conclude
    // anything at all. Hudi metadata mixes absolute and relative forms, so a raw map lookup misses
    // an entry that is right there -- and the file below IS committed. Read as "the partition is
    // absent" that is a refusal on a healthy file; read as "the partition is present and does not
    // name it" it would be a DANGLING verdict on one. Normalising both sides is what makes it
    // neither.
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieCommitMetadata absolutelyKeyed = new HoodieCommitMetadata();
    absolutelyKeyed.addWriteStat("file:/tmp/table/" + PARTITION,
        writeStat(FILE_GROUP_ID, PARTITION + "/" + BASE_FILE));
    HoodieTimeline completedActive =
        activeTimelineWithStats(Collections.singletonList(INSTANT_TIME), absolutelyKeyed);

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME),
        "the commit names this file under an absolutely-spelled key for the same partition");
  }

  @Test
  void absolutelyKeyedCommitStillCondemnsAFileItDoesNotName() throws IOException {
    // The other half: the same spelling mismatch, and this time the commit really does not name the
    // file. Normalisation must not make this one unadjudicable either.
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieCommitMetadata absolutelyKeyed = new HoodieCommitMetadata();
    absolutelyKeyed.addWriteStat("/tmp/table/" + PARTITION,
        writeStat("fg2", PARTITION + "/fg2_0-1-0_1005.parquet"));
    HoodieTimeline completedActive =
        activeTimelineWithStats(Collections.singletonList(INSTANT_TIME), absolutelyKeyed);

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void partitionPathsAreNormalisedToOneComparableForm() {
    InstantMembershipResolver resolver = new InstantMembershipResolver(
        bareMetaClient(), completedTimelineOver(Collections.emptyList()), null, null);

    assertEquals(PARTITION, resolver.relativePartitionPath(PARTITION));
    assertEquals(PARTITION, resolver.relativePartitionPath("/" + PARTITION),
        "a leading slash is a spelling, not a different partition");
    assertEquals(PARTITION, resolver.relativePartitionPath(PARTITION + "/"));
    assertEquals(PARTITION, resolver.relativePartitionPath("/tmp/table/" + PARTITION));
    assertEquals(PARTITION, resolver.relativePartitionPath("file:/tmp/table/" + PARTITION));
    assertEquals(PARTITION, resolver.relativePartitionPath("file:///tmp/table/" + PARTITION));
    assertEquals("", resolver.relativePartitionPath(""),
        "the non-partitioned table's partition, which must still match a commit's own empty key");
    assertEquals("", resolver.relativePartitionPath(null));
    // /tmp/table is NOT a parent of /tmp/table2, so this is not below the table root: it is
    // trimmed and left alone rather than sliced. What matters is that it does not come back equal
    // to this table's partition -- a bare-prefix test would slice it to "2/2026/08/12", and an
    // off-by-one the other way would make another table's partition match ours.
    assertEquals("tmp/table2/" + PARTITION, resolver.relativePartitionPath("/tmp/table2/" + PARTITION));
  }

  @Test
  void zeroStatsUnderAnActionThatRecordsNoneIsInstantNotFound() throws IOException {
    // NOTE: the three cases above this one are the partition-normalisation set; see
    // commitKeyedOnAnAbsolutePartitionPathStillMatchesTheRelativeOne.
    //
    // A clean, a rollback or a compaction plan records no write stats at all, so it says nothing
    // about any partition -- getWriteStatsAddedForInstant returns an empty list for such an action
    // by its own default. Its silence must stay a refusal: reading it as "this commit did not write
    // the file" would condemn files on the strength of an instant that never claimed to have
    // written anything.
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieInstant clean = Mockito.mock(HoodieInstant.class);
    Mockito.when(clean.getAction()).thenReturn(HoodieTimeline.CLEAN_ACTION);
    Mockito.when(clean.requestedTime()).thenReturn(INSTANT_TIME);
    HoodieTimeline completedActive = Mockito.mock(HoodieTimeline.class);
    Mockito.when(completedActive.getInstants()).thenReturn(Collections.singletonList(clean));

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void metadataReadFailureIsInstantNotFound() throws IOException {
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList(INSTANT_TIME));
    Mockito.when(completedActive.readCommitMetadata(any()))
        .thenThrow(new IOException("commit file unreadable"));

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        new InstantMembershipResolver(metaClient, completedActive, INSTANT_TIME, null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void logFileNamedOnlyByGetLogFilesIsCommittedThroughTheArchive() throws IOException {
    // CommitPathProjector unions path, getLogFiles() and getCdcStats() when it materializes the
    // archived working set, so a log file the archived commit names only through its log-file list
    // is COMMITTED through the archive exactly as it would be through the active path -- see
    // fileNamedOnlyInGetLogFilesIsCommitted for the active-path twin of this test.
    String logFile = ".fg1_1005.log.1_0-1-0";
    HoodieTableMetaClient metaClient = bareMetaClient();
    // The instant is off the active timeline entirely, so the lookup falls to the archive.
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        new InstantMembershipResolver(metaClient, completedActive, "2000", archivedNaming(logFile))
            .lookup(PARTITION, FILE_GROUP_ID, logFile, INSTANT_TIME));
  }

  /**
   * The NEW capability, and the riskiest assertion in this class: an ordinary log file resolved
   * through the archive can now be {@code DANGLING}. Within the supported range (Hudi 0.14+),
   * HUDI-1434 (released 0.7.0) makes a log file's own write-stat {@code path} complete regardless
   * of archived-schema vintage, so {@code getLogFiles()}'s absence from a pre-1.0 archived record
   * costs only defensive over-naming, not primary evidence -- see the class Javadoc's rewritten
   * bullet. Naming a DIFFERENT file for this exact group and instant makes the fixture unambiguous,
   * and the beacon assertion is required, not incidental: a beacon-less version of this test would
   * pass on {@code IMR_ARCHIVE_RECORDED_NOTHING} just as easily as on the intended
   * {@code IMR_ARCHIVE_NOT_NAMED}, and the two are different facts about the table.
   */
  @Test
  void logFileTheArchivedCommitDoesNotNameIsDangling() throws IOException {
    String logFile = ".fg1_1005.log.1_0-1-0";
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));
    InstantMembershipResolver resolver =
        new InstantMembershipResolver(metaClient, completedActive, "2000", archivedNaming(BASE_FILE));

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        resolver.lookup(PARTITION, FILE_GROUP_ID, logFile, INSTANT_TIME),
        "the archived commit named a different file for this exact group and instant; an ordinary "
            + "log file has no claim on it and is condemned exactly as a base file would be");
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_NOT_NAMED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, logFile, INSTANT_TIME));
  }

  /**
   * <b>Proves the routing fix, not just the archived read.</b> Every other archive test in this file
   * (before this one) is trivially safe under either routing rule: {@code completedActive} is either
   * this same instant or an unrelated one on a NON-empty timeline, but {@code lookup}'s two candidate
   * routing rules -- "is the instant absent from {@code completedActiveByTime}" and "does
   * {@link InstantBuckets#pokesArchive} say so" -- agree on every one of them by construction. This
   * test and its {@code Dangling} sibling below use a real, non-empty cutoff ({@code "2000"}) with
   * the looked-up instant ({@link #INSTANT_TIME}, {@code "1005"}) below it, which is what
   * {@code FsMdtDiffDimension.adjudicateBatch} warms the working set up for -- the same bucketing
   * expression -- so a base file, not just a log or CDC file, is proven to reach {@code COMMITTED}
   * through the routing this task changed.
   */
  @Test
  void baseFileNamedInTheArchiveIsCommittedThroughTheLivePath() throws IOException {
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        new InstantMembershipResolver(metaClient, completedActive, "2000", archivedNaming(BASE_FILE))
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /** The {@code DANGLING} sibling of the test above -- see its Javadoc. */
  @Test
  void baseFileTheArchivedCommitDoesNotNameIsDanglingThroughTheLivePath() throws IOException {
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));
    InstantMembershipResolver resolver = new InstantMembershipResolver(metaClient, completedActive,
        "2000", archivedNaming("fg1_9-9-9_1005.parquet"));

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_NOT_NAMED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * The third routing arm: at or after the cutoff (inside the window the active timeline is
   * authoritative for) but not on {@code completedActive}. Deliberately refuses rather than
   * condemning -- see {@link Branch#IMR_INSIDE_WINDOW_BUT_NOT_COMPLETED}'s Javadoc for why this
   * class does not mirror {@code DanglingAdjudicator}'s equivalent row here.
   */
  @Test
  void insideTheWindowButNotOnTheActiveTimelineRefuses() throws IOException {
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));
    InstantMembershipResolver resolver =
        new InstantMembershipResolver(metaClient, completedActive, "2000", null);

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, "2500"),
        "\"2500\" is at or after the \"2000\" cutoff but is not on completedActive; refuse, never "
            + "condemn");
    BranchBeacons.assertTakes(Branch.IMR_INSIDE_WINDOW_BUT_NOT_COMPLETED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, "2500"));
  }

  /**
   * The one carve-out that survives, and the test that stops a live-data-deletion path: a CDC
   * file's name appears ONLY in {@code cdcStats}, never in {@code path}. {@code cdcStats} was added
   * to the archived schema by HUDI-6776, first released in Hudi 1.0.0-beta1 -- inside the supported
   * range -- so a record archived by a supported 0.14.x or 0.15.x Hudi genuinely cannot carry it,
   * and every archived record for a CDC-enabled table written by such a Hudi is path-only for CDC
   * purposes. A naive "the archived names don't include it" reading would delete a live file on
   * every such table. Pinned here, unambiguously, so a future reader cannot miss it.
   */
  @Test
  void cdcFileThroughTheArchiveIsRefusedNotCondemned() throws IOException {
    String cdcFile = ".fg1_1005.log.1_0-1-2.cdc";
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));
    InstantMembershipResolver resolver =
        new InstantMembershipResolver(metaClient, completedActive, "2000", archivedNaming(BASE_FILE));

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, cdcFile, INSTANT_TIME),
        "a CDC file is named nowhere but cdcStats, which a pre-1.0.0-beta1 archived record cannot "
            + "carry; refusing it is the only safe answer, never DANGLING");
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_CDC_REFUSED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, cdcFile, INSTANT_TIME));
  }

  @Test
  void cdcFileNamedOnlyInCdcStatsIsCommittedThroughTheArchive() throws IOException {
    // Mirrored into the archived path: CDC files are recorded only in getCdcStats(),
    // and CommitPathProjector unions it in exactly as namesOfStats does for the active path -- see
    // fileNamedOnlyInCdcStatsIsCommitted for the active-path twin of this test.
    String cdcFile = ".fg1_20260815120000.log.1_0-1-2.cdc";
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        new InstantMembershipResolver(metaClient, completedActive, "2000", archivedNaming(cdcFile))
            .lookup(PARTITION, FILE_GROUP_ID, cdcFile, INSTANT_TIME));
  }

  @Test
  void indeterminateArchiveResultIsInstantNotFound() throws IOException {
    // The working set's own safety property: a search that could not cover what it needed to is
    // never treated as an absence, whatever it means for the file.
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        new InstantMembershipResolver(metaClient, completedActive, "2000", archivedIndeterminate())
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void notFoundArchiveResultIsInstantNotFound() throws IOException {
    // NOT_FOUND is a claim about the ARCHIVE, not about this file: every byte that could have held
    // the instant was read cleanly and it was not there, which says nothing about whether the file
    // this instant is supposed to have written is on storage for a reason.
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        new InstantMembershipResolver(metaClient, completedActive, "2000", archivedNotFound())
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void nullWorkingSetIsInstantNotFound() throws IOException {
    // Fail-fast mode, expressed as a null working set: no archive was read at all, so the lookup
    // must never touch the meta client either -- not even a call to getTableFormat().
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        new InstantMembershipResolver(metaClient, completedActive, "2000", null)
            .lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));

    Mockito.verify(metaClient, Mockito.never()).getTableFormat();
    Mockito.verify(metaClient, Mockito.never()).getArchivePath();
    Mockito.verify(metaClient, Mockito.never()).getStorage();
  }

  /**
   * {@link ArchivedInstantWorkingSet#stateOf} THROWS {@code IllegalArgumentException} for an
   * instant that was never a target of the pass. A
   * bucket-scoped working set makes an instant that is neither on the completed active
   * timeline nor a target of the archive bucket a live possibility rather than a theoretical one --
   * left unguarded, that instant would fall into {@link InstantMembershipResolver}'s blanket catch
   * and produce a misattributed {@code IMR_THREW} beacon plus a per-record WARN-with-stacktrace
   * inside a Spark task loop, which this repo's logging policy forbids.
   */
  @Test
  void instantNotATargetOfTheWorkingSetIsInstantNotFound() throws IOException {
    HoodieTableMetaClient metaClient = bareMetaClient();
    HoodieTimeline completedActive = completedTimelineOver(Collections.singletonList("2000"));
    ArchivedInstantWorkingSet<ArchivedCommitPaths> archived =
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
                Collections.singleton("9999"))
            .notFound("9999")
            .build();
    InstantMembershipResolver resolver =
        new InstantMembershipResolver(metaClient, completedActive, "2000", archived);

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME),
        "INSTANT_TIME was never a target of this working set; must refuse, not throw");
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_INSTANT_NOT_A_TARGET,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  /**
   * A savepoint carries the instant time of the commit it protects. Handing a membership resolver a
   * timeline that contains both, when it de-duplicates with putIfAbsent, makes the answer depend on
   * iteration order -- so the timeline handed to it must never contain the savepoint at all.
   */
  @Test
  void savepointSharingACommitsInstantTimeIsNotTheInstantResolved(@TempDir Path dir) throws IOException {
    HoodieTableMetaClient metaClient = tableWithCommitAndSavepointAt(dir, "20250101000000000");

    HoodieTimeline handedToTheResolver =
        metaClient.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants();

    assertEquals(1, handedToTheResolver.countInstants(),
        "getCommitsTimeline() must drop the savepoint before completed-filtering");
    assertEquals(HoodieTimeline.COMMIT_ACTION,
        handedToTheResolver.firstInstant().get().getAction());
  }

  // ---------------------------------------------------------------------------------------------
  //  Fixtures, adapted from TestDanglingAdjudicator
  // ---------------------------------------------------------------------------------------------

  private static HoodieInstant instantAt(String time) {
    HoodieInstant instant = Mockito.mock(HoodieInstant.class);
    Mockito.when(instant.getAction()).thenReturn(HoodieTimeline.COMMIT_ACTION);
    Mockito.when(instant.requestedTime()).thenReturn(time);
    return instant;
  }

  private static HoodieCommitMetadata metadataNaming(List<HoodieWriteStat> stats) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (HoodieWriteStat stat : stats) {
      metadata.addWriteStat(PARTITION, stat);
    }
    return metadata;
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

  /** A completed-active timeline whose one commit at {@link #INSTANT_TIME} records exactly these
   *  bare names as stat paths for {@link #FILE_GROUP_ID}. */
  private static HoodieTimeline activeTimelineNaming(String... names) throws IOException {
    List<HoodieWriteStat> stats = new ArrayList<>();
    for (String name : names) {
      stats.add(writeStat(FILE_GROUP_ID, PARTITION + "/" + name));
    }
    return activeTimelineWithStats(Collections.singletonList(INSTANT_TIME), metadataNaming(stats));
  }

  private static HoodieTimeline activeTimelineWithStats(List<String> instantTimes,
                                                        HoodieCommitMetadata metadata)
      throws IOException {
    HoodieTimeline completed = completedTimelineOver(instantTimes);
    Mockito.when(completed.readCommitMetadata(any())).thenReturn(metadata);
    return completed;
  }

  /** A meta client with nothing stubbed beyond what every lookup needs for logging. */
  private static HoodieTableMetaClient bareMetaClient() {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    return metaClient;
  }

  /**
   * A completed archived commit that recorded write stats and names nothing for the group proves the
   * group was not written at that instant, whatever shape the absence takes; one with no write stats
   * still refuses. Each fixture is projected from a real archived record so the partition matching is
   * the projector's own.
   */
  @Test
  void archivedCommitRecordingThePartitionButNotTheGroupIsDangling() throws IOException {
    InstantMembershipResolver resolver = archiveResolver(
        archivedCompletedRecording(PARTITION, "fg-other", PARTITION + "/fg-other_0-1-0_1005.parquet"));

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_GROUP_UNCLAIMED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void archivedCommitNotRecordingThePartitionIsDangling() throws IOException {
    InstantMembershipResolver resolver = archiveResolver(
        archivedCompletedRecording("2026/08/13", "fg-other", "2026/08/13/fg-other_0-1-0_1005.parquet"));

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_GROUP_UNCLAIMED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void archivedCommitRecordingTheGroupWithNoPathIsDangling() throws IOException {
    InstantMembershipResolver resolver = archiveResolver(
        archivedCompletedRecording(PARTITION, FILE_GROUP_ID, null));

    assertEquals(InstantMembershipResolver.Membership.DANGLING,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME),
        "a stat for the group with no path names no file, so the group is unclaimed in a commit that recorded write stats");
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_GROUP_UNCLAIMED,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void archivedCommitRecordingTheGroupUnderAnAbsolutePartitionSpellingIsCommitted() throws IOException {
    InstantMembershipResolver resolver = archiveResolver(archivedCompletedRecording(
        "/tmp/table/" + PARTITION, FILE_GROUP_ID, PARTITION + "/" + BASE_FILE));

    assertEquals(InstantMembershipResolver.Membership.COMMITTED,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME),
        "an absolute partition key must not read as the group being absent from the commit");
  }

  @Test
  void archivedCommitWithNoWriteStatsIsRefused() throws IOException {
    InstantMembershipResolver resolver = archiveResolver(projectedCompleted(
        Collections.<String, List<org.apache.hudi.avro.model.HoodieWriteStat>>emptyMap()));

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
    BranchBeacons.assertTakes(Branch.IMR_ARCHIVE_RECORDED_NOTHING,
        () -> resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  @Test
  void pendingRecordOnlyStillRefusesAnAbsentGroup() throws IOException {
    InstantMembershipResolver resolver = archiveResolver(
        ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(Collections.singleton(INSTANT_TIME))
            .found(INSTANT_TIME, ArchivedCommitPaths.namesOnly(
                Collections.<HoodieFileGroupId, Set<String>>emptyMap()))
            .build());

    assertEquals(InstantMembershipResolver.Membership.INSTANT_NOT_FOUND,
        resolver.lookup(PARTITION, FILE_GROUP_ID, BASE_FILE, INSTANT_TIME));
  }

  private static InstantMembershipResolver archiveResolver(
      ArchivedInstantWorkingSet<ArchivedCommitPaths> archived) {
    return new InstantMembershipResolver(bareMetaClient(),
        completedTimelineOver(Collections.singletonList("2000")), "2000", archived);
  }

  /** A completed archived record of {@link #INSTANT_TIME} with one write stat, as the projector reads it. */
  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> archivedCompletedRecording(
      String partitionKey, String fileId, String path) throws IOException {
    org.apache.hudi.avro.model.HoodieWriteStat stat = new org.apache.hudi.avro.model.HoodieWriteStat();
    stat.setFileId(fileId);
    stat.setPath(path);
    return projectedCompleted(Collections.singletonMap(partitionKey, Collections.singletonList(stat)));
  }

  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> projectedCompleted(
      Map<String, List<org.apache.hudi.avro.model.HoodieWriteStat>> partitionToWriteStats)
      throws IOException {
    org.apache.hudi.avro.model.HoodieReplaceCommitMetadata metadata =
        new org.apache.hudi.avro.model.HoodieReplaceCommitMetadata();
    metadata.setPartitionToWriteStats(partitionToWriteStats);
    org.apache.hudi.avro.model.HoodieArchivedMetaEntry envelope =
        new org.apache.hudi.avro.model.HoodieArchivedMetaEntry();
    envelope.setHoodieReplaceCommitMetadata(metadata);
    envelope.setActionState("COMPLETED");
    CommitPathProjector projector = new CommitPathProjector(new StoragePath("/tmp/table"),
        Collections.singleton(new HoodieFileGroupId(PARTITION, FILE_GROUP_ID)));
    return ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(Collections.singleton(INSTANT_TIME))
        .found(INSTANT_TIME, projector.project(envelope))
        .build();
  }

  /**
   * A {@code FOUND} working set for {@link #INSTANT_TIME} whose sole file group,
   * ({@link #PARTITION}, {@link #FILE_GROUP_ID}), records exactly the given bare names -- mirroring
   * what {@link CommitPathProjector} would have projected from a real archived record's
   * {@code path} / {@code getLogFiles()} / {@code getCdcStats()} union.
   */
  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> archivedNaming(
      String... names) {
    Map<HoodieFileGroupId, Set<String>> content = new HashMap<>();
    content.put(new HoodieFileGroupId(PARTITION, FILE_GROUP_ID),
        new LinkedHashSet<>(Arrays.asList(names)));
    return ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
            Collections.singleton(INSTANT_TIME))
        .found(INSTANT_TIME, ArchivedCommitPaths.namesOnly(content))
        .build();
  }

  /** A working set whose one pass over {@link #INSTANT_TIME} could not cover what it needed to. */
  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> archivedIndeterminate() {
    return ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
            Collections.singleton(INSTANT_TIME))
        .indeterminate(INSTANT_TIME)
        .build();
  }

  /** A working set whose one pass read every byte that could have held {@link #INSTANT_TIME} and
   *  found it absent. */
  private static ArchivedInstantWorkingSet<ArchivedCommitPaths> archivedNotFound() {
    return ArchivedInstantWorkingSet.<ArchivedCommitPaths>builder(
            Collections.singleton(INSTANT_TIME))
        .notFound(INSTANT_TIME)
        .build();
  }

  private static HoodieTableMetaClient tableWithCommitAndSavepointAt(Path dir, String instantTime)
      throws IOException {
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration(false));
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("savepoint_tie")
        .initTable(conf, dir.toAbsolutePath().toString());
    TimelineLayout layout = metaClient.getTimelineLayout();
    String timelinePath = metaClient.getTimelinePath().toString();
    for (String action : new String[] {HoodieTimeline.COMMIT_ACTION, HoodieTimeline.SAVEPOINT_ACTION}) {
      HoodieInstant instant = layout.getInstantGenerator().createNewInstant(
          HoodieInstant.State.COMPLETED, action, instantTime, instantTime + "1");
      Files.write(Paths.get(timelinePath, layout.getInstantFileNameGenerator().getFileName(instant)),
          "{\"partitionToWriteStats\":{}}".getBytes());
    }
    return metaClient;
  }
}
