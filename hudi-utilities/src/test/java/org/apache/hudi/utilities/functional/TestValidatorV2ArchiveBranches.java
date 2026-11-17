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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.ArchivedCommitPaths;
import org.apache.hudi.utilities.util.Branch;
import org.apache.hudi.utilities.util.BranchBeacon;
import org.apache.hudi.utilities.util.CommitPathProjector;
import org.apache.hudi.utilities.util.ValidationMode;
import org.apache.hudi.utilities.util.archive.ArchiveFile;
import org.apache.hudi.utilities.util.archive.ArchiveTimelineInstantBatchResolver;
import org.apache.hudi.utilities.util.archive.ArchiveTimelineInstantBatchResolvers;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;
import org.apache.hudi.utilities.util.archive.HoodieArchiveFileCatalog;
import org.apache.hudi.utilities.util.archive.HoodieArchiveFileScanner;
import org.apache.hudi.utilities.util.archive.InstantRange;
import org.apache.hudi.utilities.util.archive.InstantSearchState;
import org.apache.hudi.utilities.util.archive.LiveTargets;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The archived-timeline search, against a <b>real archived timeline</b>, along its resolver's three
 * states: {@link InstantSearchState#FOUND}, {@link InstantSearchState#NOT_FOUND} and
 * {@link InstantSearchState#INDETERMINATE}.
 *
 * <p>Only {@code NOT_FOUND} means every byte that could have held the instant was read cleanly and
 * it was not there. {@code INDETERMINATE} means the search could not cover what it needed to — a
 * corrupt block, an unlistable directory, a projector that could not decode. Collapsing the two is
 * the single worst thing a caller downstream can do, because only the first ever licenses treating a
 * file as unreferenced.
 *
 * <p><b>A real archive, not a mocked loader.</b> {@code TestV1ArchiveTimelineInstantBatchResolver}
 * covers the decode and the concurrency with a mocked {@code ArchiveFileCatalog}/
 * {@code ArchiveFileScanner} pair, which is the right level for those — including every
 * {@code INDETERMINATE} path a real, undamaged archive never takes, such as a corrupt block or an
 * unlistable directory; {@code TestHoodieArchiveInstantSearch} does the same for the scanner
 * against genuine archive bytes without going through a whole batch resolver. What neither can
 * establish on its own is that the two compose: that bytes Hudi's own archiver wrote, then damaged
 * the same way {@code TestHoodieArchiveInstantSearch} damages them, still reach the resolver — not a
 * mock standing in for it — as {@code INDETERMINATE}. Most of these cases archive a real table
 * version 6 and read what the archiver left undamaged, because an undamaged archive never produces
 * {@code INDETERMINATE} on its own; one case composes the damage in on top, which is the only way to
 * cover that seam at all.
 *
 * <p>The end-to-end cases then show what the search drives: a stranger file group under an archived
 * completed commit that did not name it is {@code DANGLING}, exactly as under an active one, and so is
 * one on a table that has never archived; an archive that cannot answer stays {@code NOT_SETTLED}.
 */
@Tag("functional")
public class TestValidatorV2ArchiveBranches extends ValidatorV2FunctionalTestBase {

  private static final String COW = "COPY_ON_WRITE";

  /** A file id no writer will ever mint. */
  private static final String STRANGER_FILE_ID = "00000000-1111-2222-3333-444444444444";

  /** Older than any instant a writer produces, fixed width so the timeline orders it first. */
  private static final String ANCIENT_INSTANT = "19700101000000000";

  // ============================================================== the search's three answers

  /**
   * <b>Found.</b> An archived instant, asked about a file group its commit really wrote, comes back
   * {@code FOUND} — and the names it projects for the group are the bare name of the path the
   * commit recorded.
   *
   * <p>This is the only answer that lets anything downstream conclude the commit completed, so it is
   * asserted on the projected name rather than on the state alone: an implementation that reported
   * {@code FOUND} with an absent or empty entry for the group would satisfy "found" while meaning the
   * opposite.
   */
  @Test
  void archivedCommitAskedAboutAFileGroupItWroteAnswersWithItsPaths() throws Exception {
    writeTable(COW, V6);
    ArchivedWrite written = oldestCommittedWrite();
    List<String> archived = archiveTimeline(writeOptions(COW, V6));
    assertTrue(archived.contains(written.instantTime),
        "precondition: the commit whose write this asks about has to have been archived, or the "
            + "question is about the active timeline. Archived: " + archived + ", asking about "
            + written);

    HoodieFileGroupId groupId = new HoodieFileGroupId(written.partition, written.fileId);
    ArchivedInstantWorkingSet<ArchivedCommitPaths> workingSet =
        warmUpArchive(Collections.singleton(written.instantTime), Collections.singleton(groupId));

    assertEquals(InstantSearchState.FOUND, workingSet.stateOf(written.instantTime),
        "the archive holds this instant, so it has to answer FOUND: " + workingSet.stateOf(written.instantTime));
    Map<HoodieFileGroupId, Set<String>> byGroup =
        workingSet.contentOf(written.instantTime).map(ArchivedCommitPaths::namesByGroup)
            .orElse(Collections.emptyMap());
    Set<String> names = byGroup.get(groupId);
    assertTrue(names != null && !names.isEmpty(),
        "the answer has to name the paths it recorded, not be absent or empty -- an absent or empty "
            + "entry for the group is the 'answered, nothing here' answer and means the opposite: "
            + byGroup);
    assertTrue(names.contains(bareName(written.path)),
        "the recorded name has to be the one the archived commit itself records. Answer: "
            + names + ", archived commit records " + written.path);
  }

  /**
   * <b>Not found.</b> Asked in the SAME pass as an archived instant, an instant that is still on the
   * <em>active</em> timeline comes back {@code NOT_FOUND} — the search covers the whole archive and
   * genuinely does not hold it, never merely "this batch's read said nothing about it".
   *
   * <p>The instant asked about is the newest completed commit, which has never been archived: every
   * archive file is read in full and none holds it, a proven absence. It is asked in the same pass as
   * the archived member to prove the search is precise per instant rather than an answer for the
   * batch as a whole.
   */
  @Test
  void archivedRangeAskedAboutAnInstantItDoesNotHoldAnswersNotFound() throws Exception {
    writeTable(COW, V6);
    ArchivedWrite written = oldestCommittedWrite();
    List<String> archived = archiveTimeline(writeOptions(COW, V6));
    assertTrue(archived.contains(written.instantTime),
        "precondition: the archived half of the pair has to really be archived: " + archived);
    String stillActive = metaClient().getActiveTimeline().filterCompletedInstants().lastInstant()
        .get().requestedTime();
    assertFalse(archived.contains(stillActive),
        "precondition: the instant asked about must NOT be archived, or this asks the wrong "
            + "question. Archived: " + archived);

    HoodieFileGroupId groupId = new HoodieFileGroupId(written.partition, written.fileId);
    Set<String> wanted = new HashSet<>();
    wanted.add(written.instantTime);
    wanted.add(stillActive);
    ArchivedInstantWorkingSet<ArchivedCommitPaths> workingSet =
        warmUpArchive(wanted, Collections.singleton(groupId));

    assertEquals(InstantSearchState.FOUND, workingSet.stateOf(written.instantTime),
        "the archived member of the SAME pass still answers FOUND, which is the point of asking "
            + "about both together: " + workingSet.stateOf(written.instantTime));
    assertEquals(InstantSearchState.NOT_FOUND, workingSet.stateOf(stillActive),
        "an instant that has never been archived is a proven absence, not a refusal");
  }

  /**
   * <b>Not found, the other edge: an instant older than every archived file's own range.</b>
   * Loading a single closed range spanning every wanted instant would make a range this narrow and
   * this old deliver nothing at all, indistinguishable from a read that could not be performed. The
   * per-file search of {@link ArchiveTimelineInstantBatchResolvers} tells the two apart: every
   * archive file is read cleanly and none holds {@code ANCIENT_INSTANT}, a proven absence, not an
   * unanswered question.
   *
   * <p>Genuine unsettled answers — a corrupt block, an unlistable archive directory — are exercised
   * at the levels that can manufacture them on demand; see the class javadoc.
   */
  @Test
  void instantOlderThanTheWholeArchiveAnswersNotFound() throws Exception {
    writeTable(COW, V6);
    ArchivedWrite written = oldestCommittedWrite();
    archiveTimeline(writeOptions(COW, V6));

    HoodieFileGroupId groupId = new HoodieFileGroupId(written.partition, written.fileId);
    ArchivedInstantWorkingSet<ArchivedCommitPaths> workingSet =
        warmUpArchive(Collections.singleton(ANCIENT_INSTANT), Collections.singleton(groupId));

    assertEquals(InstantSearchState.NOT_FOUND, workingSet.stateOf(ANCIENT_INSTANT),
        "no archive file holds an instant this old, which is a proven absence, not a refusal");
  }

  /**
   * <b>Indeterminate, composed end to end.</b> A REAL archive file, written by this table's own
   * {@code TimelineArchiverV1}, damaged the same way {@code TestHoodieArchiveInstantSearch} damages
   * synthetic ones -- corrupting the first block's header so it reads back as a corrupt block --
   * reaches the resolver as {@code INDETERMINATE} for an instant that file holds, never as
   * {@code NOT_FOUND}.
   *
   * <p>This is the seam neither adjacent layer covers alone. The scanner-level test proves a damaged
   * file's instants resolve indeterminate; the resolver-level unit test proves
   * {@code INDETERMINATE} propagates out of {@code warmUp} -- but against a scanner double that
   * returns whatever the test tells it to, never against bytes a corruption actually touched. Collapse
   * either half and this assertion would pass unconditionally: an uncorrupted archive would answer
   * {@code FOUND} (wrong direction, not a false absence) but a resolver that mis-reported the damage as
   * {@code NOT_FOUND} instead of {@code INDETERMINATE} would fail this exact assertion, which is the
   * property this test exists to pin down -- confusing "I could not read the archive" for "the instant
   * is proven absent" is what would let a corrupt block authorise deleting a live file.
   */
  @Test
  void realCorruptedArchiveReachesTheResolverAsIndeterminateNeverAsNotFound() throws Exception {
    writeTable(COW, V6);
    ArchivedWrite written = oldestCommittedWrite();
    List<String> archived = archiveTimeline(writeOptions(COW, V6));
    assertTrue(archived.contains(written.instantTime),
        "precondition: the instant under test has to really be archived: " + archived);

    HoodieArchiveFileCatalog catalog = new HoodieArchiveFileCatalog(metaClient());
    HoodieArchiveFileScanner scanner = new HoodieArchiveFileScanner(metaClient());
    ArchiveFile coveringFile = fileCovering(catalog, scanner, written.instantTime);
    corruptFirstBlockHeader(coveringFile);

    HoodieFileGroupId groupId = new HoodieFileGroupId(written.partition, written.fileId);
    ArchivedInstantWorkingSet<ArchivedCommitPaths> workingSet =
        warmUpArchive(Collections.singleton(written.instantTime), Collections.singleton(groupId));

    assertEquals(InstantSearchState.INDETERMINATE, workingSet.stateOf(written.instantTime),
        "a real corrupt block must reach the resolver as an unresolved read, never as a proven "
            + "absence: " + workingSet.stateOf(written.instantTime));
  }

  // ================================================= what the three answers drive, end to end

  /**
   * <b>A stranger file group under an archived completed commit is {@code DANGLING}.</b>
   *
   * <p>The archived commit recorded write stats, and none for the stranger's file group, which is
   * the same evidence the active path condemns on: the archive answers by the active path's rules,
   * so archiving a commit never turns proof into a refusal.
   */
  @Test
  void orphanUnderAnArchivedInstantIsDangling() throws Exception {
    writeTable(COW, V6);
    Map<String, String> options = writeOptions(COW, V6);
    List<String> archived = archiveTimeline(options);
    StoragePath orphan = strangerAt(anyFile(".parquet", false), archived.get(0));

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    run.tookBranch(Branch.ASL_ARCHIVE_RECORDED_NOTHING, Branch.FDA_ARM23_ARCHIVED_COMPLETED_FS_EXCLUSIVE,
            Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING)
        .didNotTakeBranch(Branch.FDA_ARM23_UNRESOLVED_EXHAUSTIVE, Branch.ASL_UNRESOLVED);
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "the archived commit completed, recorded write stats, and named nothing for this file group");
    assertFalse(run.passed(), "a file its writing commit never named is a dangling file");
  }

  /**
   * <b>The archived commit recorded the stranger's partition, just not its file group.</b> The
   * active path calls this {@code IMR_ACTIVE_FILE_GROUP_UNCLAIMED}; its archived twin must agree.
   */
  @Test
  void orphanInAPartitionTheArchivedCommitRecordedIsDangling() throws Exception {
    writeTable(COW, V6);
    ArchivedWrite written = oldestCommittedWrite();
    List<String> archived = archiveTimeline(writeOptions(COW, V6));
    assertTrue(archived.contains(written.instantTime),
        "precondition: the commit has to be archived. Archived: " + archived + ", asking about " + written);
    StoragePath orphan = strangerAt(anyFileIn(new StoragePath(basePath, written.partition), ".parquet", false),
        written.instantTime);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    run.tookBranch(Branch.FDA_ARM23_ARCHIVED_COMPLETED_FS_EXCLUSIVE, Branch.IMR_ARCHIVE_GROUP_UNCLAIMED,
            Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING)
        .didNotTakeBranch(Branch.FDA_ARM23_UNRESOLVED_EXHAUSTIVE);
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText());
  }

  /**
   * <b>The same absence under an archived clustering replacecommit.</b> A replacecommit archives its
   * write stats under {@code hoodieReplaceCommitMetadata} rather than {@code hoodieCommitMetadata},
   * so this pins that the projector reads that shape and the stranger is condemned exactly as under
   * an archived commit.
   */
  @Test
  void orphanInAPartitionAnArchivedReplaceCommitRecordedIsDangling() throws Exception {
    writeTable(COW, V6);
    Map<String, String> options = writeOptions(COW, V6);
    ArchivedWrite clustered = clusterInline(options);
    List<String> archived = archiveTimeline(options);
    assertTrue(archived.contains(clustered.instantTime),
        "precondition: the replacecommit has to be archived. Archived: " + archived + ", clustered " + clustered);
    StoragePath orphan = strangerAt(anyFileIn(new StoragePath(basePath, clustered.partition), ".parquet", false),
        clustered.instantTime);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    run.tookBranch(Branch.FDA_ARM23_ARCHIVED_COMPLETED_FS_EXCLUSIVE, Branch.IMR_ARCHIVE_GROUP_UNCLAIMED,
            Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING)
        .didNotTakeBranch(Branch.FDA_ARM23_UNRESOLVED_EXHAUSTIVE, Branch.IMR_ARCHIVE_RECORDED_NOTHING);
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "the archived replacecommit completed, recorded write stats, and named nothing for this file group");
    assertFalse(run.passed(), "a file its writing replacecommit never named is a dangling file");
  }

  /**
   * <b>The same absence on a table that has never archived is {@code DANGLING}</b>, because there is
   * no archived history that could have named the instant.
   *
   * <p>A different proof from the cases above, reaching the same verdict: there the archived commit
   * completed and did not name the file group, here no archived history exists at all. On table
   * version 6 an absent archive directory is itself the evidence, which is trusted only on a
   * pre-version-8 layout and is why this suite is fixed at version 6.
   */
  @Test
  void orphanOnAnUnarchivedTableIsDangling() throws Exception {
    writeTable(COW, V6);
    assertFalse(archiveHasContent(),
        "precondition: nothing may be in the archive, or the absence stops being trustworthy and "
            + "this becomes the NOT_SETTLED case under a different name");
    StoragePath orphan = strangerAt(anyFile(".parquet", false), ANCIENT_INSTANT);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    run.tookBranch(Branch.ASL_ABSENCE_TRUSTED_NULL, Branch.FDA_FS_PREV_NULL_AFT_NULL_DANGLING)
        .didNotTakeBranch(Branch.ASL_UNRESOLVED);
    assertFalse(run.passed(), "a file no history can account for is a dangling file");
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "no instant on the timeline at either read wrote this file and the archive held nothing "
            + "that could have named one");
  }

  // ============================================================ the one-pass guarantee, end to end

  /**
   * <b>One task, several damaged file groups, one archive open.</b> The whole point of resolving the
   * archived timeline once per Spark task rather than once per file group: several independently
   * dangling file groups, all sharing a single writing instant so
   * {@link org.apache.hudi.utilities.util.InstantRangeBalancer} cannot split them across partitions
   * and they are guaranteed to land in the SAME task, still cost exactly one archive open between
   * them.
   *
   * <p>{@link org.apache.hudi.utilities.util.archive.V1ArchiveTimelineInstantBatchResolver#warmUp}
   * hits {@code V1ABR_NO_ARCHIVE_DIRECTORY} once per CALL, before it has looked at how many instants
   * that call was asked about -- so on this never-archived table, the number of times that beacon
   * fires in the run's log IS the number of times {@code warmUp} was invoked. Meanwhile every one of
   * the several orphans independently reaches {@link Branch#ASL_ABSENCE_TRUSTED_NULL} and
   * {@link Branch#FDA_FS_PREV_NULL_AFT_NULL_DANGLING}, proving they were each genuinely adjudicated
   * as their own file group rather than folded together. Had the archive still been opened once per
   * file group -- as it was before this effort -- the two counts would agree; asserting they do not
   * is the whole claim.
   */
  @Test
  void oneTaskOpensTheArchiveExactlyOnceHoweverManyFileGroupsItHolds() throws Exception {
    writeTable(COW, V6);
    assertFalse(archiveHasContent(),
        "precondition: nothing may be in the archive, or V1ABR_NO_ARCHIVE_DIRECTORY never fires");
    StoragePath original = anyFile(".parquet", false);
    List<String> strangerFileIds = Arrays.asList(
        "00000000-1111-2222-3333-444444444440",
        "00000000-1111-2222-3333-444444444441",
        "00000000-1111-2222-3333-444444444442",
        "00000000-1111-2222-3333-444444444443");
    for (String fileId : strangerFileIds) {
      strangerAt(original, fileId, ANCIENT_INSTANT);
    }

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed(), "several files no history can account for are all dangling");
    assertEquals(strangerFileIds.size(), countBranch(run.log(), Branch.FDA_FS_PREV_NULL_AFT_NULL_DANGLING),
        "precondition: every stranger file group must independently reach the DANGLING row, or this "
            + "does not exercise several file groups at all: " + run.log());
    assertEquals(strangerFileIds.size(), countBranch(run.log(), Branch.ASL_ABSENCE_TRUSTED_NULL),
        "precondition: each file group must independently ask the archive lookup about its own "
            + "instant, or the archive-open count below proves nothing: " + run.log());
    assertEquals(1L, countBranch(run.log(), Branch.V1ABR_NO_ARCHIVE_DIRECTORY),
        "the archive must be opened exactly once for the whole task, however many file groups it "
            + "holds -- opening it once per file group would make this " + strangerFileIds.size()
            + " instead of 1: " + run.log());
  }

  // The "found" answer has no end-to-end counterpart here, and that is a reachability limit rather
  // than an omission. Reaching ASL_FOUND_COMPLETED from a real run needs a file whose base instant is
  // ARCHIVED and which is still both on storage and in the metadata table -- and archival cannot
  // advance ahead of the cleaner, so by the time an instant is archived the cleaner has deleted the
  // slices it wrote. Manufacturing one is no help either: a stranger file in a file group the archive
  // never recorded is exactly the "not found" case below. The answer itself is asserted above,
  // against the real archive, which is the level at which it is expressible. See the workspace
  // inventory's sanction for ASL_FOUND_COMPLETED.

  // ---------------------------------------------------------------- fixtures

  /** One (instant, partition, file group, path) tuple the archived timeline itself records. */
  private static final class ArchivedWrite {
    private final String instantTime;
    private final String partition;
    private final String fileId;
    private final String path;

    private ArchivedWrite(String instantTime, String partition, String fileId, String path) {
      this.instantTime = instantTime;
      this.partition = partition;
      this.fileId = fileId;
      this.path = path;
    }

    @Override
    public String toString() {
      return "instant=" + instantTime + " partition=" + partition + " fileId=" + fileId
          + " path=" + path;
    }
  }

  /**
   * A write recorded by the <b>oldest completed commit</b>, read off the ACTIVE timeline — call this
   * before archiving, and archive afterwards.
   *
   * <p>Read while the commit is still active on purpose, and not out of the archive. The archived
   * timeline handed back by {@code getArchivedTimeline()} carries its instants without their commit
   * metadata — a locator that went looking for write stats there found none across 21 archived
   * instants — and the point of these cases is to check what the <em>search under test</em> can get
   * out of the archive, so having the fixture depend on a second archived read would make a failure
   * ambiguous between the two. Capturing the tuple beforehand also makes the assertion stronger: the
   * expected path is the one the writer recorded, not one derived from a name.
   */
  private ArchivedWrite oldestCommittedWrite() throws IOException {
    HoodieTableMetaClient client = metaClient();
    HoodieInstant oldest = client.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .firstInstant().orElseThrow(() -> new IllegalStateException(
            "no completed commit on the active timeline of " + basePath));
    HoodieCommitMetadata metadata = client.getActiveTimeline().readCommitMetadata(oldest);
    for (Map.Entry<String, List<HoodieWriteStat>> perPartition
        : metadata.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : perPartition.getValue()) {
        if (stat.getFileId() != null && stat.getPath() != null) {
          return new ArchivedWrite(oldest.requestedTime(), perPartition.getKey(),
              stat.getFileId(), stat.getPath());
        }
      }
    }
    throw new IllegalStateException("the oldest commit of " + basePath + " records no write stat with "
        + "both a file id and a path: " + metadata.getPartitionToWriteStats());
  }

  /**
   * Upserts with inline clustering after every commit, and returns a write the resulting completed
   * replacecommit recorded, read off the active timeline before anything is archived.
   */
  private ArchivedWrite clusterInline(Map<String, String> writeOptions) throws IOException {
    Map<String, String> clustering = new HashMap<>(writeOptions);
    clustering.put("hoodie.clustering.inline", "true");
    clustering.put("hoodie.clustering.inline.max.commits", "1");
    upsert(clustering);
    HoodieTableMetaClient client = metaClient();
    HoodieInstant replace = client.getActiveTimeline().getCompletedReplaceTimeline().lastInstant()
        .orElseThrow(() -> new IllegalStateException(
            "precondition: inline clustering has to have completed a replacecommit on " + basePath));
    HoodieReplaceCommitMetadata metadata = client.getActiveTimeline().readReplaceCommitMetadata(replace);
    for (Map.Entry<String, List<HoodieWriteStat>> perPartition
        : metadata.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : perPartition.getValue()) {
        if (stat.getFileId() != null && stat.getPath() != null) {
          return new ArchivedWrite(replace.requestedTime(), perPartition.getKey(),
              stat.getFileId(), stat.getPath());
        }
      }
    }
    throw new IllegalStateException("the replacecommit " + replace + " records no write stat with "
        + "both a file id and a path: " + metadata.getPartitionToWriteStats());
  }

  /** A byte copy of {@code original} in a file group of its own, at a caller-chosen instant. */
  private StoragePath strangerAt(StoragePath original, String instantTime) throws IOException {
    return strangerAt(original, STRANGER_FILE_ID, instantTime);
  }

  /** The same as above, with a caller-chosen file id so several strangers can coexist as distinct file groups. */
  private StoragePath strangerAt(StoragePath original, String fileId, String instantTime) throws IOException {
    StoragePath stranger = new StoragePath(original.getParent(),
        fileId + "_1-0-0_" + instantTime + ".parquet");
    copyFile(original, stranger);
    return stranger;
  }

  /**
   * How many times {@code branch} was beaconed in {@code logged}. {@link ObservedRun#branches()}
   * collapses to a set and cannot answer this; this mirrors {@link #branchesIn} but counts instead
   * of deduplicating, because the one-pass guarantee is a claim about a COUNT.
   */
  private static long countBranch(List<String> logged, Branch branch) {
    long count = 0;
    for (String line : logged) {
      int at = line.indexOf(BranchBeacon.PREFIX);
      if (at < 0) {
        continue;
      }
      if (branch.name().equals(line.substring(at + BranchBeacon.PREFIX.length()).trim())) {
        count++;
      }
    }
    return count;
  }

  /**
   * Runs ONE archived pass over {@code targets}, projecting {@code wantedGroups}' bare names, against
   * this test's real on-disk archive — the same call
   * {@code DuplicateBaseFileDimension.adjudicateBatch} and {@code FsMdtDiffDimension} make per task.
   */
  private ArchivedInstantWorkingSet<ArchivedCommitPaths> warmUpArchive(
      Set<String> targets, Set<HoodieFileGroupId> wantedGroups) {
    ArchiveTimelineInstantBatchResolver<ArchivedCommitPaths> resolver =
        ArchiveTimelineInstantBatchResolvers.forTable(metaClient(),
            new CommitPathProjector(metaClient().getBasePath(), wantedGroups));
    return resolver.warmUp(targets);
  }

  /** The bare file name off a write stat's recorded path, which is what the projector names things by. */
  private static String bareName(String path) {
    return new StoragePath(path).getName();
  }

  /** The one archive file that holds {@code instantTime}, or a failure naming what does exist. */
  private static ArchiveFile fileCovering(HoodieArchiveFileCatalog catalog, HoodieArchiveFileScanner scanner,
                                          String instantTime) throws IOException {
    List<ArchiveFile> files = catalog.list();
    for (ArchiveFile file : files) {
      LiveTargets targets = LiveTargets.of(Collections.singleton(instantTime));
      scanner.scan(file, InstantRange.all(), targets, (instant, record) -> {
      });
      if (targets.isFound(instantTime)) {
        return file;
      }
    }
    throw new IllegalStateException("no archive file among " + files + " holds " + instantTime);
  }

}
