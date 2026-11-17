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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.HoodieMetadataTableValidatorV2;
import org.apache.hudi.utilities.util.Branch;
import org.apache.hudi.utilities.util.DanglingReportWriter;
import org.apache.hudi.utilities.util.FsMdtDiffDimension;
import org.apache.hudi.utilities.util.LogFileInstantReader;
import org.apache.hudi.utilities.util.ValidationMode;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The FS-MDT diff dimension along its one deciding axis: the <b>state of a mismatching file's
 * writing instant before the run and after it</b>.
 *
 * <p>That pair — {@code (prev, aft)} — is what {@code FileDiffAdjudicator} selects a row on, and it
 * is a fact about what happened to the table <em>between</em> the run's two timeline reads. Most
 * cells therefore cannot be produced by any arrangement made before a run starts, which is why this
 * suite drives them through
 * {@link HoodieMetadataTableValidatorV2.PhaseObserver}: real table activity, performed at the run's
 * own phase seams, while the run keeps ownership of the ordering.
 *
 * <p><b>Mismatch granularity stops at which side holds the file.</b> The nine fine-grained
 * {@code MismatchPattern} shapes (file group missing, file slice missing, and the rest) collapse to
 * {@code FS_EXCLUSIVE} / {@code MDT_EXCLUSIVE} / {@code BOTH_DISAGREE} before adjudication and
 * nothing switches on the finer grain, so these cases pick whichever shape is cheapest to build and
 * spend their assertions on the state pair and the verdict instead.
 *
 * <p><b>Table version 6 only</b> — see {@link #V6}. The two layouts decide the dangling family of
 * rows differently, so a suite parameterised over both would assert two decision paths under one
 * name.
 *
 * <p>Every case asserts the row (the beacon), the verdict (the report), and how the run ended.
 */
@Tag("functional")
public class TestValidatorV2FsMdtBranches extends ValidatorV2FunctionalTestBase {

  private static final String COW = "COPY_ON_WRITE";
  private static final String MOR = "MERGE_ON_READ";

  /** A file id no writer will ever mint, so a planted file provably belongs to no real file group. */
  private static final String STRANGER_FILE_ID = "00000000-1111-2222-3333-444444444444";

  /**
   * Older than any instant a writer produces, fixed width so the timeline's lexicographic comparison
   * orders it before the oldest commit — so no timeline read can place it and the unresolved arms fire.
   */
  private static final String ANCIENT_INSTANT = "19700101000000000";

  // ============================================================ (COMPLETED, COMPLETED)
  // Both reads agree the commit was finished, so the metadata table's answer is authoritative in
  // both directions and the only remaining question is what the commit says about the file.

  /**
   * <b>An FS-exclusive base file that the completed commit <em>does</em> name is
   * {@code MDT_CORRUPTION}</b> — the primary condition this validator exists to detect, and the one
   * row that says the metadata table lost a committed file.
   *
   * <p>The commit completed before {@code T_prev}, so the metadata snapshot was taken after it
   * finished and its silence about a file the commit claims is a fault rather than lag. Had the same
   * commit completed <em>during</em> the run the metadata table would be legitimately behind and the
   * verdict {@code SANCTIONED} — which is
   * {@link #fileUnderAnInstantThatCompletedDuringTheRunIsSanctioned}, and the two rows differ in the
   * prev state alone.
   */
  @Test
  void baseFileACompletedCommitNamesAndTheMetadataTableLacksIsCorruption() throws Exception {
    writeTable(COW, V6);
    StoragePath lost = strangerCopiedFrom(anyFile(".parquet", false));
    String writtenBy = instantOf(lost);
    nameFileInCommit(writtenBy, relativePartitionOf(lost), lost.getName());

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed(), "a committed file the metadata table does not list is a fault in it");
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED, Branch.IMR_ACTIVE_NAMED)
        .didNotTakeBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING);

    JsonNode record = recordFor(readDiff(), lost.getName());
    assertEquals("MDT_CORRUPTION", record.get("verdict").asText(), "" + record);
    assertEquals("FS_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals(writtenBy, record.get("writingInstant").asText());
    assertEquals(0, countOfVerdict(readDiff(), "DANGLING"),
        "the same file with the commit silent about it would be DANGLING -- a delete "
            + "authorisation. Nothing here may be one: " + readDiff());
  }

  /**
   * <b>The control for the row above, with the one input that decides it changed:</b> the commit is
   * not edited, so it says nothing about the file and the verdict is {@code DANGLING}.
   *
   * <p>Worth its own case because the two verdicts license opposite actions — delete the file, or
   * rebuild the metadata table — off the same pattern, the same states and the same storage. A
   * membership lookup that quietly stopped reading the commit would turn every one of these into the
   * other, and only a pair of cases differing in exactly that input can see it.
   */
  @Test
  void baseFileNoCompletedCommitNamesIsDangling() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed(), "a file nothing references is a dangling file");
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING,
            Branch.IMR_ACTIVE_FILE_GROUP_UNCLAIMED)
        .didNotTakeBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED);
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "the commit was read and does not name this file; the only difference from "
            + "baseFileACompletedCommitNamesAndTheMetadataTableLacksIsCorruption is that write stat");
  }

  /**
   * <b>The commit claims the file group and declines this file: {@code DANGLING} via
   * {@code IMR_ACTIVE_NOT_NAMED}.</b>
   *
   * <p>Separated from {@link #baseFileNoCompletedCommitNamesIsDangling} by the evidence, not the
   * verdict. There the orphan's file id is one nothing ever minted, so no write stat matches the
   * group and membership answers through {@code IMR_ACTIVE_FILE_GROUP_UNCLAIMED} -- "the commit
   * records this partition and nothing for this group". Here the commit is edited to carry a stat
   * for that very file id naming a <em>different</em> file, so the commit demonstrably speaks about
   * the group and declines this member. Both are positive proof and both reach {@code DANGLING}, but
   * they are different reads, and only a case each keeps them apart.
   */
  @Test
  void baseFileACompletedCommitDeclinesWhileClaimingItsGroupIsDangling() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));
    String writtenBy = instantOf(orphan);
    // Same file id, different file: nameFileInCommit takes the stat's file id from the name's first
    // field, so this claims the orphan's group without naming the orphan.
    nameFileInCommit(writtenBy, relativePartitionOf(orphan),
        orphan.getName().replace(".parquet", "-other.parquet"));

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed(), "a commit that speaks about the group and declines this file is proof");
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING, Branch.IMR_ACTIVE_NOT_NAMED)
        .didNotTakeBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED,
            Branch.IMR_ACTIVE_FILE_GROUP_UNCLAIMED);
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText());
  }

  /**
   * <b>Every slice of a file group the metadata table does not know is condemned in one run.</b>
   *
   * <p>Two layers of one orphan group, each under its own completed commit that does not name it.
   * Both are condemned by the same run, so a mitigation clears the whole group in one pass.
   */
  @Test
  void everyLayerOfAnFsExclusiveFileGroupIsDanglingInOneRun() throws Exception {
    writeTable(COW, V6);
    insertMore(writeOptions(COW, V6), "002", 5);
    List<String> commits = new ArrayList<>();
    metaClient().getCommitsTimeline().filterCompletedInstants().getInstants()
        .forEach(instant -> commits.add(instant.requestedTime()));
    assertEquals(2, commits.size(), "the fixture needs two completed commits: " + commits);
    StoragePath seed = anyFile(".parquet", false);
    StoragePath olderLayer = strangerAt(seed, commits.get(0));
    StoragePath newerLayer = strangerAt(seed, commits.get(1));

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed(), "an orphan file group is dangling");
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING)
        .didNotTakeBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED);
    JsonNode diff = readDiff();
    for (StoragePath layer : new StoragePath[] {newerLayer, olderLayer}) {
      JsonNode record = recordFor(diff, layer.getName());
      assertEquals("DANGLING", record.get("verdict").asText(), "" + record);
      assertEquals(instantOf(layer), record.get("writingInstant").asText());
    }
    assertEquals(2, countOfVerdict(diff, "DANGLING"),
        "both layers and nothing else; the older one must not wait for the next run: " + diff);
  }

  /**
   * <b>An older layer its completed commit names is metadata-table corruption, not dangling.</b>
   *
   * <p>Reporting the older slices of an orphan group must not change how each file is judged: the
   * layer the commit names is a committed file the metadata table lost, never a delete
   * authorisation.
   */
  @Test
  void anOlderLayerItsCommitNamesIsCorruptionNotDangling() throws Exception {
    writeTable(COW, V6);
    insertMore(writeOptions(COW, V6), "002", 5);
    List<String> commits = new ArrayList<>();
    metaClient().getCommitsTimeline().filterCompletedInstants().getInstants()
        .forEach(instant -> commits.add(instant.requestedTime()));
    assertEquals(2, commits.size(), "the fixture needs two completed commits: " + commits);
    StoragePath seed = anyFile(".parquet", false);
    StoragePath named = strangerAt(seed, commits.get(0));
    StoragePath unnamed = strangerAt(seed, commits.get(1));
    nameFileInCommit(commits.get(0), relativePartitionOf(named), named.getName());

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed());
    JsonNode diff = readDiff();
    assertEquals("MDT_CORRUPTION", recordFor(diff, named.getName()).get("verdict").asText(),
        "a file its own commit names is the metadata table's to index: " + diff);
    assertEquals("DANGLING", recordFor(diff, unnamed.getName()).get("verdict").asText());
    assertEquals(1, countOfVerdict(diff, "DANGLING"),
        "only the unnamed layer may be deleted: " + diff);
  }

  /**
   * <b>A duplicate pair inside an older orphan layer takes a second round: a pinned limitation.</b>
   *
   * <p>The storage view keeps one base file per slice, so the FS-MDT diff condemns that member and
   * not the other. The duplicate dimension names both, but its adjudication refuses a file group the
   * commit records nothing for ({@code DAJ_ACTIVE_NO_RECORDED_PATHS}) and promotes neither. The
   * survivor is condemned on the next round, as the group's only slice. No file is ever named twice.
   */
  @Test
  void duplicatePairInAnOlderOrphanLayerTakesASecondRound() throws Exception {
    writeTable(COW, V6);
    insertMore(writeOptions(COW, V6), "002", 5);
    List<String> commits = new ArrayList<>();
    metaClient().getCommitsTimeline().filterCompletedInstants().getInstants()
        .forEach(instant -> commits.add(instant.requestedTime()));
    StoragePath seed = anyFile(".parquet", false);
    StoragePath older = strangerAt(seed, commits.get(0));
    StoragePath olderCopy = duplicateOf(older);
    StoragePath newer = strangerAt(seed, commits.get(1));

    ObservedRun first = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(first.passed());
    first.tookBranch(Branch.DAJ_ACTIVE_NO_RECORDED_PATHS);
    JsonNode suspects = readSuspects();
    assertNotNull(recordOrNull(suspects, older.getName()), suspects.toString());
    assertNotNull(recordOrNull(suspects, olderCopy.getName()), suspects.toString());
    JsonNode diff = readDiff();
    Set<String> named = new HashSet<>();
    for (JsonNode record : diff.get("records")) {
      assertTrue(named.add(record.get("fileName").asText()), "named twice: " + diff);
      assertEquals("DANGLING", record.get("verdict").asText(), diff.toString());
    }
    assertTrue(named.contains(newer.getName()), diff.toString());
    assertEquals(2, named.size(), "the newer layer and one member of the pair: " + diff);
    StoragePath survivor = named.contains(older.getName()) ? olderCopy : older;
    for (String fileName : named) {
      deleteFromStorage(new StoragePath(seed.getParent(), fileName));
    }
    storage.deleteDirectory(runDir());

    ObservedRun second = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(second.passed());
    JsonNode secondDiff = readDiff();
    assertEquals("DANGLING", recordFor(secondDiff, survivor.getName()).get("verdict").asText());
    assertEquals(1, secondDiff.get("recordCount").asLong(), secondDiff.toString());
  }

  /**
   * <b>Replaced file groups gain no records.</b>
   *
   * <p>An insert-overwrite leaves the replaced groups' slices on storage until the cleaner runs.
   * Both views drop replaced groups, so neither the latest-slice comparison nor the read of every
   * slice may see them.
   */
  @Test
  void replacedFileGroupsGainNoRecords() throws Exception {
    writeTable(COW, V6);
    int groupsBefore = baseFilesByFileGroup().size();
    makeInsertDf("002", 5).write().format("hudi").options(writeOptions(COW, V6))
        .option(DataSourceWriteOptions.OPERATION().key(),
            WriteOperationType.INSERT_OVERWRITE.value())
        .mode(SaveMode.Append)
        .save(basePath);
    assertTrue(metaClient().reloadActiveTimeline().getCompletedReplaceTimeline().countInstants() > 0,
        "precondition: a completed replace commit");
    assertTrue(baseFilesByFileGroup().size() > groupsBefore,
        "precondition: the replaced groups are still on storage beside the new ones");

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertTrue(run.passed(), "a table whose only extra slices belong to replaced groups is healthy");
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "a run with no finding writes no report");
  }

  /**
   * <b>The same cell for a log file that the delta commit names.</b>
   *
   * <p>A log file is not the same object as a base file to this validator and must be covered
   * separately: on table version 6 its <em>name</em> carries the slice's base commit rather than the
   * instant that wrote it, so the writing instant comes from the first block's header
   * ({@code LogFileInstantReader}) instead. A row reached on a base file therefore proves nothing
   * about the same row on a log file — the two arrive with their instant established by different
   * machinery.
   *
   * <p>The orphan is a byte copy of a real log file at a new log version, so its first block is a
   * real block whose header names a real completed delta commit. That commit is then edited to claim
   * it.
   */
  @Test
  void logFileACompletedCommitNamesAndTheMetadataTableLacksIsCorruption() throws Exception {
    writeTable(MOR, V6);
    StoragePath lost = orphanLogFileBeside(anyFile(".log.", true));
    String writtenBy = writingInstantOfLogFile(lost);
    nameFileInCommit(writtenBy, relativePartitionOf(lost), lost.getName());

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed());
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED)
        .didNotTakeBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING);
    JsonNode record = recordFor(readDiff(), lost.getName());
    assertEquals("MDT_CORRUPTION", record.get("verdict").asText(),
        "a log file the delta commit names and the metadata table lacks is the same loss of a "
            + "committed file as a base file's: " + record);
    assertEquals("LOG", record.get("fileType").asText());
    assertEquals("LOG_BLOCK_HEADER", record.get("instantSource").asText(),
        "and it was attributed by its first block's header, never by its name -- which is what "
            + "makes this case distinct from the base-file one rather than a restatement of it: "
            + record);
  }

  /**
   * <b>The log-file twin of the DANGLING row.</b> Same orphan, same block-header attribution, and the
   * delta commit is left alone — so nothing names it and the verdict is {@code DANGLING}.
   *
   * <p>Paired with the case above for the same reason the base-file pair exists: the two verdicts
   * license opposite actions off identical storage, and only the write stat differs.
   */
  @Test
  void logFileNoCompletedCommitNamesIsDangling() throws Exception {
    writeTable(MOR, V6);
    StoragePath orphan = orphanLogFileBeside(anyFile(".log.", true));

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed(), "a log file nothing references is a dangling file");
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING)
        .didNotTakeBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED);
    JsonNode record = recordFor(readDiff(), orphan.getName());
    assertEquals("DANGLING", record.get("verdict").asText(), "" + record);
    assertEquals("LOG", record.get("fileType").asText());
    assertEquals("LOG_BLOCK_HEADER", record.get("instantSource").asText(), "" + record);
  }

  /**
   * <b>Same cell, and the instant cannot answer: {@code NOT_SETTLED}.</b>
   *
   * <p>The file is attributed to a <b>completed compaction that recorded no write stat at all</b>, so
   * membership is unresolvable rather than negative. The row matters because it is what keeps the two
   * fatal rows above honest: "the commit does not name this file" and "the commit never claimed to
   * write anything" look identical through an empty write-stat list, and reading the second as the
   * first would condemn every file whose instant is a compaction plan, a clean or a rollback.
   *
   * <p><b>A compaction, and not a clean, and the reason is reachability rather than taste.</b> The
   * file-system side of the comparison is built from
   * {@code filterCompletedAndCompactionInstants}, so a file attributed to a completed <em>clean</em>
   * is invisible to it and produces no diff record at all — the case would pass while asserting
   * nothing. A compaction instant is admitted, which is what puts the file in front of the
   * adjudicator with an instant whose metadata claims nothing.
   */
  @Test
  void fileAttributedToACompletedInstantThatClaimsNothingIsNotSettled() throws Exception {
    writeTable(COW, V6);
    StoragePath seed = anyFile(".parquet", false);
    String compaction = plantInflightCompaction();
    StoragePath orphan = strangerAt(seed, compaction);
    completeCompaction(compaction, null, null);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_UNKNOWN,
            Branch.IMR_ACTIVE_NO_WRITE_STATS_AT_ALL)
        .didNotTakeBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING,
            Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED);
    JsonNode record = recordFor(readDiff(), orphan.getName());
    assertEquals("NOT_SETTLED", record.get("verdict").asText(),
        "an instant that never claimed to have written anything cannot be read as one declining to "
            + "claim this file: " + record);
    assertTrue(run.passed(),
        "and NOT_SETTLED is non-fatal by design -- a verdict reached because a question could not "
            + "be answered would be a bug: " + run.thrown());
  }

  // ============================== (pending, COMPLETED) — the observer's second seam
  // The instant was not completed when the metadata snapshot was pinned and was completed by the
  // time the run finished. No arrangement made before a run starts can produce that.
  //
  // THE ROUTE MATTERS, and only one works. An ordinary commit landing at the run's FIRST seam does
  // NOT reach these rows: the file-system side of the comparison is built from
  // TimelineUtils.getVisibleTimelineForFsView(metaClient), metaClient.getActiveTimeline() is cached
  // (HoodieTableMetaClient:560-565), and HoodieFileGroup gates slice visibility on
  // containsOrBeforeTimelineStarts -- so the new commit's files are invisible to BOTH sides and
  // produce no diff record at all. (That is itself a property worth having, and
  // TestValidatorV2Reconciliation#commitLandingAfterTheSnapshotIsInTheDeltaAndNotInTheDiff asserts
  // it.) What DOES reach them is a pending COMPACTION: filterCompletedAndCompactionInstants includes
  // compaction instants even while they are pending, so a base file beside a pending compaction is
  // visible to the file-system side and absent from the metadata table, and completing that
  // compaction at the run's second seam moves the aft state to COMPLETED.

  /**
   * What the mid-run compaction's own metadata says about the file planted beside it. The verdict is
   * the same for all three, which is the row's whole content.
   */
  private enum WhatTheCompactionRecords {
    /** A write stat naming this very file. */
    NAMES_THIS_FILE,
    /** A write stat naming a different file of the same file group, so it demonstrably declines this one. */
    NAMES_ANOTHER_FILE,
    /** No write stat at all, so membership is unresolvable rather than negative. */
    NOTHING_AT_ALL
  }

  /**
   * <b>An FS-exclusive file whose writing instant was open at {@code T_prev} and completed during the
   * run is {@code SANCTIONED}, whatever that instant's commit metadata says about it.</b>
   *
   * <p>The twin of {@link #baseFileACompletedCommitNamesAndTheMetadataTableLacksIsCorruption},
   * separated by the prev state alone: the metadata table was loaded before this instant completed,
   * so it is legitimately behind rather than damaged. Merge the two rows and every table service
   * completing while a validation runs is reported as metadata-table corruption.
   *
   * <p><b>Membership is not consulted on this row, and that is the deliberate part.</b> An instant
   * still open when the run began may retry a task and leave a second attempt on storage under its
   * own instant; the commit records only the attempt that won, so a file carrying this instant is its
   * own output whether or not the commit names it. Reading that silence as a denial condemns a live
   * writer's ordinary output, which is the failure mode this row exists to avoid.
   *
   * <p><b>The cost, and it is real.</b> An orphan whose writing instant completes between the two
   * reads is never reported, whenever the file itself was written. The same shape under an instant
   * already completed at {@code T_prev} is still caught -- see
   * {@link #baseFileACompletedCommitDeclinesWhileClaimingItsGroupIsDangling} and
   * {@code TestValidatorV2Reconciliation#retryOrphanLogFileUnderACompletedCommitIsDangling} -- so
   * what goes dark is the mid-run window alone.
   *
   * <p>The compaction is completed at the run's second seam, after phase 1 has listed storage and
   * before {@code T_aft} is read, which is exactly the window the row describes.
   */
  @ParameterizedTest
  @EnumSource(WhatTheCompactionRecords.class)
  void fileUnderAnInstantThatCompletedDuringTheRunIsSanctioned(WhatTheCompactionRecords records)
      throws Exception {
    writeTable(COW, V6);
    StoragePath seed = anyFile(".parquet", false);
    String compaction = plantInflightCompaction();
    StoragePath produced = strangerAt(seed, compaction);
    String partition = relativePartitionOf(produced);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            switch (records) {
              case NAMES_THIS_FILE:
                completeCompaction(compaction, partition, produced.getName());
                break;
              case NAMES_ANOTHER_FILE:
                completeCompaction(compaction, partition,
                    produced.getName().replace(".parquet", "-other.parquet"));
                break;
              case NOTHING_AT_ALL:
                completeCompaction(compaction, null, null);
                break;
              default:
                throw new IllegalStateException("unhandled " + records);
            }
          }
        });

    run.tookBranch(Branch.FDA_FS_PREV_OPEN_AFT_DONE_SANCTIONED)
        .didNotTakeBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_COMMITTED,
            Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING)
        // The row's whole content: the commit is never asked. Without this the case asserts the
        // verdict a membership-consulting row would also have reached on the NAMES_THIS_FILE arm.
        .tookNoBranchNamed("IMR_");
    JsonNode diff = readDiff();
    JsonNode record = recordFor(diff, produced.getName());
    assertEquals("SANCTIONED", record.get("verdict").asText(),
        "the instant completed during the run, so this file is its own output whatever the commit "
            + "recorded (" + records + "): " + record);
    assertEquals("FS_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals(compaction, record.get("writingInstant").asText(),
        "and it is judged on the instant it carries, which is the one that completed mid-run");
    assertEquals(0, countOfVerdict(diff, "DANGLING"),
        () -> "nothing in this run may be a delete authorisation: " + diff);
    assertTrue(run.passed(), "lag is not a finding: " + run.thrown());
  }

  /** What a completed commit may record that does not claim a file carrying its instant. */
  private enum HowTheCommitDeclines {
    /** Another file of the same file group: the task-retry shape. */
    NAMES_A_SIBLING,
    /** A file of another file group in the same partition. */
    CLAIMS_ANOTHER_FILE_GROUP,
    /** Nothing in this file's partition. */
    CLAIMS_ANOTHER_PARTITION
  }

  /**
   * <b>The sanction on the open-to-completed row lasts one run.</b> The file the row sanctions in the
   * run its instant completes in is condemned by the next run, where the same instant is completed
   * at both reads and its commit's answer is authoritative.
   */
  @ParameterizedTest
  @EnumSource(HowTheCommitDeclines.class)
  void anOrphanSanctionedInTheRunItsInstantCompletesIsCondemnedByTheNextRun(
      HowTheCommitDeclines declines) throws Exception {
    writeTable(COW, V6);
    StoragePath seed = anyFile(".parquet", false);
    String compaction = plantInflightCompaction();
    StoragePath orphan = strangerAt(seed, compaction);
    String partition = relativePartitionOf(orphan);

    ObservedRun first = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            switch (declines) {
              case NAMES_A_SIBLING:
                completeCompaction(compaction, partition,
                    orphan.getName().replace(".parquet", "-other.parquet"));
                break;
              case CLAIMS_ANOTHER_FILE_GROUP:
                completeCompaction(compaction, partition,
                    orphan.getName().replace(STRANGER_FILE_ID, "11111111-2222-3333-4444-555555555555"));
                break;
              case CLAIMS_ANOTHER_PARTITION:
                completeCompaction(compaction, partition + "-elsewhere", orphan.getName());
                break;
              default:
                throw new IllegalStateException("unhandled " + declines);
            }
          }
        });
    first.tookBranch(Branch.FDA_FS_PREV_OPEN_AFT_DONE_SANCTIONED);
    assertEquals("SANCTIONED", recordFor(readDiff(), orphan.getName()).get("verdict").asText());
    assertTrue(first.passed(), "run 1 sanctions the orphan: " + first.thrown());

    storage.deleteDirectory(new StoragePath(basePath, OUTPUT_DIR));
    ObservedRun second = observeRun(ValidationMode.EXHAUSTIVE, true);

    second.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING)
        .didNotTakeBranch(Branch.FDA_FS_PREV_OPEN_AFT_DONE_SANCTIONED);
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "the commit completed before run 2 began and does not claim the file (" + declines + ")");
    assertFalse(second.passed(), "run 2 condemns the orphan run 1 sanctioned");
  }

  // ============================ (COMPLETED, pending or gone) — the observer's second seam
  // The instant left completed while the run was in progress, which only a rollback or a revert can
  // do, and which nothing before the run can arrange.

  /**
   * <b>A completed commit reverted mid-run with no rollback plan anywhere is a {@code CODE_BUG}, in
   * both modes.</b>
   *
   * <p>{@code HoodieActiveTimeline.revertToInflight} is only ever called from inside an
   * already-in-progress rollback ({@code CopyOnWriteRollbackActionExecutor},
   * {@code MergeOnReadRollbackActionExecutor}), which itself only runs against a rollback instant
   * already {@code REQUESTED} or {@code INFLIGHT} on the timeline. So a completed instant genuinely
   * leaving completed always comes with a rollback plan on the timeline somewhere; a departure with
   * no plan at all -- readable or not -- cannot arise from any write path this table supports. It is
   * a defect, not an inconclusive table, and must fail the run rather than merely report
   * {@code NOT_SETTLED}. We do not test the detailed shape of this any further than that: see the
   * class javadoc's "second exception" for the reasoning.
   */
  @Test
  void commitRevertedWithNoRollbackPlanIsACodeBugInExhaustiveMode() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));
    String writtenBy = instantOf(orphan);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, false,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToInflight(writtenBy);
          }
        });

    assertTrue(run.thrown().isPresent(),
        "an unexplained departure from completed cannot arise from any real write path: " + run.thrown());
    assertTrue(run.thrown().get() instanceof HoodieValidationException, "" + run.thrown().get());
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_GONE);
  }

  /** The fail-fast twin of {@link #commitRevertedWithNoRollbackPlanIsACodeBugInExhaustiveMode}. */
  @Test
  void commitRevertedWithNoRollbackPlanIsACodeBugInFailFastMode() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));
    String writtenBy = instantOf(orphan);

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, false,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToInflight(writtenBy);
          }
        });

    assertTrue(run.thrown().isPresent(), "" + run.thrown());
    assertTrue(run.thrown().get() instanceof HoodieValidationException, "" + run.thrown().get());
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_GONE);
  }

  /**
   * <b>The same departure with a rollback that accounts for it is <em>still</em> a
   * {@code CODE_BUG}.</b>
   *
   * <p>A rollback requested before the run targets the instant, so the reconciler could say how the
   * departure happened -- and the verdict does not change, because that is not what this row asks.
   * The rule is that a run never observes a completed instant in any other state; a plan explains
   * the mechanism, not the observation. The pair with the case above is what pins that: identical
   * arrangements but for the plan, and the same verdict either way.
   */
  @Test
  void revertIsACodeBugEvenWhenAPlannedRollbackExplainsIt() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));
    String writtenBy = instantOf(orphan);
    plantPendingRollback(writtenBy, relativePartitionOf(orphan), orphan.getName());

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToInflight(writtenBy);
          }
        });

    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_GONE);
    assertEquals("CODE_BUG", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "a rollback plan accounts for HOW the instant left completed, not for a run having "
            + "observed it leave");
    assertFalse(run.passed(), "and the row is fatal however the departure is explained");
  }

  /**
   * <b>One step further along the same departure: only {@code .requested} remains, and a rollback
   * naming the instant does not change it either -- {@code CODE_BUG}, exactly as at the earlier
   * {@code .inflight} point.</b>
   *
   * <p>{@code BaseRollbackActionExecutor#deleteInflightAndRequestedInstant} deletes an instant's
   * {@code .inflight} file before its {@code .requested} file, so a rollback caught mid-delete can
   * leave a once-completed instant with only {@code .requested} present -- one step closer to
   * {@code not exists} than {@link #revertIsACodeBugEvenWhenAPlannedRollbackExplainsIt} reaches, and never
   * a path back to {@code COMPLETED}. {@code isPendingOrGone} already treats {@code REQUESTED},
   * {@code INFLIGHT} and {@code NULL} identically, so this proves that merged handling actually holds
   * at this further point in the sequence, not merely at the one point every other case in this row
   * happens to use.
   */
  @Test
  void revertLeftAtRequestedOnlyIsACodeBugTheSameAsInflight() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));
    String writtenBy = instantOf(orphan);
    plantPendingRollback(writtenBy, relativePartitionOf(orphan), orphan.getName());

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToRequestedOnly(writtenBy);
          }
        });

    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_GONE);
    assertEquals("CODE_BUG", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "the same verdict at the requested-only point in the same departure");
    assertFalse(run.passed(), "" + run.thrown());
  }

  /**
   * <b>The terminal point of the same departure: the instant is fully gone, and a rollback that
   * completed during the run does not change the verdict either -- {@code CODE_BUG}.</b>
   *
   * <p>By the time nothing of the instant remains, the rollback that removed it may also have
   * finished, so the explaining evidence naturally comes from {@link ServiceActivityDelta}'s
   * completed-rollback loop (see {@link #plantCompletedRollback}) rather than a plan still pending at
   * either read, which is what {@link #revertIsACodeBugEvenWhenAPlannedRollbackExplainsIt} and
   * {@link #revertLeftAtRequestedOnlyIsACodeBugTheSameAsInflight} use. Exercising the completed
   * loop matters even though it cannot change the verdict: it is the one arrangement where the
   * explaining evidence arrives from {@code ServiceActivityDelta} rather than from a pending plan,
   * and the row has to be unmoved by that too. Same row, same merged {@code isPendingOrGone}
   * handling -- the third and final point in "(.req,.inflight) -&gt; (.req) -&gt; (not exists)".
   */
  @Test
  void revertLeftAtNotExistsIsACodeBugEvenWithACompletedRollback() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));
    String writtenBy = instantOf(orphan);
    String partition = relativePartitionOf(orphan);
    String fileName = orphan.getName();
    // The instant being reverted is about to be removed from the timeline entirely -- leave a
    // second, untouched commit on the table, or the active timeline would have no completed
    // instant left at all once the revert lands, which breaks phase 1's own listing rather than
    // exercising the row under test.
    upsert(writeOptions(COW, V6));

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToNotExists(writtenBy);
            try {
              plantCompletedRollback(writtenBy, partition, fileName);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });

    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_GONE);
    assertEquals("CODE_BUG", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "a rollback that COMPLETED during the run reaches the row through ServiceActivityDelta "
            + "rather than through a pending plan, and moves it no more than a pending one does");
    assertFalse(run.passed(), "" + run.thrown());
  }

  // ==================================================== MDT-exclusive, (COMPLETED, COMPLETED)

  /**
   * <b>A base file a completed commit's metadata table still names and storage has lost is an
   * {@code OUTBOUND_DELETION}</b> — something outside Hudi deleted committed data.
   *
   * <p>This is the one MDT-exclusive row that reaches a fatal verdict from {@code r()} answering
   * "nothing accounts for it", and the asymmetry is deliberate: on this side "the file is gone" is
   * the finding rather than the absence of one, so only a clean or rollback that positively claims
   * the file withdraws it.
   */
  @Test
  void baseFileDeletedBehindTheMetadataTableIsAnOutboundDeletion() throws Exception {
    writeTable(COW, V6);
    StoragePath victim = anyFile(".parquet", false);
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertFalse(run.passed(), "committed data disappearing from storage is a fault");
    run.tookBranch(Branch.FDA_MDT_PREV_DONE_AFT_DONE_OUTBOUND_DELETION,
            Branch.PSR_NOTHING_ACCOUNTS_FOR_IT)
        .didNotTakeBranch(Branch.FDA_MDT_PREV_DONE_AFT_DONE_SANCTIONED);
    JsonNode record = recordFor(readDiff(), victim.getName());
    assertEquals("OUTBOUND_DELETION", record.get("verdict").asText(), "" + record);
    assertEquals("MDT_EXCLUSIVE", record.get("diffPattern").asText());
  }

  /**
   * <b>The same disappearance, explained by a clean that planned it: {@code SANCTIONED}.</b>
   *
   * <p>A clean pending at {@code T_prev} is the shape the service-activity delta cannot see — its
   * state never changes during the run, so it contributes nothing to a state diff while having
   * already deleted files the metadata table still names. The pair with the row above is the
   * difference between a healthy table with a table service in progress and a page-out.
   */
  @Test
  void deletionAPendingCleanPlannedIsSanctioned() throws Exception {
    writeTable(COW, V6);
    StoragePath victim = anyFile(".parquet", false);
    String partition = relativePartitionOf(victim);
    plantPendingClean(partition, victim.getName());
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    run.tookBranch(Branch.FDA_MDT_PREV_DONE_AFT_DONE_SANCTIONED, Branch.PSR_FILE_CLAIMED)
        .didNotTakeBranch(Branch.FDA_MDT_PREV_DONE_AFT_DONE_OUTBOUND_DELETION);
    assertEquals("SANCTIONED", recordFor(readDiff(), victim.getName()).get("verdict").asText(),
        "a clean that had undertaken to delete this file explains its absence");
    assertTrue(run.passed(), "" + run.thrown());
  }

  /**
   * <b>An unreadable, still-{@code .requested} clean plan sanctions the deletion rather than
   * condemning it.</b>
   *
   * <p>The same disappearance as
   * {@link #baseFileDeletedBehindTheMetadataTableIsAnOutboundDeletion}, with one input changed: a
   * clean is pending and its plan cannot be read. A {@code .requested}-only plan may simply not be
   * fully flushed yet, so this is loosened to {@code SANCTIONED} rather than left
   * {@code NOT_SETTLED}: the mismatch it might explain is exactly the shape a write still in
   * progress leaves behind. See
   * {@link #deletionWhoseAlreadyInflightCleanPlanCannotBeReadStaysUnresolved} for the same
   * corruption once that race is over.
   *
   * <p>This is still the guard that keeps the fatal MDT-exclusive row honest.
   * {@code OUTBOUND_DELETION} pages someone; reaching it because one clean plan happened to be
   * corrupt would page them about a healthy table. The plan is corrupted by overwriting the
   * requested instant file with bytes that are neither valid avro nor a valid plan, which is what an
   * interrupted plan write leaves behind.
   */
  @Test
  void deletionWhoseRequestedOnlyCleanPlanCannotBeReadIsSanctioned() throws Exception {
    writeTable(COW, V6);
    StoragePath victim = anyFile(".parquet", false);
    plantPendingClean(relativePartitionOf(victim), victim.getName());
    corruptThePendingCleanPlan();
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    run.tookBranch(Branch.FDA_MDT_PREV_DONE_AFT_DONE_SANCTIONED,
            Branch.PSR_REQUESTED_ONLY_PLAN_UNREADABLE_SANCTIONED)
        .didNotTakeBranch(Branch.FDA_MDT_PREV_DONE_AFT_DONE_OUTBOUND_DELETION,
            Branch.FDA_MDT_PREV_DONE_AFT_DONE_UNRESOLVED);
    assertEquals("SANCTIONED", recordFor(readDiff(), victim.getName()).get("verdict").asText(),
        "a .requested-only clean plan that could not be read may simply not be fully flushed yet, "
            + "so the mismatch it might explain is sanctioned rather than left unresolved");
    assertTrue(run.passed(), "" + run.thrown());
  }

  /**
   * <b>Once a clean plan has been promoted to {@code .inflight}, an unreadable plan stays
   * {@code NOT_SETTLED} -- the loosening above is narrow by design.</b>
   *
   * <p>{@link #deletionWhoseRequestedOnlyCleanPlanCannotBeReadIsSanctioned}'s loosening exists
   * because a {@code .requested}-only plan may simply not be fully flushed yet. Once the writer has
   * promoted it to {@code .inflight} that race is over, so a plan that still cannot be read at that
   * point is genuine corruption and must stay refused rather than sanctioned.
   */
  @Test
  void deletionWhoseAlreadyInflightCleanPlanCannotBeReadStaysUnresolved() throws Exception {
    writeTable(COW, V6);
    StoragePath victim = anyFile(".parquet", false);
    plantPendingClean(relativePartitionOf(victim), victim.getName());
    promotePendingCleanToInflight();
    corruptThePendingCleanPlan();
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    run.tookBranch(Branch.FDA_MDT_PREV_DONE_AFT_DONE_UNRESOLVED, Branch.PSR_PLAN_UNREADABLE)
        .didNotTakeBranch(Branch.FDA_MDT_PREV_DONE_AFT_DONE_OUTBOUND_DELETION,
            Branch.FDA_MDT_PREV_DONE_AFT_DONE_SANCTIONED);
    assertEquals("NOT_SETTLED", recordFor(readDiff(), victim.getName()).get("verdict").asText(),
        "a clean plan already promoted past .requested that still cannot be read is genuine "
            + "corruption, and a verdict reached by exclusion would page someone about a healthy "
            + "table");
    assertTrue(run.passed(), "refusing is never fatal: " + run.thrown());
  }

  /**
   * <b>MDT-exclusive, and the instant left completed with no rollback plan anywhere: a
   * {@code CODE_BUG}, in both modes.</b>
   *
   * <p>The MDT-exclusive counterpart of
   * {@link #commitRevertedWithNoRollbackPlanIsACodeBugInExhaustiveMode}, and the same
   * {@code (COMPLETED, pending-or-gone)} cell reached from the other side of the comparison. Both
   * sides have to be covered because the two rows are separate code with separate reconciler calls:
   * the FS-exclusive one withdraws a would-be {@code DANGLING}, this one withdraws a would-be
   * {@code OUTBOUND_DELETION}, and a change that fixed one and not the other would be invisible to a
   * suite that tested only the FS side.
   */
  @Test
  void deletionUnderACommitRevertedWithNoRollbackPlanIsACodeBugInExhaustiveMode() throws Exception {
    writeTable(COW, V6);
    StoragePath victim = anyFile(".parquet", false);
    String writtenBy = instantOf(victim);
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, false,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToInflight(writtenBy);
          }
        });

    assertTrue(run.thrown().isPresent(),
        "an unexplained departure from completed cannot arise from any real write path: " + run.thrown());
    assertTrue(run.thrown().get() instanceof HoodieValidationException, "" + run.thrown().get());
    run.tookBranch(Branch.FDA_MDT_PREV_DONE_AFT_GONE);
  }

  /**
   * The fail-fast twin of {@link #deletionUnderACommitRevertedWithNoRollbackPlanIsACodeBugInExhaustiveMode}.
   */
  @Test
  void deletionUnderACommitRevertedWithNoRollbackPlanIsACodeBugInFailFastMode() throws Exception {
    writeTable(COW, V6);
    StoragePath victim = anyFile(".parquet", false);
    String writtenBy = instantOf(victim);
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, false,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToInflight(writtenBy);
          }
        });

    assertTrue(run.thrown().isPresent(), "" + run.thrown());
    assertTrue(run.thrown().get() instanceof HoodieValidationException, "" + run.thrown().get());
    run.tookBranch(Branch.FDA_MDT_PREV_DONE_AFT_GONE);
  }

  /**
   * <b>The same departure with a rollback that accounts for it, on the MDT-exclusive side: still a
   * {@code CODE_BUG}.</b>
   *
   * <p>The pair with the case above completes the {@code (COMPLETED, gone)} cell on this side, and
   * says the same thing its FS-exclusive twin
   * ({@link #revertIsACodeBugEvenWhenAPlannedRollbackExplainsIt}) says: the plan is not consulted,
   * because the observation rather than its explanation is the finding. Both sides need their own
   * case -- they are separate code -- and a change that dropped the rule on one and not the other
   * would otherwise be invisible.
   */
  @Test
  void deletionUnderARevertIsACodeBugEvenWhenAPlannedRollbackExplainsIt() throws Exception {
    writeTable(COW, V6);
    StoragePath victim = anyFile(".parquet", false);
    String writtenBy = instantOf(victim);
    plantPendingRollback(writtenBy, relativePartitionOf(victim), victim.getName());
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToInflight(writtenBy);
          }
        });

    run.tookBranch(Branch.FDA_MDT_PREV_DONE_AFT_GONE);
    assertEquals("CODE_BUG", recordFor(readDiff(), victim.getName()).get("verdict").asText(),
        "the MDT-exclusive side reads the departure the same way the FS-exclusive side does");
    assertFalse(run.passed(), "" + run.thrown());
  }

  // ============================================ the two claims a beacon cannot see
  // Both were carried by TestFsMdtDiffDimensionE2E and both were nearly lost with it: a beacon harvest
  // is blind to a test whose claim is that NOTHING happened (no record, so no beacon) and to a test
  // whose claim is about a run CONFIGURATION (same beacons as an ordinary run). Restored here after a
  // review pointed out that the deletion's "zero unique branches" argument cannot reach either.

  /**
   * <b>A real clean moves storage and the metadata table together, so the comparison has nothing to
   * report.</b>
   *
   * <p>The writer's own cleaner — a completed clean instant, files really deleted — and the run then
   * produces <em>no record at all</em>, not a record explained away. That distinction is the whole
   * value of the case: every other clean scenario in these suites plants a <em>pending</em> clean and
   * asserts it <em>explains</em> a difference, which is the opposite fixture. Without this one nothing
   * in the tree runs Hudi's cleaner, and a regression that made an ordinary clean look like an
   * MDT-exclusive deletion would fail no test.
   *
   * <p>It also cannot be covered by a branch assertion, by construction: a run that emits no record
   * emits no adjudication beacon, so this claim is invisible to the coverage harvest and has to be
   * asserted on the absence of the output directory instead.
   */
  @Test
  void realCleanLeavesTheComparisonNothingToReport() throws Exception {
    writeCowTableWithASupersededSlice(V6);
    runClean(writeOptions(COW, V6), 1);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true);

    assertTrue(run.passed(),
        "a clean is not a difference: it removes the file from storage and from the metadata table, "
            + "and the comparison sees two sides that agree: " + run.thrown());
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "and it produces no record at all -- not a SANCTIONED one. A directory here would mean the "
            + "comparison found something to explain away, which is a different (and worse) shape");
    // No FS-MDT verdict row fired, rather than "no row at all": the duplicate dimension beacons its
    // healthy-slice decision on every table, so an empty set is not the property. Anything with an
    // FDA_ or SWT_ prefix would mean the comparison produced a record and then explained it away,
    // which is a different and worse shape than producing none.
    run.tookNoBranchNamed("FDA_").tookNoBranchNamed("SWT_");
  }

  /**
   * <b>The FS-MDT dimension alone is a whole run.</b>
   *
   * <p>Runs with {@code --validate-duplicate-base-files=false} and reads the FS-MDT documents back —
   * the mirror of what {@code TestValidatorV2DuplicateBranches} does for the other dimension. The two
   * dimensions are independent by design and either one is a complete run; this is the half of that
   * contract nothing else asserts, and a change that made switching dimension A off skip the report
   * writing or short-circuit phase 2 would otherwise be caught by no test.
   *
   * <p>Invisible to a branch harvest for the same reason as the case above, differently: an isolated
   * run takes the <em>same</em> rows as a both-dimensions run, so it can never show a unique branch
   * however much attribution power it carries.
   */
  @Test
  void fsMdtDimensionAloneIsAWholeRun() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true,
        /* validateDuplicateBaseFiles */ false, /* validateFsMdtDiff */ true, null);

    assertFalse(run.passed(), "the orphan still fails the run with this dimension alone");
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING)
        .didNotTakeBranch(Branch.DUP_SLICE_CARDINALITY_FINDING,
            Branch.DAJ_ACTIVE_NOT_NAMED, Branch.DAJ_ACTIVE_REFERENCED);
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "and the FS-MDT documents are written in full, so the verdict is attributable to this "
            + "dimension and to nothing else");
    assertEquals("DANGLING",
        recordFor(readDiffAdjudicated(), orphan.getName()).get("verdict").asText(),
        "both of its documents, not just the raw one");
    assertFalse(storage.exists(new StoragePath(runDir(), DanglingReportWriter.SUSPECTS_DIR)),
        "and the dimension that did not run writes no directory: nothing looked, so there is nothing "
            + "for a mitigation job to read as this run's conclusion about duplicates");
  }

  // ============================== the run-level outcomes: "nothing was validated"
  // A run whose window cannot be trusted must reach NO verdict rather than a wrong one, and both modes
  // answer that differently. Reachable only through the phase seams: archival has to happen BETWEEN the
  // two archive readings, which is exactly what the second seam is.

  /**
   * <b>A run the archiver overlapped adjudicates nothing, and says so.</b>
   *
   * <p>The archiver removes completed instants from the active timeline and leaves nothing on it to say
   * so, so a run it overlapped has a delta missing a whole class of change: an instant archived mid-run
   * is indistinguishable, from the active timeline alone, from one a rollback erased. Rather than
   * adjudicate against an account of the window that is knowingly incomplete, the dimension stops.
   *
   * <p>Asserted in <b>both modes</b> because they answer differently and both answers are deliberate
   * residuals: fail-fast reports success with a stable log marker (a caller cannot tell this from a
   * clean run by return value, which is why the marker is a contract), and exhaustive raises — the mode
   * exists to produce a findings document, and producing none silently is the one outcome it must not
   * have.
   *
   * <p>The damaged file is planted so the run has something it <em>would</em> have condemned: the point
   * is not that a clean table passes, it is that a <b>dangling file goes unreported</b> because the run
   * refuses to conclude. That is the cost of the trade, and it should be visible in the test.
   */
  @Test
  void runTheArchiverOverlappedReachesNoVerdictInEitherMode() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerCopiedFrom(anyFile(".parquet", false));

    ObservedRun failFast = observeRun(ValidationMode.FAIL_FAST, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            archiveUnchecked(writeOptions(COW, V6));
          }
        });

    failFast.tookBranch(Branch.FSM_ARCHIVAL_RAN_INCONCLUSIVE,
            Branch.MTV_ARCHIVAL_FAIL_FAST_SKIPPED)
        .tookNoBranchNamed("FDA_")
        .tookNoBranchNamed("SWT_");
    assertTrue(failFast.passed(),
        "fail-fast reports success, having validated nothing: " + failFast.thrown());
    assertTrue(anyLineContains(failFast.log(), "MDT_VALIDATOR_ARCHIVAL_SKIPPED"),
        "and the stable marker is the only in-band signal a caller gets, so it must be there: "
            + failFast.log().size() + " lines captured");
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "the orphan at " + orphan.getName() + " goes unreported -- which is the cost of the trade, "
            + "not a claim that the table is healthy");

    // The same interleaving, exhaustive: it raises rather than silently producing no document.
    ObservedRun exhaustive = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            archiveUnchecked(writeOptions(COW, V6));
          }
        });
    exhaustive.tookBranch(Branch.MTV_ARCHIVAL_EXHAUSTIVE_FAILED);
    assertFalse(exhaustive.passed(),
        "exhaustive must not report success on a run that produced no findings document");
  }

  /**
   * <b>The {@code (COMPLETED, gone)} cell with an unreadable {@code .requested}-only plan, on both
   * sides: {@code CODE_BUG}, the same as with a readable one.</b>
   *
   * <p>Elsewhere in the validator a {@code .requested}-only plan that cannot be read is treated
   * gently, because it may simply not be fully flushed yet -- that is
   * {@link #deletionWhoseRequestedOnlyCleanPlanCannotBeReadIsSanctioned}, and it still holds on the
   * {@code (COMPLETED, COMPLETED)} row. This row is where that reasoning does not apply: it never
   * asks a plan anything, so a plan's readability cannot move it. Paired with
   * {@link #revertWithAnInflightUnreadablePlanIsACodeBugOnBothSides}, which is the same corruption
   * one promotion later, the two say the plan's state is irrelevant here in both directions.
   *
   * <p>Both sides in one case because the arrangement they need is identical: a corrupt pending
   * clean, and the writing instant taken off the completed timeline at the second seam.
   */
  @Test
  void revertWithARequestedOnlyUnreadablePlanIsACodeBugOnBothSides() throws Exception {
    writeTable(COW, V6);
    // The committed file the orphan is copied from is ALSO the MDT-exclusive victim: same instant, same
    // partition, and one revert then moves both records into the (COMPLETED, gone) cell. Named
    // explicitly rather than looked up by instant — a lookup by instant in this partition can return
    // the orphan itself, and deleting that leaves no FS-exclusive record and nothing to adjudicate.
    StoragePath victim = anyFile(".parquet", false);
    // FS-exclusive: an orphan on storage the metadata table never had, under the victim's instant.
    StoragePath orphan = strangerCopiedFrom(victim);
    String writtenBy = instantOf(orphan);
    plantPendingClean(relativePartitionOf(orphan), orphan.getName());
    corruptThePendingCleanPlan();
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToInflight(writtenBy);
          }
        });

    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_GONE, Branch.FDA_MDT_PREV_DONE_AFT_GONE);
    assertEquals("CODE_BUG", recordFor(readDiff(), orphan.getName()).get("verdict").asText());
    assertEquals("CODE_BUG", recordFor(readDiff(), victim.getName()).get("verdict").asText());
    assertFalse(run.passed(), "" + run.thrown());
  }

  /**
   * <b>The same shape once the plan has been promoted to {@code .inflight} and is still unreadable:
   * {@code CODE_BUG} on both sides, unchanged.</b>
   *
   * <p>The other half of the pair with
   * {@link #revertWithARequestedOnlyUnreadablePlanIsACodeBugOnBothSides}. On the
   * {@code (COMPLETED, COMPLETED)} row these two arrangements reach different verdicts
   * ({@code SANCTIONED} vs {@code NOT_SETTLED}) precisely because the promotion ends the flush race
   * -- see {@link #deletionWhoseAlreadyInflightCleanPlanCannotBeReadStaysUnresolved}. Here they must
   * NOT differ, and this case is the cheap half of saying so: its assertions are identical to its
   * twin's, because the input it varies (the plan's state) feeds a path this row does not consult.
   * Either case alone would catch a re-plumbing through {@code r()}; the pair documents the
   * indifference at both plan states rather than adding coverage.
   */
  @Test
  void revertWithAnInflightUnreadablePlanIsACodeBugOnBothSides() throws Exception {
    writeTable(COW, V6);
    StoragePath victim = anyFile(".parquet", false);
    StoragePath orphan = strangerCopiedFrom(victim);
    String writtenBy = instantOf(orphan);
    plantPendingClean(relativePartitionOf(orphan), orphan.getName());
    promotePendingCleanToInflight();
    corruptThePendingCleanPlan();
    deleteFromStorage(victim);

    ObservedRun run = observeRun(ValidationMode.EXHAUSTIVE, true,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforePostPhaseOneTimelineRead() {
            revertToInflight(writtenBy);
          }
        });

    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_GONE, Branch.FDA_MDT_PREV_DONE_AFT_GONE);
    assertEquals("CODE_BUG", recordFor(readDiff(), orphan.getName()).get("verdict").asText());
    assertEquals("CODE_BUG", recordFor(readDiff(), victim.getName()).get("verdict").asText());
    assertFalse(run.passed(),
        "promoting the plan ends the flush race that matters on the (COMPLETED, COMPLETED) row and "
            + "changes nothing on this one: " + run.thrown());
  }

  // ==================================================================== fail-fast smoke test

  /**
   * <b>Fail-fast reaches the same verdict and persists nothing.</b>
   *
   * <p>A smoke test by design: the mode's contract is that it fails on the first fatal verdict and
   * writes no document a mitigation job could act on. Which row it took is the exhaustive
   * cases' subject; what this pins down is that the mode still fails, still raises at the product
   * default, and still leaves the output root absent.
   */
  @Test
  void failFastFailsOnAnOrphanAndPersistsNothing() throws Exception {
    writeTable(COW, V6);
    strangerCopiedFrom(anyFile(".parquet", false));

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, false);

    assertTrue(run.thrown().isPresent(),
        "at the product default a fatal verdict raises rather than returning");
    assertTrue(run.thrown().get() instanceof HoodieValidationException,
        "and raises the data-correctness exception, not an infrastructure one: " + run.thrown().get());
    // Tied to the planted orphan rather than to "something threw": a HoodieValidationException from
    // an unrelated path would satisfy the two assertions above. Review finding.
    run.tookBranch(Branch.FDA_FS_PREV_DONE_AFT_DONE_DANGLING);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast persists nothing at all");
  }

  /**
   * <b>Fail-fast sanctions what it cannot resolve, because it does not read the archive.</b>
   *
   * <p>The two modes differ here and nowhere else in the adjudication: exhaustive falls back to the
   * archive for an instant the active timeline does not hold and reports {@code NOT_SETTLED} when the
   * archive cannot place it; fail-fast is handed
   * {@code ArchivedStateLookup.neverReadsTheArchive()} and therefore <em>always</em> arrives at the
   * unresolved arm, where it answers {@code SANCTIONED} rather than drawing a verdict from ignorance.
   *
   * <p>That is a deliberate trade — the mode exists to fail quickly on what a listing can settle, and
   * an archived scan is the expensive read — and it is asserted rather than assumed because the
   * alternative reading of the same code (report the unresolved state as a finding) would make every
   * fail-fast run over a table with aged-off commits fail.
   *
   * <p>The orphan is attributed to an instant no timeline holds, which is what forces the arm.
   */
  @Test
  void failFastSanctionsAnInstantItWillNotReadTheArchiveFor() throws Exception {
    writeTable(COW, V6);
    StoragePath orphan = strangerAt(anyFile(".parquet", false), ANCIENT_INSTANT);

    ObservedRun run = observeRun(ValidationMode.FAIL_FAST, true);

    assertTrue(run.passed(),
        "fail-fast reads no archive, so it cannot establish anything about this instant and must "
            + "not condemn the file: " + run.thrown());
    run.tookBranch(Branch.FDA_ARM23_UNRESOLVED_FAIL_FAST)
        .didNotTakeBranch(Branch.FDA_ARM23_UNRESOLVED_EXHAUSTIVE,
            Branch.FDA_FS_PREV_NULL_AFT_NULL_DANGLING);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "and fail-fast persists nothing whatever it concludes");

    // The same table in exhaustive mode reaches a VERDICT on the same file, which is what makes the
    // pair a statement about the modes rather than about this file. It does not take the exhaustive
    // unresolved arm here: this table has never archived, so on a version 6 layout the absence of any
    // archived history is itself trustworthy and the archived lookup answers NULL rather than
    // refusing. The exhaustive arm needs an archive that HAS content and still cannot answer, which is
    // TestValidatorV2ArchiveBranches#realCorruptedArchiveReachesTheResolverAsIndeterminateNeverAsNotFound.
    ObservedRun exhaustive = observeRun(ValidationMode.EXHAUSTIVE, true);
    exhaustive.tookBranch(Branch.ASL_ABSENCE_TRUSTED_NULL,
            Branch.FDA_FS_PREV_NULL_AFT_NULL_DANGLING)
        .didNotTakeBranch(Branch.FDA_ARM23_UNRESOLVED_FAIL_FAST);
    assertFalse(exhaustive.passed(),
        "and it fails the run: the same file fail-fast declined to judge, exhaustive condemns, "
            + "because only exhaustive went and asked");
    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "which is the cost of the trade fail-fast makes: it cannot find this file");
  }

  /**
   * <b>The ordering the whole design rests on, asserted as a refusal.</b>
   *
   * <p>{@code T_aft} must not be read until phase 1 has been forced to completion, or every diff
   * record would describe storage as it was <em>after</em> the timeline delta that is supposed to
   * explain it. Here the guard is asked directly with no phase 1 at all — the mutation that moves the
   * read above the listing — and it has to refuse rather than answer.
   *
   * <p>It lives in this suite rather than a unit one because it is the precondition every case above
   * silently depends on: the observer's second seam is only meaningful if the barrier is real.
   */
  @Test
  void theTimelineIsNotReadUntilPhaseOneHasBeenForced() {
    // Mutation 1: the T_aft read moved above the phase-1 call, so the guard is asked with no phase 1
    // at all.
    IllegalStateException noPhaseOne = assertThrows(IllegalStateException.class,
        () -> FsMdtDiffDimension.readAfterBarrier(null,
            () -> "a timeline this call must never produce"));
    assertTrue(noPhaseOne.getMessage().contains("has not run at all"),
        "the refusal has to say which of the two mutations it caught: " + noPhaseOne.getMessage());

    // Mutation 2 — the one the barrier actually exists for, and the one a test asserting only
    // mutation 1 cannot see: phase 1's forcing count() replaced by a lazy transformation, so the
    // object exists and no executor ever ran it. Deleting `|| !phaseOne.isMaterialised()` passes
    // mutation 1's assertion and would let every diff record describe storage as it was AFTER the
    // delta meant to explain it. Review finding.
    FsMdtDiffDimension.PhaseOne neverRan =
        new FsMdtDiffDimension.PhaseOne(null, 0L, /* expectedTasks */ 4L, /* observedTasks */ 0L);
    IllegalStateException notMaterialised = assertThrows(IllegalStateException.class,
        () -> FsMdtDiffDimension.readAfterBarrier(neverRan,
            () -> "a timeline this call must never produce"));
    assertTrue(notMaterialised.getMessage().contains("ran 0 of 4 tasks"),
        "and this arm has to name what it saw, so an operator can tell a missing phase 1 from an "
            + "unmaterialised one: " + notMaterialised.getMessage());

    // A partially-materialised phase 1 is refused too: "some tasks ran" is not the property.
    FsMdtDiffDimension.PhaseOne partlyRan =
        new FsMdtDiffDimension.PhaseOne(null, 0L, 4L, 3L);
    assertThrows(IllegalStateException.class,
        () -> FsMdtDiffDimension.readAfterBarrier(partlyRan, () -> "never"));
  }

  // ---------------------------------------------------------------- fixtures

  /**
   * A compaction instant left <b>inflight</b>, with a real plan behind it.
   *
   * <p>Written through the active timeline's own transitions rather than by placing files, so the
   * naming, the layout and the plan encoding are the writer's on every table version. The plan has no
   * operations: the file this fixture is about is planted beside it, and a plan with operations would
   * additionally make the file system view expect their outputs.
   *
   * <p>A <em>compaction</em> rather than a commit, and that is the whole point — see the block comment
   * above the cases that use it. {@code filterCompletedAndCompactionInstants} admits a pending
   * compaction, so a base file carrying its instant is visible to the file-system side of the
   * comparison while the metadata table, which only learns of completed instants, has never heard of
   * it.
   *
   * @return the instant time
   */
  private String plantInflightCompaction() throws IOException {
    String instantTime = instantAfterEverything();
    HoodieTableMetaClient client = metaClient();
    HoodieActiveTimeline timeline = client.getActiveTimeline();
    HoodieInstant requested = client.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    timeline.saveToCompactionRequested(requested, HoodieCompactionPlan.newBuilder()
        .setOperations(new ArrayList<>()).setVersion(2).build());
    timeline.transitionCompactionRequestedToInflight(requested);
    return instantTime;
  }

  /**
   * Completes an inflight compaction, optionally recording one write stat.
   *
   * <p>Callable from an observer, so it throws nothing checked. Pass {@code partition} and
   * {@code fileName} as null to complete it recording nothing at all, which is the shape that makes
   * membership unresolvable rather than negative.
   */
  private void completeCompaction(String instantTime, String partition, String fileName) {
    HoodieTableMetaClient client = metaClient();
    HoodieActiveTimeline timeline = client.getActiveTimeline();
    HoodieInstant inflight = timeline.filterPendingCompactionTimeline().getInstantsAsStream()
        .filter(i -> instantTime.equals(i.requestedTime())).findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no pending compaction at " + instantTime + " on " + basePath));
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.setOperationType(WriteOperationType.COMPACT);
    if (partition != null && fileName != null) {
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setFileId(fileName.split("_")[0]);
      stat.setPath(partition.isEmpty() ? fileName : partition + "/" + fileName);
      stat.setPartitionPath(partition);
      metadata.addWriteStat(partition, stat);
    }
    timeline.transitionCompactionInflightToComplete(false, inflight, metadata);
    HoodieInstant completed = metaClient().getActiveTimeline().filterCompletedInstants()
        .getInstantsAsStream().filter(i -> instantTime.equals(i.requestedTime())).findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "the compaction has to have actually completed, or the run's aft state is unchanged and "
                + "the case proves nothing: " + instantTime + " on " + basePath));
    // Read the write stat back, the way nameFileInCommit does. Without this the caller asserts
    // against whatever the transition happened to persist: a fixture that silently recorded nothing,
    // or recorded a name a later rename made identical to another variant's, would leave a
    // parameterised case claiming three distinct commits while exercising one.
    HoodieCommitMetadata written;
    try {
      written = metaClient().getActiveTimeline().readCommitMetadata(completed);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    List<HoodieWriteStat> recorded = written.getPartitionToWriteStats().get(partition);
    if (partition == null || fileName == null) {
      assertTrue(written.getPartitionToWriteStats().isEmpty(),
          "this fixture was asked to record nothing, and a commit that records something is a "
              + "different membership answer: " + written.getPartitionToWriteStats());
    } else {
      assertNotNull(recorded, "the commit records nothing for " + partition + " after the edit");
      boolean named = false;
      for (HoodieWriteStat stat : recorded) {
        named |= stat.getPath() != null && stat.getPath().endsWith(fileName);
      }
      assertTrue(named, "the commit does not name " + fileName + " after the edit; the caller would "
          + "be asserting against a commit that recorded something else");
    }
  }

  /** {@link #archiveTimeline} for an observer, which cannot throw a checked exception. */
  private void archiveUnchecked(Map<String, String> writeOptions) {
    try {
      archiveTimeline(writeOptions);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Whether any captured log line contains {@code needle}. */
  private static boolean anyLineContains(List<String> logged, String needle) {
    for (String line : logged) {
      if (line.contains(needle)) {
        return true;
      }
    }
    return false;
  }

  /**
   * A byte copy of {@code original} under a file id nothing has ever heard of, keeping the original's
   * write token and instant — so it is FS-exclusive, belongs to no real file group, and its writing
   * instant is a completed commit the timeline can be asked about.
   */
  private StoragePath strangerCopiedFrom(StoragePath original) throws IOException {
    String[] parts = original.getName().split("_");
    StoragePath stranger = new StoragePath(original.getParent(),
        STRANGER_FILE_ID + "_" + parts[1] + "_" + parts[2]);
    copyFile(original, stranger);
    return stranger;
  }

  /** The same, at a caller-chosen instant, so the file can be attributed to any instant on the table. */
  private StoragePath strangerAt(StoragePath original, String instantTime) throws IOException {
    StoragePath stranger = new StoragePath(original.getParent(),
        STRANGER_FILE_ID + "_1-0-0_" + instantTime + ".parquet");
    copyFile(original, stranger);
    return stranger;
  }

  /**
   * A byte copy of {@code logFile} at a new log <b>version</b>, which the metadata table cannot know
   * about.
   *
   * <p>A copy rather than a synthesised file, so its first block is a real log block whose header
   * names the real completed delta commit that wrote the original — which is the only thing that
   * makes the instant readable at all on table version 6, where the file's own name carries the
   * slice's base commit instead.
   */
  private StoragePath orphanLogFileBeside(StoragePath logFile) throws IOException {
    StoragePath orphan = new StoragePath(logFile.getParent(),
        logFile.getName().replaceFirst("\\.log\\.\\d+_", ".log.9_"));
    copyFile(logFile, orphan);
    assertTrue(storage.exists(orphan), "the orphan log file was not written: " + orphan);
    return orphan;
  }

  /**
   * The instant a log file's first block header names, read with the product's own reader.
   *
   * <p>Never parsed out of the file name: below table version 8 the name carries the slice's base
   * commit, so a fixture that read the name would edit the wrong commit and the case would assert
   * the DANGLING row while claiming to assert the COMMITTED one.
   */
  private String writingInstantOfLogFile(StoragePath logFile) {
    LogFileInstantReader.Result result = LogFileInstantReader.read(metaClient(), logFile);
    assertTrue(result.isResolved(),
        "precondition: the log file's writing instant has to be readable from its first block, "
            + "otherwise it never reaches an adjudication row at all. Reason: " + result.getReason());
    return result.getInstantTime();
  }

  /** The instant a base file's name carries. */
  private static String instantOf(StoragePath baseFile) {
    return baseFile.getName().split("_")[2].split("\\.")[0];
  }

  /**
   * Adds a write stat naming {@code fileName} to the completed instant {@code instantTime}, in place.
   *
   * <p>Read and written back through the meta client's own {@code CommitMetadataSerDe}: version 6
   * writes commit metadata as JSON and version 8 and above as Avro, so a fixture that picked an
   * encoding would leave an unreadable commit on the other layout and every caller would be
   * asserting against a run that refused to read the commit rather than one that read it.
   */
  private void nameFileInCommit(String instantTime, String partition, String fileName)
      throws IOException {
    HoodieTableMetaClient client = metaClient();
    HoodieInstant commit = client.getActiveTimeline().filterCompletedInstants()
        .getInstantsAsStream().filter(i -> instantTime.equals(i.requestedTime())).findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no completed instant at " + instantTime + " on " + basePath));
    HoodieCommitMetadata metadata = client.getActiveTimeline().readCommitMetadata(commit);
    HoodieWriteStat stat = new HoodieWriteStat();
    // Never name.split("_")[0]: a log file is named ".<fileId>_<baseCommit>.log.<v>_<token>", so the
    // first field carries a LEADING DOT and the stat then belongs to a file group that does not
    // exist. getWriteStatsAddedForInstant filters on an exact file-id match, so the edit would land
    // in the commit and change no answer -- the membership lookup would still say DANGLING and the
    // case would assert the row it meant to rule out.
    stat.setFileId(FSUtils.isLogFile(fileName)
        ? FSUtils.getFileIdFromLogPath(new StoragePath(fileName))
        : fileName.split("_")[0]);
    stat.setPath(partition.isEmpty() ? fileName : partition + "/" + fileName);
    stat.setPartitionPath(partition);
    metadata.addWriteStat(partition, stat);
    StoragePath instantFile = new StoragePath(client.getTimelinePath(),
        client.getInstantFileNameGenerator().getFileName(commit));
    try (OutputStream out = storage.create(instantFile, true)) {
      client.getCommitMetadataSerDe().getInstantWriter(metadata).get().writeToStream(out);
    }
    boolean named = false;
    for (HoodieWriteStat after : metaClient().getActiveTimeline().readCommitMetadata(commit)
        .getPartitionToWriteStats().get(partition)) {
      named |= after.getPath() != null && after.getPath().endsWith(fileName);
    }
    assertTrue(named, "the commit does not name " + fileName + " after the edit; a serde mismatch "
        + "would leave every caller asserting against a refused read instead of a verdict");
  }

  /** Promotes the sole pending clean to {@code .inflight}, leaving its plan content unchanged. */
  private void promotePendingCleanToInflight() {
    HoodieTableMetaClient client = metaClient();
    HoodieInstant pending = client.getActiveTimeline().getCleanerTimeline()
        .filterInflightsAndRequested().getInstantsAsStream().findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no pending clean on " + basePath + " to promote"));
    client.getActiveTimeline().transitionCleanRequestedToInflight(pending);
  }

  /**
   * Overwrites the one pending clean's requested instant file with bytes that are not a plan.
   *
   * <p>What an interrupted plan write leaves behind, and the only way to make
   * {@code PendingServiceState} report a read failure for the partition rather than an empty plan —
   * an <em>empty</em> plan is a clean that planned to delete nothing, which is a different answer.
   */
  private void corruptThePendingCleanPlan() throws IOException {
    HoodieTableMetaClient client = metaClient();
    HoodieInstant pending = client.getActiveTimeline().getCleanerTimeline()
        .filterInflightsAndRequested().getInstantsAsStream().findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no pending clean on " + basePath + " to corrupt"));
    // Resolved to the REQUESTED-state file explicitly, regardless of pending's current state: the
    // plan's actual content always lives in .clean.requested (CleanerUtils#getCleanRequestInstant
    // resolves there even once the instant is promoted to .inflight, whose own file is an empty
    // marker), so corrupting whatever file `pending` currently names would silently corrupt nothing
    // once a caller has promoted the clean before calling this.
    HoodieInstant requested = client.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, pending.requestedTime());
    StoragePath planFile = new StoragePath(client.getTimelinePath(),
        client.getInstantFileNameGenerator().getFileName(requested));
    assertTrue(storage.exists(planFile), "the plan file to corrupt is missing: " + planFile);
    try (OutputStream out = storage.create(planFile, true)) {
      out.write("this is not a clean plan".getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Takes a completed instant back to inflight through the writer's own transition.
   *
   * <p>Wrapped so it can be called from an observer, which cannot throw a checked exception.
   */
  private void revertToInflight(String instantTime) {
    HoodieTableMetaClient client = metaClient();
    HoodieInstant completed = client.getActiveTimeline().filterCompletedInstants()
        .getInstantsAsStream().filter(i -> instantTime.equals(i.requestedTime())).findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no completed instant at " + instantTime + " to revert on " + basePath));
    client.getActiveTimeline().revertToInflight(completed);
    assertFalse(metaClient().getActiveTimeline().filterCompletedInstants()
            .containsInstant(instantTime),
        "the revert has to have actually taken the instant off the completed timeline, or the run's "
            + "aft state is unchanged and the case proves nothing");
  }

  /**
   * Takes the departure one step further than {@link #revertToInflight}: after the writer's revert
   * leaves {@code .requested} and {@code .inflight} both present, this also removes
   * {@code .inflight} -- the exact point strictly between
   * {@code BaseRollbackActionExecutor#deleteInflightAndRequestedInstant}'s two deletes, which removes
   * {@code .inflight} first and {@code .requested} second. One step closer to {@code not exists}
   * than {@link #revertToInflight} reaches, and never a path back to completed.
   */
  private void revertToRequestedOnly(String instantTime) {
    revertToInflight(instantTime);
    HoodieTableMetaClient client = metaClient();
    HoodieInstant inflight = client.getActiveTimeline().getInstantsAsStream()
        .filter(i -> i.isInflight() && instantTime.equals(i.requestedTime())).findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no inflight instant at " + instantTime + " to strip down to requested-only on "
                + basePath));
    client.getActiveTimeline().deleteInflight(inflight);
  }

  /**
   * Takes the departure to its terminal state: after {@link #revertToRequestedOnly} leaves only
   * {@code .requested}, this deletes that too -- {@code deleteInflightAndRequestedInstant}'s second
   * delete, and the same terminal state a rollback of an instant that never completed reaches. Never
   * a path back to completed.
   */
  private void revertToNotExists(String instantTime) {
    revertToRequestedOnly(instantTime);
    HoodieTableMetaClient client = metaClient();
    HoodieInstant requested = client.getActiveTimeline().getInstantsAsStream()
        .filter(i -> i.isRequested() && instantTime.equals(i.requestedTime())).findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no requested instant at " + instantTime + " to delete on " + basePath));
    client.getActiveTimeline().deletePending(requested);
  }

}
