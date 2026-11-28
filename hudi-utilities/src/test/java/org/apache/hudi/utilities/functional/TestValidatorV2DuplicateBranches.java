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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.HoodieMetadataTableValidatorV2;
import org.apache.hudi.utilities.util.Branch;
import org.apache.hudi.utilities.util.DanglingReportWriter;
import org.apache.hudi.utilities.util.InstantBuckets;
import org.apache.hudi.utilities.util.ValidationMode;
import org.apache.hudi.utilities.util.archive.ArchiveFile;
import org.apache.hudi.utilities.util.archive.HoodieArchiveFileCatalog;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The duplicate-base-file dimension along its one deciding axis: <b>how many committed base files a
 * file slice holds</b>.
 *
 * <p>One, two and three, end to end over a real table version 6, plus the four
 * {@link org.apache.hudi.utilities.util.DanglingAdjudicator} outcomes the members are then put to,
 * and the {@link org.apache.hudi.utilities.util.DuplicateInstantWindow} sanction rule a cardinality
 * finding is now put through before either mode decides its fate. Cardinality is what the
 * dimension's verdict is decided from — "two or more is a broken invariant" — and it is decided from
 * a raw storage listing before any timeline is consulted, so it is the axis a suite has to walk
 * rather than sample.
 *
 * <p><b>Every run here has the FS-MDT comparison switched off.</b> A duplicate in a latest slice is
 * visible to both dimensions, so a run with both enabled cannot attribute its failure to this one:
 * the assertion would keep passing with this dimension deleted outright. {@code
 * --validate-fs-mdt-diff=false} makes the verdict provably this dimension's, which is also why these
 * cases can damage the <em>latest</em> slice — where a retried Spark task actually leaves its orphan
 * — rather than a superseded one.
 *
 * <p><b>Each case asserts three things, not one:</b> which adjudication row fired (the beacon),
 * what the documents say, and how the run ended. Several rows return the same outcome, so a test
 * that asserted only the outcome could not tell a fixture that stopped building its shape from one
 * that still did. See {@link ValidatorV2FunctionalTestBase.ObservedRun}.
 *
 * <p><b>The adjudication is walked on both of its paths.</b> Which one a suspect takes is decided
 * by {@code InstantBuckets.pokesArchive} — an instant at or after the earliest completed write
 * commit still active is the active timeline's to answer, an older one is the archive's — and the
 * two paths share no code past that split. The archived half of the suite therefore archives a real
 * table first and then damages it, so that the five archived rows a duplicate can reach are reached
 * against an archive Hudi's own {@code TimelineArchiverV1} wrote rather than against a stand-in.
 * Version 6 is what makes that possible at all: the v2 layout has no batch resolver, so on it every
 * archive-bucket suspect refuses ({@code DAJ_ARCHIVE_UNPROVEN}) and none of these rows exists.
 */
@Tag("functional")
public class TestValidatorV2DuplicateBranches extends ValidatorV2FunctionalTestBase {

  /** Older than any instant a writer produces, fixed width so the timeline orders it first. */
  private static final String ANCIENT_INSTANT = "19700101000000000";

  /** A file id no writer will ever mint, so its file group is one no commit has ever recorded. */
  private static final String STRANGER_FILE_ID = "00000000-1111-2222-3333-444444444444";

  /**
   * <b>Cardinality 2 under an instant with no file anywhere on the timeline is a genuine finding</b>,
   * not a tolerated in-flight write.
   *
   * <p>Two base files are planted in a file group of their own under an instant that is requested,
   * inflight, completed and archived nowhere — {@link #plantUncommittedBaseFiles}. This is exactly
   * the case a single-point-in-time gate could not tell apart from a genuinely in-flight write, both
   * looking like "not completed" to a one-shot check. {@link org.apache.hudi.utilities.util.DuplicateInstantWindow}
   * closes that gap with a window: the instant is absent from {@code T_prevD} in every state (never
   * requested or inflight, so it is not in {@code T_prevD_inflight}) and never appears at
   * {@code T_aftD} either, since nothing ever writes it (so it is not in {@code T_aftD_exclusive}
   * either). Outside the filter set on both counts, the pair must be reported — two parquet files
   * sharing one file-slice identity with nothing accounting for either one is what garbage looks
   * like, not what a retried Spark task's in-flight write looks like.
   */
  @Test
  void cardinalityTwoWithNoInstantAnywhereIsReported() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    List<StoragePath> planted = plantUncommittedBaseFiles(anyFile(".parquet", false), 2);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertFalse(run.passed(),
        "two base files under an instant with no file anywhere on the timeline is a broken "
            + "invariant, not an in-flight write: " + planted);
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING, Branch.DUP_SURVIVED_FILTER_SET)
        .didNotTakeBranch(Branch.DUP_SANCTIONED_PREV_INFLIGHT, Branch.DUP_SANCTIONED_AFT_EXCLUSIVE);

    JsonNode suspects = readSuspects();
    for (StoragePath member : planted) {
      assertNotNull(recordOrNull(suspects, member.getName()),
          "every member of the reported pair must be named: " + suspects);
      assertEquals(2, recordFor(suspects, member.getName()).get("duplicateSetSize").asInt(),
          "the record carries the cardinality the verdict was decided from: " + suspects);
    }

    // The same table in fail-fast, which is the mode whose suspect log exists at all.
    ObservedRun failFast = observeDuplicateRun(ValidationMode.FAIL_FAST);
    assertFalse(failFast.passed(), "and it fails fail-fast too: " + failFast.thrown());
    for (StoragePath member : planted) {
      assertTrue(namesAFile(suspectLinesIn(failFast.log()), member.getName()),
          "the executor's suspect line is the only record a fail-fast run leaves: "
              + suspectLinesIn(failFast.log()));
    }
  }

  /**
   * <b>Cardinality 1 — the healthy table.</b> Every slice holds exactly one base file, the
   * cardinality test fails, and no member is emitted.
   *
   * <p>The control for every case below it: same writer, same table version, same run configuration,
   * and the only thing that differs in the cases that fail is the extra copy on storage.
   */
  @Test
  void cardinalityOneIsAHealthyTable() throws Exception {
    writeTable("COPY_ON_WRITE", V6);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertTrue(run.passed(), "an undamaged table passes: " + run.thrown());
    run.tookBranch(Branch.DUP_SLICE_HOLDS_ONE_BASE_FILE)
        .didNotTakeBranch(Branch.DUP_SLICE_CARDINALITY_FINDING,
            Branch.DAJ_ACTIVE_NOT_NAMED, Branch.DAJ_ARCHIVE_NOT_NAMED,
            Branch.DAJ_ARCHIVE_ABSENT);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)));

    ObservedRun failFast = observeDuplicateRun(ValidationMode.FAIL_FAST);
    assertTrue(failFast.passed(), "" + failFast.thrown());
    assertEquals(Collections.emptyList(), suspectLinesIn(failFast.log()),
        "and the suspect log is silent, asserted in the only mode that writes it");
  }

  /**
   * <b>A healthy table is unaffected by a legitimate write landing strictly inside this
   * dimension's own active window.</b> The timeline moving while the dimension runs is not itself
   * suspicious: cardinality is decided from a raw listing, and an ordinary, unrelated commit that
   * completes between the two timeline reads ({@code T_prevD}/{@code T_aftD}) never creates a file
   * slice holding more than one base file. This is the concurrent-writer counterpart of
   * {@link #cardinalityOneIsAHealthyTable} — same healthy fixture, but with real timeline activity
   * happening during the run rather than none at all.
   */
  @Test
  void healthyTableIsUnaffectedByALegitimateWriteInsideTheWindow() throws Exception {
    writeTable("COPY_ON_WRITE", V6);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforeDuplicatePostPhaseOneTimelineRead() {
            upsertUnchecked(writeOptions("COPY_ON_WRITE", V6));
          }
        });

    assertTrue(run.passed(),
        "an ordinary, unrelated commit completing during the run's own window must not trip this "
            + "dimension: " + run.thrown());
    run.didNotTakeBranch(Branch.DUP_SLICE_CARDINALITY_FINDING);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "no finding at any point, so nothing is written");
  }

  /**
   * <b>Archival mid-window is inconclusive even on a table with zero suspects from phase 1.</b>
   * Mirrors {@link #archivalRunningMidWindowReachesNoVerdictInEitherMode}, but on a table with
   * nothing planted at all, to prove the check is unconditional rather than gated on there being an
   * actual finding to lose track of.
   *
   * <p>Confirmed by reading {@code DuplicateBaseFileDimension.run}: the archival probe
   * ({@code ArchivalProbe.archivalRan}) is evaluated immediately after the phase-1 barrier count and
   * before {@code counts.isEmpty()} or any window/classification logic ever sees a single suspect —
   * so a healthy, zero-suspect table still gets no verdict once the archiver has moved mid-window,
   * exactly as a damaged one does.
   */
  @Test
  void archivalMidWindowIsInconclusiveEvenWithNoSuspectsFromPhaseOne() throws Exception {
    writeTable("COPY_ON_WRITE", V6);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforeDuplicatePostPhaseOneTimelineRead() {
            archiveUnchecked(writeOptions("COPY_ON_WRITE", V6));
          }
        });

    assertFalse(run.passed(),
        "archival mid-window forces the whole dimension inconclusive unconditionally, even with "
            + "zero suspects from phase 1: " + run.thrown());
    run.tookBranch(Branch.MTV_ARCHIVAL_EXHAUSTIVE_FAILED);
  }

  /**
   * <b>Cardinality 2 — the Spark-retry shape.</b> A committed base file copied to a second write
   * token: the commit names the original and nothing names the copy.
   *
   * <p>The raw document names <b>both</b> members, because half a duplicate set explains nothing to
   * the human reading it; the adjudicated document names <b>only the copy</b>, because that is the
   * one no completed commit references and therefore the only one a mitigation job may delete.
   * Promoting both would delete live data, which is why the two adjudicator rows are asserted by
   * name: {@code DAJ_ACTIVE_REFERENCED} for the original and {@code DAJ_ACTIVE_NOT_NAMED} for the
   * copy, both off the same completed commit.
   */
  @Test
  void cardinalityTwoNamesBothMembersAndPromotesOnlyTheOrphan() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    StoragePath original = anyFile(".parquet", false);
    StoragePath copy = duplicateOf(original);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertFalse(run.passed(), "a slice holding two committed base files is a broken invariant");
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING,
            Branch.DAJ_ACTIVE_REFERENCED, Branch.DAJ_ACTIVE_NOT_NAMED)
        .didNotTakeBranch(Branch.DAJ_ARCHIVE_ABSENT, Branch.DAJ_ARCHIVE_UNPROVEN);

    JsonNode suspects = readSuspects();
    assertNotNull(recordOrNull(suspects, original.getName()),
        "the raw document has to name the member the commit does vouch for: " + suspects);
    assertNotNull(recordOrNull(suspects, copy.getName()),
        "and the one it does not: " + suspects);
    assertEquals(2, recordFor(suspects, copy.getName()).get("duplicateSetSize").asInt(),
        "the record carries the cardinality the verdict was decided from: " + suspects);
    assertEquals(2, recordFor(suspects, original.getName()).get("duplicateSetSize").asInt(),
        "for every member, not only the orphan: " + suspects);

    JsonNode adjudicated = readAdjudicated();
    assertNotNull(recordOrNull(adjudicated, copy.getName()),
        "the copy is the one nothing references, so it is the one that may be deleted: "
            + adjudicated);
    assertNull(recordOrNull(adjudicated, original.getName()),
        "the original is named by the commit; promoting it would authorise deleting live data: "
            + adjudicated);
  }

  /**
   * <b>Cardinality 3 — a task retried twice.</b> Two extra copies of one committed base file.
   *
   * <p>Worth its own case rather than folding into the pair above, because "two or more" is a
   * threshold and a rule implemented as "exactly two" passes every cardinality-2 assertion while
   * under-reporting every real triple. All three members must be named, all three must carry
   * {@code duplicateSetSize = 3}, and exactly the two copies — not one of them, and not the original
   * — may be promoted.
   */
  @Test
  void cardinalityThreeNamesAllThreeAndPromotesBothOrphans() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    StoragePath original = anyFile(".parquet", false);
    StoragePath firstCopy = duplicateOf(original, "9-9-9");
    StoragePath secondCopy = duplicateOf(original, "8-8-8");

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertFalse(run.passed(), "three base files in one slice is the same broken invariant");
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING,
        Branch.DAJ_ACTIVE_REFERENCED, Branch.DAJ_ACTIVE_NOT_NAMED);

    JsonNode suspects = readSuspects();
    for (StoragePath member : Arrays.asList(original, firstCopy, secondCopy)) {
      JsonNode record = recordFor(suspects, member.getName());
      assertEquals(3, record.get("duplicateSetSize").asInt(),
          "every member of a triple carries the cardinality 3, including " + member.getName()
              + ": " + suspects);
    }

    JsonNode adjudicated = readAdjudicated();
    assertNotNull(recordOrNull(adjudicated, firstCopy.getName()),
        "both copies are unreferenced, so both are deletable: " + adjudicated);
    assertNotNull(recordOrNull(adjudicated, secondCopy.getName()),
        "including the second one, which a rule written for pairs would miss: " + adjudicated);
    assertNull(recordOrNull(adjudicated, original.getName()),
        "and the committed original still may not be deleted: " + adjudicated);
    assertEquals(2, adjudicated.get("records").size(),
        "exactly the two orphans, nothing else: " + adjudicated);
  }

  /**
   * <b>Fail-fast smoke test, at both cardinalities that are findings.</b>
   *
   * <p>Fail-fast persists nothing at all, so its executor-side suspect line is the run's only
   * evidence and is what this asserts on — one line per member, naming the file rather than counting
   * them. The mode's contract is that it fails and writes nothing; the verdict itself is the
   * exhaustive cases' subject, so this stays a smoke test rather than repeating them.
   *
   * <p>{@code ignoreFailed = true} so the executor collects instead of throwing, which is what lets
   * the run reach the driver and return a verdict rather than blowing up the stage — the product
   * default is covered by {@code TestValidatorV2ExhaustiveMode}.
   */
  @Test
  void failFastFailsAndWritesNothingAtBothFindingCardinalities() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    StoragePath original = anyFile(".parquet", false);
    StoragePath firstCopy = duplicateOf(original, "9-9-9");

    ObservedRun pair = observeDuplicateRun(ValidationMode.FAIL_FAST);
    assertFalse(pair.passed(), "fail-fast fails on a duplicate base file");
    assertTrue(namesAFile(suspectLinesIn(pair.log()), original.getName()),
        "the suspect line is the only record fail-fast leaves, so it names every member: "
            + suspectLinesIn(pair.log()));
    assertTrue(namesAFile(suspectLinesIn(pair.log()), firstCopy.getName()),
        "including the copy: " + suspectLinesIn(pair.log()));
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast writes no report at all");

    StoragePath secondCopy = duplicateOf(original, "8-8-8");
    ObservedRun triple = observeDuplicateRun(ValidationMode.FAIL_FAST);
    assertFalse(triple.passed(), "and on a triple");
    assertTrue(namesAFile(suspectLinesIn(triple.log()), secondCopy.getName()),
        "naming the third member too: " + suspectLinesIn(triple.log()));
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "and still writes nothing");
  }

  /**
   * <b>A duplicate log file is not a duplicate.</b> The dimension is named for the one invariant a
   * raw listing settles by itself, and a log file's presence breaks no cardinality rule: below table
   * version 8 its name carries the slice's base commit rather than the writing one, so "the commit
   * does not name it" is not evidence about a log file at all.
   *
   * <p>The MOR fixture puts real log files in the latest slices; a copy of one is planted beside it.
   * The dimension must neither report it nor ask the adjudicator about it, and the adjudicator's own
   * refusal row is asserted absent rather than present: a log file that reached
   * {@code DanglingAdjudicator} at all would mean the detector had started emitting them.
   */
  @Test
  void duplicatedLogFileIsNeitherCountedNorAdjudicated() throws Exception {
    writeTable("MERGE_ON_READ", V6);
    StoragePath logFile = anyFile(".log.", true);
    // A copy at a new log VERSION, so the planted file is a real log file by FSUtils' own anchored
    // pattern. A ".copy" suffix would match neither LOG_FILE_PATTERN nor a base-file extension --
    // the detector would drop it on the same line that drops a .crc, and no log-file rule would be
    // exercised at all. The test must fail if the gate is widened from isBaseFile to isDataFile,
    // which is the regression it exists to catch.
    StoragePath logCopy = new StoragePath(logFile.getParent(),
        logFile.getName().replaceFirst("\\.log\\.\\d+_", ".log.9_"));
    copyFile(logFile, logCopy);
    assertTrue(FSUtils.isLogFile(logCopy), "the planted file must really be a log file: " + logCopy);
    assertFalse(FSUtils.isBaseFile(logCopy), "and must not be a base file: " + logCopy);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertTrue(run.passed(),
        "a second log file beside an existing one breaks no cardinality rule: " + run.thrown());
    run.didNotTakeBranch(Branch.DUP_SLICE_CARDINALITY_FINDING, Branch.DAJ_NEVER_COMMITTED,
        Branch.DAJ_ACTIVE_NOT_NAMED, Branch.DAJ_ARCHIVE_NOT_NAMED);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "and nothing is written: a log file is not this dimension's question at all");

    ObservedRun failFast = observeDuplicateRun(ValidationMode.FAIL_FAST);
    assertTrue(failFast.passed(), "" + failFast.thrown());
    assertEquals(Collections.emptyList(), suspectLinesIn(failFast.log()),
        "and it is named as a suspect in neither mode: " + suspectLinesIn(failFast.log()));
  }

  /**
   * <b>The healthy retried-task shape, built as a real table fixture rather than a fake gate.</b>
   * Two base files under an instant genuinely <b>requested-then-inflight</b> at {@code T_prevD} must
   * be sanctioned, not reported — this is the case the dimension's class javadoc has always
   * described, and the mechanism this whole redesign exists to keep working.
   *
   * <p>Unlike {@link #cardinalityTwoWithNoInstantAnywhereIsReported}'s instant, which never appears
   * on the timeline at all, this one is planted with a real requested-then-inflight commit behind
   * it, so it is genuinely in {@code T_prevD_inflight}.
   *
   * <p>A sanctioned pair is now dropped before either report is built, not merely marked
   * {@code failsValidation = false} inside one: with nothing else wrong on this table, the pair
   * leaves no document behind at all, exactly as a table with zero suspects from phase 1 does.
   */
  @Test
  void genuinelyInflightDuplicateIsSanctionedAndTheRunStaysClean() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    List<StoragePath> planted = plantInflightDuplicateBaseFiles(anyFile(".parquet", false), 2);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertTrue(run.passed(),
        "an instant requested-then-inflight at T_prevD is an ordinary write racing this run, not a "
            + "fault: " + run.thrown());
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING, Branch.DUP_SANCTIONED_PREV_INFLIGHT)
        .didNotTakeBranch(Branch.DUP_SANCTIONED_AFT_EXCLUSIVE, Branch.DUP_SURVIVED_FILTER_SET);

    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "a sanctioned pair leaves nothing behind to report -- the only suspect the run found was "
            + "dropped before the driver ever saw it: " + planted);

    // Fail-fast tolerates it too, now via the two-timeline-read path.
    ObservedRun failFast = observeDuplicateRun(ValidationMode.FAIL_FAST);
    assertTrue(failFast.passed(), "and fail-fast tolerates it too: " + failFast.thrown());
    assertEquals(Collections.emptyList(), suspectLinesIn(failFast.log()),
        "a sanctioned pair is never a fail-fast suspect line: " + suspectLinesIn(failFast.log()));
  }

  /**
   * <b>An instant that appears mid-window is sanctioned too, at the other seam.</b> The base files
   * are already on storage when phase 1 lists them — so the cardinality finding exists — but the
   * instant naming them is only created between the dimension's own two timeline reads, which is
   * reachable only through the {@link HoodieMetadataTableValidatorV2.PhaseObserver} seams this
   * dimension gained: absent from {@code T_prevD} in every state, and new (here, completed) by
   * {@code T_aftD}.
   *
   * <p>The fail-fast half runs on its <b>own, fresh table</b> rather than re-running the exhaustive
   * setup above: {@link #completeCommitNamingNothing} leaves the instant genuinely completed on the
   * timeline once it has run, so a second run against the same table would see it as an ordinary
   * completed commit already present at {@code T_prevD} — the AFT_EXCLUSIVE seam this test exists
   * for, not the PREV_INFLIGHT one {@link #genuinelyInflightDuplicateIsSanctionedAndTheRunStaysClean}
   * covers. A fresh table keeps both halves genuinely exercising the same seam.
   */
  @Test
  void anInstantThatFirstAppearsAtTAftDIsSanctionedAsAftExclusive() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    List<StoragePath> planted = plantUncommittedBaseFiles(anyFile(".parquet", false), 2);
    String instantTime = instantTimeOf(planted.get(0));

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforeDuplicatePostPhaseOneTimelineRead() {
            completeCommitNamingNothing(instantTime);
          }
        });

    assertTrue(run.passed(),
        "an instant that appeared during the run's own window is sanctioned, not reported: "
            + run.thrown());
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING, Branch.DUP_SANCTIONED_AFT_EXCLUSIVE)
        .didNotTakeBranch(Branch.DUP_SANCTIONED_PREV_INFLIGHT, Branch.DUP_SURVIVED_FILTER_SET);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "sanctioned as AFT_EXCLUSIVE, dropped before either report is built, and nothing else on "
            + "this table is wrong: " + planted);

    // Fail-fast tolerates the same AFT_EXCLUSIVE seam, on a fresh table for the reason above.
    resetForFreshTable();
    writeTable("COPY_ON_WRITE", V6);
    List<StoragePath> plantedForFailFast = plantUncommittedBaseFiles(anyFile(".parquet", false), 2);
    String failFastInstantTime = instantTimeOf(plantedForFailFast.get(0));

    ObservedRun failFast = observeDuplicateRun(ValidationMode.FAIL_FAST,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforeDuplicatePostPhaseOneTimelineRead() {
            completeCommitNamingNothing(failFastInstantTime);
          }
        });

    assertTrue(failFast.passed(),
        "fail-fast tolerates the AFT_EXCLUSIVE seam too: " + failFast.thrown());
    assertEquals(Collections.emptyList(), suspectLinesIn(failFast.log()),
        "a pair sanctioned as AFT_EXCLUSIVE is never a fail-fast suspect line: "
            + suspectLinesIn(failFast.log()));
  }

  /**
   * <b>Archival mid-window forces the whole dimension inconclusive</b>, mirroring
   * {@code TestValidatorV2FsMdtBranches#runTheArchiverOverlappedReachesNoVerdictInEitherMode} for
   * this dimension's own, independent window: the run refuses to sanction-or-report a suspect
   * against an account of the window it cannot stand behind.
   */
  @Test
  void archivalRunningMidWindowReachesNoVerdictInEitherMode() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    StoragePath original = anyFile(".parquet", false);
    StoragePath copy = duplicateOf(original);

    ObservedRun failFast = observeDuplicateRun(ValidationMode.FAIL_FAST,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforeDuplicatePostPhaseOneTimelineRead() {
            archiveUnchecked(writeOptions("COPY_ON_WRITE", V6));
          }
        });

    failFast.tookBranch(Branch.MTV_ARCHIVAL_FAIL_FAST_SKIPPED)
        .didNotTakeBranch(Branch.DUP_SANCTIONED_PREV_INFLIGHT, Branch.DUP_SANCTIONED_AFT_EXCLUSIVE,
            Branch.DUP_SURVIVED_FILTER_SET);
    assertTrue(failFast.passed(),
        "fail-fast reports success, having validated nothing: " + failFast.thrown());
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "the duplicate at " + copy.getName() + " goes unreported -- the cost of the trade, not a "
            + "claim that the table is healthy");

    // A fresh table for the same interleaving, exhaustive: it raises rather than silently producing
    // no document.
    resetForFreshTable();
    writeTable("COPY_ON_WRITE", V6);
    duplicateOf(anyFile(".parquet", false));
    ObservedRun exhaustive = observeDuplicateRun(ValidationMode.EXHAUSTIVE,
        new HoodieMetadataTableValidatorV2.PhaseObserver() {
          @Override
          public void beforeDuplicatePostPhaseOneTimelineRead() {
            archiveUnchecked(writeOptions("COPY_ON_WRITE", V6));
          }
        });
    exhaustive.tookBranch(Branch.MTV_ARCHIVAL_EXHAUSTIVE_FAILED);
    assertFalse(exhaustive.passed(),
        "exhaustive must not report success on a run that reached no verdict");
  }

  /**
   * <b>Fail-fast's new two-timeline-read shape still throws on a filter-set-surviving duplicate</b>,
   * now from after the classify stage rather than from raw phase 1. The complementary half —
   * fail-fast does NOT throw when the only duplicate present is filter-set-sanctioned — is
   * {@link #genuinelyInflightDuplicateIsSanctionedAndTheRunStaysClean}'s own fail-fast assertion.
   */
  @Test
  void failFastStillThrowsOnASurvivingDuplicateUnderTheTwoReadPath() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    StoragePath original = anyFile(".parquet", false);
    StoragePath copy = duplicateOf(original);

    ObservedRun run = observeDuplicateRun(ValidationMode.FAIL_FAST);

    assertFalse(run.passed(), "a genuine duplicate still fails fail-fast: " + run.thrown());
    assertTrue(namesAFile(suspectLinesIn(run.log()), copy.getName()),
        "" + suspectLinesIn(run.log()));
  }

  /**
   * <b>A mixed partition: one sanctioned group and one surviving group.</b> Proves the surviving
   * group alone fails the partition, and that the sanctioned group leaves no trace in the suspects
   * report at all — not merely tolerated inside it with {@code failsValidation = false} the way an
   * earlier shape of the classify stage handled it, but dropped before the report is built, even in
   * a partition that otherwise fails.
   */
  @Test
  void mixedPartitionFailsOnlyOnTheSurvivingGroup() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    StoragePath original = anyFile(".parquet", false);
    StoragePath survivingCopy = duplicateOf(original);
    List<StoragePath> sanctionedPair = plantInflightDuplicateBaseFiles(original, 2);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertFalse(run.passed(), "the surviving group alone must fail the partition: " + run.thrown());
    run.tookBranch(Branch.DUP_SANCTIONED_PREV_INFLIGHT, Branch.DUP_SURVIVED_FILTER_SET);

    JsonNode suspects = readSuspects();
    assertTrue(recordFor(suspects, survivingCopy.getName()).get("failsValidation").asBoolean(),
        "the surviving pair fails: " + suspects);
    for (StoragePath member : sanctionedPair) {
      assertNull(recordOrNull(suspects, member.getName()),
          "a sanctioned suspect must not reach the report at all, even in the very same partition's "
              + "otherwise-failing report: " + suspects);
    }

    JsonNode adjudicated = readAdjudicated();
    assertNotNull(recordOrNull(adjudicated, survivingCopy.getName()),
        "the surviving copy is unreferenced, so it is deletable: " + adjudicated);
    for (StoragePath member : sanctionedPair) {
      assertNull(recordOrNull(adjudicated, member.getName()),
          "a sanctioned suspect must never reach adjudication at all: " + adjudicated);
    }
  }

  // ======================================================== the archived path, end to end

  /**
   * <b>Cardinality 2 under an instant that has been ARCHIVED: the archived commit still names the
   * original, and nothing names the copy.</b>
   *
   * <p>The archived-path twin of {@link #cardinalityTwoNamesBothMembersAndPromotesOnlyTheOrphan},
   * and the pair is the point. Identical damage — a committed base file copied to a second write
   * token — but the commit that wrote it has been moved off the active timeline, so the question is
   * put to {@code DanglingAdjudicator}'s archived path instead of its active one and every row that
   * decides it is a different one.
   *
   * <p><b>Both members are asserted, not only the condemned one.</b> The archived projection is
   * keyed by file group precisely so that a verdict is reached from the names the commit recorded
   * for <em>this</em> group; an implementation that flattened it, or that read the projection as
   * "nothing here" and condemned on that, would promote the original too — which is a delete of
   * live data. {@code DAJ_ARCHIVE_REFERENCED} for the original and {@code DAJ_ARCHIVE_NOT_NAMED}
   * for the copy is what makes that provable, and neither row is reachable from a
   * {@code NOT_NAMED}-only assertion.
   *
   * <p>{@code DAJ_ARCHIVE_NOT_NAMED} is one of the two archived rows that authorise a delete. Until
   * this case it was reached by no functional test on any table.
   */
  @Test
  void anArchivedCommitStillNamesItsOwnFileAndOnlyTheCopyIsPromoted() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    List<String> archived = archiveTimeline(writeOptions("COPY_ON_WRITE", V6));
    StoragePath original = baseFileWrittenByAnArchivedInstant(archived);
    StoragePath copy = duplicateOf(original);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertFalse(run.passed(),
        "a slice holding two base files is a broken invariant whichever timeline its instant now "
            + "lives on: " + run.thrown());
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING, Branch.DUP_SURVIVED_FILTER_SET,
            Branch.DAJ_ARCHIVE_REFERENCED, Branch.DAJ_ARCHIVE_NOT_NAMED)
        .didNotTakeBranch(Branch.DAJ_ACTIVE_REFERENCED, Branch.DAJ_ACTIVE_NOT_NAMED,
            Branch.DAJ_NEVER_COMMITTED, Branch.DAJ_ARCHIVE_ABSENT,
            Branch.DAJ_ARCHIVE_NO_RECORDED_PATHS, Branch.DAJ_ARCHIVE_UNPROVEN);

    JsonNode suspects = readSuspects();
    assertNotNull(recordOrNull(suspects, original.getName()),
        "the raw document has to name the member the archived commit does vouch for: " + suspects);
    assertNotNull(recordOrNull(suspects, copy.getName()),
        "and the one it does not: " + suspects);

    JsonNode adjudicated = readAdjudicated();
    assertNotNull(recordOrNull(adjudicated, copy.getName()),
        "nothing on either timeline references the copy, so it is the one that may be deleted: "
            + adjudicated);
    assertEquals("DANGLING_FILES", recordFor(adjudicated, copy.getName()).get("pattern").asText(),
        "promoted out of the duplicate pattern, which is what a mitigation job acts on: "
            + adjudicated);
    assertNull(recordOrNull(adjudicated, original.getName()),
        "the archived commit records the original's path for this very file group; promoting it "
            + "would authorise deleting live data: " + adjudicated);
    assertEquals(1, adjudicated.get("records").size(),
        "exactly the copy, nothing else: " + adjudicated);
  }

  /**
   * <b>A duplicate whose instant is on neither timeline, with a real archive that read cleanly and
   * did not hold it, is DANGLING.</b>
   *
   * <p>This is {@code DAJ_ARCHIVE_ABSENT}, the second of the two archived rows that authorise a
   * delete, and the only one that can be reached with an archive that has content: the instant is
   * older than the whole active window, so it goes to the archive bucket, and the search then reads
   * every archive file that could have held it and reports {@code NOT_FOUND}. Nothing anywhere
   * accounts for either copy.
   *
   * <p><b>The archive must hold something.</b> An empty or absent archive directory short-circuits
   * the resolver before any search runs, so a fixture that skipped {@link #archiveTimeline} would
   * assert this row against a table whose archive was never read — the precondition below is what
   * keeps the {@code NOT_FOUND} an answer rather than a default. {@code V1ABR_NOT_FOUND} is
   * asserted alongside it for the same reason: it fires only on the searched path.
   *
   * <p>The contrast with {@link #duplicateTheArchiveCannotBeReadForIsNotSettled} is the whole
   * reason the search distinguishes its three answers — identical storage, identical suspects,
   * opposite verdicts, and the only difference is whether the archive could be read.
   */
  @Test
  void duplicateUnderAnInstantTheArchiveProvesAbsentIsDangling() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    archiveTimeline(writeOptions("COPY_ON_WRITE", V6));
    assertTrue(archiveHasContent(),
        "precondition: the archive has to hold something, or the resolver short-circuits before it "
            + "searches and the absence below is a default rather than an answer");
    List<StoragePath> planted =
        plantDuplicatesUnderInstant(anyFile(".parquet", false), ANCIENT_INSTANT, 2);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertFalse(run.passed(),
        "two base files under an instant no timeline holds is a broken invariant: " + planted);
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING, Branch.DUP_SURVIVED_FILTER_SET,
            Branch.V1ABR_NOT_FOUND, Branch.DAJ_ARCHIVE_ABSENT)
        .didNotTakeBranch(Branch.DAJ_NEVER_COMMITTED, Branch.DAJ_ARCHIVE_REFERENCED,
            Branch.DAJ_ARCHIVE_NOT_NAMED, Branch.DAJ_ARCHIVE_NO_RECORDED_PATHS,
            Branch.DAJ_ARCHIVE_UNPROVEN);

    JsonNode suspects = readSuspects();
    JsonNode adjudicated = readAdjudicated();
    for (StoragePath member : planted) {
      assertNotNull(recordOrNull(suspects, member.getName()),
          "every member of the reported pair must be named: " + suspects);
      assertNotNull(recordOrNull(adjudicated, member.getName()),
          "neither copy is referenced by anything, so both are deletable: " + adjudicated);
      assertEquals("DANGLING_FILES",
          recordFor(adjudicated, member.getName()).get("pattern").asText(),
          "both are promoted out of the duplicate pattern: " + adjudicated);
    }
    assertEquals(planted.size(), adjudicated.get("records").size(),
        "exactly the planted pair, nothing else: " + adjudicated);
  }

  /**
   * <b>The same duplicate, against an archive that cannot be read, is NOT_SETTLED.</b>
   *
   * <p>One archive file is damaged the way {@code TestValidatorV2ArchiveBranches} damages one — a
   * block size in a header disagreeing with the one in its footer, which is what
   * {@code HoodieLogFileReader.isBlockCorrupted} looks for. The search cannot tell what that file
   * held, so it reports every instant it did not yield as {@code INDETERMINATE} rather than absent. The instant is in none of the archive files the
   * search <em>could</em> read, so an implementation that collapsed the two states would call it
   * proven-absent and authorise deleting both copies.
   *
   * <p><b>One archive file, and a second would not strengthen it.</b> {@link #archiveTimeline}
   * leaves this table with a single archive file, so "the instant is in none of the files the search
   * could read" is here a claim about an empty remainder. That is not a weakening: the searcher
   * scans every file under {@code InstantRange.all()} and records damage over that same unbounded
   * range, so the damage covers every target however many undamaged files sit beside it.
   * What a second file would buy is the opposite case — an instant found in a readable file
   * <em>despite</em> damage elsewhere — which is a different claim and not this row's.
   *
   * <p><b>The run still fails, and that is not the same claim.</b> A duplicate slice fails its
   * validation on cardinality alone — adjudication only decides whether a mitigation job may act on
   * it — so the verdict here is read off the adjudicated document, which is never written when
   * nothing survived ({@code DanglingReportWriter.writeAdjudicated}). The suspects document naming
   * both members and no adjudicated document at all is what {@code NOT_SETTLED} looks like from
   * outside.
   */
  @Test
  void duplicateTheArchiveCannotBeReadForIsNotSettled() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    archiveTimeline(writeOptions("COPY_ON_WRITE", V6));
    ArchiveFile damaged = corruptAnArchiveFile();
    List<StoragePath> planted =
        plantDuplicatesUnderInstant(anyFile(".parquet", false), ANCIENT_INSTANT, 2);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertFalse(run.passed(),
        "the cardinality finding stands on its own; being unable to adjudicate it does not make the "
            + "slice healthy: " + run.thrown());
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING, Branch.DUP_SURVIVED_FILTER_SET,
            Branch.V1ABR_INDETERMINATE, Branch.DAJ_ARCHIVE_UNPROVEN)
        .didNotTakeBranch(Branch.DAJ_ARCHIVE_ABSENT, Branch.DAJ_ARCHIVE_NOT_NAMED,
            Branch.DAJ_ARCHIVE_REFERENCED, Branch.DAJ_ARCHIVE_NO_RECORDED_PATHS,
            Branch.DAJ_NEVER_COMMITTED);

    JsonNode suspects = readSuspects();
    for (StoragePath member : planted) {
      assertNotNull(recordOrNull(suspects, member.getName()),
          "the pair is still reported -- it is the deletion that is refused, not the finding: "
              + suspects);
    }
    assertFalse(
        storage.exists(new StoragePath(runDir(), DanglingReportWriter.ADJUDICATED_DIR)),
        "a run that could not read " + damaged.path() + " may promote nothing, and the adjudicated "
            + "document is not written at all when nothing survived");
  }

  /**
   * <b>A duplicate in a file group the archived commit never wrote is NOT_SETTLED, not dangling.</b>
   *
   * <p>The instant resolves — it is the very instant {@link
   * #anArchivedCommitStillNamesItsOwnFileAndOnlyTheCopyIsPromoted} condemns a copy under — but the
   * projection carries no entry for <em>this</em> file group, because that commit recorded nothing
   * for it. It reads exactly like "nothing references these files" and is refused anyway: an absent
   * entry cannot be told apart from a projection that matched no wanted group at all, and only the
   * first would be evidence ({@code DanglingAdjudicator} refuses the same shape on the active path,
   * for a differently-spelled reason).
   *
   * <p>This is the last of the five archived rows, and the pair with the case above is what gives
   * it teeth: the same archived commit, the same run configuration, and the only difference is
   * whether the file group under the duplicate is one that commit wrote. An implementation that
   * answered the membership question out of the batch's names as a whole, rather than out of this
   * group's entry, would condemn here — and the shuffle decides which other groups share the batch,
   * so a delete would depend on that.
   */
  @Test
  void duplicateInAFileGroupTheArchivedCommitNeverWroteIsNotSettled() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    List<String> archived = archiveTimeline(writeOptions("COPY_ON_WRITE", V6));
    StoragePath written = baseFileWrittenByAnArchivedInstant(archived);
    String archivedInstant = FSUtils.getCommitTime(written.getName());
    List<StoragePath> planted = plantDuplicatesUnderInstant(written, archivedInstant, 2);

    ObservedRun run = observeDuplicateRun(ValidationMode.EXHAUSTIVE);

    assertFalse(run.passed(), "the cardinality finding stands on its own: " + run.thrown());
    run.tookBranch(Branch.DUP_SLICE_CARDINALITY_FINDING, Branch.DUP_SURVIVED_FILTER_SET,
            Branch.V1ABR_FOUND, Branch.DAJ_ARCHIVE_NO_RECORDED_PATHS)
        .didNotTakeBranch(Branch.DAJ_ARCHIVE_ABSENT, Branch.DAJ_ARCHIVE_NOT_NAMED,
            Branch.DAJ_ARCHIVE_REFERENCED, Branch.DAJ_ARCHIVE_UNPROVEN,
            Branch.DAJ_NEVER_COMMITTED);

    JsonNode suspects = readSuspects();
    for (StoragePath member : planted) {
      assertNotNull(recordOrNull(suspects, member.getName()),
          "the pair is reported whatever the adjudication then refuses to conclude: " + suspects);
    }
    assertFalse(
        storage.exists(new StoragePath(runDir(), DanglingReportWriter.ADJUDICATED_DIR)),
        "an archived commit that recorded nothing for this file group is not evidence that nothing "
            + "recorded these files, so neither copy may be promoted");
  }

  // ---------------------------------------------------------------- fixtures

  /**
   * A run of this dimension alone, with the FS-MDT comparison off.
   *
   * <p>{@code ignoreFailed = true} throughout: a duplicate fails its partition on the executor, and
   * at the product default that partition's result never reaches the driver, so a suite that wants
   * to read the driver's documents has to let the failure be collected.
   */
  private ObservedRun observeDuplicateRun(ValidationMode mode) {
    return observeDuplicateRun(mode, null);
  }

  private ObservedRun observeDuplicateRun(ValidationMode mode,
                                          HoodieMetadataTableValidatorV2.PhaseObserver observer) {
    return observeRun(mode, true, /* validateDuplicateBaseFiles */ true,
        /* validateFsMdtDiff */ false, observer);
  }

  /**
   * A file group of its own, holding {@code count} base files under an instant left
   * <b>requested-then-inflight</b>, with a real commit behind it rather than a fake gate.
   *
   * <p>Written through the active timeline's own transitions rather than by placing files, so the
   * naming and the state are the writer's on every table version. Mirrors
   * {@code TestValidatorV2FsMdtBranches#plantInflightCompaction}, using a commit instant rather than
   * a compaction: the finding under test is about base files, not about a pending service.
   *
   * @return the planted files, in the order they were written
   */
  private List<StoragePath> plantInflightDuplicateBaseFiles(StoragePath seed, int count)
      throws IOException {
    String instantTime = instantAfterEverything();
    HoodieTableMetaClient client = metaClient();
    HoodieActiveTimeline timeline = client.getActiveTimeline();
    HoodieInstant requested = client.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, instantTime);
    timeline.createNewInstant(requested);
    timeline.transitionRequestedToInflight(requested, Option.empty());
    assertTrue(metaClient().getActiveTimeline().filterInflightsAndRequested()
            .containsInstant(instantTime),
        "precondition: the instant these files name must genuinely be requested or inflight, or "
            + "this fixture proves nothing");

    String fileId = UUID.randomUUID().toString();
    List<StoragePath> planted = new ArrayList<>();
    for (int copy = 0; copy < count; copy++) {
      StoragePath path = new StoragePath(seed.getParent(),
          fileId + "_" + copy + "-0-0_" + instantTime + ".parquet");
      copyFile(seed, path);
      planted.add(path);
    }
    return planted;
  }

  /** The instant time a file planted by {@link #plantUncommittedBaseFiles} carries in its name. */
  private static String instantTimeOf(StoragePath file) {
    String[] parts = file.getName().replace(".parquet", "").split("_");
    return parts[parts.length - 1];
  }

  /**
   * Creates and completes, at the run's second seam, a commit at {@code instantTime} that claims no
   * write stat at all — the instant only starts existing on the timeline here, so this is what makes
   * it {@code T_aftD}-exclusive rather than genuinely inflight at {@code T_prevD}.
   */
  private void completeCommitNamingNothing(String instantTime) {
    HoodieTableMetaClient client = metaClient();
    HoodieActiveTimeline timeline = client.getActiveTimeline();
    HoodieInstant requested = client.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, instantTime);
    timeline.createNewInstant(requested);
    timeline.transitionRequestedToInflight(requested, Option.empty());
    HoodieInstant inflight = client.reloadActiveTimeline().filterInflightsAndRequested()
        .getInstantsAsStream()
        .filter(i -> instantTime.equals(i.requestedTime()) && i.isInflight())
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no inflight commit at " + instantTime + " on " + basePath));
    timeline.saveAsComplete(inflight, Option.of(new HoodieCommitMetadata()));
    assertTrue(metaClient().getActiveTimeline().filterCompletedInstants().containsInstant(instantTime),
        "the instant has to have actually completed at T_aftD's read, or the case proves nothing");
  }

  /** {@link #archiveTimeline} for an observer, which cannot throw a checked exception. */
  private void archiveUnchecked(Map<String, String> writeOptions) {
    try {
      archiveTimeline(writeOptions);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** {@link #upsert} for an observer, which cannot throw a checked exception. */
  private void upsertUnchecked(Map<String, String> writeOptions) {
    try {
      upsert(writeOptions);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ------------------------------------------------------- fixtures for the archived path

  /**
   * A base file still on storage whose writing instant has been <b>archived</b> — the only shape
   * that puts a duplicate suspect on the archived path against a commit that can still answer for
   * it.
   *
   * <p>Both halves are asserted rather than assumed, and the file being gone is the ordinary case:
   * archival cannot advance ahead of the cleaner, so most slices an archived commit wrote have been
   * deleted by the time it is archived. {@link #archiveTimeline} leaves one behind on purpose, by
   * inserting a file group it then never updates. If that stops being true this throws here rather
   * than handing back a file whose instant is still active — which would send the suspect to the
   * ACTIVE bucket and quietly assert the archived rows against the active path.
   *
   * @param archived the instant times {@link #archiveTimeline} reported the archive now holds
   */
  private StoragePath baseFileWrittenByAnArchivedInstant(List<String> archived) throws IOException {
    Set<String> archivedInstants = new HashSet<>(archived);
    HoodieTimeline completedActive =
        metaClient().getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    for (List<StoragePath> fileGroup : baseFilesByFileGroup().values()) {
      for (StoragePath baseFile : fileGroup) {
        String instantTime = FSUtils.getCommitTime(baseFile.getName());
        if (archivedInstants.contains(instantTime)
            && !completedActive.containsInstant(instantTime)) {
          return baseFile;
        }
      }
    }
    throw new IllegalStateException("no base file under " + basePath + " both was written by one of "
        + "the archived instants " + archived + " and still exists on storage, so no duplicate can "
        + "be planted under an instant the archive can answer for. The cleaner deletes the slices an "
        + "archived commit wrote, so this depends on archiveTimeline's first insert round leaving a "
        + "file group behind; base files present: " + baseFilesByFileGroup());
  }

  /**
   * A file group of its own holding {@code count} base files, all under a caller-chosen instant that
   * is older than the whole active timeline — which is what routes the suspects to the archived pass
   * rather than the active one.
   *
   * <p>The bucketing precondition is asserted with {@code InstantBuckets.pokesArchive}, the
   * product's own expression, rather than restated: {@link
   * org.apache.hudi.utilities.util.DanglingAdjudicator} and
   * {@code DuplicateBaseFileDimension.adjudicateBatch} both split on it, and a fixture that chose an
   * instant on the wrong side of that cutoff would assert an archived row against a run that never
   * opened the archive at all.
   *
   * <p>The file group is a stranger's, so nothing but the caller's choice of instant decides what
   * the archive can say about these files. Copies are made from {@code seed} so they are readable
   * parquet.
   *
   * @param seed        an existing base file to copy the bytes of
   * @param instantTime the instant to name the copies with
   * @param count       how many copies to plant
   * @return the planted files, in the order they were written
   */
  private List<StoragePath> plantDuplicatesUnderInstant(StoragePath seed, String instantTime,
                                                        int count) throws IOException {
    String earliest = earliestActiveWriteCommit();
    assertTrue(InstantBuckets.pokesArchive(instantTime, earliest),
        "precondition: " + instantTime + " has to be older than the earliest completed write commit "
            + "still active (" + earliest + "), or these suspects take the active bucket and no "
            + "archived row is reached at all");
    List<StoragePath> planted = new ArrayList<>();
    for (int copy = 0; copy < count; copy++) {
      StoragePath path = new StoragePath(seed.getParent(),
          STRANGER_FILE_ID + "_" + copy + "-0-0_" + instantTime + ".parquet");
      copyFile(seed, path);
      planted.add(path);
    }
    return planted;
  }

  /** The archival cutoff every suspect is bucketed against, read the way the driver reads it. */
  private String earliestActiveWriteCommit() {
    return InstantBuckets.earliestCompletedWriteCommit(
        metaClient().getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
  }

  /**
   * Damages one archive file, so the searcher reports every instant it did not yield as
   * {@code INDETERMINATE} rather than absent.
   *
   * @return the file that was damaged, so a failure message can name it
   */
  private ArchiveFile corruptAnArchiveFile() throws IOException {
    List<ArchiveFile> files = new HoodieArchiveFileCatalog(metaClient()).list();
    assertFalse(files.isEmpty(),
        "precondition: the archive has to hold a file to damage, or nothing here is unreadable");
    ArchiveFile damaged = files.get(0);
    corruptFirstBlockHeader(damaged);
    return damaged;
  }

}
