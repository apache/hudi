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

import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.DanglingReportWriter;
import org.apache.hudi.utilities.util.ValidationMode;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dangling-file dimension end to end, on real Spark-written tables, at <b>both table versions
 * a fleet actually holds</b>.
 *
 * <p>Every other suite for this validator builds its inputs by hand or with Mockito, and two defects
 * were found exactly where a hand-built shape and a real table's shape diverged. So each case here
 * damages a table the Spark datasource wrote, runs the whole job, and reads the reports back off
 * storage — and each is parameterised on {@code hoodie.write.table.version}, because the two
 * layouts genuinely disagree about things this dimension rests on:
 *
 * <ul>
 *   <li><b>where the archive lives, and whether it exists before the first archival.</b> The
 *       version-8-and-later layout creates {@code .hoodie/timeline/history} at table creation; the
 *       version-6 layout has no {@code .hoodie/archived} until something is archived. That is the
 *       whole input to whether an archived absence can be trusted, and it is why
 *       {@link #anOrphanOlderThanTheActiveWindowIsSettledByTheArchiveLayout} exists;
 *   <li><b>what instant a log file's name carries</b> — its slice's base commit below version 8,
 *       and the commit that wrote it at and above. Reading the instant out of the name is therefore
 *       right on one of these tables and wrong on the other, and
 *       {@link #morUnreferencedLogFileIsAdjudicatedOnItsBlockHeaderInstant} runs on both.
 * </ul>
 *
 * <p><b>How a duplicate base file is fabricated.</b> By copying one under a new write token, which
 * is the shape a retried Spark task actually leaves and needs no surgery on the metadata table:
 * the commit recorded the first task attempt's file name, so the second attempt's file is on
 * storage and in nobody's commit.
 *
 * <p><b>Why the duplicate always goes in a superseded slice.</b> A file slice holding two base
 * files has no defined "the" base file — the file system view picks one by listing order — so a
 * duplicate planted in a <em>latest</em> slice sometimes fails the run through the slice comparison
 * instead of through this dimension, and an {@code assertFalse(runValidator(...))} that passes for
 * that reason would keep passing if the dangling dimension were deleted outright. For a file group
 * the metadata table knows, the comparison looks only at the latest file slice, so a duplicate in a
 * superseded slice is provably invisible to it — asserted below by the FS-MDT diff carrying no
 * record at all,
 * which is a stronger statement than "the report has no record for this file".
 *
 * @see TestValidatorV2ExhaustiveMode the same harness, one table version, damage by damage
 */
@Tag("functional")
public class TestValidatorV2DanglingFiles extends ValidatorV2FunctionalTestBase {

  /**
   * Fail-fast finds the duplicate set from the listing alone and fails on it, at both table
   * versions, and persists nothing whatsoever.
   *
   * <p>The logged suspects are asserted, not just the verdict. Fail-fast writes no report, so
   * "the run failed" on its own does not say <em>what</em> failed it — and the superseded-slice
   * isolation, which the exhaustive twin below can prove by an empty FS-MDT diff, has no artifact
   * to be proved by here. The log line is the only evidence a fail-fast run leaves, so it is the
   * thing that has to name the file.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void failFastFailsOnADuplicateAndWritesNothingAtAll(int tableVersion) throws Exception {
    writeCowTableWithASupersededSlice(tableVersion);
    StoragePath keeper = anySupersededBaseFile();
    StoragePath duplicate = duplicateOf(keeper);

    List<String> logged = runCapturingValidatorLogs(() -> assertFalse(
        runValidator(ValidationMode.FAIL_FAST),
        "the invariant is broken; the default mode must not miss it at table version "
            + tableVersion));

    List<String> suspectLines = suspectLinesIn(logged);
    assertEquals(2, suspectLines.size(),
        "both members of the set are logged, once each: " + logged);
    for (StoragePath member : new StoragePath[] {keeper, duplicate}) {
      assertTrue(namesAFile(suspectLines, member.getName()),
          "an operator has to be able to go and look at " + member.getName() + "; got: "
              + suspectLines);
    }
    assertTrue(anyContains(suspectLines, "reason=DUPLICATE_BASE_FILE"),
        "the run has to have failed on the duplicate set and not on something else: "
            + suspectLines);

    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast persists nothing -- no report directory is created at all");
  }

  /**
   * Exhaustive mode on the same damage: two suspects are recorded raw, and exactly one of them —
   * the copy the commit does not name — reaches the adjudicated report the mitigation job deletes
   * on.
   *
   * <p>The whole document is asserted, record count included, which is only meaningful because of
   * the isolation: with an empty FS-MDT diff there is nothing else that could have
   * contributed a record, so "one record" is exactly "one deletable file".
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void exhaustivePromotesOnlyTheProvenOrphan(int tableVersion) throws Exception {
    writeCowTableWithASupersededSlice(tableVersion);
    StoragePath keeper = anySupersededBaseFile();
    StoragePath duplicate = duplicateOf(keeper);

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE),
        "one base file per slice is an invariant; two fails the run on cardinality alone");

    assertEquals(0, readDiff().get("recordCount").asLong(),
        "precondition: the latest-slice comparison must have found nothing, so the duplicate set "
            + "is the only thing that can have failed this run: " + readDiff());

    JsonNode suspects = readSuspects();
    assertEquals(2, suspects.get("recordCount").asLong(),
        "the raw report still shows both; no timeline was consulted, so no keeper is known: "
            + suspects);
    assertEquals(2, suspects.get("failingRecordCount").asLong());
    assertEquals(2, suspects.get("counts").get("DANGLING_FILE_SUSPECTS").asLong());
    assertEquals("DUPLICATE_BASE_FILE",
        recordFor(suspects, duplicate.getName()).get("suspectReason").asText());
    assertEquals("DUPLICATE_BASE_FILE",
        recordFor(suspects, keeper.getName()).get("suspectReason").asText());

    JsonNode adjudicated = readAdjudicated();
    assertEquals(1, adjudicated.get("records").size(),
        "only the copy is proven unreferenced: " + adjudicated);
    JsonNode proven = adjudicated.get("records").get(0);
    assertEquals(duplicate.getName(), proven.get("fileName").asText(),
        "the file the commit does NOT name is the dangling one");
    assertEquals("DANGLING_FILES", proven.get("pattern").asText());
    assertEquals("DUPLICATE_BASE_FILE", proven.get("suspectReason").asText(),
        "an audit of the deletion has to be able to read what put the file on the list");
    assertEquals("FS", proven.get("side").asText());
    assertEquals(1, adjudicated.get("counts").get("DANGLING_FILES").asLong());
    assertNull(adjudicated.get("counts").get("DANGLING_FILE_SUSPECTS"),
        "no suspect count survives adjudication: " + adjudicated.get("counts"));
    for (JsonNode record : adjudicated.get("records")) {
      assertFalse("DANGLING_FILE_SUSPECTS".equals(record.get("pattern").asText()),
          "an unproven category reaches the adjudicated report by no route: " + record);
    }
  }

  /**
   * The control, and it is not a trivial one: the table is the <em>same shape</em> as the two above
   * — two commits, so every file group has a superseded slice and a latest one, and therefore two
   * base files under one file id — with only the copied file missing.
   *
   * <p>That is what makes it worth running. A detector that grouped candidates by file id rather
   * than by file id <em>and instant</em> would call every one of those pairs a duplicate set, fail
   * every healthy table that has ever been updated, and be caught by nothing that only ever looks
   * at a single-commit table.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void cleanTableIsUnaffectedInBothModes(int tableVersion) throws Exception {
    writeCowTableWithASupersededSlice(tableVersion);

    for (ValidationMode mode : new ValidationMode[] {
        ValidationMode.FAIL_FAST, ValidationMode.EXHAUSTIVE}) {
      assertTrue(runValidator(mode),
          mode + " must not invent a finding on a healthy table at table version " + tableVersion);
      assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
          mode + " wrote a report for a clean table at table version " + tableVersion);
    }
  }

  /**
   * An orphan whose instant <em>is</em> a completed commit, in a file group that commit records
   * nothing for. The commit was found, read, records this partition, and declines to claim a file
   * whose own name says that very instant wrote it — positive proof, on both table versions, so the
   * FS-MDT diff settles it {@code DANGLING} and the run fails on it.
   *
   * <p><b>What an empty write-stat read does and does not license.</b> {@code getWriteStats} is
   * keyed on the raw partition-path string, and one spelling disagreement returns zero stats for
   * <em>every</em> file in a partition, condemning the lot. {@code InstantMembershipResolver}
   * normalises both sides of the partition key before comparing them, so a spelling mismatch cannot
   * produce the empty read at all. What stays a refusal is a commit that never claimed to
   * have written anything — unreadable, an action recording no write stats, or a commit naming no
   * partition whatsoever — and that half is pinned by {@code TestInstantMembershipResolver}, which
   * is where a commit can be given exactly that shape.
   *
   * <p>The dangling dimension is asserted to have found nothing at all, and that is the second half
   * of the case: a lone base file in its own file group breaks no cardinality invariant, so it is
   * not a suspect, and the delete authorisation this run produces comes from the diff's own
   * evidence — the file is on storage and the metadata table does not list it — and from no other
   * route.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void fsExclusiveOrphanUnderACompletedCommitIsProvenDangling(int tableVersion) throws Exception {
    writeTable("COPY_ON_WRITE", tableVersion);
    StoragePath stranger = strangerBesideACommittedBaseFile();

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE),
        "a file group on storage the metadata table does not list fails the run, as it always has");

    JsonNode record = recordFor(readDiff(), stranger.getName());
    assertEquals("DANGLING", record.get("verdict").asText(),
        "the commit completed before T_prev, was read, records this partition, and does not name "
            + "this file group at table version " + tableVersion + ": " + record);
    assertEquals("FS_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals("FS_EXCLUSIVE_FILE_GROUP", record.get("sourcePattern").asText());
    assertEquals("DANGLING",
        recordFor(readDiffAdjudicated(), stranger.getName()).get("verdict").asText());

    assertFalse(storage.exists(new StoragePath(runDir(), DanglingReportWriter.SUSPECTS_DIR)),
        "a lone base file in its own file group breaks no cardinality invariant, so the duplicate "
            + "dimension must not have contributed anything to this verdict");
  }

  /**
   * <b>A gap, recorded rather than blessed.</b> A base file left behind by a write that started
   * after the oldest commit and never finished — the crashed-writer shape — is invisible to this
   * validator end to end: the run passes and no report is written at all.
   *
   * <p>The file system view calls a file group committed when the timeline
   * {@code containsOrBeforeTimelineStarts} its instant, so an instant strictly inside the active
   * window that is on no commit reads as <em>in flight</em>, and an in-flight file is deliberately
   * not a divergence — a concurrent writer's half-written output must not fail a validation run.
   * The dangling detector does its own listing and would see the file, but a single base file under
   * a single file id breaks no cardinality invariant, so it is not a suspect either. Nothing in
   * this validator reaches it.
   *
   * <p>This is asserted here because it is not visible from any of the unit suites — every one of
   * them hands the analyzer a file list directly and so skips the visibility filter entirely — and
   * because it decides the shape of any future work on orphan cleanup: the files this tool can
   * currently prove deletable are the ones a commit <em>does</em> speak to, plus those older than
   * the whole active timeline. Making these visible is a change to the validator's committed-file
   * filter, not to the adjudication, and whoever makes it should have this test fail in their face.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void anOrphanInsideTheActiveWindowIsInvisibleToTheValidator(int tableVersion) throws Exception {
    writeCowTableWithASupersededSlice(tableVersion);
    StoragePath stranger = strangerUnderInstant(justAfterTheOldestCommit());
    assertTrue(storage.exists(stranger), "precondition: the orphan really is on storage");

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE),
        "GAP: this pins a known limitation, not a guarantee. An orphan base file whose instant is "
            + "inside the active window and on no commit reads as an in-flight write, and is "
            + "invisible to the run. If you are reading this because you MADE it visible, the "
            + "failure is your change working -- read this test's javadoc, then delete the case "
            + "rather than restoring the behaviour it pins");

    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "GAP: as above. No finding of any kind is produced for it today, in the mode that writes "
            + "everything -- a failure here is the gap being closed, not a regression");
  }

  /**
   * The version-dependent branch, on the two layouts it distinguishes.
   *
   * <p>The orphan here is named with an instant <b>older than the oldest commit the active timeline
   * holds</b>, which is the only way to reach the archived pass on a table this size. The archive
   * has nothing to say about it either way — nothing has ever been archived — and what happens next
   * is decided by the layout alone:
   *
   * <ul>
   *   <li><b>version 6</b>: {@code .hoodie/archived} does not exist until the first archival, so an
   *       empty read is ordinary and the absence is proof. The instant is placed as
   *       {@code NULL} at both reads, nothing explains the file, and it is {@code DANGLING} — the
   *       run fails;
   *   <li><b>versions 8 and 9</b>: {@code .hoodie/timeline/history} is created <em>empty</em> with
   *       the table, so an archive that is present and answers nothing is indistinguishable by
   *       listing from one something emptied. The guard refuses on that ambiguity, the instant is
   *       unresolved at {@code T_prev}, and a file the run could not place may only be
   *       {@code NOT_SETTLED} — which is not a validation failure, so <b>the run passes</b>. Note
   *       what that costs, and it is a stated design risk: on these versions no base file older
   *       than the whole active window is ever proven dangling.
   * </ul>
   *
   * <p><b>The version-6 case is the control and it stays fatal.</b> Asserting the two outcomes in
   * one test rather than deleting the distinction is the point: a change that made the guard fire
   * on 6, or stopped it firing on 8, breaks this in one parameterisation and not the other.
   *
   * <p>Version 8 is parameterised rather than argued about. It shares version 9's timeline layout
   * and sits on the boundary the guard is written against
   * ({@code LAYOUT_CREATES_ARCHIVE_DIR_FROM = 8}), so it is the one version where an off-by-one in
   * that comparison would show up and nowhere else.
   *
   * <p>This is the case that is <em>only</em> expressible against a real table: the input is the
   * physical archive directory the writer created, and a mock of the meta client would have been
   * given whichever answer the test author expected. Invert the layout comparison — refuse on 6 and
   * promote on 8 and 9 — and this test fails in every parameterisation at once, while nothing built
   * by hand would notice.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 8, 9})
  void anOrphanOlderThanTheActiveWindowIsSettledByTheArchiveLayout(int tableVersion)
      throws Exception {
    writeTable("COPY_ON_WRITE", tableVersion);
    StoragePath ancient = strangerUnderInstant(ANCIENT_INSTANT);

    boolean passed = runValidator(ValidationMode.EXHAUSTIVE);

    JsonNode diff = readDiff();
    JsonNode record = recordFor(diff, ancient.getName());
    assertEquals("FS_EXCLUSIVE_FILE_GROUP", record.get("sourcePattern").asText(),
        "precondition: the comparison sees it whatever the adjudication then does with it");
    assertEquals(ANCIENT_INSTANT, record.get("writingInstant").asText());

    if (tableVersion < 8) {
      assertFalse(passed,
          "a table whose layout creates no archive until it archives has nothing to hide behind, "
              + "so the orphan is proven and the run fails: " + diff);
      assertEquals("DANGLING", record.get("verdict").asText(),
          "the writing instant never completed and no clean or rollback explains this file: "
              + record);
      assertEquals(0, diff.get("notSettledCount").asLong(), diff.toString());
      assertEquals("DANGLING",
          recordFor(readDiffAdjudicated(), ancient.getName()).get("verdict").asText());
    } else {
      assertTrue(passed,
          "an archive that exists and answered nothing is not proof, and a file the run could not "
              + "place must never fail it: " + diff);
      assertEquals("NOT_SETTLED", record.get("verdict").asText(),
          "the archive could not establish whether the instant ever completed: " + record);
      assertEquals(1, diff.get("notSettledCount").asLong(),
          "the refusal is reported, not swallowed -- an operator has to see how much of the run "
              + "was unadjudicable: " + diff);
      assertEquals("NOT_SETTLED",
          recordFor(readDiffAdjudicated(), ancient.getName()).get("verdict").asText(),
          "NOT_SETTLED is not SANCTIONED, so it is still carried into the actionable document");
    }
  }

  /**
   * <b>A detect, delete, re-validate loop clears a three-layer orphan file group in one round, on
   * every timeline layout.</b>
   *
   * <p>The layouts differ in how commit metadata is encoded and archived, which is what membership
   * reads to condemn each layer, so the one-round guarantee is asserted on each.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 8, 9})
  void everyLayerOfAnOrphanFileGroupIsClearedInOneRound(int tableVersion) throws Exception {
    writeTable("COPY_ON_WRITE", tableVersion);
    insertMore(writeOptions("COPY_ON_WRITE", tableVersion), "002", 5);
    insertMore(writeOptions("COPY_ON_WRITE", tableVersion), "003", 5);
    List<String> commits = completedCommits();
    assertEquals(3, commits.size(), "the fixture needs three completed commits: " + commits);
    StoragePath seed = anyFile(".parquet", false);
    for (String commit : commits) {
      orphanLayerAt(seed, commit);
    }

    List<List<String>> rounds = deleteCondemnedUntilClean(seed);

    assertEquals(1, rounds.size(), "rounds needed: " + rounds);
    assertEquals(3, rounds.get(0).size(), "condemned in round one: " + rounds);
  }

  /**
   * <b>An orphan layer older than the whole active window is reported with the rest of its group,
   * and the archive layout decides its verdict exactly as it does for a lone orphan.</b>
   *
   * <p>The control is {@link #anOrphanOlderThanTheActiveWindowIsSettledByTheArchiveLayout}: the
   * older layer must get the verdict a single file at that instant gets, so reporting it with its
   * group never widens what may be deleted.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 8, 9})
  void anOrphanLayerOlderThanTheActiveWindowIsSettledByTheArchiveLayout(int tableVersion)
      throws Exception {
    writeTable("COPY_ON_WRITE", tableVersion);
    StoragePath seed = anyFile(".parquet", false);
    String commit = completedCommits().get(0);
    StoragePath newer = orphanLayerAt(seed, commit);
    StoragePath ancient = orphanLayerAt(seed, ANCIENT_INSTANT);

    boolean passed = runValidator(ValidationMode.EXHAUSTIVE);

    assertFalse(passed, "the newer layer is a proven orphan at every version");
    JsonNode diff = readDiff();
    assertEquals("DANGLING", recordFor(diff, newer.getName()).get("verdict").asText());
    JsonNode record = recordFor(diff, ancient.getName());
    assertEquals("FS_EXCLUSIVE_FILE_GROUP", record.get("sourcePattern").asText(),
        "an older slice of a group the metadata table does not know is reported: " + record);
    assertEquals(ANCIENT_INSTANT, record.get("writingInstant").asText());
    assertEquals(tableVersion < 8 ? "DANGLING" : "NOT_SETTLED", record.get("verdict").asText(),
        "the same verdict a lone orphan at this instant gets: " + record);
    assertEquals(2, diff.get("recordCount").asLong(), diff.toString());
  }

  /**
   * <b>Superseded slices of file groups the metadata table knows are still not compared.</b>
   *
   * <p>Every table the cleaner has not yet reached looks like this, so a record here would be a
   * false positive on healthy tables. The population counts latest slices only.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 8, 9})
  void supersededSlicesOfKnownFileGroupsGainNoRecords(int tableVersion) throws Exception {
    writeCowTableWithASupersededSlice(tableVersion);
    int fileGroups = baseFilesByFileGroup().size();
    assertTrue(baseFilesByFileGroup().values().stream().anyMatch(files -> files.size() > 1),
        "precondition: some file group holds a superseded slice: " + baseFilesByFileGroup());

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE), "a healthy table must pass");
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "a run with no finding writes no report; " + fileGroups + " file groups were compared");
  }

  /**
   * <b>An orphan layer inside the active window under an instant no timeline entry holds is not
   * reported.</b>
   *
   * <p>Only committed slices are read, and an instant inside the window that the timeline does not
   * hold is not committed. The newer layer is reported as before.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 8, 9})
  void anOrphanLayerUnderAnInWindowInstantNoTimelineHoldsIsNotReported(int tableVersion)
      throws Exception {
    writeTable("COPY_ON_WRITE", tableVersion);
    insertMore(writeOptions("COPY_ON_WRITE", tableVersion), "002", 5);
    String inWindow = justAfterTheOldestCommit();
    List<String> commits = completedCommits();
    assertTrue(inWindow.compareTo(commits.get(1)) < 0,
        "precondition: the instant sits inside the window, below the newer layer: " + commits);
    StoragePath seed = anyFile(".parquet", false);
    StoragePath newer = orphanLayerAt(seed, commits.get(1));
    StoragePath inWindowLayer = orphanLayerAt(seed, inWindow);

    runValidator(ValidationMode.EXHAUSTIVE);

    JsonNode diff = readDiff();
    assertEquals("DANGLING", recordFor(diff, newer.getName()).get("verdict").asText());
    assertNull(recordOrNull(diff, inWindowLayer.getName()),
        "a slice under an uncommitted in-window instant is not read: " + diff);
    assertEquals(1, diff.get("recordCount").asLong(), diff.toString());
  }

  // ---------------------------------------------------------------- fixtures

  private List<String> completedCommits() {
    List<String> commits = new ArrayList<>();
    metaClient().getCommitsTimeline().filterCompletedInstants().getInstants()
        .forEach(instant -> commits.add(instant.requestedTime()));
    return commits;
  }

  /** A layer of one orphan file group: the same stranger file id at {@code instantTime}. */
  private StoragePath orphanLayerAt(StoragePath seed, String instantTime) throws IOException {
    return strangerCopiedFrom(seed, "1-0-0_" + instantTime + ".parquet");
  }

  /**
   * Validates, deletes every file the run condemned, and validates again until the table passes,
   * as a dangling-file mitigation does; returns what each round condemned.
   */
  private List<List<String>> deleteCondemnedUntilClean(StoragePath seed) throws IOException {
    List<List<String>> rounds = new ArrayList<>();
    while (!runValidator(ValidationMode.EXHAUSTIVE) && rounds.size() < 5) {
      List<String> condemned = new ArrayList<>();
      for (JsonNode record : readDiff().get("records")) {
        if ("DANGLING".equals(record.get("verdict").asText())) {
          String fileName = record.get("fileName").asText();
          condemned.add(fileName);
          deleteFromStorage(new StoragePath(seed.getParent(), fileName));
        }
      }
      assertFalse(condemned.isEmpty(), "a failed run must condemn something: " + readDiff());
      rounds.add(condemned);
      storage.deleteDirectory(runDir());
    }
    assertTrue(rounds.size() < 5, "still dirty after rounds " + rounds);
    return rounds;
  }

  /**
   * Older than any instant a writer will produce, and fixed width so the timeline's lexicographic
   * comparison reads it as older than the oldest commit on the active timeline.
   */
  private static final String ANCIENT_INSTANT = "19700101000000000";

  /**
   * A base file under a file id nothing has ever heard of, carrying the same completed instant as
   * the file it was copied from — so the timeline can be asked about it and has a definite answer.
   */
  private StoragePath strangerBesideACommittedBaseFile() throws IOException {
    StoragePath original = anyFile(".parquet", false);
    String[] parts = original.getName().split("_");
    return strangerCopiedFrom(original, parts[1] + "_" + parts[2]);
  }

  /**
   * One tick after the oldest commit on the table, at that commit's own instant width — so on a
   * two-commit table it falls strictly inside the active window and belongs to no commit.
   */
  private String justAfterTheOldestCommit() throws IOException {
    String oldest = null;
    for (List<StoragePath> fileGroup : baseFilesByFileGroup().values()) {
      for (StoragePath baseFile : fileGroup) {
        String instant = baseFile.getName().split("_")[2].split("\\.")[0];
        if (oldest == null || instant.compareTo(oldest) < 0) {
          oldest = instant;
        }
      }
    }
    return String.format("%0" + oldest.length() + "d", Long.parseLong(oldest) + 1);
  }

  /**
   * The same, under an instant no commit ever used and which is older than the whole active
   * timeline — the only shape that reaches the archived pass on a young table.
   */
  private StoragePath strangerUnderInstant(String instantTime) throws IOException {
    StoragePath original = anyFile(".parquet", false);
    return strangerCopiedFrom(original, "9-9-9_" + instantTime + ".parquet");
  }

  private StoragePath strangerCopiedFrom(StoragePath original, String tokenAndInstant)
      throws IOException {
    StoragePath stranger = new StoragePath(original.getParent(),
        "00000000-1111-2222-3333-444444444444_" + tokenAndInstant);
    copyFile(original, stranger);
    return stranger;
  }

  private static boolean anyContains(List<String> lines, String needle) {
    for (String line : lines) {
      if (line.contains(needle)) {
        return true;
      }
    }
    return false;
  }

  /**
   * <b>An instant with no file anywhere on the timeline is a genuine finding</b>, not the
   * committed-instant gate's escape hatch.
   *
   * <p>This pins the corrected behaviour {@link org.apache.hudi.utilities.util.DuplicateInstantWindow}
   * exists for. A single-point-in-time check cannot separate "genuinely in flight right now" from
   * "an instant with no file anywhere, that never existed and never will" — both answer "not
   * completed" identically — and this fixture builds the second population: an instant that
   * completed no commit, is not requested or inflight at {@code T_prevD}, and never appears at
   * {@code T_aftD} either, because nothing ever writes it. Two parquet files sharing one file-slice
   * identity with nothing accounting for either one is what garbage looks like, not what a retried
   * Spark task's in-flight write looks like — so the pair must be reported.
   *
   * <p>Fail-fast at its product default ({@code ignoreFailed = false}) throws on the executor.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void duplicateUnderAnInstantWithNoFileAnywhereFailsTheRun(int tableVersion) throws Exception {
    writeCowTableWithASupersededSlice(tableVersion);
    List<StoragePath> attempts = retriedTaskUnderInstant(justAfterTheOldestCommit());
    for (StoragePath attempt : attempts) {
      assertTrue(storage.exists(attempt),
          "precondition: both task attempts really are on storage: " + attempt);
    }

    List<String> logged = new ArrayList<>();
    Throwable[] raised = {null};
    runCapturingValidatorLogs(() -> {
      try {
        runValidator(ValidationMode.FAIL_FAST, false);
      } catch (Throwable t) {
        raised[0] = t;
      }
    }, logged);

    assertTrue(raised[0] instanceof HoodieValidationException,
        "an instant no file anywhere on the timeline accounts for is a broken invariant, not an "
            + "in-flight write, at table version " + tableVersion + ": " + raised[0]);
    for (StoragePath attempt : attempts) {
      assertTrue(namesAFile(suspectLinesIn(logged), attempt.getName()),
          "and it must be reported as a suspect: " + suspectLinesIn(logged));
    }
  }

  /**
   * The off-switch skips the dimension, on a table that fails with it on.
   *
   * <p>Asserting the run passes is not enough on its own — a dimension that ran and found nothing
   * would pass too. The suspect log has to be empty as well, which is what says the listing never
   * happened rather than happening and finding nothing.
   */
  @Test
  void theDimensionCanBeTurnedOff() throws Exception {
    writeCowTableWithASupersededSlice();
    duplicateOf(anySupersededBaseFile());

    List<String> logged = new ArrayList<>();
    AtomicBoolean passed = new AtomicBoolean();
    runCapturingValidatorLogs(
        () -> passed.set(runValidator(ValidationMode.FAIL_FAST, false, false)), logged);

    assertTrue(passed.get(), "--validate-duplicate-base-files=false must skip the dimension");
    assertEquals(Collections.emptyList(), suspectLinesIn(logged),
        "and skipping it means listing nothing and reporting nothing");
  }

  /** Two attempts of one task under {@code instantTime}, differing only in their write token. */
  private List<StoragePath> retriedTaskUnderInstant(String instantTime) throws IOException {
    StoragePath original = anyFile(".parquet", false);
    return Arrays.asList(
        strangerCopiedFrom(original, "1-1-0_" + instantTime + ".parquet"),
        strangerCopiedFrom(original, "1-1-1_" + instantTime + ".parquet"));
  }
}
