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

package org.apache.hudi.tools;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration-style tests for {@link TimelineInspector}. Each test builds a tiny Hudi table
 * in a {@link TempDir}, populates it with hand-crafted instants whose metadata mentions a
 * known file id, then invokes the tool's {@code main(...)} via stdout capture.
 *
 * <p>No Spark; this lives in {@code hudi-common} so it runs alongside the other timeline
 * tests and stays fast.
 */
class TestTimelineInspector {

  private static final String PARTITION = "p0";
  private static final String INGEST_INSTANT = "20260601000000000";
  private static final String CLEAN_INSTANT  = "20260602000000000";
  private static final String ROLLBACK_INSTANT = "20260603000000000";
  private static final String REPLACE_INSTANT = "20260604000000000";
  private static final String FILE_ID_KEEP    = "11111111-1111-1111-1111-111111111111-0";
  private static final String FILE_ID_REPLACED = "22222222-2222-2222-2222-222222222222-0";
  // Name of the parquet for FILE_ID_KEEP at the ingest commit.
  private static final String BASE_FILE_KEEP =
      FILE_ID_KEEP + "_0-1-1_" + INGEST_INSTANT + ".parquet";
  // Log file for FILE_ID_KEEP rolled back by the rollback instant.
  private static final String ROLLED_BACK_LOG =
      "." + FILE_ID_KEEP + "_" + INGEST_INSTANT + ".log.1_0-2-2";
  // Replaced base file (different file id) carried by the replace commit.
  private static final String REPLACED_BASE_FILE =
      FILE_ID_REPLACED + "_0-3-3_" + REPLACE_INSTANT + ".parquet";

  private PrintStream originalOut;
  private PrintStream originalErr;
  private ByteArrayOutputStream captured;
  private String basePath;

  @BeforeEach
  void setUp(@TempDir Path tempDir) throws Exception {
    basePath = tempDir.resolve("hudi_table").toString();
    HoodieTableMetaClient meta = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);

    // Touch the partition directory so writers can land files there.
    FileCreateUtils.createPartitionMetaFile(basePath, PARTITION);

    HoodieActiveTimeline timeline = meta.getActiveTimeline();

    // 1) Ingestion commit: writes BASE_FILE_KEEP under PARTITION for FILE_ID_KEEP.
    HoodieCommitMetadata commit = new HoodieCommitMetadata();
    commit.setOperationType(WriteOperationType.INSERT);
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(FILE_ID_KEEP);
    stat.setPath(PARTITION + "/" + BASE_FILE_KEEP);
    stat.setPartitionPath(PARTITION);
    stat.setNumWrites(100);
    stat.setNumInserts(100);
    stat.setNumUpdateWrites(0);
    stat.setNumDeletes(0);
    stat.setPrevCommit("null");
    commit.addWriteStat(PARTITION, stat);
    HoodieInstant commitInstant = meta.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, INGEST_INSTANT);
    timeline.createNewInstant(commitInstant);
    timeline.transitionRequestedToInflight(commitInstant, Option.empty());
    timeline.saveAsComplete(meta.getInstantGenerator().createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, INGEST_INSTANT),
        Option.of(commit));

    // 2) Clean instant: lists the base file under successDeleteFiles for PARTITION.
    HoodieCleanMetadata clean = new HoodieCleanMetadata();
    clean.setStartCleanTime(CLEAN_INSTANT);
    clean.setTimeTakenInMillis(1L);
    clean.setTotalFilesDeleted(1);
    clean.setEarliestCommitToRetain(INGEST_INSTANT);
    clean.setLastCompletedCommitTimestamp(INGEST_INSTANT);
    clean.setVersion(2);
    HoodieCleanPartitionMetadata cleanPartition = new HoodieCleanPartitionMetadata();
    cleanPartition.setPartitionPath(PARTITION);
    cleanPartition.setPolicy("KEEP_LATEST_FILE_VERSIONS");
    cleanPartition.setDeletePathPatterns(Collections.singletonList(PARTITION + "/" + BASE_FILE_KEEP));
    cleanPartition.setSuccessDeleteFiles(Collections.singletonList(PARTITION + "/" + BASE_FILE_KEEP));
    cleanPartition.setFailedDeleteFiles(Collections.emptyList());
    cleanPartition.setIsPartitionDeleted(false);
    clean.setPartitionMetadata(Collections.singletonMap(PARTITION, cleanPartition));
    clean.setBootstrapPartitionMetadata(Collections.emptyMap());
    HoodieInstant cleanInstant = meta.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, CLEAN_INSTANT);
    timeline.createNewInstant(cleanInstant);
    timeline.transitionCleanRequestedToInflight(cleanInstant);
    timeline.transitionCleanInflightToComplete(true,
        meta.getInstantGenerator().createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, CLEAN_INSTANT),
        Option.of(clean));

    // 3) Rollback instant: rolls back a hypothetical ingest commit and lists a log file.
    HoodieRollbackMetadata rollback = new HoodieRollbackMetadata();
    rollback.setStartRollbackTime(ROLLBACK_INSTANT);
    rollback.setTimeTakenInMillis(2L);
    rollback.setTotalFilesDeleted(1);
    rollback.setCommitsRollback(Collections.singletonList("20260530000000000"));
    rollback.setInstantsRollback(Collections.singletonList(
        new HoodieInstantInfo("20260530000000000", HoodieTimeline.COMMIT_ACTION)));
    HoodieRollbackPartitionMetadata rbp = new HoodieRollbackPartitionMetadata();
    rbp.setPartitionPath(PARTITION);
    rbp.setSuccessDeleteFiles(Collections.singletonList(PARTITION + "/" + ROLLED_BACK_LOG));
    rbp.setFailedDeleteFiles(Collections.emptyList());
    rollback.setPartitionMetadata(Collections.singletonMap(PARTITION, rbp));
    HoodieInstant rollbackInstant = meta.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, ROLLBACK_INSTANT);
    timeline.createNewInstant(rollbackInstant);
    timeline.transitionRollbackRequestedToInflight(rollbackInstant);
    timeline.transitionRollbackInflightToComplete(true,
        meta.getInstantGenerator().createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, ROLLBACK_INSTANT),
        rollback);

    // 4) Replace commit: replaces FILE_ID_KEEP via partitionToReplaceFileIds; writes a new
    // base file for FILE_ID_REPLACED.
    HoodieReplaceCommitMetadata replace = new HoodieReplaceCommitMetadata();
    replace.setOperationType(WriteOperationType.CLUSTER);
    Map<String, java.util.List<String>> p2rfi = new HashMap<>();
    p2rfi.put(PARTITION, Collections.singletonList(FILE_ID_KEEP));
    replace.setPartitionToReplaceFileIds(p2rfi);
    HoodieWriteStat rwStat = new HoodieWriteStat();
    rwStat.setFileId(FILE_ID_REPLACED);
    rwStat.setPath(PARTITION + "/" + REPLACED_BASE_FILE);
    rwStat.setPartitionPath(PARTITION);
    rwStat.setNumWrites(50);
    rwStat.setNumInserts(50);
    rwStat.setNumUpdateWrites(0);
    rwStat.setNumDeletes(0);
    rwStat.setPrevCommit("null");
    replace.addWriteStat(PARTITION, rwStat);
    HoodieInstant replaceInstant = meta.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, REPLACE_INSTANT);
    timeline.createNewInstant(replaceInstant);
    timeline.transitionReplaceRequestedToInflight(replaceInstant, Option.empty());
    timeline.saveAsComplete(
        meta.getInstantGenerator().createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, REPLACE_INSTANT),
        Option.of(replace));

    originalOut = System.out;
    originalErr = System.err;
    captured = new ByteArrayOutputStream();
    System.setOut(new PrintStream(captured, true, "UTF-8"));
    // Pipe stderr to the same buffer so warn lines don't escape during the test.
    System.setErr(new PrintStream(captured, true, "UTF-8"));
  }

  @AfterEach
  void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  // ---- parse-filename -------------------------------------------------------

  @Test
  void parseFilenameDecodesBaseFile() throws Exception {
    runMain("--parse-filename", BASE_FILE_KEEP);
    String out = capturedAsString();
    assertTrue(out.contains("kind                 : base"), out);
    assertTrue(out.contains("fileId               : " + FILE_ID_KEEP), out);
    assertTrue(out.contains("commitTime           : " + INGEST_INSTANT), out);
    assertTrue(out.contains("fileExtension        : .parquet"), out);
    assertTrue(out.contains("writeToken           : 0-1-1"), out);
  }

  @Test
  void parseFilenameDecodesLogFile() throws Exception {
    runMain("--parse-filename", ROLLED_BACK_LOG);
    String out = capturedAsString();
    assertTrue(out.contains("kind                 : log"), out);
    assertTrue(out.contains("fileId               : " + FILE_ID_KEEP), out);
    assertTrue(out.contains("baseInstantTime      : " + INGEST_INSTANT), out);
    assertTrue(out.contains("logVersion           : 1"), out);
  }

  @Test
  void parseFilenameJsonOutput() throws Exception {
    runMain("--parse-filename", BASE_FILE_KEEP, "--output", "json");
    String out = capturedAsString();
    assertTrue(out.contains("\"kind\""), out);
    assertTrue(out.contains("\"fileId\" : \"" + FILE_ID_KEEP + "\""), out);
    // Sanity: well-formed JSON object.
    assertTrue(out.trim().startsWith("{") && out.trim().endsWith("}"), out);
  }

  // ---- find-file-id ---------------------------------------------------------

  @Test
  void findFileIdSurfacesIngestionWriteStat() throws Exception {
    runMain("--base-path", basePath, "--find-file-id", FILE_ID_KEEP, "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains(INGEST_INSTANT), out);
    assertTrue(out.contains("commit"), out);
    assertTrue(out.contains("writeStat"), out);
    // We should NOT match unrelated instants by accident.
    assertFalse(out.contains(REPLACED_BASE_FILE), out);
  }

  @Test
  void findFileIdSurfacesCleanAndRollbackAndReplace() throws Exception {
    runMain("--base-path", basePath, "--find-file-id", FILE_ID_KEEP, "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains("cleanSuccessDelete"), out);
    assertTrue(out.contains(CLEAN_INSTANT), out);
    assertTrue(out.contains("replaceFileId"), out);
    assertTrue(out.contains(REPLACE_INSTANT), out);
  }

  @Test
  void findFileIdRollbackOfCommitMatchesByInstantTime() throws Exception {
    runMain("--base-path", basePath, "--find-file-id", "20260530000000000",
        "--actions", "rollback", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains("rollbackOfCommit"), out);
    assertTrue(out.contains(ROLLBACK_INSTANT), out);
  }

  @Test
  void findFileIdActionFilterRestrictsScope() throws Exception {
    runMain("--base-path", basePath, "--find-file-id", FILE_ID_KEEP,
        "--actions", "clean", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains("cleanSuccessDelete"), out);
    // Without commit/replacecommit in the filter, those rows must not appear.
    assertFalse(out.contains("writeStat"), out);
    assertFalse(out.contains("replaceFileId"), out);
  }

  @Test
  void findFileIdNoMatchEmitsEmptyMarker() throws Exception {
    runMain("--base-path", basePath, "--find-file-id", "doesnotexist", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains("(no events)"), out);
  }

  // ---- --lifecycle ----------------------------------------------------------

  @Test
  void lifecycleCollapsesToLandmarks() throws Exception {
    runMain("--base-path", basePath, "--find-file-id", FILE_ID_KEEP,
        "--lifecycle", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains("CREATED"), out);
    assertTrue(out.contains("CLEANED"), out);
    assertTrue(out.contains("REPLACED"), out);
    assertTrue(out.contains("summary:"), out);
  }

  // ---- show-instant ---------------------------------------------------------

  @Test
  void showInstantPrintsAllStatesForCommit() throws Exception {
    runMain("--base-path", basePath, "--show-instant", INGEST_INSTANT, "--no-archived");
    String out = capturedAsString();
    assertTrue(out.contains(INGEST_INSTANT), out);
    assertTrue(out.contains("action        : commit"), out);
    assertTrue(out.contains("state         : COMPLETED"), out);
    // Body content from the commit metadata.
    assertTrue(out.contains(FILE_ID_KEEP), out);
  }

  @Test
  void showInstantJsonOutputWrapsStates() throws Exception {
    runMain("--base-path", basePath, "--show-instant", REPLACE_INSTANT,
        "--no-archived", "--output", "json");
    String out = capturedAsString();
    assertTrue(out.contains("\"instant\""), out);
    assertTrue(out.contains("\"states\""), out);
    assertTrue(out.contains("\"COMPLETED\""), out);
    assertTrue(out.contains(FILE_ID_REPLACED), out);
  }

  @Test
  void showInstantMissingInstantPrintsNoInstantMessage() {
    int code = runMainExpectingExit("--base-path", basePath, "--show-instant", "19990101000000000",
        "--no-archived");
    assertTrue(code == 3, "expected exit code 3, got " + code);
    assertTrue(capturedAsString().contains("No instant found with time=19990101000000000"),
        capturedAsString());
  }

  // ---- commit-stats ---------------------------------------------------------

  @Test
  void commitStatsListsIngestionCommitsWithTotals() throws Exception {
    runMain("--base-path", basePath, "--commit-stats", "--no-archived", "--quiet");
    String out = capturedAsString();
    // Both ingestion commits surface as rows.
    assertTrue(out.contains(INGEST_INSTANT), out);
    assertTrue(out.contains(REPLACE_INSTANT), out);
    // Commit metadata aggregates match the harness:
    //   ingest:  100 inserts, 0 updates, 0 deletes, 1 file inserted, 0 updated, 0 deleted
    //   replace: 50 inserts, 0 updates, 0 deletes, 1 file inserted, 0 updated, 1 replaced
    assertTrue(out.contains("100"), out);
    assertTrue(out.contains("50"), out);
    assertTrue(out.contains("CLUSTER"), out);
    assertTrue(out.contains("INSERT"), out);
    // Footer shows totals + averages over 2 commits: 150/2 = 75.00
    assertTrue(out.contains("totals (over 2 commits): inserts=150 updates=0 deletes=0"), out);
    assertTrue(out.contains("avg per commit:           inserts=75.00 updates=0.00 deletes=0.00"), out);
  }

  @Test
  void commitStatsRespectsTimeRange() throws Exception {
    // Range covers only the replacecommit.
    runMain("--base-path", basePath, "--commit-stats",
        "--start-instant", REPLACE_INSTANT, "--end-instant", REPLACE_INSTANT,
        "--no-archived", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains(REPLACE_INSTANT), out);
    assertFalse(out.contains(INGEST_INSTANT), out);
    // Single commit, footer divides by 1.
    assertTrue(out.contains("totals (over 1 commits): inserts=50"), out);
  }

  @Test
  void commitStatsReplaceCommitReportsFilesDeletedFromReplacedFileIds() throws Exception {
    runMain("--base-path", basePath, "--commit-stats",
        "--start-instant", REPLACE_INSTANT, "--end-instant", REPLACE_INSTANT,
        "--no-archived", "--quiet");
    String out = capturedAsString();
    // The replacecommit replaces exactly 1 file id (FILE_ID_KEEP) — filesDeleted column must be 1.
    // Find the row for REPLACE_INSTANT and verify the last few numeric columns:
    // numInserts=50, numUpdates=0, numDeletes=0, filesInserted=1, filesUpdated=0, filesDeleted=1.
    String replaceRow = null;
    for (String line : out.split("\n")) {
      if (line.contains(REPLACE_INSTANT) && line.contains("replacecommit")) {
        replaceRow = line;
        break;
      }
    }
    assertTrue(replaceRow != null, "no replacecommit row in output:\n" + out);
    // Column order: instant timeline action operation
    //               numInserts numUpdates numDeletes filesInserted filesUpdated filesDeleted bytes partitions
    String[] cols = replaceRow.trim().split("\\s+");
    // Tail-anchored asserts so we don't get tripped up by the leading variable-width columns.
    assertTrue(cols[cols.length - 3].equals("1"),
        "expected filesDeleted=1, got cols=" + java.util.Arrays.toString(cols));
  }

  @Test
  void commitStatsJsonOutputCarriesTotals() throws Exception {
    runMain("--base-path", basePath, "--commit-stats", "--no-archived",
        "--output", "json", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains("\"commits\""), out);
    assertTrue(out.contains("\"totals\""), out);
    assertTrue(out.contains("\"totalInserts\" : 150"), out);
    assertTrue(out.contains("\"avgInsertsPerCommit\" : 75.0"), out);
    // Per-commit stats are present.
    assertTrue(out.contains("\"numInserts\" : 100"), out);
    assertTrue(out.contains("\"numInserts\" : 50"), out);
  }

  @Test
  void commitStatsActionFilterRestrictsScope() throws Exception {
    runMain("--base-path", basePath, "--commit-stats",
        "--actions", "replacecommit", "--no-archived", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains(REPLACE_INSTANT), out);
    assertFalse(out.contains(INGEST_INSTANT), out);
  }

  @Test
  void commitStatsEmptyRangeEmitsEmptyMarker() throws Exception {
    runMain("--base-path", basePath, "--commit-stats",
        "--start-instant", "19990101000000000", "--end-instant", "19990101000000001",
        "--no-archived", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains("(no ingestion commits in range)"), out);
  }

  @Test
  void commitStatsRejectsNonIngestionActionFilter() {
    runMainExpectingExit("--base-path", basePath, "--commit-stats",
        "--actions", "clean", "--no-archived", "--quiet");
    assertTrue(capturedAsString().contains("--actions filter must include at least"),
        capturedAsString());
  }

  @Test
  void commitStatsDefaultsToDescendingByInstant() throws Exception {
    runMain("--base-path", basePath, "--commit-stats", "--no-archived", "--quiet");
    String out = capturedAsString();
    int newer = out.indexOf(REPLACE_INSTANT);
    int older = out.indexOf(INGEST_INSTANT);
    assertTrue(newer >= 0 && older >= 0,
        "expected both instants in output: " + out);
    assertTrue(newer < older,
        "default sort should put newer instant (" + REPLACE_INSTANT
            + ") above older (" + INGEST_INSTANT + "); output:\n" + out);
  }

  @Test
  void commitStatsAscendingSortPutsOldestFirst() throws Exception {
    runMain("--base-path", basePath, "--commit-stats", "--no-archived",
        "--sort", "asc", "--quiet");
    String out = capturedAsString();
    int newer = out.indexOf(REPLACE_INSTANT);
    int older = out.indexOf(INGEST_INSTANT);
    assertTrue(older >= 0 && newer >= 0,
        "expected both instants in output: " + out);
    assertTrue(older < newer,
        "asc sort should put older instant first; output:\n" + out);
  }

  @Test
  void commitStatsDescLimitReturnsLatest() throws Exception {
    // With default desc sort and --limit 1, only the newer commit should appear.
    runMain("--base-path", basePath, "--commit-stats", "--no-archived",
        "--limit", "1", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains(REPLACE_INSTANT), out);
    assertFalse(out.contains(INGEST_INSTANT), out);
  }

  @Test
  void commitStatsRejectsInvalidSortValue() {
    runMainExpectingExit("--base-path", basePath, "--commit-stats",
        "--sort", "sideways", "--no-archived", "--quiet");
    assertTrue(capturedAsString().contains("--sort must be asc or desc"),
        capturedAsString());
  }

  // ---- phase-timings --------------------------------------------------------

  @Test
  void phaseTimingsHappyPathSixPhasesPerInstant(@TempDir Path tempDir) throws Exception {
    // Use a dedicated table for this test so the existing harness's commit/clean/rollback
    // mtimes don't leak into the aggregates.
    String tablePath = tempDir.resolve("pt_table").toString();
    HoodieTableMetaClient meta = HoodieTestUtils.init(tablePath, HoodieTableType.MERGE_ON_READ);

    String i1 = "20260601000000000";
    String i2 = "20260601001000000";
    // Layout per instant (epoch ms):
    //   T0 = 1000  T1 = 1500   (sourceReadIndexSfh = 500)
    //   T2 = 2000  T3 = 2200   (dataWrite = 500, mdtPrep = 200)
    //   T4 = 2900  T5 = 3000   (mdtWrite = 700, mdtTail = 100)
    //                          (total = 2000)
    // Spacing by whole seconds — local FS getModificationTime can have second-level resolution
    // on macOS / some Linux setups, so sub-second deltas collapse.
    long[] base1 = {1_000_000L, 1_005_000L, 1_010_000L, 1_012_000L, 1_019_000L, 1_020_000L};
    long[] base2 = {2_000_000L, 2_005_000L, 2_010_000L, 2_012_000L, 2_019_000L, 2_020_000L};

    writeIngestInstantWithMtimes(meta, i1, HoodieTimeline.DELTA_COMMIT_ACTION, base1);
    writeMdtInstantWithMtimes(meta, i1, base1);
    writeIngestInstantWithMtimes(meta, i2, HoodieTimeline.DELTA_COMMIT_ACTION, base2);
    writeMdtInstantWithMtimes(meta, i2, base2);

    runMain("--base-path", tablePath, "--phase-timings", "--quiet");
    String out = capturedAsString();

    // Both instants should appear with the exact phase durations we synthesized.
    assertTrue(out.contains(i1), out);
    assertTrue(out.contains(i2), out);
    // Pick the line for i1 and verify the per-phase ms.
    String i1Row = lineContaining(out, i1);
    String[] cols = i1Row.trim().split("\\s+");
    // Columns: instant, action, sourceReadIndexSfh_ms, dataWrite_ms, mdtPrep_ms,
    //          mdtWrite_ms, mdtTail_ms, total_ms
    assertTrue(cols.length >= 8, "expected 8 columns, got " + cols.length + ": " + i1Row);
    assertTrue(cols[2].equals("5000"), "sourceReadIndexSfh=5000 expected, got " + cols[2]);
    assertTrue(cols[3].equals("5000"), "dataWrite=5000 expected, got " + cols[3]);
    assertTrue(cols[4].equals("2000"), "mdtPrep=2000 expected, got " + cols[4]);
    assertTrue(cols[5].equals("7000"), "mdtWrite=7000 expected, got " + cols[5]);
    assertTrue(cols[6].equals("1000"), "mdtTail=1000 expected, got " + cols[6]);
    assertTrue(cols[7].equals("20000"), "total=20000 expected, got " + cols[7]);
    // Aggregate footer present.
    assertTrue(out.contains("aggregates (per action, ms):"), out);
    assertTrue(out.contains("deltacommit (n=2)"), out);
    assertTrue(out.contains("share of total (mean):"), out);
  }

  @Test
  void phaseTimingsMdtAbsentFallsBackToThreePhases(@TempDir Path tempDir) throws Exception {
    String tablePath = tempDir.resolve("pt_table_no_mdt").toString();
    HoodieTableMetaClient meta = HoodieTestUtils.init(tablePath, HoodieTableType.MERGE_ON_READ);
    String inst = "20260601000000000";
    // Spacing by whole seconds; sub-second deltas can collapse on local FS.
    long[] base = {1_000_000L, 1_005_000L, 0L, 0L, 0L, 1_020_000L};
    writeIngestInstantWithMtimes(meta, inst, HoodieTimeline.DELTA_COMMIT_ACTION, base);
    // No MDT writer; .hoodie/metadata/ does not exist.

    runMain("--base-path", tablePath, "--phase-timings", "--quiet");
    String out = capturedAsString();

    assertTrue(out.contains("metadata table directory not found"), out);
    assertTrue(out.contains(inst), out);
    // MDT phase columns must be blank ('-'). dataWrite is T5-T1 = 15000. Total = 20000.
    String row = lineContaining(out, inst);
    String[] cols = row.trim().split("\\s+");
    assertTrue(cols[2].equals("5000"), "sourceReadIndexSfh=5000 expected, got " + cols[2]);
    assertTrue(cols[3].equals("15000"), "dataWrite=15000 expected (T5-T1), got " + cols[3]);
    assertTrue(cols[4].equals("-"), "mdtPrep=- expected, got " + cols[4]);
    assertTrue(cols[5].equals("-"), "mdtWrite=- expected, got " + cols[5]);
    assertTrue(cols[6].equals("-"), "mdtTail=- expected, got " + cols[6]);
    assertTrue(cols[7].equals("20000"), "total=20000 expected, got " + cols[7]);
  }

  @Test
  void phaseTimingsDropsInstantWithMissingState(@TempDir Path tempDir) throws Exception {
    String tablePath = tempDir.resolve("pt_table_partial").toString();
    HoodieTableMetaClient meta = HoodieTestUtils.init(tablePath, HoodieTableType.MERGE_ON_READ);
    String good = "20260601000000000";
    String bad = "20260601001000000";
    long[] g = {1_000_000L, 1_005_000L, 1_010_000L, 1_012_000L, 1_019_000L, 1_020_000L};
    long[] b = {2_000_000L, 2_005_000L, 2_010_000L, 2_012_000L, 2_019_000L, 2_020_000L};
    writeIngestInstantWithMtimes(meta, good, HoodieTimeline.DELTA_COMMIT_ACTION, g);
    writeMdtInstantWithMtimes(meta, good, g);

    // For the bad instant: leave the active timeline as completed but delete the .requested
    // marker so the row builder skips it. Need to go through the full transition first.
    writeIngestInstantWithMtimes(meta, bad, HoodieTimeline.DELTA_COMMIT_ACTION, b);
    writeMdtInstantWithMtimes(meta, bad, b);
    FileSystem fs = (FileSystem) meta.getStorage().getFileSystem();
    org.apache.hadoop.fs.Path req = new org.apache.hadoop.fs.Path(meta.getTimelinePath().toString(),
        bad + ".deltacommit.requested");
    fs.delete(req, false);

    runMain("--base-path", tablePath, "--phase-timings", "--quiet");
    String out = capturedAsString();

    assertTrue(out.contains(good), out);
    // The bad row should not appear in the body table, and the skipped footer must mention it.
    assertFalse(out.contains(bad + "  deltacommit"), out);
    assertTrue(out.contains("skipped 1 instant(s)"), out);
    assertTrue(out.contains("requested-missing=1"), out);
  }

  @Test
  void phaseTimingsReplacecommitOptInIncludesReplaceRows(@TempDir Path tempDir) throws Exception {
    String tablePath = tempDir.resolve("pt_table_replace").toString();
    HoodieTableMetaClient meta = HoodieTestUtils.init(tablePath, HoodieTableType.MERGE_ON_READ);
    String ingestTs = "20260601000000000";
    String replaceTs = "20260601001000000";
    long[] ig = {1_000_000L, 1_005_000L, 1_010_000L, 1_012_000L, 1_019_000L, 1_020_000L};
    long[] rp = {2_000_000L, 2_005_000L, 2_010_000L, 2_012_000L, 2_019_000L, 2_020_000L};
    writeIngestInstantWithMtimes(meta, ingestTs, HoodieTimeline.DELTA_COMMIT_ACTION, ig);
    writeMdtInstantWithMtimes(meta, ingestTs, ig);
    writeIngestInstantWithMtimes(meta, replaceTs, HoodieTimeline.REPLACE_COMMIT_ACTION, rp);
    writeMdtInstantWithMtimes(meta, replaceTs, rp);

    // Without --include-replacecommit: only the deltacommit row should appear.
    runMain("--base-path", tablePath, "--phase-timings", "--quiet");
    String outNoOpt = capturedAsString();
    assertTrue(outNoOpt.contains(ingestTs), outNoOpt);
    assertFalse(outNoOpt.contains(replaceTs), outNoOpt);

    // With opt-in: both rows present, footer breaks them out per action.
    runMain("--base-path", tablePath, "--phase-timings", "--include-replacecommit", "--quiet");
    String outWithOpt = capturedAsString();
    assertTrue(outWithOpt.contains(ingestTs), outWithOpt);
    assertTrue(outWithOpt.contains(replaceTs), outWithOpt);
    assertTrue(outWithOpt.contains("deltacommit (n=1)"), outWithOpt);
    assertTrue(outWithOpt.contains("replacecommit (n=1)"), outWithOpt);
  }

  @Test
  void phaseTimingsJsonOutputCarriesAggregates(@TempDir Path tempDir) throws Exception {
    String tablePath = tempDir.resolve("pt_table_json").toString();
    HoodieTableMetaClient meta = HoodieTestUtils.init(tablePath, HoodieTableType.MERGE_ON_READ);
    String inst = "20260601000000000";
    long[] base = {1_000_000L, 1_005_000L, 1_010_000L, 1_012_000L, 1_019_000L, 1_020_000L};
    writeIngestInstantWithMtimes(meta, inst, HoodieTimeline.DELTA_COMMIT_ACTION, base);
    writeMdtInstantWithMtimes(meta, inst, base);

    runMain("--base-path", tablePath, "--phase-timings", "--output", "json", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains("\"instants\""), out);
    assertTrue(out.contains("\"aggregates\""), out);
    assertTrue(out.contains("\"deltacommit\""), out);
    assertTrue(out.contains("\"mdtPresent\" : true"), out);
    assertTrue(out.contains("\"sourceReadIndexSfhMs\" : 5000"), out);
    assertTrue(out.contains("\"totalMs\" : 20000"), out);
  }

  @Test
  void phaseTimingsLimitReturnsLatest(@TempDir Path tempDir) throws Exception {
    String tablePath = tempDir.resolve("pt_table_limit").toString();
    HoodieTableMetaClient meta = HoodieTestUtils.init(tablePath, HoodieTableType.MERGE_ON_READ);
    String older = "20260601000000000";
    String newer = "20260601001000000";
    // Spacing by whole seconds — local FS getModificationTime can have second-level resolution
    // on macOS / some Linux setups, so sub-second deltas collapse.
    long[] base1 = {1_000_000L, 1_005_000L, 1_010_000L, 1_012_000L, 1_019_000L, 1_020_000L};
    long[] base2 = {2_000_000L, 2_005_000L, 2_010_000L, 2_012_000L, 2_019_000L, 2_020_000L};
    writeIngestInstantWithMtimes(meta, older, HoodieTimeline.DELTA_COMMIT_ACTION, base1);
    writeMdtInstantWithMtimes(meta, older, base1);
    writeIngestInstantWithMtimes(meta, newer, HoodieTimeline.DELTA_COMMIT_ACTION, base2);
    writeMdtInstantWithMtimes(meta, newer, base2);

    runMain("--base-path", tablePath, "--phase-timings", "--limit", "1", "--quiet");
    String out = capturedAsString();
    assertTrue(out.contains(newer), out);
    assertFalse(out.contains(older), out);
  }

  // ---- argument validation --------------------------------------------------

  @Test
  void missingModePrintsUsage() {
    runMainExpectingExit("--base-path", basePath);
    assertTrue(capturedAsString().contains("specify --show-instant"), capturedAsString());
  }

  @Test
  void invalidActionPrintsRejection() {
    runMainExpectingExit("--base-path", basePath, "--find-file-id", "x",
        "--actions", "ingest");
    assertTrue(capturedAsString().contains("unknown actions"), capturedAsString());
  }

  @Test
  void lifecycleWithoutFindFileIdPrintsRejection() {
    runMainExpectingExit("--base-path", basePath, "--show-instant", INGEST_INSTANT,
        "--lifecycle");
    assertTrue(capturedAsString().contains("--lifecycle is only valid with --find-file-id"),
        capturedAsString());
  }

  // ---- helpers --------------------------------------------------------------

  private void runMain(String... args) {
    captured.reset();
    try {
      TimelineInspector.run(args);
    } catch (TimelineInspector.ExitException e) {
      // Swallow — tests that care about exit codes use runMainExpectingExit.
    }
  }

  /**
   * Invokes {@link TimelineInspector#run} and returns the exit status from
   * the thrown {@link TimelineInspector.ExitException}, or 0 if it returned normally.
   */
  private int runMainExpectingExit(String... args) {
    captured.reset();
    try {
      TimelineInspector.run(args);
      return 0;
    } catch (TimelineInspector.ExitException e) {
      return e.status;
    }
  }

  /**
   * Builds a fully-formed ingest instant (requested → inflight → completed) on the active
   * timeline, then forces the modification times of the three on-disk state files to the
   * provided epochs. Used to synthesize phase-timing fixtures.
   *
   * @param mtimes  [t0Requested, t1Inflight, _unused_T2, _unused_T3, _unused_T4, t5Completed]
   */
  private void writeIngestInstantWithMtimes(HoodieTableMetaClient meta, String instantTime,
                                            String action, long[] mtimes) throws Exception {
    HoodieActiveTimeline timeline = meta.getActiveTimeline();
    HoodieInstant requested = meta.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, action, instantTime);
    timeline.createNewInstant(requested);
    if (HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)) {
      timeline.transitionReplaceRequestedToInflight(requested, Option.empty());
      HoodieReplaceCommitMetadata rcm = new HoodieReplaceCommitMetadata();
      rcm.setOperationType(WriteOperationType.CLUSTER);
      timeline.saveAsComplete(meta.getInstantGenerator().createNewInstant(
          HoodieInstant.State.INFLIGHT, action, instantTime), Option.of(rcm));
    } else {
      timeline.transitionRequestedToInflight(requested, Option.empty());
      HoodieCommitMetadata cm = new HoodieCommitMetadata();
      cm.setOperationType(WriteOperationType.UPSERT);
      timeline.saveAsComplete(meta.getInstantGenerator().createNewInstant(
          HoodieInstant.State.INFLIGHT, action, instantTime), Option.of(cm));
    }
    FileSystem fs = (FileSystem) meta.getStorage().getFileSystem();
    // Reload to pick up the completion-time suffix in V2 layout — completed instant files
    // are named "<requestedTime>_<completionTime>.<action>".
    meta = HoodieTableMetaClient.reload(meta);
    HoodieInstant completed = meta.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(i -> instantTime.equals(i.requestedTime()) && action.equals(i.getAction()))
        .findFirst().orElseThrow(() -> new AssertionError("completed instant not found: " + instantTime));
    String completedName = meta.getInstantFileNameGenerator().getFileName(completed);
    setMtime(fs, meta.getTimelinePath() + "/" + instantTime + "." + action + ".requested",
        mtimes[0]);
    setMtime(fs, meta.getTimelinePath() + "/" + instantTime + "." + action + ".inflight",
        mtimes[1]);
    setMtime(fs, meta.getTimelinePath() + "/" + completedName, mtimes[5]);
  }

  /**
   * Hand-writes the three MDT state files under {@code <base>/.hoodie/metadata/.hoodie/}
   * for the given instant and forces their mtimes. We bypass the real MDT writer because
   * all the row builder cares about is the existence and mtime of these files.
   */
  private void writeMdtInstantWithMtimes(HoodieTableMetaClient meta, String instantTime,
                                         long[] mtimes) throws Exception {
    FileSystem fs = (FileSystem) meta.getStorage().getFileSystem();
    String mdtDotHoodie = meta.getBasePath() + "/"
        + HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH + "/"
        + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTableMetaClient.TIMELINEFOLDER_NAME;
    fs.mkdirs(new org.apache.hadoop.fs.Path(mdtDotHoodie));
    touch(fs, mdtDotHoodie + "/" + instantTime + ".deltacommit.requested");
    touch(fs, mdtDotHoodie + "/" + instantTime + ".deltacommit.inflight");
    touch(fs, mdtDotHoodie + "/" + instantTime + ".deltacommit");
    setMtime(fs, mdtDotHoodie + "/" + instantTime + ".deltacommit.requested", mtimes[2]);
    setMtime(fs, mdtDotHoodie + "/" + instantTime + ".deltacommit.inflight", mtimes[3]);
    setMtime(fs, mdtDotHoodie + "/" + instantTime + ".deltacommit", mtimes[4]);
  }

  private static void touch(FileSystem fs, String pathStr) throws Exception {
    org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(pathStr);
    if (!fs.exists(p)) {
      fs.create(p, true).close();
    }
  }

  private static void setMtime(FileSystem fs, String pathStr, long mtime) throws Exception {
    fs.setTimes(new org.apache.hadoop.fs.Path(pathStr), mtime, -1);
  }

  private static String lineContaining(String haystack, String needle) {
    for (String line : haystack.split("\n")) {
      if (line.contains(needle)) {
        return line;
      }
    }
    throw new AssertionError("no line containing '" + needle + "' in:\n" + haystack);
  }

  private String capturedAsString() {
    System.out.flush();
    System.err.flush();
    return new String(captured.toByteArray(), StandardCharsets.UTF_8);
  }

}
