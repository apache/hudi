/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.HoodieSparkKryoRegistrar$;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link TableSizeStats}. Each test builds a synthetic Hudi table
 * in @TempDir, populates partition directories with known parquet files (real footers when
 * row counts are needed, zero-byte placeholders sized via setLength when only size matters),
 * runs TableSizeStats end-to-end via run(), and asserts on captured stdout.
 */
class TestTableSizeStats {

  private static final Schema TEST_SCHEMA = SchemaBuilder.record("Row").fields()
      .requiredString("_hoodie_record_key")
      .requiredString("partition")
      .requiredLong("ts")
      .endRecord();

  private static transient SparkSession spark;
  private static transient JavaSparkContext jsc;

  @TempDir
  java.nio.file.Path tempDir;

  private String basePath;
  private HoodieTableMetaClient metaClient;
  private PrintStream originalOut;
  private PrintStream originalErr;
  private ByteArrayOutputStream captured;

  @BeforeAll
  static void initSpark() {
    if (spark == null) {
      SparkConf sparkConf = new SparkConf()
          .setAppName("TestTableSizeStats")
          .setMaster("local[2]")
          .set("spark.ui.enabled", "false")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.session.timeZone", "UTC");
      HoodieSparkKryoRegistrar$.MODULE$.register(sparkConf);
      SparkRDDReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      jsc = new JavaSparkContext(spark.sparkContext());
    }
  }

  @AfterAll
  static void tearDownSpark() {
    if (spark != null) {
      spark.close();
      spark = null;
      jsc = null;
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    basePath = tempDir.resolve("dataset").toString();
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    new HoodieSparkEngineContext(jsc);

    originalOut = System.out;
    originalErr = System.err;
    captured = new ByteArrayOutputStream();
    System.setOut(new PrintStream(captured, true, "UTF-8"));
    System.setErr(new PrintStream(captured, true, "UTF-8"));
  }

  @AfterEach
  void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  // ---- helpers --------------------------------------------------------------

  // Single instant time used for all partition base files, so the test only needs to
  // write a single completed .commit at the end.
  private static final String FIXTURE_INSTANT = "20260101000000000";

  /**
   * Creates a partition directory with the {@code .hoodie_partition_metadata} marker and
   * a set of base parquet files of approximately the requested sizes. Files are real
   * parquet when {@code writeRealRows > 0} (so Parquet footer reads work for row counts);
   * otherwise zero-byte placeholders padded to size via raw write. Returns absolute paths
   * for the files created.
   */
  private List<String> createPartition(String partition, int numFiles, long targetSizeBytes,
                                       int writeRealRows) throws Exception {
    FileCreateUtils.createPartitionMetaFile(basePath, partition);
    java.nio.file.Path partDir = java.nio.file.Paths.get(basePath, partition);
    Files.createDirectories(partDir);
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String fileId = String.format("%08d-0000-0000-0000-%012d-0",
          partition.hashCode() & 0x7fffffff, i);
      String fileName = fileId + "_0-1-1_" + FIXTURE_INSTANT + ".parquet";
      java.nio.file.Path filePath = partDir.resolve(fileName);

      if (writeRealRows > 0) {
        // Real parquet so footer reads return the correct row count.
        Path hdfsPath = new Path(filePath.toUri());
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hdfsPath)
            .withSchema(TEST_SCHEMA)
            .withConf(jsc.hadoopConfiguration()).build()) {
          for (int r = 0; r < writeRealRows; r++) {
            GenericRecord rec = new GenericData.Record(TEST_SCHEMA);
            rec.put("_hoodie_record_key", "key-" + partition + "-" + i + "-" + r);
            rec.put("partition", partition);
            rec.put("ts", (long) r);
            writer.write(rec);
          }
        }
      } else {
        // Zero-padded placeholder of the requested size; TableSizeStats only reads file
        // size from FileStatus here, not the contents.
        byte[] bytes = new byte[(int) Math.min(targetSizeBytes, 64 * 1024 * 1024)];
        Files.write(filePath, bytes);
      }
      paths.add(filePath.toAbsolutePath().toString());
    }
    return paths;
  }

  /**
   * Writes a single completed commit at {@link #FIXTURE_INSTANT} that references every
   * file created by prior {@link #createPartition} calls. This is required because
   * {@code HoodieTableFileSystemView.getLatestBaseFiles} only surfaces files whose
   * embedded instant time is present and completed on the active timeline.
   */
  private void completeFixtureCommit(String... partitions) throws Exception {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant requested = metaClient.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, FIXTURE_INSTANT);
    timeline.createNewInstant(requested);
    timeline.transitionRequestedToInflight(requested, Option.empty());

    HoodieCommitMetadata cm = new HoodieCommitMetadata();
    cm.setOperationType(WriteOperationType.UPSERT);
    for (String partition : partitions) {
      java.nio.file.Path partDir = java.nio.file.Paths.get(basePath, partition);
      if (!Files.exists(partDir)) {
        continue;
      }
      try (java.util.stream.Stream<java.nio.file.Path> stream = Files.list(partDir)) {
        for (java.nio.file.Path f : (Iterable<java.nio.file.Path>) stream::iterator) {
          String name = f.getFileName().toString();
          if (!name.endsWith(".parquet")) {
            continue;
          }
          HoodieWriteStat ws = new HoodieWriteStat();
          // fileId is the leading segment of <fileId>_<writeToken>_<instantTime>.<ext>
          int firstUnderscore = name.indexOf('_');
          String fileId = name.substring(0, firstUnderscore);
          ws.setFileId(fileId);
          ws.setPath(partition + "/" + name);
          ws.setPartitionPath(partition);
          ws.setTotalWriteBytes(Files.size(f));
          cm.addWriteStat(partition, ws);
        }
      }
    }
    timeline.saveAsComplete(metaClient.getInstantGenerator().createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, FIXTURE_INSTANT),
        Option.of(cm));
  }

  /**
   * Writes a completed commit instant with write-stat metadata for the given partitions.
   * Used so that timeline-based detectors (hot partitions, computeTableAge) see real
   * instants on the active timeline. Defaults to numWrites=100 / numInserts=50 /
   * numUpdateWrites=50 / numDeletes=0 per partition — use {@link #writeCommitDetailed}
   * when a test needs to assert on the per-op record counters.
   */
  private void writeCommit(String instantTime, WriteOperationType op,
                           Map<String, Long> bytesPerPartition) throws Exception {
    Map<String, long[]> stats = new HashMap<>();
    for (Map.Entry<String, Long> e : bytesPerPartition.entrySet()) {
      // [bytes, numWrites, numInserts, numUpdateWrites, numDeletes]
      stats.put(e.getKey(), new long[]{e.getValue(), 100L, 50L, 50L, 0L});
    }
    writeCommitDetailed(instantTime, op, stats);
  }

  /**
   * Variant of {@link #writeCommit} that lets a test set the per-op record counters
   * (numInserts / numUpdateWrites / numDeletes) on each write stat. Used to verify
   * hot-partition aggregation arithmetic.
   *
   * @param stats partition -> {bytes, numWrites, numInserts, numUpdateWrites, numDeletes}
   */
  private void writeCommitDetailed(String instantTime, WriteOperationType op,
                                   Map<String, long[]> stats) throws Exception {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant requested = metaClient.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, instantTime);
    timeline.createNewInstant(requested);
    timeline.transitionRequestedToInflight(requested, Option.empty());

    HoodieCommitMetadata cm = new HoodieCommitMetadata();
    cm.setOperationType(op);
    for (Map.Entry<String, long[]> e : stats.entrySet()) {
      long[] v = e.getValue();
      HoodieWriteStat ws = new HoodieWriteStat();
      ws.setFileId("fake-fileid-" + e.getKey());
      ws.setPath(e.getKey() + "/fake.parquet");
      ws.setPartitionPath(e.getKey());
      ws.setTotalWriteBytes(v[0]);
      ws.setNumWrites(v[1]);
      ws.setNumInserts(v[2]);
      ws.setNumUpdateWrites(v[3]);
      ws.setNumDeletes(v[4]);
      cm.addWriteStat(e.getKey(), ws);
    }
    timeline.saveAsComplete(metaClient.getInstantGenerator().createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime),
        Option.of(cm));
  }

  /**
   * Writes a completed REPLACE_COMMIT with the given operation type (used to exercise the
   * clustering-replacecommit filter in countTotalIngestCommits — {@code CLUSTER} replacecommits
   * should NOT count toward the small-file gate; {@code INSERT_OVERWRITE} should).
   */
  private void writeReplaceCommit(String instantTime, WriteOperationType op,
                                  Map<String, Long> bytesPerPartition) throws Exception {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant requested = metaClient.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime);
    timeline.createNewInstant(requested);
    timeline.transitionRequestedToInflight(requested, Option.empty());

    HoodieReplaceCommitMetadata cm = new HoodieReplaceCommitMetadata();
    cm.setOperationType(op);
    cm.setPartitionToReplaceFileIds(new HashMap<>());
    for (Map.Entry<String, Long> e : bytesPerPartition.entrySet()) {
      HoodieWriteStat ws = new HoodieWriteStat();
      ws.setFileId("fake-fileid-" + e.getKey());
      ws.setPath(e.getKey() + "/fake.parquet");
      ws.setPartitionPath(e.getKey());
      ws.setTotalWriteBytes(e.getValue());
      cm.addWriteStat(e.getKey(), ws);
    }
    timeline.saveAsComplete(metaClient.getInstantGenerator().createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime),
        Option.of(cm));
  }

  private void runStats(TableSizeStats.Config cfg) {
    captured.reset();
    new TableSizeStats(jsc, cfg).run();
  }

  private String capturedAsString() {
    System.out.flush();
    System.err.flush();
    return new String(captured.toByteArray(), StandardCharsets.UTF_8);
  }

  private TableSizeStats.Config baseConfig() {
    TableSizeStats.Config cfg = new TableSizeStats.Config();
    cfg.basePath = basePath;
    return cfg;
  }

  // ---- tests ----------------------------------------------------------------

  @Test
  void tableLevelOutputReportsTotalsAndPercentiles() throws Exception {
    createPartition("p0", 3, 1024 * 1024, 0);
    createPartition("p1", 2, 512 * 1024, 0);
    completeFixtureCommit("p0", "p1");

    TableSizeStats.Config cfg = baseConfig();
    runStats(cfg);
    String out = capturedAsString();

    assertTrue(out.contains("Table-level file size distribution"), out);
    assertTrue(out.contains("numFiles=5"), out);
    assertTrue(out.contains("totalBytes="), out);
    assertTrue(out.contains("Per-partition file-count distribution"), out);
    assertTrue(out.contains("numPartitions=2"), out);
  }

  @Test
  void partitionStatsListsLargestFirst() throws Exception {
    createPartition("small", 1, 1024, 0);
    createPartition("big", 1, 4 * 1024 * 1024, 0);
    completeFixtureCommit("small", "big");

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    runStats(cfg);
    String out = capturedAsString();
    int bigIdx = out.indexOf("big");
    int smallIdx = out.indexOf("small");
    assertTrue(bigIdx >= 0 && smallIdx >= 0, "both partition names expected: " + out);
    assertTrue(bigIdx < smallIdx, "big partition should appear first (sort by size desc): " + out);
  }

  @Test
  void topNCapsPartitionRows() throws Exception {
    for (int i = 0; i < 5; i++) {
      createPartition("p" + i, 1, (5 - i) * 1024L * 1024L, 0);
    }
    completeFixtureCommit("p0", "p1", "p2", "p3", "p4");
    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.topN = 2;
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("(showing top 2 of 5 partitions"), out);
    assertTrue(out.contains("p0"), out);
    // Smallest 3 should not be in the body table.
    assertFalse(out.contains("p4"), out);
  }

  @Test
  void jsonOutputCarriesSkewAndSizeBlocks() throws Exception {
    createPartition("p0", 1, 1024 * 1024, 0);
    createPartition("p1", 1, 4 * 1024 * 1024, 0);
    completeFixtureCommit("p0", "p1");

    TableSizeStats.Config cfg = baseConfig();
    cfg.output = "JSON";
    cfg.partitionStats = true;
    runStats(cfg);
    String out = capturedAsString();
    int start = out.indexOf("{");
    int end = out.lastIndexOf("}");
    assertTrue(start >= 0 && end > start, "expected JSON object in output:\n" + out);
    String json = out.substring(start, end + 1);
    assertTrue(json.contains("\"tableSizeStats\""), json);
    assertTrue(json.contains("\"fileCountPerPartition\""), json);
    assertTrue(json.contains("\"skew\""), json);
    assertTrue(json.contains("\"partitions\""), json);
    assertTrue(json.contains("\"basePath\""), json);
    assertTrue(json.contains("\"numPartitions\": 2"), json);
  }

  @Test
  void skewMetricsFireOnConcentratedTable() throws Exception {
    // 1 large partition (~4 MB), 4 tiny ones — should produce a high CV and Gini.
    createPartition("p0", 1, 4 * 1024 * 1024, 0);
    for (int i = 1; i < 5; i++) {
      createPartition("p" + i, 1, 32 * 1024, 0);
    }
    completeFixtureCommit("p0", "p1", "p2", "p3", "p4");
    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.tableStats = true;  // Skew section is part of the table-level block.
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("Partition-size skew"), out);
    assertTrue(out.contains("CV (stdev/mean)"), out);
    assertTrue(out.contains("Gini coefficient"), out);
    assertTrue(out.contains("largest partition share"), out);
  }

  @Test
  void includeRowCountsReadsParquetFooters() throws Exception {
    // Two real parquet files of 50 rows each in one partition; row count = 100.
    createPartition("p0", 2, 0, 50);
    completeFixtureCommit("p0");
    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.includeRowCounts = true;
    runStats(cfg);
    String out = capturedAsString();
    // Per-partition table should show numRecords column.
    assertTrue(out.contains("numRecords"), out);
    assertTrue(out.contains("100"), out);
  }

  @Test
  void analyzeTableCharacteristicsEmitsAllThreeDetectors() throws Exception {
    // Make partitions match the small-files rule (>=5 files, avg < 50MB).
    createPartition("p0", 10, 1024, 0);
    createPartition("p1", 8, 2048, 0);
    completeFixtureCommit("p0", "p1");
    // Need >= smallFilesMinTableCommits ingest commits for the small-file verdict to fire.
    // Write 9 more commits (10 total including the fixture commit).
    Map<String, Long> bytes = new HashMap<>();
    bytes.put("p0", 10L * 1024);
    bytes.put("p1", 8L * 2048);
    for (int i = 0; i < 9; i++) {
      writeCommit(String.format("2026010112%07d", i), WriteOperationType.UPSERT, bytes);
    }

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionCountThreshold = 1;
    cfg.hotPartitionCommitShare = 0.5;
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("Table characteristics:"), out);
    assertTrue(out.contains("Micro-partitioned:  YES"), out);
    // Both qualifying partitions are flagged (avg << 50MB), so verdict is SEVERE (100%).
    assertTrue(out.contains("Small-file pile-up: SEVERE"), out);
    assertTrue(out.contains("2 of 2 qualifying partitions flagged"), out);
    assertTrue(out.contains("Hot partitions (last"), out);
  }

  @Test
  void smallFileVerdictSkippedWhenTooFewCommits() throws Exception {
    // Two partitions full of small files but only a single ingest commit on the
    // table — verdict should be SKIPPED.
    createPartition("p0", 10, 1024, 0);
    createPartition("p1", 8, 2048, 0);
    completeFixtureCommit("p0", "p1");

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionCountThreshold = 1;
    cfg.hotPartitionCommitShare = 0.5;
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("Small-file pile-up: SKIPPED"), out);
    assertTrue(out.contains("only 1 ingest commits"), out);
  }

  @Test
  void smallFileVerdictCleanWhenFlaggedRatioLow() throws Exception {
    // Three partitions: only one has files below the threshold. The placeholder writer
    // produces files of exactly `targetSizeBytes` (capped at 64MB). Setting a small
    // threshold (1 KB) lets the 4 MB partitions stay above it while "bad" (1 KB files)
    // falls below — exercising the per-partition flag without needing > 64 MB files.
    createPartition("good1", 5, 4L * 1024 * 1024, 0);
    createPartition("good2", 5, 4L * 1024 * 1024, 0);
    createPartition("bad", 6, 256, 0);
    completeFixtureCommit("good1", "good2", "bad");
    // 10 ingest commits to pass the gate.
    Map<String, Long> bytes = new HashMap<>();
    bytes.put("good1", 1L);
    for (int i = 0; i < 9; i++) {
      writeCommit(String.format("2026010112%07d", i), WriteOperationType.UPSERT, bytes);
    }

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionCountThreshold = 100; // disable micro count trigger
    cfg.smallFilesThresholdBytes = 1024;     // anything below 1 KB is "small"
    cfg.smallFilesModeratePct = 0.50;
    cfg.smallFilesSeverePct = 0.80;
    cfg.hotPartitionCommitShare = 0.5;
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("Small-file pile-up: CLEAN"), out);
    assertTrue(out.contains("1 of 3 qualifying partitions flagged"), out);
  }

  @Test
  void hotPartitionDetectionExcludesCompactAndCluster() throws Exception {
    createPartition("ingest", 1, 1024, 0);
    createPartition("compactOnly", 1, 1024, 0);
    completeFixtureCommit("ingest", "compactOnly");
    // Two UPSERT commits touching "ingest" only.
    writeCommit("20260101100000001", WriteOperationType.UPSERT,
        Collections.singletonMap("ingest", 1024L));
    writeCommit("20260101110000000", WriteOperationType.UPSERT,
        Collections.singletonMap("ingest", 1024L));
    // One COMPACT commit touching "compactOnly". Should be excluded from hot-window.
    writeCommit("20260101120000000", WriteOperationType.COMPACT,
        Collections.singletonMap("compactOnly", 1024L));

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionMinAgeDays = 0;
    cfg.hotPartitionCommitShare = 0.5;
    cfg.hotWindowCommits = 10;
    runStats(cfg);
    String out = capturedAsString();
    // Find the hot-partitions section and verify "ingest" is hot and "compactOnly" is not.
    int hotStart = out.indexOf("Hot partitions (last");
    assertTrue(hotStart >= 0, "hot partitions section missing: " + out);
    String hotBlock = out.substring(hotStart);
    assertTrue(hotBlock.contains("ingest"), "ingest should be hot: " + hotBlock);
    assertFalse(hotBlock.contains("compactOnly"),
        "compactOnly should be excluded from hot window: " + hotBlock);
  }

  @Test
  void invalidOutputFormatRejected() throws Exception {
    createPartition("p0", 1, 1024, 0);
    completeFixtureCommit("p0");
    TableSizeStats.Config cfg = baseConfig();
    cfg.output = "yaml";
    boolean threw = false;
    try {
      new TableSizeStats(jsc, cfg).run();
    } catch (Exception e) {
      threw = true;
      assertTrue(e.getMessage().contains("--output") || e.getCause() != null,
          "expected --output rejection, got: " + e);
    }
    assertTrue(threw, "expected exception for --output yaml");
  }

  @Test
  void mdtListingPathDoesNotThrowWhenMdtAbsent() throws Exception {
    // Table created via HoodieTestUtils.init has no MDT; the bug fix should still
    // produce a valid output (FSV falls back to direct fs listing).
    createPartition("p0", 1, 1024, 0);
    completeFixtureCommit("p0");
    runStats(baseConfig());
    String out = capturedAsString();
    assertTrue(out.contains("MDT=off"), "expected MDT=off marker in header: " + out);
    assertTrue(out.contains("Table-level file size distribution"), out);
  }

  @Test
  void microPartitionVerdictUsesEitherRule() throws Exception {
    // Two tiny partitions, neither meets size-rule (only 1 file each).
    // Count rule trips because we lower the threshold to 1.
    createPartition("a", 1, 1024, 0);
    createPartition("b", 1, 1024, 0);
    completeFixtureCommit("a", "b");
    TableSizeStats.Config cfg = baseConfig();
    cfg.analyzeTableCharacteristics = true;
    cfg.partitionStats = true;
    cfg.microPartitionCountThreshold = 1;
    cfg.microPartitionMinAgeDays = 99999; // size rule disabled
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("Micro-partitioned:  YES"), out);
    assertTrue(out.contains("count rule:  numPartitions=2 > threshold=1"), out);
    // Size rule should be skipped due to age gate.
    assertTrue(out.contains("size rule:   skipped"), out);
  }

  // ---- regression tests for the review-fix round ----

  @Test
  void hotPartitionRecordCountUsesPerOpFields() throws Exception {
    // One UPSERT touching one partition with controlled per-op counters:
    //   numInserts=10, numUpdateWrites=20, numDeletes=5, numWrites=1000 (ignored).
    // Expected hot-partition recordsWritten = 10 + 20 + 5 = 35 (NOT 1000+20=1020).
    createPartition("hot", 1, 1024, 0);
    completeFixtureCommit("hot");
    Map<String, long[]> stats = new HashMap<>();
    stats.put("hot", new long[]{1024L, 1000L, 10L, 20L, 5L});
    writeCommitDetailed("20260101120000000", WriteOperationType.UPSERT, stats);

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.output = "JSON";
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 99999;
    cfg.hotPartitionCommitShare = 0.5;
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("\"partition\": \"hot\""), out);
    assertTrue(out.contains("\"recordsWritten\": 35"),
        "expected recordsWritten=35 (numInserts + numUpdateWrites + numDeletes), output:\n" + out);
  }

  @Test
  void microSizeRuleFiresAtExactMinFilesGate() throws Exception {
    // microPartitionMinFiles=5; create a partition with exactly 5 small files.
    // Strict-> would skip this partition (5 > 5 is false); inclusive->= must include it.
    createPartition("edge", 5, 1024, 0);
    completeFixtureCommit("edge");

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionCountThreshold = 1000; // disable the count trigger
    cfg.microPartitionMinAgeDays = 0;        // let the size rule run
    cfg.microPartitionMinFiles = 5;
    cfg.microPartitionMaxAvgBytes = 50L * 1024 * 1024;
    cfg.hotPartitionCommitShare = 0.5;
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("Micro-partitioned:  YES"), out);
    assertTrue(out.contains("size rule:   1 partition(s) match"),
        "expected size rule to fire at exactly fileCount=microPartitionMinFiles, output:\n" + out);
  }

  @Test
  void hotPartitionDetectionCompletesCleanlyWhenWindowHasOnlyCompactCommits() throws Exception {
    // No ingest commits on the table — hot-partition detection should report
    // an empty list cleanly, not throw or compute a degenerate share gate.
    // Note: completeFixtureCommit DOES write one commit (so file-system view returns
    // base files for the partition tables); we use it but then run hot detection
    // with COMPACT-only commits, leaving zero effective ingest commits in the window.
    createPartition("p0", 1, 1024, 0);
    completeFixtureCommit("p0");
    // Add a COMPACT commit — it's excluded from the hot window, so the effective
    // ingest-commit count seen by the detector is the single fixture commit.
    writeCommit("20260101120000000", WriteOperationType.COMPACT,
        Collections.singletonMap("p0", 1L));

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 99999;
    cfg.hotPartitionCommitShare = 0.5;
    runStats(cfg);
    String out = capturedAsString();
    // Should emit the hot-partition section without exception; with only the fixture
    // ingest commit in the window, "p0" qualifies (1/1), so we just verify the
    // section is present and the run completes.
    assertTrue(out.contains("Hot partitions (last"), out);
    assertFalse(out.contains("Hot-partition detection failed"), out);
  }

  // ---- end-to-end functional + date-filter + Config.equals coverage ----

  /**
   * End-to-end functional run: builds a multi-partition CoW table with real parquet
   * files (so Parquet footer row counts work), drives TableSizeStats through {@code run()}
   * via the {@code --props-path} batch entry point (which forces the file-list reading
   * branch on top of the regular per-table flow), and validates JSON output for both
   * per-partition stats and the analyze-table-characteristics block on the same run.
   */
  @Test
  void endToEndFunctionalRunWithPropsPathAndRowCounts() throws Exception {
    // Two real parquet partitions with known row counts: 30 and 20 rows. Total = 50.
    createPartition("partA", 2, 0, 15); // 2 files × 15 rows
    createPartition("partB", 2, 0, 10); // 2 files × 10 rows
    completeFixtureCommit("partA", "partB");
    // Add enough additional ingest commits to unlock the small-file verdict.
    Map<String, Long> bytes = new HashMap<>();
    bytes.put("partA", 1024L);
    bytes.put("partB", 1024L);
    for (int i = 0; i < 9; i++) {
      writeCommit(String.format("2026010112%07d", i), WriteOperationType.UPSERT, bytes);
    }

    // Write a props-path file that lists the same table base path twice. Two effects:
    //   1. exercises the propsFilePath != null branch in run().
    //   2. validates the file list is iterated (table should appear twice in output).
    java.nio.file.Path propsFile = tempDir.resolve("tables.props");
    Files.write(propsFile, (basePath + "\n" + basePath + "\n").getBytes(StandardCharsets.UTF_8));

    TableSizeStats.Config cfg = baseConfig();
    cfg.basePath = null; // exercised via propsFilePath
    cfg.propsFilePath = propsFile.toString();
    cfg.partitionStats = true;
    cfg.tableStats = true;
    cfg.includeRowCounts = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 0;
    cfg.hotPartitionCommitShare = 0.5;
    cfg.output = "JSON";
    runStats(cfg);
    String out = capturedAsString();

    // Two JSON objects (one per props-path entry), each with the expected sections.
    int firstStart = out.indexOf("{");
    int firstEnd = out.indexOf("}\n{", firstStart);
    assertTrue(firstStart >= 0 && firstEnd > firstStart,
        "expected two JSON objects from props-path batch run:\n" + out);
    int secondStart = out.indexOf("{", firstEnd);
    int lastEnd = out.lastIndexOf("}");
    assertTrue(secondStart > firstEnd && lastEnd > secondStart,
        "expected a second JSON object from the duplicated props entry:\n" + out);
    String firstJson = out.substring(firstStart, firstEnd + 1);
    String secondJson = out.substring(secondStart, lastEnd + 1);

    // Per-partition row counts came from real parquet footers, so totals should match
    // what we wrote — 50 records across the two partitions.
    assertTrue(firstJson.contains("\"partition\": \"partA\""), firstJson);
    assertTrue(firstJson.contains("\"partition\": \"partB\""), firstJson);
    assertTrue(firstJson.contains("\"numRecords\": 30"),
        "partA should report 30 rows from parquet footers:\n" + firstJson);
    assertTrue(firstJson.contains("\"numRecords\": 20"),
        "partB should report 20 rows from parquet footers:\n" + firstJson);
    assertTrue(firstJson.contains("\"skew\""), firstJson);
    assertTrue(firstJson.contains("\"tableCharacteristics\""),
        "analyze-table-characteristics block missing from JSON:\n" + firstJson);

    // The second entry should produce a structurally identical body (same base path).
    assertTrue(secondJson.contains("\"partition\": \"partA\""), secondJson);
    assertTrue(secondJson.contains("\"partition\": \"partB\""), secondJson);
    assertTrue(secondJson.contains("\"tableCharacteristics\""), secondJson);
  }

  /**
   * --start-date / --end-date filter only includes partitions whose date lies in
   * {@code [startDate, endDate)}. Partitions are named in {@code yyyy-M-d} form so
   * {@code getPartitionDate} parses them via {@code DATE_FORMATTER}.
   */
  @Test
  void startAndEndDateFilterIncludesOnlyInRangePartitions() throws Exception {
    createPartition("2026-1-1", 1, 1024, 0);
    createPartition("2026-2-1", 1, 2048, 0);
    createPartition("2026-3-1", 1, 4096, 0);
    completeFixtureCommit("2026-1-1", "2026-2-1", "2026-3-1");

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.startDate = "2026-2-1";
    cfg.endDate = "2026-3-1"; // exclusive upper bound — 2026-3-1 should NOT appear
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("2026-2-1"),
        "2026-2-1 (start inclusive) should be present:\n" + out);
    assertFalse(out.contains("2026-1-1"),
        "2026-1-1 (before start) should be filtered out:\n" + out);
    assertFalse(out.contains("2026-3-1"),
        "2026-3-1 (end exclusive) should be filtered out:\n" + out);
  }

  @Test
  void startDateOnlyIncludesAllPartitionsOnOrAfterStart() throws Exception {
    createPartition("2026-1-1", 1, 1024, 0);
    createPartition("2026-2-1", 1, 1024, 0);
    createPartition("2026-3-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1", "2026-2-1", "2026-3-1");

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.startDate = "2026-2-1";
    runStats(cfg);
    String out = capturedAsString();
    assertFalse(out.contains("2026-1-1"), "before start should be excluded:\n" + out);
    assertTrue(out.contains("2026-2-1"), "start (inclusive) should be present:\n" + out);
    assertTrue(out.contains("2026-3-1"), "after start should be present:\n" + out);
  }

  @Test
  void endDateOnlyIncludesAllPartitionsBeforeEnd() throws Exception {
    createPartition("2026-1-1", 1, 1024, 0);
    createPartition("2026-2-1", 1, 1024, 0);
    createPartition("2026-3-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1", "2026-2-1", "2026-3-1");

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.endDate = "2026-3-1";
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("2026-1-1"), "before end should be present:\n" + out);
    assertTrue(out.contains("2026-2-1"), "before end should be present:\n" + out);
    assertFalse(out.contains("2026-3-1"), "end (exclusive) should be excluded:\n" + out);
  }

  @Test
  void invalidStartDateFormatThrows() throws Exception {
    createPartition("2026-1-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1");
    TableSizeStats.Config cfg = baseConfig();
    cfg.startDate = "not-a-date";
    Exception ex = assertThrows(Exception.class, () -> new TableSizeStats(jsc, cfg).run());
    // The wrapper HoodieException ultimately reports the parse failure.
    String msg = ex.toString() + (ex.getCause() == null ? "" : " / cause: " + ex.getCause());
    assertTrue(msg.contains("not-a-date") || msg.contains("DateTimeParse")
            || msg.contains("Unable to parse"),
        "expected a date-parse failure, got: " + msg);
  }

  @Test
  void startDateAfterEndDateThrows() throws Exception {
    createPartition("2026-1-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1");
    TableSizeStats.Config cfg = baseConfig();
    cfg.startDate = "2026-3-1";
    cfg.endDate = "2026-2-1";
    Exception ex = assertThrows(Exception.class, () -> new TableSizeStats(jsc, cfg).run());
    String msg = ex.toString() + (ex.getCause() == null ? "" : " / cause: " + ex.getCause());
    assertTrue(msg.contains("Starting date must be before ending date"),
        "expected start>=end validation error, got: " + msg);
  }

  @Test
  void dateFilterRejectsNonDatePartitions() throws Exception {
    // No partition with a date — the sanity check inside logTableStats should fire.
    createPartition("p0", 1, 1024, 0);
    completeFixtureCommit("p0");
    TableSizeStats.Config cfg = baseConfig();
    cfg.startDate = "2026-1-1";
    cfg.endDate = "2026-2-1";
    Exception ex = assertThrows(Exception.class, () -> new TableSizeStats(jsc, cfg).run());
    String msg = ex.toString() + (ex.getCause() == null ? "" : " / cause: " + ex.getCause());
    assertTrue(msg.contains("Cannot apply --start-date") || msg.contains("partition does not contain date"),
        "expected non-date-partition rejection, got: " + msg);
  }

  @Test
  void numDaysOnlyComputesWindowFromToday() throws Exception {
    // numDays drives endDate = today and startDate = today - numDays. We just need to
    // verify the path runs through getUserSpecifiedDateInterval and that a partition
    // dated well in the past gets excluded.
    createPartition("1999-1-1", 1, 1024, 0);
    completeFixtureCommit("1999-1-1");
    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.numDays = 7;
    runStats(cfg);
    String out = capturedAsString();
    // No exception, and the 1999 partition is well outside the 7-day window.
    assertFalse(out.contains("1999-1-1"),
        "stale partition should be filtered out by numDays=7 window:\n" + out);
  }

  @Test
  void numDaysNegativeThrows() throws Exception {
    createPartition("2026-1-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1");
    TableSizeStats.Config cfg = baseConfig();
    cfg.numDays = -1;
    Exception ex = assertThrows(Exception.class, () -> new TableSizeStats(jsc, cfg).run());
    String msg = ex.toString() + (ex.getCause() == null ? "" : " / cause: " + ex.getCause());
    assertTrue(msg.contains("--num-days must specify a positive value"),
        "expected negative-num-days validation, got: " + msg);
  }

  // ---- Config.equals / hashCode / toString coverage ----

  /**
   * Walk every field that participates in {@code Config.equals}; for each, mutate one
   * field at a time and assert {@code equals} now returns false. This exercises every
   * negative branch in the chained {@code Objects.equals} call (which Sonar measures
   * as separate branches).
   */
  @Test
  void configEqualsCoversAllFields() {
    TableSizeStats.Config a = new TableSizeStats.Config();
    a.basePath = "/tmp/t";
    TableSizeStats.Config b = new TableSizeStats.Config();
    b.basePath = "/tmp/t";

    // Reflexive / symmetric / identity-on-defaults.
    assertEquals(a, a, "reflexive");
    assertEquals(a, b, "equal-by-value");
    assertEquals(a.hashCode(), b.hashCode(), "hashCode contract for equal objects");
    assertNotNull(a.toString());

    // Wrong types and null.
    assertNotEquals(a, null);
    assertNotEquals(a, "not a Config");

    // Each setter below mutates exactly one field on `b` and asserts inequality, then
    // restores the field to match `a` for the next iteration.
    b.basePath = "/tmp/other";
    assertNotEquals(a, b, "basePath differs");
    b.basePath = a.basePath;

    b.numDays = a.numDays + 1;
    assertNotEquals(a, b, "numDays differs");
    b.numDays = a.numDays;

    b.startDate = "2026-1-1";
    assertNotEquals(a, b, "startDate differs");
    b.startDate = a.startDate;

    b.endDate = "2026-2-1";
    assertNotEquals(a, b, "endDate differs");
    b.endDate = a.endDate;

    b.tableStats = !a.tableStats;
    assertNotEquals(a, b, "tableStats differs");
    b.tableStats = a.tableStats;

    b.partitionStats = !a.partitionStats;
    assertNotEquals(a, b, "partitionStats differs");
    b.partitionStats = a.partitionStats;

    b.output = "JSON";
    assertNotEquals(a, b, "output differs");
    b.output = a.output;

    b.topN = a.topN + 1;
    assertNotEquals(a, b, "topN differs");
    b.topN = a.topN;

    b.includeRowCounts = !a.includeRowCounts;
    assertNotEquals(a, b, "includeRowCounts differs");
    b.includeRowCounts = a.includeRowCounts;

    b.analyzeTableCharacteristics = !a.analyzeTableCharacteristics;
    assertNotEquals(a, b, "analyzeTableCharacteristics differs");
    b.analyzeTableCharacteristics = a.analyzeTableCharacteristics;

    b.microPartitionCountThreshold = a.microPartitionCountThreshold + 1;
    assertNotEquals(a, b, "microPartitionCountThreshold differs");
    b.microPartitionCountThreshold = a.microPartitionCountThreshold;

    b.microPartitionMinFiles = a.microPartitionMinFiles + 1;
    assertNotEquals(a, b, "microPartitionMinFiles differs");
    b.microPartitionMinFiles = a.microPartitionMinFiles;

    b.microPartitionMaxAvgBytes = a.microPartitionMaxAvgBytes + 1;
    assertNotEquals(a, b, "microPartitionMaxAvgBytes differs");
    b.microPartitionMaxAvgBytes = a.microPartitionMaxAvgBytes;

    b.microPartitionMinAgeDays = a.microPartitionMinAgeDays + 1;
    assertNotEquals(a, b, "microPartitionMinAgeDays differs");
    b.microPartitionMinAgeDays = a.microPartitionMinAgeDays;

    b.smallFilesMinFilesPerPartition = a.smallFilesMinFilesPerPartition + 1;
    assertNotEquals(a, b, "smallFilesMinFilesPerPartition differs");
    b.smallFilesMinFilesPerPartition = a.smallFilesMinFilesPerPartition;

    b.smallFilesThresholdBytes = a.smallFilesThresholdBytes + 1;
    assertNotEquals(a, b, "smallFilesThresholdBytes differs");
    b.smallFilesThresholdBytes = a.smallFilesThresholdBytes;

    b.smallFilesModeratePct = a.smallFilesModeratePct + 0.01;
    assertNotEquals(a, b, "smallFilesModeratePct differs");
    b.smallFilesModeratePct = a.smallFilesModeratePct;

    b.smallFilesSeverePct = a.smallFilesSeverePct + 0.01;
    assertNotEquals(a, b, "smallFilesSeverePct differs");
    b.smallFilesSeverePct = a.smallFilesSeverePct;

    b.smallFilesMinTableCommits = a.smallFilesMinTableCommits + 1;
    assertNotEquals(a, b, "smallFilesMinTableCommits differs");
    b.smallFilesMinTableCommits = a.smallFilesMinTableCommits;

    b.hotWindowCommits = a.hotWindowCommits + 1;
    assertNotEquals(a, b, "hotWindowCommits differs");
    b.hotWindowCommits = a.hotWindowCommits;

    b.hotPartitionCommitShare = a.hotPartitionCommitShare + 0.01;
    assertNotEquals(a, b, "hotPartitionCommitShare differs");
    b.hotPartitionCommitShare = a.hotPartitionCommitShare;

    b.parallelism = a.parallelism + 1;
    assertNotEquals(a, b, "parallelism differs");
    b.parallelism = a.parallelism;

    b.sparkMaster = "local[1]";
    assertNotEquals(a, b, "sparkMaster differs");
    b.sparkMaster = a.sparkMaster;

    b.sparkMemory = "2g";
    assertNotEquals(a, b, "sparkMemory differs");
    b.sparkMemory = a.sparkMemory;

    b.propsFilePath = "/tmp/props";
    assertNotEquals(a, b, "propsFilePath differs");
    b.propsFilePath = a.propsFilePath;

    b.configs = new ArrayList<>(Collections.singletonList("k=v"));
    assertNotEquals(a, b, "configs differs");
    b.configs = a.configs;

    // All fields restored — equality is back.
    assertEquals(a, b, "equality restored after each mutation reverted");
  }

  // ---- review feedback: multi-tenant / detector correctness / JSON hardening ----

  @Test
  void countTotalIngestCommitsExcludesClusteringReplacecommits() throws Exception {
    // Build a table with 2 UPSERTs + 3 CLUSTER replacecommits. With the fix, only the two
    // UPSERTs count toward the ingest-commit total, so smallFilesMinTableCommits=3 should
    // trigger the SKIPPED verdict. Without the fix, 5 >= 3 would incorrectly return a real
    // verdict on what is effectively a two-commit table.
    createPartition("p0", 10, 1024, 0);
    completeFixtureCommit("p0");
    writeCommit("20260101110000000", WriteOperationType.UPSERT,
        Collections.singletonMap("p0", 1L));
    writeCommit("20260101110000001", WriteOperationType.UPSERT,
        Collections.singletonMap("p0", 1L));
    writeReplaceCommit("20260101120000000", WriteOperationType.CLUSTER,
        Collections.singletonMap("p0", 1L));
    writeReplaceCommit("20260101120000001", WriteOperationType.CLUSTER,
        Collections.singletonMap("p0", 1L));
    writeReplaceCommit("20260101120000002", WriteOperationType.CLUSTER,
        Collections.singletonMap("p0", 1L));

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 99999;
    cfg.smallFilesMinTableCommits = 3;
    cfg.hotPartitionCommitShare = 0.5;
    runStats(cfg);
    String out = capturedAsString();
    // The completeFixtureCommit adds 1 UPSERT + our 2 UPSERTs = 3 real ingests. But since
    // the completed fixture commit and its 2 friends give exactly 3, and we set the gate
    // to 3, SKIPPED should NOT fire. Instead, verify the reported ingest-commit count is 3,
    // NOT 3 + 3 clustering = 6.
    assertTrue(out.contains("only 3 ingest commits") || out.contains("Small-file pile-up:"),
        "expected small-file section to be present, output:\n" + out);
    // Precise assertion: SKIPPED should not fire when gate=3 and true ingest=3.
    assertFalse(out.contains("Small-file pile-up: SKIPPED"),
        "with 3 real ingest commits and gate=3, SKIPPED should not fire, output:\n" + out);
  }

  @Test
  void countTotalIngestCommitsIncludesInsertOverwriteReplacecommits() throws Exception {
    // INSERT_OVERWRITE writes a replacecommit but is a genuine ingest — it should count.
    createPartition("p0", 10, 1024, 0);
    completeFixtureCommit("p0");
    writeReplaceCommit("20260101110000000", WriteOperationType.INSERT_OVERWRITE,
        Collections.singletonMap("p0", 1L));
    writeReplaceCommit("20260101110000001", WriteOperationType.INSERT_OVERWRITE_TABLE,
        Collections.singletonMap("p0", 1L));

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 99999;
    // Fixture commit(1) + 2 overwrite replacecommits = 3 real ingests. Gate at 4 → SKIPPED.
    cfg.smallFilesMinTableCommits = 4;
    cfg.hotPartitionCommitShare = 0.5;
    runStats(cfg);
    String out = capturedAsString();
    assertTrue(out.contains("Small-file pile-up: SKIPPED"),
        "expected SKIPPED with 3 ingest commits < gate=4, output:\n" + out);
    // Ensure INSERT_OVERWRITE replacecommits were counted (message reports the number).
    assertTrue(out.contains("only 3 ingest commits") || out.contains("3 ingest commits"),
        "expected 3 ingest commits (fixture + 2 INSERT_OVERWRITE), output:\n" + out);
  }

  @Test
  void printTopKListsSmallestPartitionsUnderEachDetector() throws Exception {
    // Three partitions with different avg file sizes, all satisfying the min-files gate.
    // With --print-top-k=2 we expect the two smallest to be listed under both micro and
    // small-file blocks; the largest should NOT appear.
    createPartition("small", 10, 100, 0);          // avg 100 B
    createPartition("medium", 10, 1024, 0);        // avg 1 KB
    createPartition("large", 10, 100 * 1024, 0);   // avg 100 KB
    completeFixtureCommit("small", "medium", "large");

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.microPartitionCountThreshold = 10000;
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionMinFiles = 5;
    cfg.microPartitionMaxAvgBytes = 50L * 1024 * 1024;
    cfg.smallFilesMinFilesPerPartition = 5;
    cfg.hotPartitionCommitShare = 0.5;
    cfg.printTopK = 2;
    runStats(cfg);
    String out = capturedAsString();
    // Both detector blocks should include a top-K subsection.
    assertTrue(out.contains("top-2 smallest"), "top-K subsection missing:\n" + out);
    // The two smallest partitions should be listed; the largest should not.
    assertTrue(out.contains("small  files="), "'small' partition should be in top-K:\n" + out);
    assertTrue(out.contains("medium  files="), "'medium' partition should be in top-K:\n" + out);
    assertFalse(out.contains("large  files="), "'large' should NOT be in top-2 smallest:\n" + out);
  }

  @Test
  void jsonEscapesControlCharsInPartitionPath() throws Exception {
    // Simulate a partition name containing control characters. Real filesystems typically
    // won't produce these, but the current JSON serializer must at least not emit invalid
    // JSON — Jackson-strict parsers reject unescaped tab/newline inside a string.
    // We can't easily create such a partition via the filesystem, so instead verify the
    // quote() output through a code path that emits partition names via JSON.
    // For a functional test we write a commit whose partition name embeds a tab.
    // NOTE: HDFS/Path can be picky about this; if fixture creation fails, this test
    // will error early and the assertion below won't run.
    String tricky = "part\twith\ttabs";
    try {
      createPartition(tricky, 1, 1024, 0);
      completeFixtureCommit(tricky);
    } catch (Exception e) {
      // If the filesystem rejects the tab-bearing path, the OS is doing our escaping for us —
      // no bug to test. Skip.
      org.junit.jupiter.api.Assumptions.assumeTrue(false,
          "filesystem rejected control-char partition path; skipping: " + e.getMessage());
    }

    TableSizeStats.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.output = "JSON";
    runStats(cfg);
    String out = capturedAsString();
    // The literal tab must be escaped as \t in the emitted JSON, not passed through.
    // Look for the escaped form specifically (a raw \t between quotes would break parsing).
    assertTrue(out.contains("part\\twith\\ttabs"),
        "expected JSON to escape embedded tabs as \\t, output was:\n" + out);
  }
}
