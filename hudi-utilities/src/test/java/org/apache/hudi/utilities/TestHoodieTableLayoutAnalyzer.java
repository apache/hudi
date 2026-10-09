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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieTableLayoutAnalyzer} end to end. Each test builds a synthetic Hudi table
 * in a temp dir, populates partition directories with known parquet files (real footers when
 * row counts are needed, zero-filled placeholders of a given size when only size matters),
 * runs the analyzer through {@code run()}, and asserts on captured stdout.
 */
class TestHoodieTableLayoutAnalyzer {

  private static final Schema TEST_SCHEMA = SchemaBuilder.record("Row").fields()
      .requiredString("_hoodie_record_key")
      .requiredString("partition")
      .requiredLong("ts")
      .endRecord();

  // Single instant time used for all partition base files, so a test only needs to
  // write a single completed commit at the end.
  private static final String FIXTURE_INSTANT = "20260101000000000";

  private static transient SparkSession spark;
  private static transient JavaSparkContext jsc;

  @TempDir
  java.nio.file.Path tempDir;

  private String basePath;
  private HoodieTableMetaClient metaClient;
  private HoodieTestTable testTable;
  private PrintStream originalOut;
  private PrintStream originalErr;
  private ByteArrayOutputStream captured;

  @BeforeAll
  static void initSpark() {
    if (spark == null) {
      SparkConf sparkConf = new SparkConf()
          .setAppName("TestHoodieTableLayoutAnalyzer")
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
      spark.stop();
      spark = null;
      jsc = null;
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    basePath = tempDir.resolve("dataset").toString();
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    testTable = HoodieTestTable.of(metaClient);

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

  // ---- fixtures -------------------------------------------------------------

  /**
   * Creates a partition directory with the partition metadata marker and a set of base
   * parquet files. Files are real parquet when {@code writeRealRows > 0} (so footer reads
   * return row counts); otherwise zero-filled placeholders of {@code targetSizeBytes}.
   */
  private void createPartition(String partition, int numFiles, long targetSizeBytes, int writeRealRows) throws Exception {
    testTable.withPartitionMetaFiles(partition);
    java.nio.file.Path partDir = Paths.get(basePath, partition);
    Files.createDirectories(partDir);
    for (int i = 0; i < numFiles; i++) {
      String fileId = String.format("%08d-0000-0000-0000-%012d-0", partition.hashCode() & 0x7fffffff, i);
      String fileName = fileId + "_0-1-1_" + FIXTURE_INSTANT + ".parquet";
      java.nio.file.Path filePath = partDir.resolve(fileName);

      if (writeRealRows > 0) {
        Path hadoopPath = new Path(filePath.toUri());
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
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
        // The analyzer only reads the file length here, not the contents.
        byte[] bytes = new byte[(int) Math.min(targetSizeBytes, 64 * 1024 * 1024)];
        Files.write(filePath, bytes);
      }
    }
  }

  /**
   * Writes a single completed commit at {@link #FIXTURE_INSTANT} that references every
   * file created by prior {@link #createPartition} calls. The file system view only
   * surfaces base files whose embedded instant time is completed on the active timeline.
   */
  private void completeFixtureCommit(String... partitions) throws Exception {
    HoodieCommitMetadata cm = new HoodieCommitMetadata();
    cm.setOperationType(WriteOperationType.UPSERT);
    for (String partition : partitions) {
      java.nio.file.Path partDir = Paths.get(basePath, partition);
      if (!Files.exists(partDir)) {
        continue;
      }
      try (Stream<java.nio.file.Path> stream = Files.list(partDir)) {
        for (java.nio.file.Path f : (Iterable<java.nio.file.Path>) stream::iterator) {
          String name = f.getFileName().toString();
          if (!name.endsWith(".parquet")) {
            continue;
          }
          HoodieWriteStat ws = new HoodieWriteStat();
          // fileId is the leading segment of <fileId>_<writeToken>_<instantTime>.<ext>
          ws.setFileId(name.substring(0, name.indexOf('_')));
          ws.setPath(partition + "/" + name);
          ws.setPartitionPath(partition);
          ws.setTotalWriteBytes(Files.size(f));
          cm.addWriteStat(partition, ws);
        }
      }
    }
    testTable.addCommit(FIXTURE_INSTANT, Option.of(cm));
  }

  /**
   * Writes a completed commit with write stats for the given partitions, so that the
   * timeline-based detectors (hot partitions, ingest-commit count) see real instants.
   * Defaults to numWrites=100 / numInserts=50 / numUpdateWrites=50 / numDeletes=0 per
   * partition; use {@link #writeCommitDetailed} to control the per-op record counters.
   */
  private void writeCommit(String instantTime, WriteOperationType op, Map<String, Long> bytesPerPartition) throws Exception {
    Map<String, long[]> stats = new HashMap<>();
    for (Map.Entry<String, Long> e : bytesPerPartition.entrySet()) {
      // [bytes, numWrites, numInserts, numUpdateWrites, numDeletes]
      stats.put(e.getKey(), new long[]{e.getValue(), 100L, 50L, 50L, 0L});
    }
    writeCommitDetailed(instantTime, op, stats);
  }

  /**
   * Variant of {@link #writeCommit} that lets a test set the per-op record counters on each
   * write stat, to verify hot-partition aggregation arithmetic.
   *
   * @param stats partition -> {bytes, numWrites, numInserts, numUpdateWrites, numDeletes}
   */
  private void writeCommitDetailed(String instantTime, WriteOperationType op, Map<String, long[]> stats) throws Exception {
    HoodieCommitMetadata cm = new HoodieCommitMetadata();
    cm.setOperationType(op);
    for (Map.Entry<String, long[]> e : stats.entrySet()) {
      long[] v = e.getValue();
      HoodieWriteStat ws = fakeWriteStat(e.getKey(), v[0]);
      ws.setNumWrites(v[1]);
      ws.setNumInserts(v[2]);
      ws.setNumUpdateWrites(v[3]);
      ws.setNumDeletes(v[4]);
      cm.addWriteStat(e.getKey(), ws);
    }
    testTable.addCommit(instantTime, Option.of(cm));
  }

  /**
   * Writes a completed replacecommit with the given operation type, to exercise the
   * clustering filter in the ingest-commit count: {@code CLUSTER} replacecommits must NOT
   * count toward the small-file gate; {@code INSERT_OVERWRITE} must.
   */
  private void writeReplaceCommit(String instantTime, WriteOperationType op, Map<String, Long> bytesPerPartition) throws Exception {
    HoodieReplaceCommitMetadata cm = new HoodieReplaceCommitMetadata();
    cm.setOperationType(op);
    for (Map.Entry<String, Long> e : bytesPerPartition.entrySet()) {
      cm.addWriteStat(e.getKey(), fakeWriteStat(e.getKey(), e.getValue()));
    }
    testTable.addReplaceCommit(instantTime, Option.empty(), Option.empty(), cm);
  }

  private static HoodieWriteStat fakeWriteStat(String partition, long bytes) {
    HoodieWriteStat ws = new HoodieWriteStat();
    ws.setFileId("fake-fileid-" + partition);
    ws.setPath(partition + "/fake.parquet");
    ws.setPartitionPath(partition);
    ws.setTotalWriteBytes(bytes);
    return ws;
  }

  private void runAnalyzer(HoodieTableLayoutAnalyzer.Config cfg) {
    captured.reset();
    new HoodieTableLayoutAnalyzer(jsc, cfg).run();
  }

  private String capturedAsString() {
    System.out.flush();
    System.err.flush();
    return new String(captured.toByteArray(), StandardCharsets.UTF_8);
  }

  private HoodieTableLayoutAnalyzer.Config baseConfig() {
    HoodieTableLayoutAnalyzer.Config cfg = new HoodieTableLayoutAnalyzer.Config();
    cfg.basePath = basePath;
    return cfg;
  }

  /** Config with the detectors on and the count / age rules neutralised unless a test sets them. */
  private HoodieTableLayoutAnalyzer.Config detectorConfig() {
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.analyzeTableCharacteristics = true;
    cfg.hotPartitionCommitShare = 0.5;
    return cfg;
  }

  private static String extractJsonObject(String out) {
    int start = out.indexOf("{");
    int end = out.lastIndexOf("}");
    assertTrue(start >= 0 && end > start, "expected JSON object in output:\n" + out);
    return out.substring(start, end + 1);
  }

  private static String failureMessage(Exception ex) {
    return ex.toString() + (ex.getCause() == null ? "" : " / cause: " + ex.getCause());
  }

  // ---- table and partition stats --------------------------------------------

  @Test
  void tableLevelOutputReportsTotalsAndPercentiles() throws Exception {
    // given two partitions with five base files between them
    createPartition("p0", 3, 1024 * 1024, 0);
    createPartition("p1", 2, 512 * 1024, 0);
    completeFixtureCommit("p0", "p1");

    // when the analyzer runs with defaults
    runAnalyzer(baseConfig());
    String out = capturedAsString();

    // then the table-level distribution and the per-partition file-count distribution are printed
    assertTrue(out.contains("Table-level file size distribution"), out);
    assertTrue(out.contains("numFiles=5"), out);
    assertTrue(out.contains("totalBytes="), out);
    assertTrue(out.contains("Per-partition file-count distribution"), out);
    assertTrue(out.contains("numPartitions=2"), out);
  }

  @Test
  void partitionStatsListsLargestFirst() throws Exception {
    // given a small and a big partition
    createPartition("small", 1, 1024, 0);
    createPartition("big", 1, 4 * 1024 * 1024, 0);
    completeFixtureCommit("small", "big");

    // when partition stats are requested
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the big partition is listed before the small one
    int bigIdx = out.indexOf("big");
    int smallIdx = out.indexOf("small");
    assertTrue(bigIdx >= 0 && smallIdx >= 0, "both partition names expected: " + out);
    assertTrue(bigIdx < smallIdx, "big partition should appear first (sort by size desc): " + out);
  }

  @Test
  void topNCapsPartitionRows() throws Exception {
    // given five partitions of decreasing size
    for (int i = 0; i < 5; i++) {
      createPartition("p" + i, 1, (5 - i) * 1024L * 1024L, 0);
    }
    completeFixtureCommit("p0", "p1", "p2", "p3", "p4");

    // when only the top two are requested
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.topN = 2;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the largest is shown, the smallest is not, and the cap is announced
    assertTrue(out.contains("(showing top 2 of 5 partitions"), out);
    assertTrue(out.contains("p0"), out);
    assertFalse(out.contains("p4"), out);
  }

  @Test
  void jsonOutputCarriesSkewAndSizeBlocks() throws Exception {
    // given two partitions
    createPartition("p0", 1, 1024 * 1024, 0);
    createPartition("p1", 1, 4 * 1024 * 1024, 0);
    completeFixtureCommit("p0", "p1");

    // when JSON output with partition stats is requested
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.output = "JSON";
    cfg.partitionStats = true;
    runAnalyzer(cfg);
    String json = extractJsonObject(capturedAsString());

    // then every top-level section is present
    assertTrue(json.contains("\"tableSizeStats\""), json);
    assertTrue(json.contains("\"fileCountPerPartition\""), json);
    assertTrue(json.contains("\"skew\""), json);
    assertTrue(json.contains("\"partitions\""), json);
    assertTrue(json.contains("\"basePath\""), json);
    assertTrue(json.contains("\"numPartitions\": 2"), json);
  }

  @Test
  void skewMetricsFireOnConcentratedTable() throws Exception {
    // given one large partition (~4 MB) and four tiny ones
    createPartition("p0", 1, 4 * 1024 * 1024, 0);
    for (int i = 1; i < 5; i++) {
      createPartition("p" + i, 1, 32 * 1024, 0);
    }
    completeFixtureCommit("p0", "p1", "p2", "p3", "p4");

    // when the table-level block (which carries the skew section) is requested alongside partition stats
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.tableStats = true;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the skew section is printed with all of its headline metrics
    assertTrue(out.contains("Partition-size skew"), out);
    assertTrue(out.contains("CV (stdev/mean)"), out);
    assertTrue(out.contains("Gini coefficient"), out);
    assertTrue(out.contains("largest partition share"), out);
  }

  @Test
  void includeRowCountsReadsParquetFooters() throws Exception {
    // given two real parquet files of 50 rows each in one partition
    createPartition("p0", 2, 0, 50);
    completeFixtureCommit("p0");

    // when row counts are requested on a table without metadata column stats
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.includeRowCounts = true;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the footer-read row count of 100 appears in the numRecords column
    assertTrue(out.contains("numRecords"), out);
    assertTrue(out.contains("100"), out);
  }

  @Test
  void invalidOutputFormatRejected() throws Exception {
    // given a valid table
    createPartition("p0", 1, 1024, 0);
    completeFixtureCommit("p0");

    // when an unsupported output format is requested
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.output = "yaml";
    Exception ex = assertThrows(Exception.class, () -> new HoodieTableLayoutAnalyzer(jsc, cfg).run());

    // then the run is rejected with a message naming the flag
    assertTrue(failureMessage(ex).contains("--output"), "expected --output rejection, got: " + failureMessage(ex));
  }

  @Test
  void mdtListingPathDoesNotThrowWhenMdtAbsent() throws Exception {
    // given a table created without a metadata table
    createPartition("p0", 1, 1024, 0);
    completeFixtureCommit("p0");

    // when the analyzer runs
    runAnalyzer(baseConfig());
    String out = capturedAsString();

    // then it falls back to storage listing and reports MDT=off
    assertTrue(out.contains("MDT=off"), "expected MDT=off marker in header: " + out);
    assertTrue(out.contains("Table-level file size distribution"), out);
  }

  // ---- detectors ------------------------------------------------------------

  @Test
  void analyzeTableCharacteristicsEmitsAllThreeDetectors() throws Exception {
    // given two partitions of many tiny files and ten ingest commits
    createPartition("p0", 10, 1024, 0);
    createPartition("p1", 8, 2048, 0);
    completeFixtureCommit("p0", "p1");
    Map<String, Long> bytes = new HashMap<>();
    bytes.put("p0", 10L * 1024);
    bytes.put("p1", 8L * 2048);
    for (int i = 0; i < 9; i++) {
      writeCommit(String.format("2026010112%07d", i), WriteOperationType.UPSERT, bytes);
    }

    // when all detectors run with the count threshold lowered and the age gate disabled
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionCountThreshold = 1;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then each detector reports: micro YES, small files SEVERE (2 of 2 flagged), hot section present
    assertTrue(out.contains("Table characteristics:"), out);
    assertTrue(out.contains("Micro-partitioned:  YES"), out);
    assertTrue(out.contains("Small-file pile-up: SEVERE"), out);
    assertTrue(out.contains("2 of 2 qualifying partitions flagged"), out);
    assertTrue(out.contains("Hot partitions (last"), out);
  }

  @Test
  void smallFileVerdictSkippedWhenTooFewCommits() throws Exception {
    // given partitions full of small files but a single ingest commit
    createPartition("p0", 10, 1024, 0);
    createPartition("p1", 8, 2048, 0);
    completeFixtureCommit("p0", "p1");

    // when the detectors run
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionCountThreshold = 1;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the small-file verdict is SKIPPED and names the commit count
    assertTrue(out.contains("Small-file pile-up: SKIPPED"), out);
    assertTrue(out.contains("only 1 ingest commits"), out);
  }

  @Test
  void smallFileVerdictCleanWhenFlaggedRatioLow() throws Exception {
    // given three qualifying partitions of which only one is below a 1 KB threshold, and ten ingest commits
    createPartition("good1", 5, 4L * 1024 * 1024, 0);
    createPartition("good2", 5, 4L * 1024 * 1024, 0);
    createPartition("bad", 6, 256, 0);
    completeFixtureCommit("good1", "good2", "bad");
    Map<String, Long> bytes = new HashMap<>();
    bytes.put("good1", 1L);
    for (int i = 0; i < 9; i++) {
      writeCommit(String.format("2026010112%07d", i), WriteOperationType.UPSERT, bytes);
    }

    // when the small-file tiers are set at 50% / 80%
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionCountThreshold = 100;
    cfg.smallFilesThresholdBytes = 1024;
    cfg.smallFilesModeratePct = 0.50;
    cfg.smallFilesSeverePct = 0.80;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then one of three flagged (33%) stays under MODERATE, so the verdict is CLEAN
    assertTrue(out.contains("Small-file pile-up: CLEAN"), out);
    assertTrue(out.contains("1 of 3 qualifying partitions flagged"), out);
  }

  @Test
  void hotPartitionDetectionExcludesCompactAndCluster() throws Exception {
    // given two upserts touching "ingest" and one compaction touching "compactOnly"
    createPartition("ingest", 1, 1024, 0);
    createPartition("compactOnly", 1, 1024, 0);
    completeFixtureCommit("ingest", "compactOnly");
    writeCommit("20260101100000001", WriteOperationType.UPSERT, Collections.singletonMap("ingest", 1024L));
    writeCommit("20260101110000000", WriteOperationType.UPSERT, Collections.singletonMap("ingest", 1024L));
    writeCommit("20260101120000000", WriteOperationType.COMPACT, Collections.singletonMap("compactOnly", 1024L));

    // when hot partitions are computed over a 10-commit window
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionMinAgeDays = 0;
    cfg.hotWindowCommits = 10;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then "ingest" is hot and the compaction-only partition is not
    int hotStart = out.indexOf("Hot partitions (last");
    assertTrue(hotStart >= 0, "hot partitions section missing: " + out);
    String hotBlock = out.substring(hotStart);
    assertTrue(hotBlock.contains("ingest"), "ingest should be hot: " + hotBlock);
    assertFalse(hotBlock.contains("compactOnly"), "compactOnly should be excluded from hot window: " + hotBlock);
  }

  @Test
  void microPartitionVerdictUsesEitherRule() throws Exception {
    // given two single-file partitions, so only the count rule can fire
    createPartition("a", 1, 1024, 0);
    createPartition("b", 1, 1024, 0);
    completeFixtureCommit("a", "b");

    // when the count threshold is 1 and the age gate disables the size rule
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionCountThreshold = 1;
    cfg.microPartitionMinAgeDays = 99999;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the verdict is YES via the count rule and the size rule is reported as skipped
    assertTrue(out.contains("Micro-partitioned:  YES"), out);
    assertTrue(out.contains("count rule:  numPartitions=2 > threshold=1"), out);
    assertTrue(out.contains("size rule:   skipped"), out);
  }

  @Test
  void hotPartitionRecordCountUsesPerOpFields() throws Exception {
    // given one upsert with numInserts=10, numUpdateWrites=20, numDeletes=5 and numWrites=1000
    createPartition("hot", 1, 1024, 0);
    completeFixtureCommit("hot");
    Map<String, long[]> stats = new HashMap<>();
    stats.put("hot", new long[]{1024L, 1000L, 10L, 20L, 5L});
    writeCommitDetailed("20260101120000000", WriteOperationType.UPSERT, stats);

    // when the hot-partition detector renders JSON
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.output = "JSON";
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 99999;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then recordsWritten is the sum of the per-op counters (35), not the file row count
    assertTrue(out.contains("\"partition\": \"hot\""), out);
    assertTrue(out.contains("\"recordsWritten\": 35"),
        "expected recordsWritten=35 (numInserts + numUpdateWrites + numDeletes), output:\n" + out);
  }

  @Test
  void microSizeRuleFiresAtExactMinFilesGate() throws Exception {
    // given a partition with exactly microPartitionMinFiles small files
    createPartition("edge", 5, 1024, 0);
    completeFixtureCommit("edge");

    // when the size rule runs with the gate at 5
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionCountThreshold = 1000;
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionMinFiles = 5;
    cfg.microPartitionMaxAvgBytes = 50L * 1024 * 1024;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the gate is inclusive and the partition matches
    assertTrue(out.contains("Micro-partitioned:  YES"), out);
    assertTrue(out.contains("size rule:   1 partition(s) match"),
        "expected size rule to fire at exactly fileCount=microPartitionMinFiles, output:\n" + out);
  }

  @Test
  void hotPartitionDetectionCompletesCleanlyWhenWindowHasOnlyCompactCommits() throws Exception {
    // given the fixture commit plus a compaction commit, which is excluded from the window
    createPartition("p0", 1, 1024, 0);
    completeFixtureCommit("p0");
    writeCommit("20260101120000000", WriteOperationType.COMPACT, Collections.singletonMap("p0", 1L));

    // when the detectors run
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 99999;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the hot-partition section is emitted and no failure is logged
    assertTrue(out.contains("Hot partitions (last"), out);
    assertFalse(out.contains("Hot-partition detection failed"), out);
  }

  @Test
  void countTotalIngestCommitsExcludesClusteringReplacecommits() throws Exception {
    // given the fixture commit, two upserts and three clustering replacecommits
    createPartition("p0", 10, 1024, 0);
    completeFixtureCommit("p0");
    writeCommit("20260101110000000", WriteOperationType.UPSERT, Collections.singletonMap("p0", 1L));
    writeCommit("20260101110000001", WriteOperationType.UPSERT, Collections.singletonMap("p0", 1L));
    writeReplaceCommit("20260101120000000", WriteOperationType.CLUSTER, Collections.singletonMap("p0", 1L));
    writeReplaceCommit("20260101120000001", WriteOperationType.CLUSTER, Collections.singletonMap("p0", 1L));
    writeReplaceCommit("20260101120000002", WriteOperationType.CLUSTER, Collections.singletonMap("p0", 1L));

    // when the small-file gate is exactly the number of real ingests (3)
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 99999;
    cfg.smallFilesMinTableCommits = 3;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the verdict is emitted (3 >= 3) rather than SKIPPED, proving clustering did not inflate the count
    assertTrue(out.contains("Small-file pile-up:"), "expected small-file section to be present, output:\n" + out);
    assertFalse(out.contains("Small-file pile-up: SKIPPED"),
        "with 3 real ingest commits and gate=3, SKIPPED should not fire, output:\n" + out);
  }

  @Test
  void countTotalIngestCommitsIncludesInsertOverwriteReplacecommits() throws Exception {
    // given the fixture commit and two insert-overwrite replacecommits (3 real ingests)
    createPartition("p0", 10, 1024, 0);
    completeFixtureCommit("p0");
    writeReplaceCommit("20260101110000000", WriteOperationType.INSERT_OVERWRITE, Collections.singletonMap("p0", 1L));
    writeReplaceCommit("20260101110000001", WriteOperationType.INSERT_OVERWRITE_TABLE, Collections.singletonMap("p0", 1L));

    // when the gate is 4
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 99999;
    cfg.smallFilesMinTableCommits = 4;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the verdict is SKIPPED and reports exactly 3 ingest commits
    assertTrue(out.contains("Small-file pile-up: SKIPPED"), "expected SKIPPED with 3 ingest commits < gate=4, output:\n" + out);
    assertTrue(out.contains("only 3 ingest commits"), "expected 3 ingest commits (fixture + 2 INSERT_OVERWRITE), output:\n" + out);
  }

  @Test
  void printTopKListsSmallestPartitionsUnderEachDetector() throws Exception {
    // given three partitions with avg file sizes of 100 B, 1 KB and 100 KB
    createPartition("small", 10, 100, 0);
    createPartition("medium", 10, 1024, 0);
    createPartition("large", 10, 100 * 1024, 0);
    completeFixtureCommit("small", "medium", "large");

    // when the top-2 smallest are requested
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.microPartitionCountThreshold = 10000;
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionMinFiles = 5;
    cfg.microPartitionMaxAvgBytes = 50L * 1024 * 1024;
    cfg.smallFilesMinFilesPerPartition = 5;
    cfg.printTopK = 2;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the two smallest are listed and the largest is not
    assertTrue(out.contains("top-2 smallest"), "top-K subsection missing:\n" + out);
    assertTrue(out.contains("small  files="), "'small' partition should be in top-K:\n" + out);
    assertTrue(out.contains("medium  files="), "'medium' partition should be in top-K:\n" + out);
    assertFalse(out.contains("large  files="), "'large' should NOT be in top-2 smallest:\n" + out);
  }

  @Test
  void detectorJsonCarriesStatusSummaryFindingsAndThresholds() throws Exception {
    // given a table whose partitions trip the size rule and the small-file SEVERE tier
    createPartition("p0", 10, 1024, 0);
    createPartition("p1", 8, 2048, 0);
    completeFixtureCommit("p0", "p1");
    Map<String, Long> bytes = new HashMap<>();
    bytes.put("p0", 10L * 1024);
    for (int i = 0; i < 9; i++) {
      writeCommit(String.format("2026010112%07d", i), WriteOperationType.UPSERT, bytes);
    }

    // when the detectors render JSON
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.output = "JSON";
    cfg.microPartitionMinAgeDays = 0;
    cfg.microPartitionMinFiles = 5;
    runAnalyzer(cfg);
    String json = extractJsonObject(capturedAsString());

    // then each detector carries a status, summary, findings and effective/observed keys
    assertTrue(json.contains("\"overallStatus\": \"FLAGGED\""), json);
    assertTrue(json.contains("\"name\": \"micro-partition\", \"status\": \"FLAGGED\""), json);
    assertTrue(json.contains("\"name\": \"small-files\", \"status\": \"SEVERE\""), json);
    assertTrue(json.contains("\"name\": \"hot-partitions\", \"status\": \"FLAGGED\""), json);
    assertTrue(json.contains("\"effective.micro.partition.min.files\": 5"), json);
    assertTrue(json.contains("\"observed.size.rule.matching.partitions\": 2"), json);
    assertTrue(json.contains("\"effective.small.files.threshold.bytes\": 52428800"), json);
    assertTrue(json.contains("\"observed.flagged.partitions\": 2"), json);
    assertTrue(json.contains("\"observed.hot.window.effective.commits\": 10"), json);
    assertTrue(json.contains("HoodieClusteringJob"), "findings should name the remedy:\n" + json);
    assertTrue(json.contains("\"summary\": \""), json);
  }

  // ---- end-to-end with --props-path and row counts ---------------------------

  /**
   * Builds a multi-partition table with real parquet files, drives the analyzer through the
   * {@code --props-path} batch entry point (listing the same table twice), and validates JSON
   * output for per-partition row counts and the detector block on the same run.
   */
  @Test
  void endToEndFunctionalRunWithPropsPathAndRowCounts() throws Exception {
    // given two real parquet partitions with 30 and 20 rows, ten ingest commits, and a props file listing the table twice
    createPartition("partA", 2, 0, 15);
    createPartition("partB", 2, 0, 10);
    completeFixtureCommit("partA", "partB");
    Map<String, Long> bytes = new HashMap<>();
    bytes.put("partA", 1024L);
    bytes.put("partB", 1024L);
    for (int i = 0; i < 9; i++) {
      writeCommit(String.format("2026010112%07d", i), WriteOperationType.UPSERT, bytes);
    }
    java.nio.file.Path propsFile = tempDir.resolve("tables.props");
    Files.write(propsFile, (basePath + "\n" + basePath + "\n").getBytes(StandardCharsets.UTF_8));

    // when the analyzer runs in batch mode with row counts and detectors, as JSON
    HoodieTableLayoutAnalyzer.Config cfg = detectorConfig();
    cfg.basePath = null;
    cfg.propsFilePath = propsFile.toString();
    cfg.tableStats = true;
    cfg.includeRowCounts = true;
    cfg.microPartitionCountThreshold = 100;
    cfg.microPartitionMinAgeDays = 0;
    cfg.output = "JSON";
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then two JSON objects are emitted, each with footer-derived row counts and the detector block
    int firstStart = out.indexOf("{");
    int firstEnd = out.indexOf("}\n{", firstStart);
    assertTrue(firstStart >= 0 && firstEnd > firstStart, "expected two JSON objects from props-path batch run:\n" + out);
    int secondStart = out.indexOf("{", firstEnd);
    int lastEnd = out.lastIndexOf("}");
    assertTrue(secondStart > firstEnd && lastEnd > secondStart, "expected a second JSON object from the duplicated props entry:\n" + out);
    String firstJson = out.substring(firstStart, firstEnd + 1);
    String secondJson = out.substring(secondStart, lastEnd + 1);

    assertTrue(firstJson.contains("\"partition\": \"partA\""), firstJson);
    assertTrue(firstJson.contains("\"partition\": \"partB\""), firstJson);
    assertTrue(firstJson.contains("\"numRecords\": 30"), "partA should report 30 rows from parquet footers:\n" + firstJson);
    assertTrue(firstJson.contains("\"numRecords\": 20"), "partB should report 20 rows from parquet footers:\n" + firstJson);
    assertTrue(firstJson.contains("\"totalRecords\": 50"), firstJson);
    assertTrue(firstJson.contains("\"skew\""), firstJson);
    assertTrue(firstJson.contains("\"tableCharacteristics\""), "detector block missing from JSON:\n" + firstJson);

    assertTrue(secondJson.contains("\"partition\": \"partA\""), secondJson);
    assertTrue(secondJson.contains("\"partition\": \"partB\""), secondJson);
    assertTrue(secondJson.contains("\"tableCharacteristics\""), secondJson);
  }

  // ---- date filtering -------------------------------------------------------

  @Test
  void startAndEndDateFilterIncludesOnlyInRangePartitions() throws Exception {
    // given three date-named partitions
    createPartition("2026-1-1", 1, 1024, 0);
    createPartition("2026-2-1", 1, 2048, 0);
    createPartition("2026-3-1", 1, 4096, 0);
    completeFixtureCommit("2026-1-1", "2026-2-1", "2026-3-1");

    // when a [start, end) window covers only the middle one
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.startDate = "2026-2-1";
    cfg.endDate = "2026-3-1";
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then only the middle partition is reported
    assertTrue(out.contains("2026-2-1"), "2026-2-1 (start inclusive) should be present:\n" + out);
    assertFalse(out.contains("2026-1-1"), "2026-1-1 (before start) should be filtered out:\n" + out);
    assertFalse(out.contains("2026-3-1"), "2026-3-1 (end exclusive) should be filtered out:\n" + out);
  }

  @Test
  void startDateOnlyIncludesAllPartitionsOnOrAfterStart() throws Exception {
    // given three date-named partitions
    createPartition("2026-1-1", 1, 1024, 0);
    createPartition("2026-2-1", 1, 1024, 0);
    createPartition("2026-3-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1", "2026-2-1", "2026-3-1");

    // when only a start date is given
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.startDate = "2026-2-1";
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then partitions on or after the start are reported
    assertFalse(out.contains("2026-1-1"), "before start should be excluded:\n" + out);
    assertTrue(out.contains("2026-2-1"), "start (inclusive) should be present:\n" + out);
    assertTrue(out.contains("2026-3-1"), "after start should be present:\n" + out);
  }

  @Test
  void endDateOnlyIncludesAllPartitionsBeforeEnd() throws Exception {
    // given three date-named partitions
    createPartition("2026-1-1", 1, 1024, 0);
    createPartition("2026-2-1", 1, 1024, 0);
    createPartition("2026-3-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1", "2026-2-1", "2026-3-1");

    // when only an end date is given
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.endDate = "2026-3-1";
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then partitions strictly before the end are reported
    assertTrue(out.contains("2026-1-1"), "before end should be present:\n" + out);
    assertTrue(out.contains("2026-2-1"), "before end should be present:\n" + out);
    assertFalse(out.contains("2026-3-1"), "end (exclusive) should be excluded:\n" + out);
  }

  @Test
  void invalidStartDateFormatThrows() throws Exception {
    // given a date-partitioned table
    createPartition("2026-1-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1");

    // when the start date is not parseable
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.startDate = "not-a-date";
    Exception ex = assertThrows(Exception.class, () -> new HoodieTableLayoutAnalyzer(jsc, cfg).run());

    // then the parse failure is reported
    String msg = failureMessage(ex);
    assertTrue(msg.contains("not-a-date") || msg.contains("DateTimeParse") || msg.contains("Unable to parse"),
        "expected a date-parse failure, got: " + msg);
  }

  @Test
  void startDateAfterEndDateThrows() throws Exception {
    // given a date-partitioned table
    createPartition("2026-1-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1");

    // when the start date is after the end date
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.startDate = "2026-3-1";
    cfg.endDate = "2026-2-1";
    Exception ex = assertThrows(Exception.class, () -> new HoodieTableLayoutAnalyzer(jsc, cfg).run());

    // then the interval is rejected
    assertTrue(failureMessage(ex).contains("Starting date must be before ending date"),
        "expected start>=end validation error, got: " + failureMessage(ex));
  }

  @Test
  void dateFilterRejectsNonDatePartitions() throws Exception {
    // given a table whose partition names carry no date
    createPartition("p0", 1, 1024, 0);
    completeFixtureCommit("p0");

    // when a date filter is applied
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.startDate = "2026-1-1";
    cfg.endDate = "2026-2-1";
    Exception ex = assertThrows(Exception.class, () -> new HoodieTableLayoutAnalyzer(jsc, cfg).run());

    // then the run is rejected up front
    String msg = failureMessage(ex);
    assertTrue(msg.contains("Cannot apply --start-date") || msg.contains("partition does not contain date"),
        "expected non-date-partition rejection, got: " + msg);
  }

  @Test
  void numDaysOnlyComputesWindowFromToday() throws Exception {
    // given a partition dated far in the past
    createPartition("1999-1-1", 1, 1024, 0);
    completeFixtureCommit("1999-1-1");

    // when a 7-day window is requested
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.numDays = 7;
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the stale partition is filtered out
    assertFalse(out.contains("1999-1-1"), "stale partition should be filtered out by numDays=7 window:\n" + out);
  }

  @Test
  void numDaysNegativeThrows() throws Exception {
    // given a date-partitioned table
    createPartition("2026-1-1", 1, 1024, 0);
    completeFixtureCommit("2026-1-1");

    // when a negative window is requested
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.numDays = -1;
    Exception ex = assertThrows(Exception.class, () -> new HoodieTableLayoutAnalyzer(jsc, cfg).run());

    // then the value is rejected
    assertTrue(failureMessage(ex).contains("--num-days must specify a positive value"),
        "expected negative-num-days validation, got: " + failureMessage(ex));
  }

  // ---- JSON hardening -------------------------------------------------------

  @Test
  void jsonEscapesControlCharsInPartitionPath() throws Exception {
    // given a partition whose name embeds tabs (skipped if the filesystem rejects the path)
    String tricky = "part\twith\ttabs";
    try {
      createPartition(tricky, 1, 1024, 0);
      completeFixtureCommit(tricky);
    } catch (Exception e) {
      Assumptions.assumeTrue(false, "filesystem rejected control-char partition path; skipping: " + e.getMessage());
    }

    // when JSON output is requested
    HoodieTableLayoutAnalyzer.Config cfg = baseConfig();
    cfg.partitionStats = true;
    cfg.output = "JSON";
    runAnalyzer(cfg);
    String out = capturedAsString();

    // then the tabs are escaped rather than emitted raw inside the string literal
    assertTrue(out.contains("part\\twith\\ttabs"), "expected JSON to escape embedded tabs as \\t, output was:\n" + out);
  }

  // ---- Config.equals / hashCode / toString ----------------------------------

  /**
   * Mutates each field that participates in {@code Config.equals} one at a time and asserts
   * inequality, so every branch of the chained comparison is exercised.
   */
  @Test
  void configEqualsCoversAllFields() {
    HoodieTableLayoutAnalyzer.Config a = new HoodieTableLayoutAnalyzer.Config();
    a.basePath = "/tmp/t";
    HoodieTableLayoutAnalyzer.Config b = new HoodieTableLayoutAnalyzer.Config();
    b.basePath = "/tmp/t";

    assertEquals(a, a, "reflexive");
    assertEquals(a, b, "equal-by-value");
    assertEquals(a.hashCode(), b.hashCode(), "hashCode contract for equal objects");
    assertNotNull(a.toString());
    assertTrue(a.toString().startsWith("HoodieTableLayoutAnalyzer {"), a.toString());

    assertNotEquals(a, null);
    assertNotEquals(a, "not a Config");

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

    b.printTopK = a.printTopK + 1;
    assertNotEquals(a, b, "printTopK differs");
    b.printTopK = a.printTopK;

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

    assertEquals(a, b, "equality restored after each mutation reverted");
  }
}
