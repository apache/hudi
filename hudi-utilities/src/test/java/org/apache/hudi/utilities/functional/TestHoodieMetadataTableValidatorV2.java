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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;
import org.apache.hudi.utilities.HoodieMetadataTableValidator;
import org.apache.hudi.utilities.HoodieMetadataTableValidatorV2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for {@link org.apache.hudi.utilities.HoodieMetadataTableValidatorV2}, run via
 * the functional suite (this package is selected by {@code UtilitiesFunctionalTestSuite}).
 */
@Tag("functional")
public class TestHoodieMetadataTableValidatorV2 extends HoodieSparkClientTestBase {

  private FileSystem fs() {
    return HadoopFSUtils.getFs(basePath, jsc.hadoopConfiguration());
  }

  protected Dataset<Row> makeInsertDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateInserts(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }

  protected Dataset<Row> makeUpdateDf(String instantTime, Integer n) {
    try {
      List<String> records = dataGen.generateUpdates(instantTime, n).stream()
          .map(r -> recordToString(r).get()).collect(Collectors.toList());
      JavaRDD<String> rdd = jsc.parallelize(records);
      return sparkSession.read().json(rdd);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * V2 validator end to end via the {@code --use-validator-v2} dispatch: the driver hosts the
   * metadata view behind the embedded timeline server and tasks fetch expected file slices
   * remotely. Covers COW and MOR (log files via upserts). After deleting a base file from
   * storage, the FS listing diverges from the metadata table and validation must fail.
   *
   * <p><b>This test also pins that v2 ignores {@code --validate-latest-file-slices}</b>, which is
   * why it is the one place in these suites that sets that flag {@code false}. The flag is v1's, it
   * defaults to {@code false}, and v2's own off switch is {@code --validate-fs-mdt-diff}. Wiring v2
   * to the v1 flag instead — which an eventual "these two names mean the same thing" cleanup would
   * do — silently turns the FS-MDT comparison off for every operator who never passed it, and every
   * other functional suite would stay green because
   * {@code ValidatorV2FunctionalTestBase#runValidator} sets it {@code true}. The deleted base file
   * below is found by the FS-MDT dimension alone (the duplicate detector reports only slices with
   * two or more base files, and there are none), so the failure asserted here cannot be reached
   * with that dimension switched off.
   */
  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  void testValidatorV2LatestFileSlices(String tableType) throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType);
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    Dataset<Row> inserts = makeInsertDf("000", 5).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);
    Dataset<Row> updates = makeUpdateDf("001", 5).cache();
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .mode(SaveMode.Append)
        .save(basePath);

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    // Deliberately false, and the assertions below depend on it: v2 must reach the FS-MDT
    // comparison anyway, because this is v1's flag and v2 reads --validate-fs-mdt-diff instead.
    // See this test's javadoc.
    config.validateLatestFileSlices = false;
    config.useValidatorV2 = true;
    // exercise the inflight-cleaning tolerance path (no-op here: no inflight cleans)
    config.skipDataFilesForCleaning = true;
    // tests share one large surefire fork; keep the view budget small so it spills instead
    config.fsViewMemoryFraction = 0.05;

    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());

    // Delete every base file of one partition directly from storage (older file versions may
    // exist after upserts, so deleting a single arbitrary file might not touch the latest
    // slice): the file-system listing now diverges from the metadata table, so v2 validation
    // must fail (collected, since ignoreFailed is set).
    Path partitionToCorrupt = null;
    int deleted = 0;
    RemoteIterator<LocatedFileStatus> files = fs().listFiles(new Path(basePath), true);
    while (files.hasNext()) {
      Path path = files.next().getPath();
      if (path.getName().endsWith(".parquet") && !FSUtils.isLogFile(path.getName())
          && !path.toString().contains(".hoodie")) {
        if (partitionToCorrupt == null) {
          partitionToCorrupt = path.getParent();
        }
        if (path.getParent().equals(partitionToCorrupt)) {
          assertTrue(fs().delete(path, false));
          deleted++;
        }
      }
    }
    assertTrue(deleted > 0, "Expected to delete at least one base file");

    // Without ignoreFailed the task-side validation exception must abort the run: Spark wraps
    // it, and the v2 driver unwraps and rethrows the original validation failure.
    HoodieMetadataTableValidator failFastValidator = new HoodieMetadataTableValidator(jsc, config);
    assertThrows(HoodieValidationException.class, failFastValidator::run);

    // With ignoreFailed the failure is collected — assert on the v2 validator directly.
    config.ignoreFailed = true;
    HoodieMetadataTableValidatorV2 v2 = new HoodieMetadataTableValidatorV2(jsc, config, new TypedProperties());
    assertFalse(v2.run());
    assertTrue(v2.hasValidationFailure());
    assertFalse(v2.getThrowables().isEmpty());
  }

  /**
   * The v2 validator is single-shot only: {@code --continuous} must be rejected outright.
   */
  /**
   * With {@code --ignore-failed false} a mismatch is raised from inside a Spark task, and Spark
   * wraps task failures in {@code SparkException}. The v2 validator has to dig the original
   * {@link HoodieValidationException} back out, because that is the type the calling job keys on
   * to distinguish a genuine metadata divergence (alert-worthy) from an infrastructure error.
   * Without the unwrapping this surfaces as a generic exception and a real corruption looks like
   * a flaky job. The v1 validator pins the same behaviour in its own suite.
   */
  @Test
  void testValidatorV2SurfacesValidationFailureThroughSparkWrapping() throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    makeInsertDf("000", 5).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);

    // An extra base file on storage that the metadata table has never heard of.
    boolean corrupted = false;
    RemoteIterator<LocatedFileStatus> files = fs().listFiles(new Path(basePath), true);
    while (files.hasNext() && !corrupted) {
      Path path = files.next().getPath();
      if (path.getName().endsWith(".parquet") && !FSUtils.isLogFile(path.getName())
          && !path.toString().contains(".hoodie")) {
        String extraFileName = java.util.UUID.randomUUID()
            + path.getName().substring(path.getName().indexOf('_'));
        FileSystem fileSystem = fs();
        org.apache.hadoop.fs.FileUtil.copy(fileSystem, path, fileSystem,
            new Path(path.getParent(), extraFileName), false, fileSystem.getConf());
        corrupted = true;
      }
    }
    assertTrue(corrupted, "Expected to inject an extra base file");

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.ignoreFailed = false;
    config.fsViewMemoryFraction = 0.05;

    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    // Not SparkException, not a bare HoodieException: the validation type must survive the trip
    // back from the executor.
    assertThrows(HoodieValidationException.class, validator::run);
  }

  @Test
  void testValidatorV2RejectsContinuousMode() {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath + "/nonexistent_table";
    config.useValidatorV2 = true;
    config.continuous = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertThrows(UnsupportedOperationException.class, validator::run);
  }

  /**
   * A missing data table is a skip, not a failure — mirroring the v1 contract.
   */
  @Test
  void testValidatorV2SkipsWhenTableMissing() {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath + "/nonexistent_table";
    config.useValidatorV2 = true;
    config.validateLatestFileSlices = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  /**
   * The meta-client-injecting constructor must skip the redundant timeline reload (one-shot)
   * and still validate successfully through the v1 path, including the inflight-cleaning filter.
   */
  @Test
  void testValidatorInjectedMetaClientSkipsTimelineReload() {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    makeInsertDf("000", 3).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.skipDataFilesForCleaning = true;

    HoodieTableMetaClient injectedMetaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
        .setBasePath(config.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
    HoodieMetadataTableValidator validator =
        new HoodieMetadataTableValidator(jsc, config, injectedMetaClient);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  /**
   * A table without a metadata table (metadata disabled at write time) is a skip, not a failure.
   */
  @Test
  void testValidatorV2SkipsWhenMetadataTableMissing() {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    writeOptions.put("hoodie.metadata.enable", "false");

    makeInsertDf("000", 3).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.useValidatorV2 = true;
    config.validateLatestFileSlices = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  /**
   * End to end v2 validation at fan-out scale: 100 partitions of tiny single-record base files
   * (partition path = record key), and a metadata table shaped as exactly one compacted base
   * HFile (from the bootstrap) plus nine log files (one per follow-up commit, compaction
   * deferred). V2 must validate all partitions cleanly; after injecting extra base files into
   * three partitions directly on storage — files the metadata table knows nothing about —
   * validation must fail for exactly those three partitions.
   */
  @Test
  void testValidatorV2EndToEndManyPartitionsAndExtraFiles() throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    // defer MDT compaction so the bootstrap base file stays the only HFile, and disable data
    // table cleaning/archival so each data commit maps to exactly one metadata log file
    writeOptions.put("hoodie.metadata.compact.max.delta.commits", "20");
    writeOptions.put("hoodie.clean.automatic", "false");
    writeOptions.put("hoodie.keep.min.commits", "30");
    writeOptions.put("hoodie.keep.max.commits", "31");

    // 100 partitions with one ~KB-scale record each: partition path = record key.
    Dataset<Row> inserts = makeInsertDf("000", 100);
    inserts.withColumn("partition_path", functions.col("_row_key"))
        .write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);
    // 7 more commits: with the bootstrap commit itself logging one delta plus the metadata
    // table bookkeeping delta, the files partition ends up with exactly nine log files
    for (int i = 1; i <= 7; i++) {
      makeInsertDf(String.format("%03d", i), 1)
          .withColumn("partition_path", functions.lit("extra_commit_partition"))
          .write().format("hudi").options(writeOptions)
          .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.INSERT.value())
          .mode(SaveMode.Append)
          .save(basePath);
    }

    // Metadata table files partition layout: exactly one base HFile, nine log files.
    int hfileCount = 0;
    int logFileCount = 0;
    RemoteIterator<LocatedFileStatus> mdtFiles =
        fs().listFiles(new Path(basePath + "/.hoodie/metadata/files"), false);
    while (mdtFiles.hasNext()) {
      String name = mdtFiles.next().getPath().getName();
      // Classify log files first: a native-format log file also ends in ".hfile".
      if (FSUtils.isLogFile(name)) {
        logFileCount++;
      } else if (name.endsWith(".hfile")) {
        hfileCount++;
      }
    }
    assertEquals(1, hfileCount, "Expected exactly one metadata table base HFile");
    assertEquals(9, logFileCount, "Expected nine metadata table log files");

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    // tests share one large surefire fork; keep the view budget small so it spills instead
    config.fsViewMemoryFraction = 0.05;

    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());

    // Inject an extra base file into three partitions directly on storage: same committed
    // instant time, fresh file group id — visible to the file system listing but absent from
    // the metadata table. Validation must catch every one of them.
    int corrupted = 0;
    RemoteIterator<LocatedFileStatus> dataFiles = fs().listFiles(new Path(basePath), true);
    java.util.Set<String> corruptedPartitions = new java.util.HashSet<>();
    while (dataFiles.hasNext() && corrupted < 3) {
      Path path = dataFiles.next().getPath();
      String partitionDir = path.getParent().getName();
      if (path.getName().endsWith(".parquet") && !FSUtils.isLogFile(path.getName())
          && !path.toString().contains(".hoodie")
          && !corruptedPartitions.contains(partitionDir)) {
        String extraFileName = java.util.UUID.randomUUID()
            + path.getName().substring(path.getName().indexOf('_'));
        FileSystem fileSystem = fs();
        org.apache.hadoop.fs.FileUtil.copy(fileSystem, path, fileSystem,
            new Path(path.getParent(), extraFileName), false, fileSystem.getConf());
        corruptedPartitions.add(partitionDir);
        corrupted++;
      }
    }
    assertEquals(3, corrupted, "Expected to corrupt three partitions");

    config.ignoreFailed = true;
    validator = new HoodieMetadataTableValidator(jsc, config);
    assertFalse(validator.run());
    assertTrue(validator.hasValidationFailure());
    assertEquals(1, validator.getThrowables().size(),
        "the dimension raises one failure however many partitions are damaged");
    String message = validator.getThrowables().get(0).getMessage();
    assertTrue(message.contains("3 partition(s)"),
        "the failure has to say how wide the damage is: " + message);
    for (String partition : corruptedPartitions) {
      assertTrue(message.contains(partition),
          "every damaged partition is named, since that is where an operator goes: " + message);
    }
  }

  /**
   * A pending compaction puts log files on a file slice whose base instant is the compaction
   * instant. That slice is visible only through a timeline that includes compaction instants, so
   * the file-system side and the metadata side must both be built from
   * {@code TimelineUtils.getVisibleTimelineForFsView}. Built from completed commits alone the two
   * sides report different base instant times for the same file group and every partition of a
   * healthy table fails validation.
   */
  @Test
  void testValidatorV2AgreesWithV1WhenACompactionIsPending() throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    writeOptions.put("hoodie.compact.inline", "false");
    writeOptions.put("hoodie.datasource.compaction.async.enable", "false");

    Map<String, String> scheduling = new HashMap<>(writeOptions);
    scheduling.put("hoodie.compact.schedule.inline", "true");
    scheduling.put("hoodie.compact.inline.max.delta.commits", "2");

    makeInsertDf("000", 20).write().format("hudi").options(scheduling)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.INSERT.value())
        .mode(SaveMode.Overwrite).save(basePath);
    for (String instant : new String[] {"001", "002"}) {
      makeUpdateDf(instant, 20).write().format("hudi").options(scheduling)
          .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
          .mode(SaveMode.Append).save(basePath);
    }
    // Delta commits after the compaction was scheduled: their log files land on a new file slice
    // whose base instant time IS the pending compaction instant.
    for (String instant : new String[] {"003", "004"}) {
      makeUpdateDf(instant, 20).write().format("hudi").options(writeOptions)
          .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
          .mode(SaveMode.Append).save(basePath);
    }

    HoodieTableMetaClient localMetaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
        .setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
    assertTrue(localMetaClient.getActiveTimeline().getInstants().stream()
            .anyMatch(instant -> "compaction".equals(instant.getAction()) && !instant.isCompleted()),
        "fixture did not leave a pending compaction on the timeline");

    HoodieMetadataTableValidator.Config v1Config = new HoodieMetadataTableValidator.Config();
    v1Config.basePath = "file:" + basePath;
    v1Config.validateLatestFileSlices = true;
    v1Config.ignoreFailed = true;
    HoodieMetadataTableValidator v1 = new HoodieMetadataTableValidator(jsc, v1Config);

    HoodieMetadataTableValidator.Config v2Config = new HoodieMetadataTableValidator.Config();
    v2Config.basePath = "file:" + basePath;
    v2Config.validateLatestFileSlices = true;
    v2Config.ignoreFailed = true;
    v2Config.useValidatorV2 = true;
    v2Config.fsViewMemoryFraction = 0.05;
    HoodieMetadataTableValidator v2 = new HoodieMetadataTableValidator(jsc, v2Config);

    assertTrue(v1.run(), "v1 must pass on this healthy table");
    assertTrue(v2.run(), "v2 must not report a pending compaction as divergence: " + v2.getThrowables());
    assertTrue(v2.getThrowables().isEmpty());
  }

  /**
   * The metadata table not listing a partition that exists on storage is the most severe form of
   * divergence there is; validating only the partitions the metadata table reports would return
   * success on exactly that case.
   */
  @Test
  void testValidatorV2DetectsPartitionMissingFromMetadata() throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    makeInsertDf("000", 20).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.INSERT.value())
        .mode(SaveMode.Overwrite).save(basePath);
    makeUpdateDf("001", 20).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .mode(SaveMode.Append).save(basePath);

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.ignoreFailed = true;
    config.useValidatorV2 = true;
    config.fsViewMemoryFraction = 0.05;
    assertTrue(new HoodieMetadataTableValidator(jsc, config).run(),
        "sanity check: the table is healthy before the extra partition is planted");

    // A partition directory that exists on storage, holding files from a completed instant, that
    // the metadata table has never heard of.
    FileSystem fileSystem = fs();
    Path source = new Path(basePath + "/2016/03/15");
    Path unknownToMetadata = new Path(basePath + "/2999/12/31");
    org.apache.hadoop.fs.FileUtil.copy(fileSystem, source, fileSystem, unknownToMetadata,
        false, true, fileSystem.getConf());
    assertTrue(fileSystem.exists(unknownToMetadata));

    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertFalse(validator.run(),
        "v2 must detect a partition present on storage but absent from the metadata table");
    assertTrue(validator.hasValidationFailure());
  }

  /**
   * v2 performs latest-file-slice validation only. Asking it for more must fail the run, not skip
   * the request and report success — a monitoring job that quietly validates less than it was told
   * to is worse than one that refuses to start.
   */
  @Test
  void testValidatorV2RejectsValidationsItDoesNotImplement() throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    makeInsertDf("000", 5).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite).save(basePath);

    for (String requested : new String[] {"allFileGroups", "secondaryIndex", "partitionStats",
        "latestBaseFiles", "lastNFileSlices", "allColumnStats", "bloomFilters"}) {
      HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
      config.basePath = "file:" + basePath;
      config.validateLatestFileSlices = true;
      config.useValidatorV2 = true;
      config.fsViewMemoryFraction = 0.05;
      switch (requested) {
        case "allFileGroups":
          config.validateAllFileGroups = true;
          break;
        case "secondaryIndex":
          config.validateSecondaryIndex = true;
          break;
        case "partitionStats":
          config.validatePartitionStats = true;
          break;
        case "latestBaseFiles":
          config.validateLatestBaseFiles = true;
          break;
        case "lastNFileSlices":
          config.validateLastNFileSlices = 5;
          break;
        case "allColumnStats":
          config.validateAllColumnStats = true;
          break;
        case "bloomFilters":
          config.validateBloomFilters = true;
          break;
        default:
          throw new UnsupportedOperationException(requested);
      }
      HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
      UnsupportedOperationException thrown =
          assertThrows(UnsupportedOperationException.class, validator::run,
              "requesting " + requested + " under v2 must fail the run, not be skipped silently");
      assertTrue(thrown.getMessage().contains("latest file slices"), thrown.getMessage());
    }
  }

  /** Only the two disk-backed view types keep the driver-hosted view bounded. */
  @Test
  void testValidatorV2RejectsUnboundedViewStorageTypes() throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    makeInsertDf("000", 5).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite).save(basePath);

    for (String storageType : new String[] {"MEMORY", "REMOTE_ONLY", "REMOTE_FIRST", "NOT_A_TYPE"}) {
      HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
      config.basePath = "file:" + basePath;
      config.validateLatestFileSlices = true;
      config.useValidatorV2 = true;
      config.fsViewStorageType = storageType;
      HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
      assertThrows(IllegalArgumentException.class, validator::run,
          "--fs-view-storage-type " + storageType + " must be rejected");
    }

    for (String storageType : new String[] {"SPILLABLE_DISK", "EMBEDDED_KV_STORE"}) {
      HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
      config.basePath = "file:" + basePath;
      config.validateLatestFileSlices = true;
      config.useValidatorV2 = true;
      config.fsViewStorageType = storageType;
      config.fsViewMemoryFraction = 0.05;
      assertTrue(new HoodieMetadataTableValidator(jsc, config).run(),
          "--fs-view-storage-type " + storageType + " must be accepted");
    }
  }

  /**
   * The executors' remote view carries the timeline snapshot taken when the validator was
   * constructed ({@code metaClient.activeTimeline} is not transient). If the driver-hosted view is
   * pinned to a timeline read later, the two hashes differ and the timeline server answers every
   * request with HTTP 400 — fatal, because the client is REMOTE_ONLY and has no fallback view.
   */
  @Test
  void testValidatorV2SurvivesACommitLandingMidRun() throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    makeInsertDf("000", 20).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.INSERT.value())
        .mode(SaveMode.Overwrite).save(basePath);
    makeUpdateDf("001", 20).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .mode(SaveMode.Append).save(basePath);

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.ignoreFailed = true;
    config.fsViewMemoryFraction = 0.05;

    // Constructing the validator is what loads the meta client and its active timeline.
    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new TypedProperties());

    // A writer commits before the fan-out reaches the driver-hosted view.
    makeUpdateDf("002", 20).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .mode(SaveMode.Append).save(basePath);

    // The verdict is against the snapshot the validator captured, so it must be a verdict at all
    // rather than a HoodieRemoteException, and that snapshot is still internally consistent.
    assertTrue(validator.run(),
        "a commit landing after construction must not fail the run: " + validator.getThrowables());
  }

  /**
   * The driver warms the server-side view and the executors request it; both must resolve to the
   * same cache key however the operator spelled {@code --base-path}, or the warm-up is discarded
   * and the metadata table is opened a second time.
   *
   * <p><b>Two runs per spelling, and the second one is the load-bearing half.</b> A healthy table
   * must pass under every spelling, but "the run returned true" is satisfied by a validator that
   * examined nothing at all — and a base path spelled without a scheme is exactly the input that
   * could make a listing come back empty, since a listed path carries {@code file:} and the
   * configured base path does not. So the same table is then damaged and the run must <em>fail with
   * the expected verdict</em>: that is the assertion that says the validator both looked and
   * concluded, under each of the four spellings.
   */
  @ParameterizedTest
  @ValueSource(strings = {"raw", "fileScheme", "fileAuthority", "trailingSlash"})
  void testValidatorV2ToleratesBasePathSpelling(String spelling) throws Exception {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    makeInsertDf("000", 10).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.INSERT.value())
        .mode(SaveMode.Overwrite).save(basePath);
    makeUpdateDf("001", 10).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .mode(SaveMode.Append).save(basePath);

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    switch (spelling) {
      case "raw":
        config.basePath = basePath;
        break;
      case "fileScheme":
        config.basePath = "file:" + basePath;
        break;
      case "fileAuthority":
        config.basePath = "file://" + basePath;
        break;
      case "trailingSlash":
        config.basePath = "file:" + basePath + "/";
        break;
      default:
        throw new UnsupportedOperationException(spelling);
    }
    config.validateLatestFileSlices = true;
    config.ignoreFailed = true;
    config.fsViewMemoryFraction = 0.05;

    // v1 first, and its verdict is recorded rather than asserted against v2's: on the bare spelling
    // it is a FALSE POSITIVE on a table nothing has damaged. HoodieMetadataValidationUtils:283-302
    // compares HoodieLogFile PATH STRINGS, so when the operator gives a base path with no scheme the
    // metadata side spells a log file "/tmp/.../x.log.1_0-35-77" and the storage side
    // "file:/tmp/.../x.log.1_0-35-77"; one healthy file appears as two and the partition fails.
    HoodieMetadataTableValidator v1 = new HoodieMetadataTableValidator(jsc, config);
    boolean v1Result = v1.run();

    config.useValidatorV2 = true;
    HoodieMetadataTableValidator v2 = new HoodieMetadataTableValidator(jsc, config);
    boolean v2Result = v2.run();

    // v2 compares BARE NAMES on both sides, so the same file matches itself and the healthy table
    // is reported healthy however --base-path was spelled. v2 does not go through
    // hasCommittedLogFiles at all, so it does not reproduce v1's behaviour here; parity with a
    // defect is not a property worth keeping.
    assertTrue(v2Result, "spelling '" + spelling + "': an undamaged table must pass; v2 throwables="
        + v2.getThrowables());
    if (!v1Result) {
      assertEquals("raw", spelling,
          "v1's path-string comparison is only expected to false-positive on the scheme-less "
              + "spelling; v1 failing on '" + spelling + "' is a v1 defect this test has not seen "
              + "before and is worth looking at rather than absorbing");
    }

    // The half that cannot pass on a validator that did nothing. Nothing else in this class or in
    // the functional suites configures a base path other than "file:" + basePath, so this is the
    // only place any spelling but that one is asked to REACH A VERDICT rather than merely return.
    Path victim = deleteOneBaseFile();
    HoodieMetadataTableValidator damaged = new HoodieMetadataTableValidator(jsc, config);
    assertFalse(damaged.run(), "spelling '" + spelling + "': the metadata table names a base file "
        + "storage no longer has, and a run that reports success here is one that examined nothing");
    String reported = damaged.getThrowables().toString();
    assertTrue(reported.contains("OUTBOUND_DELETION"),
        "spelling '" + spelling + "': a completed instant's file is gone from storage and no clean "
            + "or rollback explains it; got: " + reported);
    assertTrue(reported.contains(victim.getParent().getName()),
        "spelling '" + spelling + "': the failure must name the partition it was found in, which is "
            + "what says the listing resolved under this spelling; got: " + reported);
  }

  /**
   * Removes one base file and returns it, so a caller can assert the run names the partition it was
   * in. Throws rather than returning null: a fixture that damaged nothing turns the assertions that
   * follow it into a test of nothing.
   */
  private Path deleteOneBaseFile() throws IOException {
    RemoteIterator<LocatedFileStatus> files = fs().listFiles(new Path(basePath), true);
    while (files.hasNext()) {
      Path path = files.next().getPath();
      if (path.getName().endsWith(".parquet") && !FSUtils.isLogFile(path.getName())
          && !path.toString().contains(".hoodie")) {
        assertTrue(fs().delete(path, false), "could not delete " + path);
        return path;
      }
    }
    throw new IllegalStateException("no base file to delete under " + basePath);
  }
}
