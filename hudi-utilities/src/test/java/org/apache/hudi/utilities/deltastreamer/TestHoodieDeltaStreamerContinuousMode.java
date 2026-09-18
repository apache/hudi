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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.JavaTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utilities.HoodieClusteringJob;
import org.apache.hudi.utilities.HoodieIndexer;
import org.apache.hudi.utilities.HoodieMetadataTableValidator;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.SourceTestConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.JdbcSource;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.sources.TestParquetDFSSourceEmptyBatch;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.NoNewDataTerminationStrategy;
import org.apache.hudi.utilities.testutils.JdbcTestUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.TestReleaseResourcesStreamSync;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.assertCheckpointVersion;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.deltaStreamerTestRunner;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.prepareJsonKafkaDFSFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Continuous-mode {@link HoodieDeltaStreamer} coverage: long-running syncs, async and inline table services
 * (clustering, compaction, cleaning), the standalone clustering and indexer jobs, and hot config updates.
 * Shared helpers and the transformer, key generator and payload classes these tests name live in
 * {@link TestHoodieDeltaStreamer}.
 */
@Slf4j
public class TestHoodieDeltaStreamerContinuousMode extends HoodieDeltaStreamerTestBase {

  // Kept per class rather than hoisted to the base, because the other base subclasses deliberately do not increment testNum.
  @AfterEach
  public void perTestAfterEach() {
    testNum++;
  }

  protected HoodieDeltaStreamer initialHoodieDeltaStreamer(String tableBasePath, int totalRecords, String asyncCluster, HoodieRecordType recordType) throws IOException {
    return initialHoodieDeltaStreamer(tableBasePath, totalRecords, asyncCluster, recordType, WriteOperationType.INSERT);
  }

  protected HoodieDeltaStreamer initialHoodieDeltaStreamer(String tableBasePath, int totalRecords, String asyncCluster, HoodieRecordType recordType,
                                                           WriteOperationType writeOperationType) throws IOException {
    return initialHoodieDeltaStreamer(tableBasePath, totalRecords, asyncCluster, recordType, writeOperationType, Collections.emptySet());
  }

  protected HoodieDeltaStreamer initialHoodieDeltaStreamer(String tableBasePath, int totalRecords, String asyncCluster, HoodieRecordType recordType,
                                                           WriteOperationType writeOperationType, Set<String> customConfigs) throws IOException {
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, writeOperationType);
    addRecordMerger(recordType, cfg.configs);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "", "", asyncCluster, ""));
    cfg.configs.addAll(getAllMultiWriterConfigs());
    customConfigs.forEach(config -> cfg.configs.add(config));
    return new HoodieDeltaStreamer(cfg, jsc);
  }

  protected HoodieClusteringJob initialHoodieClusteringJob(String tableBasePath, String clusteringInstantTime, Boolean runSchedule, String scheduleAndExecute, HoodieRecordType recordType) {
    return initialHoodieClusteringJob(tableBasePath, clusteringInstantTime, runSchedule, scheduleAndExecute, null, recordType);
  }

  protected HoodieClusteringJob initialHoodieClusteringJob(String tableBasePath, String clusteringInstantTime, Boolean runSchedule, String scheduleAndExecute) {
    return initialHoodieClusteringJob(tableBasePath, clusteringInstantTime, runSchedule, scheduleAndExecute, null, HoodieRecordType.AVRO);
  }

  protected HoodieClusteringJob initialHoodieClusteringJob(String tableBasePath, String clusteringInstantTime, Boolean runSchedule, String scheduleAndExecute,
                                                           Boolean retryLastFailedJob, HoodieRecordType recordType) {
    HoodieClusteringJob.Config scheduleClusteringConfig = buildHoodieClusteringUtilConfig(tableBasePath,
        clusteringInstantTime, runSchedule, scheduleAndExecute, retryLastFailedJob);
    addRecordMerger(recordType, scheduleClusteringConfig.configs);
    scheduleClusteringConfig.configs.addAll(getAllMultiWriterConfigs());
    return new HoodieClusteringJob(jsc, scheduleClusteringConfig);
  }

  private static Stream<Arguments> continuousModeArgs() {
    return Stream.of(
        Arguments.of("AVRO", "CURRENT"),
        Arguments.of("SPARK", "CURRENT"),
        Arguments.of("AVRO", "EIGHT"),
        Arguments.of("SPARK", "EIGHT"),
        Arguments.of("AVRO", "SIX")
    );
  }

  private static Stream<Arguments> continuousModeMorArgs() {
    return Stream.of(
        Arguments.of("AVRO", "CURRENT"),
        Arguments.of("AVRO", "EIGHT"),
        Arguments.of("AVRO", "SIX")
    );
  }

  @Timeout(600)
  @ParameterizedTest
  @MethodSource("continuousModeArgs")
  void testUpsertsCOWContinuousMode(HoodieRecordType recordType, String writeTableVersion) throws Exception {
    testUpsertsContinuousMode(HoodieTableType.COPY_ON_WRITE, "continuous_cow", recordType, writeTableVersion);
  }

  @Test
  public void testUpsertsCOW_ContinuousModeDisabled() throws Exception {
    String tableBasePath = basePath + "/non_continuous_cow";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.add(String.format("%s=%s", HoodieMetricsConfig.TURN_METRICS_ON.key(), "true"));
    cfg.configs.add(String.format("%s=%s", HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key(), MetricsReporterType.INMEMORY.name()));
    cfg.continuousMode = false;
    syncOnce(cfg);
    assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
    assertRecordCount(SQL_SOURCE_NUM_RECORDS, tableBasePath, sqlContext);
    assertFalse(Metrics.isInitialized(tableBasePath), "Metrics should be shutdown");
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @Timeout(600)
  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO"})
  void testUpsertsMORContinuousModeShutdownGracefully(HoodieRecordType recordType) throws Exception {
    testUpsertsContinuousMode(HoodieTableType.MERGE_ON_READ, "continuous_cow", true, recordType, "CURRENT");
  }

  @Timeout(600)
  @ParameterizedTest
  @MethodSource("continuousModeMorArgs")
  public void testUpsertsMORContinuousMode(HoodieRecordType recordType, String writeTableVersion) throws Exception {
    testUpsertsContinuousMode(HoodieTableType.MERGE_ON_READ, "continuous_mor", recordType, writeTableVersion);
  }

  @Test
  public void testUpsertsMOR_ContinuousModeDisabled() throws Exception {
    String tableBasePath = basePath + "/non_continuous_mor";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    cfg.configs.add(String.format("%s=%s", HoodieMetricsConfig.TURN_METRICS_ON.key(), "true"));
    cfg.configs.add(String.format("%s=%s", HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key(), MetricsReporterType.INMEMORY.name()));
    cfg.continuousMode = false;
    syncOnce(cfg);
    assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
    assertRecordCount(SQL_SOURCE_NUM_RECORDS, tableBasePath, sqlContext);
    assertFalse(Metrics.isInitialized(tableBasePath), "Metrics should be shutdown");
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  private void testUpsertsContinuousMode(HoodieTableType tableType, String tempDir, HoodieRecordType recordType, String writeTableVersion) throws Exception {
    testUpsertsContinuousMode(tableType, tempDir, false, recordType, writeTableVersion);
  }

  private void testUpsertsContinuousMode(HoodieTableType tableType, String tempDir, boolean testShutdownGracefully, HoodieRecordType recordType,
                                         String writeTableVersion) throws Exception {
    String tableBasePath = basePath + "/" + tempDir;
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;
    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    addRecordMerger(recordType, cfg.configs);
    cfg.continuousMode = true;
    if (testShutdownGracefully) {
      cfg.postWriteTerminationStrategyClass = NoNewDataTerminationStrategy.class.getName();
    }
    cfg.tableType = tableType.name();
    cfg.configs.add(String.format("%s=%d", SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), totalRecords));
    cfg.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));
    if (HoodieTableVersion.SIX.name().equals(writeTableVersion)) {
      cfg.configs.add(String.format(("%s=%s"), HoodieWriteConfig.WRITE_TABLE_VERSION.key(), HoodieTableVersion.SIX.versionCode()));
    } else if (HoodieTableVersion.EIGHT.name().equals(writeTableVersion)) {
      cfg.configs.add(String.format(("%s=%s"), HoodieWriteConfig.WRITE_TABLE_VERSION.key(), HoodieTableVersion.EIGHT.versionCode()));
    }
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommits(5, tableBasePath);
        TestHelpers.assertAtleastNCompactionCommits(2, tableBasePath);
      } else {
        TestHelpers.assertAtleastNCompactionCommits(5, tableBasePath);
      }
      assertRecordCount(totalRecords, tableBasePath, sqlContext);
      assertDistanceCount(totalRecords, tableBasePath, sqlContext);
      if (testShutdownGracefully) {
        TestDataSource.returnEmptyBatch = true;
      }
      return true;
    });
    // validate table version matches
    HoodieTableMetaClient hudiTblMetaClient = HoodieTableMetaClient.builder().setBasePath(cfg.targetBasePath).setConf(context.getStorageConf()).build();
    if (writeTableVersion.equals("CURRENT")) {
      assertEquals(HoodieTableVersion.current(), hudiTblMetaClient.getTableConfig().getTableVersion());
    } else {
      assertEquals(HoodieTableVersion.valueOf(writeTableVersion), hudiTblMetaClient.getTableConfig().getTableVersion());
    }
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @ParameterizedTest
  @CsvSource(value = {"AVRO", "SPARK"})
  public void testInlineClustering(HoodieRecordType recordType) throws Exception {
    String tableBasePath = basePath + "/inlineClustering";
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    addRecordMerger(recordType, cfg.configs);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "true", "2", "", ""));
    cfg.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath);
      TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
      return true;
    });
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @Test
  public void testDeltaSyncWithPendingClustering() throws Exception {
    String tableBasePath = basePath + "/inlineClusteringPending";
    // ingest data
    int totalRecords = 2000;
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    cfg.continuousMode = false;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    syncOnce(cfg);
    // assert ingest successful
    TestHelpers.assertAtLeastNCommits(1, tableBasePath);

    // schedule a clustering job to build a clustering plan and transition to inflight
    HoodieClusteringJob clusteringJob = initialHoodieClusteringJob(tableBasePath, null, false, "schedule");
    clusteringJob.cluster(0);
    HoodieTableMetaClient meta = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    List<HoodieInstant> hoodieClusteringInstants = meta.getActiveTimeline().filterPendingClusteringTimeline().getInstants();
    HoodieInstant clusteringRequest = hoodieClusteringInstants.get(0);
    meta.getActiveTimeline().transitionClusterRequestedToInflight(clusteringRequest, Option.empty());

    // do another ingestion with inline clustering enabled
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "true", "2", "", ""));
    cfg.retryLastPendingInlineClusteringJob = true;
    syncOnce(cfg);
    String completeClusteringTimeStamp = meta.reloadActiveTimeline().getCompletedReplaceTimeline().lastInstant().get().requestedTime();
    assertEquals(clusteringRequest.requestedTime(), completeClusteringTimeStamp);
    TestHelpers.assertAtLeastNCommits(2, tableBasePath);
    TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
  }

  @Test
  public void testDeltaSyncWithPendingCompaction() throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum;
    int parquetRecordsCount = 100;
    HoodieTestDataGenerator dataGenerator = prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.compact.inline", "true");
    extraProps.setProperty("hoodie.compact.inline.max.delta.commits", "2");
    extraProps.setProperty("hoodie.datasource.write.table.type", "MERGE_ON_READ");
    extraProps.setProperty("hoodie.datasource.compaction.async.enable", "false");
    prepareParquetDFSSource(false, false, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "", extraProps, false, false);
    String tableBasePath = basePath + "test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config deltaCfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
        null, PROPS_FILENAME_TEST_PARQUET, false,
        false, 100000, false, null, "MERGE_ON_READ", "timestamp", null);
    deltaCfg.retryLastPendingInlineCompactionJob = false;

    // sync twice and trigger compaction
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(deltaCfg, jsc);
    deltaStreamer.sync();
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    prepareParquetDFSUpdates(100, PARQUET_SOURCE_ROOT, "2.parquet", false, null, null, dataGenerator, "001");
    deltaStreamer.sync();
    TestHelpers.assertAtleastNDeltaCommits(2, tableBasePath);
    TestHelpers.assertAtleastNCompactionCommits(1, tableBasePath);
    deltaStreamer.shutdownGracefully();

    // delete compaction commit
    HoodieTableMetaClient meta = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = timeline.lastInstant().get();
    String commitFileName = tableBasePath + "/.hoodie/timeline/" + INSTANT_FILE_NAME_GENERATOR.getFileName(commitInstant);
    fs.delete(new Path(commitFileName), false);

    // sync again
    prepareParquetDFSUpdates(100, PARQUET_SOURCE_ROOT, "3.parquet", false, null, null, dataGenerator, "002");
    deltaStreamer = new HoodieDeltaStreamer(deltaCfg, jsc);
    deltaStreamer.sync();
    TestHelpers.assertAtleastNDeltaCommits(3, tableBasePath);
    meta = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    timeline = meta.getActiveTimeline().getRollbackTimeline();
    assertEquals(1, timeline.getInstants().size());
    deltaStreamer.shutdownGracefully();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCleanerDeleteReplacedDataWithArchive(Boolean asyncClean) throws Exception {
    String tableBasePath = basePath + "/cleanerDeleteReplacedDataWithArchive" + asyncClean;

    int totalRecords = 3000;

    // Step 1 : Prepare and insert data without archival and cleaner.
    // Make sure that there are 6 commits including 2 replacecommits completed.
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    addRecordMerger(HoodieRecordType.AVRO, cfg.configs);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "true", "2", "", ""));
    cfg.configs.add(String.format("%s=%s", HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0"));
    cfg.configs.add(String.format("%s=%s", HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key(), "1"));
    cfg.configs.add(String.format("%s=%s", HoodieWriteConfig.MARKERS_TYPE.key(), "DIRECT"));
    cfg.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNReplaceCommits(2, tableBasePath);
      return true;
    });

    TestHelpers.assertAtLeastNCommits(6, tableBasePath);
    TestHelpers.assertAtLeastNReplaceCommits(2, tableBasePath);

    // Step 2 : Get the first replacecommit and extract the corresponding replaced file IDs.
    HoodieTableMetaClient meta = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    HoodieTimeline replacedTimeline = meta.reloadActiveTimeline().getCompletedReplaceTimeline();
    Option<HoodieInstant> firstReplaceHoodieInstant = replacedTimeline.nthFromLastInstant(1);
    assertTrue(firstReplaceHoodieInstant.isPresent());
    HoodieReplaceCommitMetadata firstReplaceMetadata =
        replacedTimeline.readReplaceCommitMetadata(firstReplaceHoodieInstant.get());
    Map<String, List<String>> partitionToReplaceFileIds = firstReplaceMetadata.getPartitionToReplaceFileIds();
    String partitionName = null;
    List<String> replacedFileIDs = null;
    for (Map.Entry<String, List<String>> entry : partitionToReplaceFileIds.entrySet()) {
      partitionName = String.valueOf(entry.getKey());
      replacedFileIDs = entry.getValue();
    }

    assertNotNull(partitionName);
    assertNotNull(replacedFileIDs);

    // Step 3 : Based to replacedFileIDs , get the corresponding complete path.
    ArrayList<String> replacedFilePaths = new ArrayList<>();
    StoragePath partitionPath = new StoragePath(meta.getBasePath(), partitionName);
    List<StoragePathInfo> hoodieFiles = meta.getStorage().listFiles(partitionPath);
    for (StoragePathInfo pathInfo : hoodieFiles) {
      String file = pathInfo.getPath().toUri().toString();
      for (Object replacedFileID : replacedFileIDs) {
        if (file.contains(String.valueOf(replacedFileID))) {
          replacedFilePaths.add(file);
        }
      }
    }

    assertFalse(replacedFilePaths.isEmpty());

    // Step 4 : Add commits with insert of 1 record and trigger sync/async cleaner and archive.
    List<String> configs = getTableServicesConfigs(1, "true", "true", "6", "", "");
    configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_POLICY.key(), "KEEP_LATEST_COMMITS"));
    configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "1"));
    configs.add(String.format("%s=%s", HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "4"));
    configs.add(String.format("%s=%s", HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "5"));
    configs.add(String.format("%s=%s", HoodieCleanConfig.ASYNC_CLEAN.key(), asyncClean));
    configs.add(String.format("%s=%s", HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key(), "1"));
    configs.add(String.format("%s=%s", HoodieWriteConfig.MARKERS_TYPE.key(), "DIRECT"));
    if (asyncClean) {
      configs.add(String.format("%s=%s", HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(),
          WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name()));
      configs.add(String.format("%s=%s", HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(),
          HoodieFailedWritesCleaningPolicy.LAZY.name()));
      configs.add(String.format("%s=%s", HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
          InProcessLockProvider.class.getName()));
    }
    addRecordMerger(HoodieRecordType.AVRO, configs);
    cfg.configs = configs;
    cfg.continuousMode = false;
    // timeline as of now. no cleaner and archival kicked in.
    // c1, c2, rc3, c4, c5, rc6,

    syncOnce(cfg);
    // after 1 round of sync, timeline will be as follows
    // just before clean
    // c1, c2, rc3, c4, c5, rc6, c7
    // after clean
    // c1, c2, rc3, c4, c5, rc6, c7, c8.clean (earliest commit to retain is c7)
    // after archival (retain 4 commits)
    // c4, c5, rc6, c7, c8.clean

    // old code has 2 sync() calls. book-keeping the sequence for now.
    // after 2nd round of sync
    // just before clean
    // c4, c5, rc6, c7, c8.clean, c9
    // after clean
    // c4, c5, rc6, c7, c8.clean, c9, c10.clean (earliest commit to retain c9)
    // after archival
    // c5, rc6, c7, c8.clean, c9, c10.clean

    // Step 5 : FirstReplaceHoodieInstant should not be retained.
    long count = meta.reloadActiveTimeline().getCompletedReplaceTimeline().getInstantsAsStream().filter(instant -> firstReplaceHoodieInstant.get().equals(instant)).count();
    assertEquals(0, count);

    // Step 6 : All the replaced files in firstReplaceHoodieInstant should be deleted through sync/async cleaner.
    for (String replacedFilePath : replacedFilePaths) {
      assertFalse(meta.getStorage().exists(new StoragePath(replacedFilePath)));
    }
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  /**
   * Tests that we release resources even on failures scenarios.
   * @param testFailureCase
   * @throws Exception
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReleaseResources(boolean testFailureCase) throws Exception {
    String tableBasePath = basePath + "/inlineClusteringPending_" + testFailureCase;
    int totalRecords = 1000;
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.continuousMode = false;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    syncOnce(cfg);
    // assert ingest successful
    TestHelpers.assertAtLeastNCommits(1, tableBasePath);

    // schedule a clustering job to build a clustering plan and leave it in pending state.
    HoodieClusteringJob clusteringJob = initialHoodieClusteringJob(tableBasePath, null, false, "schedule");
    clusteringJob.cluster(0);
    HoodieTableMetaClient tableMetaClient = HoodieTableMetaClient.builder().setConf(context.getStorageConf()).setBasePath(tableBasePath).build();
    assertEquals(1, tableMetaClient.getActiveTimeline().filterPendingClusteringTimeline().getInstants().size());

    // do another ingestion with inline clustering enabled
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "true", "2", "", ""));
    // based on if we want to test happy path or failure scenario, set the right value for retryLastPendingInlineClusteringJob.
    cfg.retryLastPendingInlineClusteringJob = !testFailureCase;
    TypedProperties properties = HoodieStreamer.combineProperties(cfg, Option.empty(), jsc.hadoopConfiguration());
    SchemaProvider schemaProvider = UtilHelpers.wrapSchemaProviderWithPostProcessor(UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, properties, jsc),
        properties, jsc, cfg.transformerClassNames);

    try (TestReleaseResourcesStreamSync streamSync = new TestReleaseResourcesStreamSync(cfg, sparkSession, schemaProvider, properties,
        jsc, fs, jsc.hadoopConfiguration(), client -> true)) {
      assertTrue(streamSync.releaseResourcesCalledSet.isEmpty());
      try {
        streamSync.syncOnce();
        if (testFailureCase) {
          fail("Should not reach here when there is conflict w/ pending clustering and when retryLastPendingInlineClusteringJob is set to false");
        }
      } catch (HoodieException e) {
        if (!testFailureCase) {
          fail("Should not reach here when retryLastPendingInlineClusteringJob is set to true");
        }
      }

      tableMetaClient = HoodieTableMetaClient.reload(tableMetaClient);
      Option<HoodieInstant> failedInstant = tableMetaClient.getActiveTimeline().getCommitTimeline().lastInstant();
      assertTrue(failedInstant.isPresent());
      assertTrue(testFailureCase ? !failedInstant.get().isCompleted() : failedInstant.get().isCompleted());

      if (testFailureCase) {
        // validate that release resource is invoked
        assertEquals(1, streamSync.releaseResourcesCalledSet.size());
        assertTrue(streamSync.releaseResourcesCalledSet.contains(failedInstant.get().requestedTime()));
      } else {
        assertTrue(streamSync.releaseResourcesCalledSet.isEmpty());
      }

      // validate heartbeat is closed or expired.
      HoodieHeartbeatClient heartbeatClient = new HoodieHeartbeatClient(tableMetaClient.getStorage(), this.basePath,
          (long) HoodieWriteConfig.CLIENT_HEARTBEAT_INTERVAL_IN_MS.defaultValue(), HoodieWriteConfig.CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES.defaultValue());
      assertTrue(heartbeatClient.isHeartbeatExpired(failedInstant.get().requestedTime()));
      heartbeatClient.close();
    }
  }

  private List<String> getAllMultiWriterConfigs() {
    List<String> configs = new ArrayList<>();
    configs.add(String.format("%s=%s", HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), InProcessLockProvider.class.getCanonicalName()));
    configs.add(String.format("%s=%s", LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000"));
    configs.add(String.format("%s=%s", HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name()));
    configs.add(String.format("%s=%s", HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.LAZY.name()));
    return configs;
  }

  private HoodieClusteringJob.Config buildHoodieClusteringUtilConfig(String basePath,
                                                                     String clusteringInstantTime,
                                                                     Boolean runSchedule) {
    return buildHoodieClusteringUtilConfig(basePath, clusteringInstantTime, runSchedule, null, null);
  }

  private HoodieClusteringJob.Config buildHoodieClusteringUtilConfig(String basePath,
                                                                     String clusteringInstantTime,
                                                                     Boolean runSchedule,
                                                                     String runningMode,
                                                                     Boolean retryLastFailedJob) {
    HoodieClusteringJob.Config config = new HoodieClusteringJob.Config();
    config.basePath = basePath;
    config.clusteringInstantTime = clusteringInstantTime;
    config.runSchedule = runSchedule;
    config.propsFilePath = UtilitiesTestBase.basePath + "/clusteringjob.properties";
    config.runningMode = runningMode;
    if (retryLastFailedJob != null) {
      config.retryLastFailedJob = retryLastFailedJob;
      if (retryLastFailedJob) {
        // Set maxProcessingTimeMs to 1ms so any inflight instant is considered stale/failed
        config.maxProcessingTimeMs = 1;
      }
    }
    config.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    return config;
  }

  private HoodieIndexer.Config buildIndexerConfig(String basePath,
                                                  String tableName,
                                                  String indexInstantTime,
                                                  String runningMode,
                                                  String indexTypes) {
    return buildIndexerConfig(basePath, tableName, indexInstantTime, runningMode, indexTypes, Collections.emptyList());
  }

  private HoodieIndexer.Config buildIndexerConfig(String basePath,
                                                  String tableName,
                                                  String indexInstantTime,
                                                  String runningMode,
                                                  String indexTypes,
                                                  List<String> configs) {
    HoodieIndexer.Config indexerConfig = new HoodieIndexer.Config();
    indexerConfig.basePath = basePath;
    indexerConfig.tableName = tableName;
    indexerConfig.indexInstantTime = indexInstantTime;
    indexerConfig.propsFilePath = UtilitiesTestBase.basePath + "/indexer.properties";
    indexerConfig.runningMode = runningMode;
    indexerConfig.indexTypes = indexTypes;
    indexerConfig.configs = configs;
    return indexerConfig;
  }

  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO", "SPARK"})
  public void testHoodieIndexer(HoodieRecordType recordType) throws Exception {
    String tableBasePath = basePath + "/asyncindexer";
    HoodieDeltaStreamer ds = initialHoodieDeltaStreamer(tableBasePath, 1000, "false", recordType, WriteOperationType.INSERT,
        Collections.singleton(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key() + "=true"));

    deltaStreamerTestRunner(ds, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath);

      Option<String> scheduleIndexInstantTime = Option.empty();
      try {
        HoodieIndexer scheduleIndexingJob = new HoodieIndexer(jsc,
            buildIndexerConfig(tableBasePath, ds.getConfig().targetTableName, null, UtilHelpers.SCHEDULE, "COLUMN_STATS"));
        scheduleIndexInstantTime = scheduleIndexingJob.doSchedule();
      } catch (Exception e) {
        log.info("Schedule indexing failed", e);
        return false;
      }
      if (scheduleIndexInstantTime.isPresent()) {
        TestHelpers.assertPendingIndexCommit(tableBasePath);
        log.info("Schedule indexing success, now build index with instant time {}", scheduleIndexInstantTime.get());
        HoodieIndexer runIndexingJob = new HoodieIndexer(jsc,
            buildIndexerConfig(tableBasePath, ds.getConfig().targetTableName, scheduleIndexInstantTime.get(), UtilHelpers.EXECUTE, "COLUMN_STATS"));
        runIndexingJob.start(0);
        log.info("Metadata indexing success");
        TestHelpers.assertCompletedIndexCommit(tableBasePath);
      } else {
        log.warn("Metadata indexing failed");
      }
      return true;
    });
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @Disabled("HUDI-8951")
  @Test
  public void testHoodieIndexerExecutionAfterCommit() throws Exception {
    String tableBasePath = basePath + "/asyncindexer_commit";
    Set<String> customConfigs = new HashSet<>();
    customConfigs.add(HoodieIndexConfig.INDEX_TYPE.key() + "=GLOBAL_SIMPLE");
    // disabling timeline server based marker type to avoid flakiness, sometimes timeline server read times out
    customConfigs.add(HoodieWriteConfig.MARKERS_TYPE.key() + "=DIRECT");
    HoodieDeltaStreamer ds = initialHoodieDeltaStreamer(tableBasePath, 100, "false", HoodieRecordType.AVRO, WriteOperationType.UPSERT, customConfigs);

    deltaStreamerTestRunner(ds, (r) -> {
      // Ensure there are two commits in the table
      TestHelpers.assertAtLeastNCommits(1, tableBasePath);

      Option<String> scheduleIndexInstantTime;
      try {
        // Schedule an indexing instant
        HoodieIndexer scheduleIndexingJob = new HoodieIndexer(jsc,
            buildIndexerConfig(tableBasePath, ds.getConfig().targetTableName, null, UtilHelpers.SCHEDULE, "RECORD_INDEX",
                Arrays.asList(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=true", HoodieWriteConfig.MARKERS_TYPE.key() + "=DIRECT")));
        scheduleIndexInstantTime = scheduleIndexingJob.doSchedule();
        TestHelpers.assertPendingIndexCommit(tableBasePath);
        log.info("Schedule indexing success, now build index with instant time {}", scheduleIndexInstantTime.get());
        // Wait for a pending commit before starting execution phase for the executor. This ensures that indexer waits for the commit to complete.
        TestHelpers.waitFor(() -> {
          HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage.getConf(), tableBasePath);
          HoodieTimeline pendingCommitsTimeline = metaClient.getCommitsTimeline().filterInflightsAndRequested();
          return !pendingCommitsTimeline.empty();
        });
        HoodieIndexer runIndexingJob = new HoodieIndexer(jsc,
            buildIndexerConfig(tableBasePath, ds.getConfig().targetTableName, scheduleIndexInstantTime.get(), UtilHelpers.EXECUTE, "RECORD_INDEX",
                Arrays.asList(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=true", HoodieWriteConfig.MARKERS_TYPE.key() + "=DIRECT")));
        runIndexingJob.start(0);
        log.info("Metadata indexing success");
        TestHelpers.assertCompletedIndexCommit(tableBasePath);
        // Assert no pending commits before indexing instant
        HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage.getConf(), tableBasePath);
        String indexCompletedTime = metaClient.reloadActiveTimeline().getAllCommitsTimeline().filterCompletedIndexTimeline().firstInstant().get().getCompletionTime();
        assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().filterInflightsAndRequested().findInstantsBefore(indexCompletedTime).empty());
      } catch (Exception e) {
        fail("Indexing job should not have failed", e);
      }
      return true;
    });

    validateRecordIndex(tableBasePath);
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  private static void validateRecordIndex(String tableBasePath) {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = tableBasePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    config.validateRecordIndexContent = true;
    config.validateRecordIndexCount = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  @Disabled("HUDI-8951")
  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO", "SPARK"})
  public void testHoodieIndexerExecutionAfterClustering(HoodieRecordType recordType) throws Exception {
    String tableBasePath = basePath + "/asyncindexer_cluster";
    // Set async clustering to run every commit
    HoodieDeltaStreamer ds = initialHoodieDeltaStreamer(tableBasePath, 1000, "true", recordType, WriteOperationType.UPSERT,
        new HashSet<>(Arrays.asList(HoodieIndexConfig.INDEX_TYPE.key() + "=GLOBAL_SIMPLE", HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key() + "=1",
            HoodieWriteConfig.MARKERS_TYPE.key() + "=DIRECT")));

    deltaStreamerTestRunner(ds, (r) -> {
      // Ensure there is one commit in the table, since clustering runs after every commit
      TestHelpers.assertAtLeastNCommits(1, tableBasePath);

      Option<String> scheduleIndexInstantTime = Option.empty();
      try {
        HoodieIndexer scheduleIndexingJob = new HoodieIndexer(jsc,
            buildIndexerConfig(tableBasePath, ds.getConfig().targetTableName, null, UtilHelpers.SCHEDULE, "RECORD_INDEX"));
        scheduleIndexInstantTime = scheduleIndexingJob.doSchedule();
        TestHelpers.assertPendingIndexCommit(tableBasePath);
        log.info("Schedule indexing success, now build index with instant time {}", scheduleIndexInstantTime.get());
        // Wait for clustering instant to be scheduled before starting execution phase of the executor
        TestHelpers.waitFor(() -> {
          HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage, tableBasePath);
          return metaClient.getActiveTimeline().getFirstPendingClusterInstant().isPresent();
        });
        try {
          HoodieIndexer runIndexingJob = new HoodieIndexer(jsc,
              buildIndexerConfig(tableBasePath, ds.getConfig().targetTableName, scheduleIndexInstantTime.get(), UtilHelpers.EXECUTE, "RECORD_INDEX",
                  Arrays.asList(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=true", HoodieMetadataConfig.METADATA_INDEX_CHECK_TIMEOUT_SECONDS.key() + "=20")));
          runIndexingJob.start(0);
          // Clustering commit fails because of conflict with indexing commit
          fail("Indexing should fail with catchup failure");
        } catch (Throwable t) {
          boolean res = JavaTestUtils.checkNestedExceptionContains(t, "Index catchup failed");
          assertTrue(res, "Indexing catchup task should have timed out");
        }
        log.info("Metadata indexing timed out");
      } catch (Exception e) {
        fail("Indexing job should not have failed", e);
      }
      return true;
    });

    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHoodieAsyncClusteringJob(boolean shouldPassInClusteringInstantTime) throws Exception {
    String tableBasePath = basePath + "/asyncClusteringJob";
    HoodieDeltaStreamer ds = initialHoodieDeltaStreamer(tableBasePath, 3000, "false", HoodieRecordType.AVRO);
    CountDownLatch countDownLatch = new CountDownLatch(1);

    deltaStreamerTestRunner(ds, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath);
      countDownLatch.countDown();
      return true;
    });

    if (countDownLatch.await(2, TimeUnit.MINUTES)) {
      Option<String> scheduleClusteringInstantTime = Option.empty();
      try {
        HoodieClusteringJob scheduleClusteringJob =
            initialHoodieClusteringJob(tableBasePath, null, true, null);
        scheduleClusteringInstantTime = scheduleClusteringJob.doSchedule();
      } catch (Exception e) {
        log.warn("Schedule clustering failed", e);
        Assertions.fail("Schedule clustering failed", e);
      }
      if (scheduleClusteringInstantTime.isPresent()) {
        log.info("Schedule clustering success, now cluster with instant time {}", scheduleClusteringInstantTime.get());
        HoodieClusteringJob.Config clusterClusteringConfig = buildHoodieClusteringUtilConfig(tableBasePath,
            shouldPassInClusteringInstantTime ? scheduleClusteringInstantTime.get() : null, false);
        HoodieClusteringJob clusterClusteringJob = new HoodieClusteringJob(jsc, clusterClusteringConfig);
        clusterClusteringJob.cluster(clusterClusteringConfig.retry);
        TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
        log.info("Cluster success");
      } else {
        log.warn("Clustering execution failed");
        Assertions.fail("Clustering execution failed");
      }
    } else {
      Assertions.fail("Deltastreamer should have completed 2 commits.");
    }
  }

  @Disabled("HUDI-6753")
  @Test
  public void testAsyncClusteringServiceSparkRecordType() throws Exception {
    testAsyncClusteringService(HoodieRecordType.SPARK);
  }

  @Test
  public void testAsyncClusteringServiceAvroRecordType() throws Exception {
    testAsyncClusteringService(HoodieRecordType.AVRO);
  }

  private void testAsyncClusteringService(HoodieRecordType recordType) throws Exception {
    String tableBasePath = basePath + "/asyncClustering";
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 2000;

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    addRecordMerger(recordType, cfg.configs);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "", "", "true", "3"));
    cfg.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    cfg.configs.add(String.format("%s=%s", "hoodie.merge.allow.duplicate.on.inserts", "false"));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
      return true;
    });
    // There should be 4 commits, one of which should be a replace commit
    TestHelpers.assertAtLeastNCommits(4, tableBasePath);
    TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
    assertDistinctRecordCount(totalRecords, tableBasePath, sqlContext);
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @Timeout(600)
  @Test
  public void testAsyncClusteringServiceWithConflictsAvro() throws Exception {
    testAsyncClusteringServiceWithConflicts(HoodieRecordType.AVRO);
  }

  /**
   * When deltastreamer writes clashes with pending clustering, deltastreamer should keep retrying and eventually succeed(once clustering completes)
   * w/o failing mid way.
   *
   * @throws Exception
   */
  private void testAsyncClusteringServiceWithConflicts(HoodieRecordType recordType) throws Exception {
    String tableBasePath = basePath + "/asyncClusteringWithConflicts_" + recordType.name();
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 2000;

    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    addRecordMerger(recordType, cfg.configs);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "", "", "true", "2"));
    cfg.configs.add(String.format("%s=%s", "hoodie.datasource.write.row.writer.enable", "false"));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      // when pending clustering overlaps w/ incoming, incoming batch will fail and hence will result in rollback.
      // But eventually the batch should succeed. so, lets check for successful commits after a completed rollback.
      HoodieDeltaStreamerTestBase.TestHelpers.assertAtLeastNCommitsAfterRollback(1, 1, tableBasePath);
      return true;
    });
    // There should be 4 commits, one of which should be a replace commit
    TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
    TestHelpers.assertAtLeastNCommits(3, tableBasePath);
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @Timeout(600)
  @Test
  public void testAsyncClusteringServiceWithCompaction() throws Exception {
    String tableBasePath = basePath + "/asyncClusteringCompaction";
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 2000;

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    addRecordMerger(HoodieRecordType.AVRO, cfg.configs);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "", "", "true", "3"));
    cfg.configs.add(String.format("%s=%s", "hoodie.merge.allow.duplicate.on.inserts", "false"));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtleastNCompactionCommits(2, tableBasePath);
      TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
      return true;
    });
    // There should be 4 commits, one of which should be a replace commit
    TestHelpers.assertAtLeastNCommits(4, tableBasePath);
    TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
    assertDistinctRecordCount(totalRecords, tableBasePath, sqlContext);

    // validate that there are no rollbacks in MDT to ensure lock provider worked.
    HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder().setBasePath(cfg.targetBasePath + "/.hoodie/metadata/").setConf(context.getStorageConf()).build();
    assertTrue(mdtMetaClient.reloadActiveTimeline().getRollbackTimeline().getInstants().isEmpty());
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAsyncClusteringJobWithRetry(boolean retryLastFailedJob) throws Exception {
    String tableBasePath = basePath + "/asyncClustering3";

    // ingest data
    int totalRecords = 3000;
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    addRecordMerger(HoodieRecordType.AVRO, cfg.configs);
    cfg.continuousMode = false;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.addAll(getTableServicesConfigs(totalRecords, "false", "false", "0", "false", "0"));
    cfg.configs.addAll(getAllMultiWriterConfigs());
    syncOnce(cfg);

    // assert ingest successful
    TestHelpers.assertAtLeastNCommits(1, tableBasePath);

    // schedule a clustering job to build a clustering plan
    HoodieClusteringJob schedule = initialHoodieClusteringJob(tableBasePath, null, false, "schedule");
    schedule.cluster(0);

    // do another ingestion
    syncOnce(cfg);

    // convert clustering request into inflight, Simulate the last clustering failed scenario
    HoodieTableMetaClient meta = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    List<HoodieInstant> hoodieClusteringInstants = meta.getActiveTimeline().filterPendingClusteringTimeline().getInstants();
    HoodieInstant clusteringRequest = hoodieClusteringInstants.get(0);
    HoodieInstant hoodieInflightInstant = meta.getActiveTimeline().transitionClusterRequestedToInflight(clusteringRequest, Option.empty());

    // trigger a scheduleAndExecute clustering job
    // when retryFailedClustering true => will rollback and re-execute failed clustering plan with same instant timestamp.
    // when retryFailedClustering false => will make and execute a new clustering plan with new instant timestamp.
    HoodieClusteringJob scheduleAndExecute = initialHoodieClusteringJob(tableBasePath, null, false, "scheduleAndExecute", retryLastFailedJob, HoodieRecordType.AVRO);
    scheduleAndExecute.cluster(0);

    String completeClusteringTimeStamp = meta.getActiveTimeline().reload().getCompletedReplaceTimeline().lastInstant().get().requestedTime();

    if (retryLastFailedJob) {
      assertEquals(clusteringRequest.requestedTime(), completeClusteringTimeStamp);
    } else {
      assertFalse(clusteringRequest.requestedTime().equalsIgnoreCase(completeClusteringTimeStamp));
    }
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @ParameterizedTest
  @ValueSource(strings = {"execute", "schedule", "scheduleAndExecute"})
  public void testHoodieAsyncClusteringJobWithScheduleAndExecute(String runningMode) throws Exception {
    String tableBasePath = basePath + "/asyncClustering2";
    HoodieDeltaStreamer ds = initialHoodieDeltaStreamer(tableBasePath, 3000, "false", HoodieRecordType.AVRO, WriteOperationType.BULK_INSERT);
    HoodieClusteringJob scheduleClusteringJob = initialHoodieClusteringJob(tableBasePath, null, true, runningMode, HoodieRecordType.AVRO);

    deltaStreamerTestRunner(ds, (r) -> {
      Exception exception = null;
      TestHelpers.assertAtLeastNCommits(2, tableBasePath);
      try {
        int result = scheduleClusteringJob.cluster(0);
        if (result == 0) {
          log.info("Cluster success");
        } else {
          log.warn("Cluster failed");
          if (!runningMode.toLowerCase().equals(UtilHelpers.EXECUTE)) {
            return false;
          }
        }
      } catch (Exception e) {
        log.warn("ScheduleAndExecute clustering failed", e);
        exception = e;
        if (!runningMode.equalsIgnoreCase(UtilHelpers.EXECUTE)) {
          return false;
        }
      }
      switch (runningMode.toLowerCase()) {
        case UtilHelpers.SCHEDULE_AND_EXECUTE: {
          TestHelpers.assertAtLeastNReplaceCommits(2, tableBasePath);
          return true;
        }
        case UtilHelpers.SCHEDULE: {
          TestHelpers.assertAtLeastNClusterRequests(2, tableBasePath);
          TestHelpers.assertNoReplaceCommits(tableBasePath);
          return true;
        }
        case UtilHelpers.EXECUTE: {
          TestHelpers.assertNoReplaceCommits(tableBasePath);
          return true;
        }
        default:
          throw new IllegalStateException("Unexpected value: " + runningMode);
      }
    });
    if (runningMode.toLowerCase(Locale.ROOT).equals(UtilHelpers.SCHEDULE_AND_EXECUTE)) {
      UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
    }
  }

  @Test
  public void testBulkInsertRowWriterContinuousModeWithAsyncClustering() throws Exception {
    testBulkInsertRowWriterContinuousMode(false, null, false,
        getTableServicesConfigs(2000, "false", "", "", "true", "3"), false);
  }

  @Test
  public void testBulkInsertRowWriterContinuousModeWithInlineClustering() throws Exception {
    testBulkInsertRowWriterContinuousMode(false, null, false,
        getTableServicesConfigs(2000, "false", "true", "3", "false", ""), false);
  }

  @Test
  public void testBulkInsertRowWriterContinuousModeWithInlineClusteringAmbiguousDates() throws Exception {
    sparkSession.sqlContext().setConf("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY");
    sparkSession.sqlContext().setConf("spark.sql.avro.datetimeRebaseModeInWrite", "LEGACY");
    sparkSession.sqlContext().setConf("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY");
    sparkSession.sqlContext().setConf("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY");
    sparkSession.sqlContext().setConf("spark.sql.avro.datetimeRebaseModeInRead", "LEGACY");
    sparkSession.sqlContext().setConf("spark.sql.parquet.int96RebaseModeInRead", "LEGACY");
    testBulkInsertRowWriterContinuousMode(false, null, false,
        getTableServicesConfigs(2000, "false", "true", "3",
            "false", ""), true);
  }

  private void testBulkInsertRowWriterContinuousMode(boolean useSchemaProvider, List<String> transformerClassNames,
                                                     boolean testEmptyBatch, List<String> customConfigs, boolean makeDatesAmbiguous) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 100;
    boolean hasTransformer = transformerClassNames != null && !transformerClassNames.isEmpty();
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null, makeDatesAmbiguous);
    prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", testEmptyBatch ? "1" : "");

    // generate data asynchronously.
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future inputGenerationFuture = executor.submit(() -> {
      try {
        int counter = 2;
        while (counter < 100) { // lets keep going. if the test times out, we will cancel the future within finally. So, safe to generate 100 batches.
          log.info("Generating data for batch {}", counter);
          prepareParquetDFSFiles(100, PARQUET_SOURCE_ROOT, Integer.toString(counter) + ".parquet", false, null, null, makeDatesAmbiguous);
          counter++;
          Thread.sleep(2000);
        }
      } catch (Exception ex) {
        log.warn("Input data generation failed", ex);
        throw new RuntimeException(ex.getMessage(), ex);
      }
    });

    // initialize configs for continuous ds
    String tableBasePath = basePath + "/test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT, testEmptyBatch ? TestParquetDFSSourceEmptyBatch.class.getName()
            : ParquetDFSSource.class.getName(),
        transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
        useSchemaProvider, 100000, false, null, null, "timestamp", null);
    cfg.continuousMode = true;
    cfg.configs.add(DataSourceWriteOptions.ENABLE_ROW_WRITER().key() + "=true");
    cfg.configs.addAll(customConfigs);

    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    // trigger continuous DS and wait until 1 replace commit is complete.
    try {
      deltaStreamerTestRunner(ds, cfg, (r) -> {
        TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
        return true;
      });
      // There should be 4 commits, one of which should be a replace commit
      TestHelpers.assertAtLeastNCommits(4, tableBasePath);
      TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath);
    } finally {
      // clean up resources
      ds.shutdownGracefully();
      inputGenerationFuture.cancel(true);
      UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
      executor.shutdown();
    }
    testNum++;
  }

  @Disabled("HUDI-6609")
  @Test
  public void testDeltaStreamerMultiwriterCheckpoint() throws Exception {
    // prep parquet source
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesMultiCheckpoint" + testNum;
    int parquetRecords = 100;
    HoodieTestDataGenerator dataGenerator = prepareParquetDFSFiles(parquetRecords, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, true,
        HoodieTestDataGenerator.TRIP_SCHEMA, HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);

    prepareParquetDFSSource(true, true, "source_uber.avsc", "target_uber.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "driver");

    // delta streamer w/ parquet source
    String tableBasePath = basePath + "/test_multi_checkpoint" + testNum;
    HoodieDeltaStreamer.Config parquetCfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
        Collections.emptyList(), PROPS_FILENAME_TEST_PARQUET, false,
        true, Integer.MAX_VALUE, false, null, null, "timestamp", null);
    parquetCfg.configs = new ArrayList<>();
    // parquetCfg.configs.add(MUTLI_WRITER_SOURCE_CHECKPOINT_ID.key() + "=parquet");
    //parquetCfg.continuousMode = false;
    HoodieDeltaStreamer parquetDs = new HoodieDeltaStreamer(parquetCfg, jsc);
    parquetDs.sync();
    assertRecordCount(100, tableBasePath, sqlContext);

    // prep json kafka source
    topicName = "topic" + testNum;
    prepareJsonKafkaDFSFiles(20, true, topicName);
    Map<String, String> kafkaExtraProps = new HashMap<>();
    // kafkaExtraProps.put(MUTLI_WRITER_SOURCE_CHECKPOINT_ID.key(), "kafka");
    prepareJsonKafkaDFSSource(PROPS_FILENAME_TEST_JSON_KAFKA, "earliest", topicName, kafkaExtraProps, false);
    // delta streamer w/ json kafka source
    HoodieDeltaStreamer kafkaDs = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
            Collections.emptyList(), PROPS_FILENAME_TEST_JSON_KAFKA, false,
            true, Integer.MAX_VALUE, false, null, null, "timestamp", null), jsc);
    kafkaDs.sync();
    int totalExpectedRecords = parquetRecords + 20;
    assertRecordCount(totalExpectedRecords, tableBasePath, sqlContext);
    //parquet again
    prepareParquetDFSUpdates(parquetRecords, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, true, HoodieTestDataGenerator.TRIP_SCHEMA, HoodieTestDataGenerator.AVRO_TRIP_SCHEMA,
        dataGenerator, "001");
    parquetDs = new HoodieDeltaStreamer(parquetCfg, jsc);
    parquetDs.sync();
    assertRecordCount(parquetRecords * 2 + 20, tableBasePath, sqlContext);

    HoodieTableMetaClient metaClient = HoodieTestUtils.init(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), tableBasePath);
    List<HoodieInstant> instants = metaClient.getCommitsTimeline().getInstants();

    ObjectMapper objectMapper = new ObjectMapper();
    HoodieCommitMetadata commitMetadata =
        metaClient.getCommitsTimeline().readCommitMetadata(instants.get(0));
    Map<String, String> checkpointVals = objectMapper.readValue(commitMetadata.getExtraMetadata().get(HoodieDeltaStreamer.CHECKPOINT_KEY), Map.class);

    String parquetFirstcheckpoint = checkpointVals.get("parquet");
    assertNotNull(parquetFirstcheckpoint);
    commitMetadata = metaClient.getCommitsTimeline().readCommitMetadata(instants.get(1));
    checkpointVals = objectMapper.readValue(commitMetadata.getExtraMetadata().get(HoodieDeltaStreamer.CHECKPOINT_KEY), Map.class);
    String kafkaCheckpoint = checkpointVals.get("kafka");
    assertNotNull(kafkaCheckpoint);
    assertEquals(parquetFirstcheckpoint, checkpointVals.get("parquet"));

    commitMetadata = metaClient.getCommitsTimeline().readCommitMetadata(instants.get(2));
    checkpointVals = objectMapper.readValue(commitMetadata.getExtraMetadata().get(HoodieDeltaStreamer.CHECKPOINT_KEY), Map.class);
    String parquetSecondCheckpoint = checkpointVals.get("parquet");
    assertNotNull(parquetSecondCheckpoint);
    assertEquals(kafkaCheckpoint, checkpointVals.get("kafka"));
    assertTrue(Long.parseLong(parquetSecondCheckpoint) > Long.parseLong(parquetFirstcheckpoint));
    parquetDs.shutdownGracefully();
    kafkaDs.shutdownGracefully();
  }

  @Test
  public void testJdbcSourceIncrementalFetchInContinuousMode() {
    try (Connection connection = DriverManager.getConnection(JdbcTestUtils.JDBC_URL, JdbcTestUtils.JDBC_USER, JdbcTestUtils.JDBC_PASS)) {
      TypedProperties props = new TypedProperties();
      props.setProperty("hoodie.streamer.jdbc.url", JdbcTestUtils.JDBC_URL);
      props.setProperty("hoodie.streamer.jdbc.driver.class", JdbcTestUtils.JDBC_DRIVER);
      props.setProperty("hoodie.streamer.jdbc.user", JdbcTestUtils.JDBC_USER);
      props.setProperty("hoodie.streamer.jdbc.password", JdbcTestUtils.JDBC_PASS);
      props.setProperty("hoodie.streamer.jdbc.table.name", "triprec");
      props.setProperty("hoodie.streamer.jdbc.incr.pull", "true");
      props.setProperty("hoodie.streamer.jdbc.table.incr.column.name", "id");

      props.setProperty("hoodie.datasource.write.recordkey.field", "ID");

      UtilitiesTestBase.Helpers.savePropsToDFS(props, storage,
          basePath + "/test-jdbc-source.properties");

      int numRecords = 1000;
      int sourceLimit = 100;
      String tableBasePath = basePath + "/triprec";
      HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, JdbcSource.class.getName(),
          null, "test-jdbc-source.properties", false,
          false, sourceLimit, false, null, null, "timestamp", null);
      cfg.continuousMode = true;
      // Add 1000 records
      JdbcTestUtils.clearAndInsert("000", numRecords, connection, new HoodieTestDataGenerator(), props);

      HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg, jsc);
      deltaStreamerTestRunner(deltaStreamer, cfg, (r) -> {
        TestHelpers.assertAtleastNCompactionCommits(numRecords / sourceLimit + ((numRecords % sourceLimit == 0) ? 0 : 1), tableBasePath);
        assertRecordCount(numRecords, tableBasePath, sqlContext);
        return true;
      });
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testConfigurationHotUpdate(HoodieTableType tableType) throws Exception {
    HoodieRecordType recordType = HoodieRecordType.AVRO;
    String tableBasePath = basePath + String.format("/configurationHotUpdate_%s_%s", tableType.name(), recordType.name());

    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    addRecordMerger(recordType, cfg.configs);
    cfg.continuousMode = true;
    cfg.tableType = tableType.name();
    cfg.configHotUpdateStrategyClass = MockConfigurationHotUpdateStrategy.class.getName();
    long upsertParallelism = 200;
    cfg.configs.add(String.format("%s=%s", HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key(), upsertParallelism));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath);
      // make sure the UPSERT_PARALLELISM_VALUE already changed (hot updated)
      Assertions.assertTrue(((HoodieStreamer.StreamSyncService) ds.getIngestionService()).getProps().getLong(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key()) > upsertParallelism);
      return true;
    });
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }
}
