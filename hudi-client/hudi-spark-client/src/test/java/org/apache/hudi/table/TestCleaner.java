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

package org.apache.hudi.table;

import org.apache.hudi.HoodieTestCommitGenerator;
import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.timeline.versioning.v2.TimelineArchiverV2;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataMigrator;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanMigrator;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV1MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.clean.CleanPlanner;
import org.apache.hudi.testutils.HoodieCleanerTestBase;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple3;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cleaning related logic.
 */
public class TestCleaner extends HoodieCleanerTestBase {

  private static final int BIG_BATCH_INSERT_SIZE = 500;
  private static final int PARALLELISM = 10;

  /**
   * Helper method to do first batch of insert for clean by versions/commits tests.
   *
   * @param context           Spark engine context
   * @param metaClient        Hoodie table meta client
   * @param client            Hoodie Client
   * @param recordGenFunction Function to generate records for insertion
   * @param insertFn          Insertion API for testing
   * @throws Exception in case of error
   */
  public static Pair<String, JavaRDD<WriteStatus>> insertFirstBigBatchForClientCleanerTest(
      HoodieSparkEngineContext context,
      HoodieTableMetaClient metaClient,
      SparkRDDWriteClient client,
      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn) throws Exception {

    /*
     * do a big insert (this is basically same as insert part of upsert, just adding it here so we can catch breakages
     * in insert(), if the implementation diverges.)
     */
    String newCommitTime = client.startCommit();

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, BIG_BATCH_INSERT_SIZE);
    JavaRDD<HoodieRecord> writeRecords = context.getJavaSparkContext().parallelize(records, PARALLELISM);

    List<WriteStatus> statusList = insertFn.apply(client, writeRecords, newCommitTime).collect();
    client.commit(newCommitTime, context.getJavaSparkContext().parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);

    // verify that there is a commit
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient).getCommitAndReplaceTimeline();
    assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(), "Expecting a single commit.");
    // Should have 100 records in table (check using Index), all in locations marked at commit
    HoodieTable table = HoodieSparkTable.create(client.getConfig(), context, metaClient);

    // We no longer write empty cleaner plans when there is nothing to be cleaned.
    assertTrue(table.getCompletedCleanTimeline().empty());
    return Pair.of(newCommitTime, context.getJavaSparkContext().parallelize(statusList));
  }

  /**
   * Helper method to do first batch of insert for clean by versions/commits tests.
   *
   * @param context           Spark engine context
   * @param client            Hoodie Client
   * @param recordGenFunction Function to generate records for insertion
   * @param insertFn          Insertion API for testing
   * @throws Exception in case of error
   */
  public static Pair<String, JavaRDD<WriteStatus>> insertFirstFailedBigBatchForClientCleanerTest(
      HoodieSparkEngineContext context,
      SparkRDDWriteClient client,
      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn) throws Exception {

    /*
     * do a big insert (this is basically same as insert part of upsert, just adding it here so we can catch breakages
     * in insert(), if the implementation diverges.)
     */
    String newCommitTime = client.startCommit();

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, BIG_BATCH_INSERT_SIZE);
    JavaRDD<HoodieRecord> writeRecords = context.getJavaSparkContext().parallelize(records, 5);

    JavaRDD<WriteStatus> statuses = insertFn.apply(client, writeRecords, newCommitTime);
    // Verify there are no errors
    assertNoWriteErrors(statuses.collect());
    // Don't invoke commit to simulate failed write
    client.getHeartbeatClient().stop(newCommitTime);
    return Pair.of(newCommitTime, statuses);
  }

  /**
   * Test Clean-Failed-Writes when Cleaning policy is by VERSIONS using insert/upsert API.
   */
  @Test
  public void testInsertAndCleanFailedWritesByVersions() throws Exception {
    testInsertAndCleanFailedWritesByVersions(SparkRDDWriteClient::insert, false);
  }

  /**
   * Test Helper for cleaning failed writes by versions logic from HoodieWriteClient API perspective.
   *
   * @param insertFn     Insert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   *                     record generation to also tag the regards (de-dupe is implicit as we use unique record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testInsertAndCleanFailedWritesByVersions(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn, boolean isPreppedAPI)
      throws Exception {
    int maxVersions = 3; // keep upto 3 versions for each file
    HoodieWriteConfig cfg = getConfigBuilder()
        .withHeartbeatIntervalInMs(3000)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(maxVersions).build())
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1).withDeleteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      final Function2<List<HoodieRecord>, String, Integer> recordInsertGenWrappedFunction =
          generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateInserts);
      insertFirstBigBatchForClientCleanerTest(context, metaClient, client, recordInsertGenWrappedFunction, insertFn);

      HoodieTable table = HoodieSparkTable.create(client.getConfig(), context, metaClient);
      assertTrue(table.getCompletedCleanTimeline().empty());

      insertFirstFailedBigBatchForClientCleanerTest(context, client, recordInsertGenWrappedFunction, insertFn);

      insertFirstFailedBigBatchForClientCleanerTest(context, client, recordInsertGenWrappedFunction, insertFn);

      Pair<String, JavaRDD<WriteStatus>> ret =
          insertFirstFailedBigBatchForClientCleanerTest(context, client, recordInsertGenWrappedFunction, insertFn);

      // Await till enough time passes such that the last failed commits heartbeats are expired
      await().atMost(10, TimeUnit.SECONDS).until(() -> client.getHeartbeatClient()
          .isHeartbeatExpired(ret.getLeft()));

      List<HoodieCleanStat> cleanStats = runCleaner(cfg);
      assertEquals(0, cleanStats.size(), "Must not clean any files");
      HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
      assertTrue(timeline.getTimelineOfActions(
          CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).filterCompletedInstants().countInstants() == 3);
      Option<HoodieInstant> rollBackInstantForFailedCommit = timeline.getTimelineOfActions(
          CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).filterCompletedInstants().lastInstant();
      HoodieRollbackMetadata rollbackMetadata =
          timeline.readRollbackMetadata(rollBackInstantForFailedCommit.get());
      // Rollback of one of the failed writes should have deleted 3 files
      assertEquals(3, rollbackMetadata.getTotalFilesDeleted());
    }
  }

  /**
   * Test earliest commit to retain should be earlier than first pending compaction in incremental cleaning scenarios.
   *
   * @throws IOException
   */
  @Test
  public void testEarliestInstantToRetainForPendingCompaction() throws IOException {
    HoodieWriteConfig writeConfig = getConfigBuilder().withPath(basePath)
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAutoClean(false)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(1)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .compactionSmallFileSize(1024 * 1024 * 1024)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .withAutoArchive(false)
            .archiveCommitsWith(2, 3)
            .build())
        .withEmbeddedTimelineServerEnabled(false).build();

    HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, writeConfig)) {

      final String partition1 = "2023/06/01";
      final String partition2 = "2023/06/02";
      String instantTime = "";
      String earliestInstantToRetain = "";

      for (int idx = 0; idx < 3; ++idx) {
        instantTime = client.startCommit();
        if (idx == 2) {
          earliestInstantToRetain = instantTime;
        }
        List<HoodieRecord> records = dataGen.generateInsertsForPartition(instantTime, 1, partition1);
        JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(jsc.parallelize(records, 1), instantTime);
        client.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      }

      Pair<String, HoodieCleanerPlan> cleanPlanPair = scheduleCleaning(client);
      instantTime = cleanPlanPair.getLeft();
      HoodieCleanerPlan cleanPlan = cleanPlanPair.getRight();
      assertEquals(cleanPlan.getFilePathsToBeDeletedPerPartition().get(partition1).size(), 1);
      assertEquals(earliestInstantToRetain, cleanPlan.getEarliestInstantToRetain().getTimestamp(),
          "clean until " + earliestInstantToRetain);
      HoodieTable table = HoodieSparkTable.create(writeConfig, context);
      // FIXME-vc: hacky
      table.setTxnManager(new TransactionManager(writeConfig, metaClient.getStorage()));
      table.clean(context, instantTime);

      instantTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInsertsForPartition(instantTime, 1, partition1);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(recordsRDD, instantTime);
      client.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty());

      instantTime = client.startCommit();
      earliestInstantToRetain = instantTime;
      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(instantTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
      SparkRDDReadClient readClient = new SparkRDDReadClient(context, writeConfig);
      JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = readClient.tagLocation(updatedRecordsRDD);
      writeStatusJavaRDD = client.upsertPreppedRecords(updatedTaggedRecordsRDD, instantTime);
      client.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty());

      table.getMetaClient().reloadActiveTimeline();
      // pending compaction
      String compactionInstantTime = client.scheduleCompaction(Option.empty()).get().toString();

      for (int idx = 0; idx < 3; ++idx) {
        instantTime = client.startCommit();
        records = dataGen.generateInsertsForPartition(instantTime, 1, partition2);
        writeStatusJavaRDD = client.insert(jsc.parallelize(records, 1), instantTime);
        client.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      }

      // earliest commit to retain should be earlier than first pending compaction in incremental cleaning scenarios.
      cleanPlan = scheduleCleaning(client).getRight();
      assertEquals(earliestInstantToRetain, cleanPlan.getEarliestInstantToRetain().getTimestamp());
    }
  }

  /**
   * Test clean non-partitioned table.
   * This test is to ensure that the clean action does not clean the whole table data.
   */
  @Test
  public void testCleanNonPartitionedTable() throws IOException {
    HoodieWriteConfig writeConfig = getConfigBuilder().withPath(basePath)
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAutoClean(false)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(1)
            .build())
        .withEmbeddedTimelineServerEnabled(false).build();
    // datagen for non-partitioned table
    initTestDataGenerator(new String[] {HoodieTestDataGenerator.NO_PARTITION_PATH});
    // init non-partitioned table
    HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, HoodieFileFormat.PARQUET,
        true, "org.apache.hudi.keygen.NonpartitionedKeyGenerator", true);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, writeConfig)) {
      String instantTime;
      for (int idx = 0; idx < 3; ++idx) {
        instantTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(instantTime, 1);
        JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(jsc.parallelize(records, 1), instantTime);
        client.commit(instantTime, writeStatusJavaRDD, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      }

      Pair<String, HoodieCleanerPlan> cleanPlanPair = scheduleCleaning(client);
      instantTime = cleanPlanPair.getLeft();
      HoodieCleanerPlan cleanPlan = cleanPlanPair.getRight();
      assertEquals(cleanPlan.getPartitionsToBeDeleted().size(), 0);
      assertEquals(cleanPlan.getFilePathsToBeDeletedPerPartition().get(HoodieTestDataGenerator.NO_PARTITION_PATH).size(), 1);
      String filePathToClean = cleanPlan.getFilePathsToBeDeletedPerPartition().get(HoodieTestDataGenerator.NO_PARTITION_PATH).get(0).getFilePath();
      // clean
      HoodieTable table = HoodieSparkTable.create(writeConfig, context);
      // FIXME-vc: hacky
      table.setTxnManager(new TransactionManager(writeConfig, metaClient.getStorage()));
      HoodieCleanMetadata cleanMetadata = table.clean(context, instantTime);
      // check the cleaned file
      assertEquals(cleanMetadata.getPartitionMetadata().get(HoodieTestDataGenerator.NO_PARTITION_PATH).getSuccessDeleteFiles().size(), 1);
      assertTrue(filePathToClean.contains(cleanMetadata.getPartitionMetadata().get(HoodieTestDataGenerator.NO_PARTITION_PATH).getSuccessDeleteFiles().get(0)));
      // ensure table is not fully cleaned and has a file group
      assertTrue(FSUtils.isTableExists(basePath, storage));
      assertTrue(table.getFileSystemView().getAllFileGroups(HoodieTestDataGenerator.NO_PARTITION_PATH).findAny().isPresent());
    }
  }

  /**
   * Tests no more than 1 clean is scheduled if hoodie.clean.multiple.enabled config is set to false.
   */
  @Test
  public void testMultiClean() throws IOException {
    HoodieWriteConfig writeConfig = getConfigBuilder()
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
            .allowMultipleCleans(false)
            .withAutoClean(false).retainCommits(1).retainFileVersions(1)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .withEmbeddedTimelineServerEnabled(false).build();

    int index = 0;
    String cleanInstantTime;
    final String partition = "2015/03/16";
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, writeConfig)) {
      // Three writes so we can initiate a clean
      for (; index < 3; ++index) {
        String newCommitTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 1, partition);
        JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(jsc.parallelize(records, 1), newCommitTime);
        client.commit(newCommitTime, writeStatusJavaRDD, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      }
    }

    try (SparkRDDWriteClient<?> client = new SparkRDDWriteClient<>(context, writeConfig)) {
      // mimic failed/leftover clean by scheduling a clean but not performing it
      Pair<String, HoodieCleanerPlan> cleanPlanPair = scheduleCleaning(client);
      cleanInstantTime = cleanPlanPair.getLeft();
      HoodieCleanerPlan cleanPlan = cleanPlanPair.getRight();
      assertEquals(cleanPlan.getFilePathsToBeDeletedPerPartition().get(partition).size(), 1);
      assertEquals(metaClient.reloadActiveTimeline().getCleanerTimeline().filterInflightsAndRequested().countInstants(), 1);
    }

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, writeConfig)) {
      // Next commit. This is required so that there is an additional file version to clean.
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 1, partition);
      JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(jsc.parallelize(records, 1), newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

      // Try to schedule another clean
      HoodieCleanMetadata cleanMetadata = client.clean();
      // When hoodie.clean.multiple.enabled is set to false, a new clean action should not be scheduled.
      // The existing requested clean should complete execution.
      assertNotNull(cleanMetadata);
      assertTrue(metaClient.reloadActiveTimeline().getCleanerTimeline()
          .filterCompletedInstants().containsInstant(cleanInstantTime));
      assertEquals(1, metaClient.getActiveTimeline().getCleanerTimeline().countInstants());

      // 1 file cleaned
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getSuccessDeleteFiles().size(), 1);
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getFailedDeleteFiles().size(), 0);
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns().size(), 1);

      // Now that there is no requested or inflight clean instant, a new clean action can be scheduled
      cleanMetadata = client.clean();
      assertNotNull(cleanMetadata);
      assertEquals(2, metaClient.reloadActiveTimeline().getCleanerTimeline().countInstants());

      // 1 file cleaned
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getSuccessDeleteFiles().size(), 1);
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getFailedDeleteFiles().size(), 0);
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns().size(), 1);
    }
  }

  /**
   * Test Clean-By-Commits using insert/upsert API.
   */
  @Test
  public void testFailedInsertAndCleanByCommits() throws Exception {
    testFailedInsertAndCleanByCommits(SparkRDDWriteClient::insert, false);
  }

  /**
   * Test Helper for Cleaning failed commits by commits logic from HoodieWriteClient API perspective.
   *
   * @param insertFn     Insert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   *                     record generation to also tag the regards (de-dupe is implicit as we use uniq record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testFailedInsertAndCleanByCommits(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn, boolean isPreppedAPI)
      throws Exception {
    int maxCommits = 3; // keep upto 3 commits from the past
    HoodieWriteConfig cfg = getConfigBuilder()
        .withHeartbeatIntervalInMs(3000)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(maxCommits).build())
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1).withDeleteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    final Function2<List<HoodieRecord>, String, Integer> recordInsertGenWrappedFunction =
        generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateInserts);
    insertFirstBigBatchForClientCleanerTest(context, metaClient, client, recordInsertGenWrappedFunction, insertFn);

    HoodieTable table = HoodieSparkTable.create(client.getConfig(), context, metaClient);
    assertTrue(table.getCompletedCleanTimeline().empty());

    insertFirstFailedBigBatchForClientCleanerTest(context, client, recordInsertGenWrappedFunction, insertFn);

    insertFirstFailedBigBatchForClientCleanerTest(context, client, recordInsertGenWrappedFunction, insertFn);

    Pair<String, JavaRDD<WriteStatus>> ret =
        insertFirstFailedBigBatchForClientCleanerTest(context, client, recordInsertGenWrappedFunction, insertFn);
    // Await till enough time passes such that the last failed commits heartbeats are expired
    await().atMost(10, TimeUnit.SECONDS).until(() -> client.getHeartbeatClient()
        .isHeartbeatExpired(ret.getLeft()));
    List<HoodieCleanStat> cleanStats = runCleaner(cfg);
    assertEquals(0, cleanStats.size(), "Must not clean any files");
    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    assertTrue(timeline.getTimelineOfActions(
        CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).filterCompletedInstants().countInstants() == 3);
    Option<HoodieInstant> rollBackInstantForFailedCommit = timeline.getTimelineOfActions(
        CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).filterCompletedInstants().lastInstant();
    HoodieRollbackMetadata rollbackMetadata =
        timeline.readRollbackMetadata(rollBackInstantForFailedCommit.get());
    // Rollback of one of the failed writes should have deleted 3 files
    assertEquals(3, rollbackMetadata.getTotalFilesDeleted());
  }

  @Test
  public void testCleanEmptyInstants() throws Exception {
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder()
            .withPath(basePath)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).build())
            .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    int commitCount = 20;
    int cleanCount = 20;

    int startInstant = 1;

    for (int i = 0; i < cleanCount; i++, startInstant++) {
      String commitTime = HoodieTestTable.makeNewCommitTime(startInstant, "%09d");
      createEmptyCleanMetadata(commitTime + "", false);
    }

    int instantClean = startInstant;

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);

    try {
      for (int i = 0; i < commitCount; i++, startInstant++) {
        String commitTime = HoodieTestTable.makeNewCommitTime(startInstant, "%09d");
        commitWithMdt(commitTime, Collections.emptyMap(), testTable, config);
      }

      List<HoodieCleanStat> cleanStats = runCleaner(config);
      HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();

      assertEquals(0, cleanStats.size(), "Must not clean any files");
      assertEquals(0, timeline.getCleanerTimeline().countInstants(), "All empty clean instants should be removed");
    } finally {
      testTable.close();
    }
  }

  @Test
  public void testCleanWithReplaceCommits() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMetadataIndexColumnStats(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(2).build())
        .build();

    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, getMetadataWriter(config), Option.of(context));
    String p0 = "2020/01/01";
    String p1 = "2020/01/02";

    // make 1 commit, with 1 file per partition
    String file1P0C0 = UUID.randomUUID().toString();
    String file1P1C0 = UUID.randomUUID().toString();
    Map<String, List<String>> part1ToFileId = Collections.unmodifiableMap(new HashMap<String, List<String>>() {
      {
        put(p0, CollectionUtils.createImmutableList(file1P0C0));
        put(p1, CollectionUtils.createImmutableList(file1P1C0));
      }
    });
    commitWithMdt("00000000000001", part1ToFileId, testTable, config, true, true);
    testTable = tearDownTestTableAndReinit(testTable, config);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    List<HoodieCleanStat> hoodieCleanStatsOne = runCleanerWithInstantFormat(config, true);
    assertEquals(0, hoodieCleanStatsOne.size(), "Must not scan any partitions and clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));

    // make next replacecommit, with 1 clustering operation. logically delete p0. No change to p1
    // notice that clustering generates empty inflight commit files
    Map<String, String> partitionAndFileId002 = testTable.forReplaceCommit("00000000000002").getFileIdsWithBaseFilesInPartitions(p0);
    String file2P0C1 = partitionAndFileId002.get(p0);
    Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> replaceMetadata =
        generateReplaceCommitMetadata("00000000000002", p0, file1P0C0, file2P0C1);
    testTable.addCluster("00000000000002", replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue());

    // run cleaner
    List<HoodieCleanStat> hoodieCleanStatsTwo = runCleanerWithInstantFormat(config, true);
    assertEquals(0, hoodieCleanStatsTwo.size(), "Must not scan any partitions and clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));

    // make next replacecommit, with 1 clustering operation. Replace data in p1. No change to p0
    // notice that clustering generates empty inflight commit files
    Map<String, String> partitionAndFileId003 = testTable.forReplaceCommit("00000000000003").getFileIdsWithBaseFilesInPartitions(p1);
    String file3P1C2 = partitionAndFileId003.get(p1);
    replaceMetadata = generateReplaceCommitMetadata("00000000000003", p1, file1P1C0, file3P1C2);
    testTable.addCluster("00000000000003", replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue());

    // run cleaner
    List<HoodieCleanStat> hoodieCleanStatsThree = runCleanerWithInstantFormat(config, true);
    assertEquals(0, hoodieCleanStatsThree.size(), "Must not scan any partitions and clean any files");
    assertTrue(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertTrue(testTable.baseFileExists(p1, "00000000000003", file3P1C2));
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));

    // make next replacecommit, with 1 clustering operation. Replace data in p0 again
    // notice that clustering generates empty inflight commit files
    Map<String, String> partitionAndFileId004 = testTable.forReplaceCommit("00000000000004").getFileIdsWithBaseFilesInPartitions(p0);
    String file4P0C3 = partitionAndFileId004.get(p0);
    replaceMetadata = generateReplaceCommitMetadata("00000000000004", p0, file2P0C1, file4P0C3);
    testTable.addCluster("00000000000004", replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue());

    // run cleaner
    List<HoodieCleanStat> hoodieCleanStatsFour = runCleaner(config, 5, true);
    assertTrue(testTable.baseFileExists(p0, "00000000000004", file4P0C3));
    assertTrue(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p1, "00000000000003", file3P1C2));
    assertFalse(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    //file1P1C0 still stays because its not replaced until 3 and its the only version available
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));

    // make next replacecommit, with 1 clustering operation. Replace all data in p1. no new files created
    // notice that clustering generates empty inflight commit files
    Map<String, String> partitionAndFileId005 = testTable.forReplaceCommit("00000000000006").getFileIdsWithBaseFilesInPartitions(p1);
    String file4P1C4 = partitionAndFileId005.get(p1);
    replaceMetadata = generateReplaceCommitMetadata("00000000000006", p0, file3P1C2, file4P1C4);
    testTable.addCluster("00000000000006", replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue());

    List<HoodieCleanStat> hoodieCleanStatsFive = runCleaner(config, 7, true);
    assertTrue(testTable.baseFileExists(p0, "00000000000004", file4P0C3));
    assertTrue(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p1, "00000000000003", file3P1C2));
    assertFalse(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    assertFalse(testTable.baseFileExists(p1, "00000000000001", file1P1C0));
  }

  private Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> generateReplaceCommitMetadata(
      String instantTime, String partition, String replacedFileId, String newFileId) {
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(WriteOperationType.CLUSTER.toString());
    requestedReplaceMetadata.setVersion(1);
    HoodieSliceInfo sliceInfo = HoodieSliceInfo.newBuilder().setFileId(replacedFileId).build();
    List<HoodieClusteringGroup> clusteringGroups = new ArrayList<>();
    clusteringGroups.add(HoodieClusteringGroup.newBuilder()
        .setVersion(1).setNumOutputFileGroups(1).setMetrics(Collections.emptyMap())
        .setSlices(Collections.singletonList(sliceInfo)).build());
    requestedReplaceMetadata.setExtraMetadata(Collections.emptyMap());
    requestedReplaceMetadata.setClusteringPlan(HoodieClusteringPlan.newBuilder()
        .setVersion(1).setExtraMetadata(Collections.emptyMap())
        .setStrategy(HoodieClusteringStrategy.newBuilder().setStrategyClassName("").setVersion(1).build())
        .setInputGroups(clusteringGroups).build());

    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    replaceMetadata.addReplaceFileId(partition, replacedFileId);
    replaceMetadata.setOperationType(WriteOperationType.CLUSTER);
    if (!StringUtils.isNullOrEmpty(newFileId)) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(partition);
      writeStat.setPath(partition + "/" + HoodieTestCommitGenerator.getBaseFilename(instantTime, newFileId));
      writeStat.setFileId(newFileId);
      writeStat.setTotalWriteBytes(1);
      writeStat.setFileSizeInBytes(1);
      replaceMetadata.addWriteStat(partition, writeStat);
    }
    return Pair.of(requestedReplaceMetadata, replaceMetadata);
  }

  @Test
  public void testCleanMetadataUpgradeDowngrade() {
    String instantTime = "000";

    String partition1 = HoodieTestUtils.DEFAULT_PARTITION_PATHS[0];
    String partition2 = HoodieTestUtils.DEFAULT_PARTITION_PATHS[1];

    String extension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    String fileName1 = "data1_1_000" + extension;
    String fileName2 = "data2_1_000" + extension;

    String filePath1 = metaClient.getBasePath() + "/" + partition1 + "/" + fileName1;
    String filePath2 = metaClient.getBasePath() + "/" + partition1 + "/" + fileName2;

    List<String> deletePathPatterns1 = Arrays.asList(filePath1, filePath2);
    List<String> successDeleteFiles1 = Collections.singletonList(filePath1);
    List<String> failedDeleteFiles1 = Collections.singletonList(filePath2);

    // create partition1 clean stat.
    HoodieCleanStat cleanStat1 = new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
        partition1, deletePathPatterns1, successDeleteFiles1,
        failedDeleteFiles1, instantTime, "");

    List<String> deletePathPatterns2 = new ArrayList<>();
    List<String> successDeleteFiles2 = new ArrayList<>();
    List<String> failedDeleteFiles2 = new ArrayList<>();

    // create partition2 empty clean stat.
    HoodieCleanStat cleanStat2 = new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
        partition2, deletePathPatterns2, successDeleteFiles2,
        failedDeleteFiles2, instantTime, "");

    // map with absolute file path.
    Map<String, Tuple3> oldExpected = new HashMap<>();
    oldExpected.put(partition1, new Tuple3<>(deletePathPatterns1, successDeleteFiles1, failedDeleteFiles1));
    oldExpected.put(partition2, new Tuple3<>(deletePathPatterns2, successDeleteFiles2, failedDeleteFiles2));

    // map with relative path.
    Map<String, Tuple3> newExpected = new HashMap<>();
    newExpected.put(partition1, new Tuple3<>(Arrays.asList(fileName1, fileName2), Collections.singletonList(fileName1),
        Collections.singletonList(fileName2)));
    newExpected.put(partition2, new Tuple3<>(deletePathPatterns2, successDeleteFiles2, failedDeleteFiles2));

    HoodieCleanMetadata metadata = CleanerUtils.convertCleanMetadata(
        instantTime,
        Option.of(0L),
        Arrays.asList(cleanStat1, cleanStat2),
        Collections.EMPTY_MAP
    );
    metadata.setVersion(CleanerUtils.CLEAN_METADATA_VERSION_1);

    // NOw upgrade and check
    CleanMetadataMigrator metadataMigrator = new CleanMetadataMigrator(metaClient);
    metadata = metadataMigrator.upgradeToLatest(metadata, metadata.getVersion());
    assertCleanMetadataPathEquals(newExpected, metadata);

    CleanMetadataMigrator migrator = new CleanMetadataMigrator(metaClient);
    HoodieCleanMetadata oldMetadata =
        migrator.migrateToVersion(metadata, metadata.getVersion(), CleanerUtils.CLEAN_METADATA_VERSION_1);
    assertEquals(CleanerUtils.CLEAN_METADATA_VERSION_1, oldMetadata.getVersion());
    assertCleanMetadataEquals(metadata, oldMetadata);
    assertCleanMetadataPathEquals(oldExpected, oldMetadata);

    HoodieCleanMetadata newMetadata = migrator.upgradeToLatest(oldMetadata, oldMetadata.getVersion());
    assertEquals(CleanerUtils.LATEST_CLEAN_METADATA_VERSION, newMetadata.getVersion());
    assertCleanMetadataEquals(oldMetadata, newMetadata);
    assertCleanMetadataPathEquals(newExpected, newMetadata);
    assertCleanMetadataPathEquals(oldExpected, oldMetadata);
  }

  private static void assertCleanMetadataEquals(HoodieCleanMetadata expected, HoodieCleanMetadata actual) {
    assertEquals(expected.getEarliestCommitToRetain(), actual.getEarliestCommitToRetain());
    assertEquals(expected.getStartCleanTime(), actual.getStartCleanTime());
    assertEquals(expected.getTimeTakenInMillis(), actual.getTimeTakenInMillis());
    assertEquals(expected.getTotalFilesDeleted(), actual.getTotalFilesDeleted());

    Map<String, HoodieCleanPartitionMetadata> map1 = expected.getPartitionMetadata();
    Map<String, HoodieCleanPartitionMetadata> map2 = actual.getPartitionMetadata();

    assertEquals(map1.keySet(), map2.keySet());

    List<String> partitions1 = map1.values().stream().map(HoodieCleanPartitionMetadata::getPartitionPath).collect(
        Collectors.toList());
    List<String> partitions2 = map2.values().stream().map(HoodieCleanPartitionMetadata::getPartitionPath).collect(
        Collectors.toList());
    assertEquals(partitions1, partitions2);

    List<String> policies1 = map1.values().stream().map(HoodieCleanPartitionMetadata::getPolicy).collect(Collectors.toList());
    List<String> policies2 = map2.values().stream().map(HoodieCleanPartitionMetadata::getPolicy).collect(Collectors.toList());
    assertEquals(policies1, policies2);
  }

  @Test
  public void testCleanPlanUpgradeDowngrade() {
    String instantTime = "000";

    String partition1 = HoodieTestUtils.DEFAULT_PARTITION_PATHS[0];
    String partition2 = HoodieTestUtils.DEFAULT_PARTITION_PATHS[1];

    String extension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    String fileName1 = "data1_1_000" + extension;
    String fileName2 = "data2_1_000" + extension;

    Map<String, List<String>> filesToBeCleanedPerPartition = new HashMap<>();
    filesToBeCleanedPerPartition.put(partition1, Arrays.asList(fileName1));
    filesToBeCleanedPerPartition.put(partition2, Arrays.asList(fileName2));

    HoodieCleanerPlan version1Plan =
        HoodieCleanerPlan.newBuilder().setEarliestInstantToRetain(HoodieActionInstant.newBuilder()
                .setAction(COMMIT_ACTION)
                .setTimestamp(instantTime).setState(State.COMPLETED.name()).build())
            .setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
            .setFilesToBeDeletedPerPartition(filesToBeCleanedPerPartition)
            .setVersion(CleanPlanV1MigrationHandler.VERSION)
            .build();

    // Upgrade and Verify version 2 plan
    HoodieCleanerPlan version2Plan =
        new CleanPlanMigrator(metaClient).upgradeToLatest(version1Plan, version1Plan.getVersion());
    assertEquals(version1Plan.getEarliestInstantToRetain(), version2Plan.getEarliestInstantToRetain());
    assertEquals(version1Plan.getPolicy(), version2Plan.getPolicy());
    assertEquals(CleanPlanner.LATEST_CLEAN_PLAN_VERSION, version2Plan.getVersion());
    // Deprecated Field is not used.
    assertEquals(0, version2Plan.getFilesToBeDeletedPerPartition().size());
    assertEquals(version1Plan.getFilesToBeDeletedPerPartition().size(),
        version2Plan.getFilePathsToBeDeletedPerPartition().size());
    assertEquals(version1Plan.getFilesToBeDeletedPerPartition().get(partition1).size(),
        version2Plan.getFilePathsToBeDeletedPerPartition().get(partition1).size());
    assertEquals(version1Plan.getFilesToBeDeletedPerPartition().get(partition2).size(),
        version2Plan.getFilePathsToBeDeletedPerPartition().get(partition2).size());
    assertEquals(new StoragePath(FSUtils.constructAbsolutePath(metaClient.getBasePath(), partition1), fileName1).toString(),
        version2Plan.getFilePathsToBeDeletedPerPartition().get(partition1).get(0).getFilePath());
    assertEquals(new StoragePath(FSUtils.constructAbsolutePath(metaClient.getBasePath(), partition2), fileName2).toString(),
        version2Plan.getFilePathsToBeDeletedPerPartition().get(partition2).get(0).getFilePath());

    // Downgrade and verify version 1 plan
    HoodieCleanerPlan gotVersion1Plan = new CleanPlanMigrator(metaClient).migrateToVersion(version2Plan,
        version2Plan.getVersion(), version1Plan.getVersion());
    assertEquals(version1Plan.getEarliestInstantToRetain(), gotVersion1Plan.getEarliestInstantToRetain());
    assertEquals(version1Plan.getPolicy(), version2Plan.getPolicy());
    assertEquals(version1Plan.getVersion(), gotVersion1Plan.getVersion());
    assertEquals(version1Plan.getFilesToBeDeletedPerPartition().size(),
        gotVersion1Plan.getFilesToBeDeletedPerPartition().size());
    assertEquals(version1Plan.getFilesToBeDeletedPerPartition().get(partition1).size(),
        gotVersion1Plan.getFilesToBeDeletedPerPartition().get(partition1).size());
    assertEquals(version1Plan.getFilesToBeDeletedPerPartition().get(partition2).size(),
        gotVersion1Plan.getFilesToBeDeletedPerPartition().get(partition2).size());
    assertEquals(version1Plan.getFilesToBeDeletedPerPartition().get(partition1).get(0),
        gotVersion1Plan.getFilesToBeDeletedPerPartition().get(partition1).get(0));
    assertEquals(version1Plan.getFilesToBeDeletedPerPartition().get(partition2).get(0),
        gotVersion1Plan.getFilesToBeDeletedPerPartition().get(partition2).get(0));
    assertTrue(gotVersion1Plan.getFilePathsToBeDeletedPerPartition().isEmpty());
    assertNull(version1Plan.getFilePathsToBeDeletedPerPartition());
  }

  private static void assertCleanMetadataPathEquals(Map<String, Tuple3> expected, HoodieCleanMetadata actual) {

    Map<String, HoodieCleanPartitionMetadata> partitionMetadataMap = actual.getPartitionMetadata();

    for (Map.Entry<String, HoodieCleanPartitionMetadata> entry : partitionMetadataMap.entrySet()) {
      String partitionPath = entry.getKey();
      HoodieCleanPartitionMetadata partitionMetadata = entry.getValue();

      assertEquals(expected.get(partitionPath)._1(), partitionMetadata.getDeletePathPatterns());
      assertEquals(expected.get(partitionPath)._2(), partitionMetadata.getSuccessDeleteFiles());
      assertEquals(expected.get(partitionPath)._3(), partitionMetadata.getFailedDeleteFiles());
    }
  }

  /**
   * Test Cleaning functionality of table.rollback() API.
   */
  @Test
  public void testCleanMarkerDataFilesOnRollback() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .addRequestedCommit("001")
        .withMarkerFiles("default", 10, IOType.MERGE);
    final int numTempFilesBefore = testTable.listAllFilesInTempFolder().length;
    assertEquals(10, numTempFilesBefore, "Some marker files are created.");

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withPath(basePath).build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    // FIXME-vc: hacky
    table.setTxnManager(new TransactionManager(config, metaClient.getStorage()));
    table.getActiveTimeline().transitionRequestedToInflight(
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, COMMIT_ACTION, "001"), Option.empty());
    metaClient.reloadActiveTimeline();
    HoodieInstant rollbackInstant = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, COMMIT_ACTION, "001");
    table.scheduleRollback(context, "002", rollbackInstant, false, config.shouldRollbackUsingMarkers(), false);
    table.rollback(context, "002", rollbackInstant, true, false);
    final int numTempFilesAfter = testTable.listAllFilesInTempFolder().length;
    assertEquals(0, numTempFilesAfter, "All temp files are deleted.");
  }

  /**
   * Test CLeaner Stat when there are no partition paths.
   */
  @Test
  public void testCleaningWithZeroPartitionPaths() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build())
        .build();

    // Make a commit, although there are no partitionPaths.
    // Example use-case of this is when a client wants to create a table
    // with just some commit metadata, but no data/partitionPaths.
    try (HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context)) {
      HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context));
      testTable.doWriteOperation("001", WriteOperationType.INSERT, Collections.emptyList(), 1);

      metaClient = HoodieTableMetaClient.reload(metaClient);

      List<HoodieCleanStat> hoodieCleanStatsOne = runCleaner(config);
      assertTrue(hoodieCleanStatsOne.isEmpty(), "HoodieCleanStats should be empty for a table with empty partitionPaths");
    }
  }

  /**
   * Test Keep Latest Commits when there are pending compactions.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testKeepLatestCommitsWithPendingCompactions(boolean isAsync) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).withAsyncClean(isAsync).retainCommits(2).build())
        .build();

    // Deletions:
    // . FileId   Base  Logs  Total Retained_Commits  Under_Compaction
    //   FileId7  1     2     3     001,003           false
    //   FileId6  1     2     3     001,003           false
    //   FileId5  1     2     3     001,003           true
    //   FileId4  1     2     3     001,003           true
    //   FileId3  1     2     3     001               true
    //   FileId2  0     0     0     000               true
    //   FileId1  0     0     0     000               false
    testPendingCompactions(config, 15, 9, false);
  }

  /**
   * Test Keep Latest Versions when there are pending compactions.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testKeepLatestVersionsWithPendingCompactions(boolean retryFailure) throws Exception {
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(2).build())
            .build();

    // Deletions:
    // . FileId   Base  Logs  Total Retained_Commits  Under_Compaction
    //   FileId7  5     10    15    009,013           false
    //   FileId6  4     8     12    007,009           false
    //   FileId5  2     4     6     003,005           true
    //   FileId4  1     2     3     001,003           true
    //   FileId3  0     0     0     000,001           true
    //   FileId2  0     0     0     000               true
    //   FileId1  0     0     0     000               false
    testPendingCompactions(config, 36, 9, retryFailure);
  }

  /**
   * Test clean previous corrupted cleanFiles.
   */
  @Test
  public void testCleanPreviousCorruptedCleanFiles() throws IOException {
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder()
            .withPath(basePath)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1).build())
            .build();

    String commitTime = HoodieTestTable.makeNewCommitTime(1, "%09d");
    List<String> cleanerFileNames = Arrays.asList(
        INSTANT_FILE_NAME_GENERATOR.makeRequestedCleanerFileName(commitTime),
        INSTANT_FILE_NAME_GENERATOR.makeInflightCleanerFileName(commitTime));
    for (String f : cleanerFileNames) {
      StoragePath commitFile = new StoragePath(Paths
          .get(metaClient.getBasePath().toString(), HoodieTableMetaClient.METAFOLDER_NAME, f).toString());
      try (OutputStream os = metaClient.getStorage().create(commitFile, true)) {
        // Write empty clean metadata
        os.write(new byte[0]);
      }
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);

    List<HoodieCleanStat> cleanStats = runCleaner(config);
    assertEquals(0, cleanStats.size(), "Must not clean any files");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRerunFailedClean(boolean simulateMetadataFailure) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMetadataIndexColumnStats(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build())
        .build();

    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, getMetadataWriter(config), Option.of(context));
    try {
      String p0 = "2020/01/01";
      String p1 = "2020/01/02";

      // make 1 commit, with 1 file per partition
      String file1P0C0 = UUID.randomUUID().toString();
      String file1P1C0 = UUID.randomUUID().toString();
      Map<String, List<String>> part1ToFileId = Collections.unmodifiableMap(new HashMap<String, List<String>>() {
        {
          put(p0, CollectionUtils.createImmutableList(file1P0C0));
          put(p1, CollectionUtils.createImmutableList(file1P1C0));
        }
      });
      commitWithMdt("00000000000001", part1ToFileId, testTable, config, true, true);
      testTable = tearDownTestTableAndReinit(testTable, config);
      metaClient = HoodieTableMetaClient.reload(metaClient);

      // make next replacecommit, with 1 clustering operation. logically delete p0. No change to p1
      // notice that clustering generates empty inflight commit files
      Map<String, String> partitionAndFileId002 = testTable.forReplaceCommit("00000000000002").getFileIdsWithBaseFilesInPartitions(p0);
      String file2P0C1 = partitionAndFileId002.get(p0);
      Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> replaceMetadata =
          generateReplaceCommitMetadata("00000000000002", p0, file1P0C0, file2P0C1);
      testTable.addCluster("00000000000002", replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue());

      // make next replacecommit, with 1 clustering operation. Replace data in p1. No change to p0
      // notice that clustering generates empty inflight commit files
      Map<String, String> partitionAndFileId003 = testTable.forReplaceCommit("00000000000003").getFileIdsWithBaseFilesInPartitions(p1);
      String file3P1C2 = partitionAndFileId003.get(p1);
      replaceMetadata = generateReplaceCommitMetadata("00000000000003", p1, file1P1C0, file3P1C2);
      testTable.addCluster("00000000000003", replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue());

      // make next replacecommit, with 1 clustering operation. Replace data in p0 again
      // notice that clustering generates empty inflight commit files
      Map<String, String> partitionAndFileId004 = testTable.forReplaceCommit("00000000000004").getFileIdsWithBaseFilesInPartitions(p0);
      String file4P0C3 = partitionAndFileId004.get(p0);
      replaceMetadata = generateReplaceCommitMetadata("00000000000004", p0, file2P0C1, file4P0C3);
      testTable.addCluster("00000000000004", replaceMetadata.getKey(), Option.empty(), replaceMetadata.getValue());

      // run cleaner with failures
      List<HoodieCleanStat> hoodieCleanStats = runCleaner(config, true, simulateMetadataFailure, 5, true);
      assertTrue(testTable.baseFileExists(p0, "00000000000004", file4P0C3));
      assertTrue(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
      assertTrue(testTable.baseFileExists(p1, "00000000000003", file3P1C2));
      assertFalse(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
      //file1P1C0 still stays because its not replaced until 3 and its the only version available
      assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));
    } finally {
      testTable.close();
    }
  }

  /**
   * Test if cleaner will fall back to full clean if commit for incremental clean is archived.
   */
  @Test
  public void testIncrementalFallbackToFullClean() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withCleanConfig(
            HoodieCleanConfig.newBuilder()
                .retainCommits(1)
                .withIncrementalCleaningMode(true)
                .build())
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder()
                .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withArchivalConfig(
            HoodieArchivalConfig.newBuilder()
                .archiveCommitsWith(4, 5).build())
        .withMarkersType(MarkerType.DIRECT.name())
        .withPath(basePath)
        .build();
    // reload because table configs could have been updated
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, getMetadataWriter(config), Option.of(context));
    try {

      String p1 = "part_1";
      String p2 = "part_2";
      testTable.withPartitionMetaFiles(p1, p2);

      // add file partition "part_1"
      String file1P1 = UUID.randomUUID().toString();
      String file2P1 = UUID.randomUUID().toString();
      Map<String, List<String>> part1ToFileId = Collections.unmodifiableMap(new HashMap<String, List<String>>() {
        {
          put(p1, CollectionUtils.createImmutableList(file1P1, file2P1));
        }
      });
      commitWithMdt("10", part1ToFileId, testTable, config);
      testTable.addClean("15");
      commitWithMdt("20", part1ToFileId, testTable, config);

      // add clean instant
      HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""),
          "", "", new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.emptyMap());
      HoodieCleanMetadata cleanMeta = new HoodieCleanMetadata("", 0L, 0,
          "20", "", new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), Collections.emptyMap());
      testTable.addClean("30", cleanerPlan, cleanMeta);

      // add file in partition "part_2"
      String file3P2 = UUID.randomUUID().toString();
      String file4P2 = UUID.randomUUID().toString();
      Map<String, List<String>> part2ToFileId = Collections.unmodifiableMap(new HashMap<String, List<String>>() {
        {
          put(p2, CollectionUtils.createImmutableList(file3P2, file4P2));
        }
      });
      commitWithMdt("30", part2ToFileId, testTable, config);
      testTable = tearDownTestTableAndReinit(testTable, config);
      commitWithMdt("40", part2ToFileId, testTable, config);
      testTable = tearDownTestTableAndReinit(testTable, config);

      // empty commits
      String file5P2 = UUID.randomUUID().toString();
      String file6P2 = UUID.randomUUID().toString();
      part2ToFileId = Collections.unmodifiableMap(new HashMap<String, List<String>>() {
        {
          put(p2, CollectionUtils.createImmutableList(file5P2, file6P2));
        }
      });
      commitWithMdt("50", part2ToFileId, testTable, config);
      testTable = tearDownTestTableAndReinit(testTable, config);
      commitWithMdt("60", part2ToFileId, testTable, config);
      testTable = tearDownTestTableAndReinit(testTable, config);

      // archive commit 1, 2
      HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
      // FIXME-vc: hacky
      table.setTxnManager(new TransactionManager(config, metaClient.getStorage()));
      new TimelineArchiverV2<>(config, table).archiveIfRequired(context, false);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertFalse(metaClient.getActiveTimeline().containsInstant("10"));
      assertFalse(metaClient.getActiveTimeline().containsInstant("20"));

      runCleaner(config);
      assertFalse(testTable.baseFileExists(p1, "10", file1P1), "Clean old FileSlice in p1 by fallback to full clean");
      assertFalse(testTable.baseFileExists(p1, "10", file2P1), "Clean old FileSlice in p1 by fallback to full clean");
      assertFalse(testTable.baseFileExists(p2, "30", file3P2), "Clean old FileSlice in p2");
      assertFalse(testTable.baseFileExists(p2, "30", file4P2), "Clean old FileSlice in p2");
      assertTrue(testTable.baseFileExists(p1, "20", file1P1), "Latest FileSlice exists");
      assertTrue(testTable.baseFileExists(p1, "20", file2P1), "Latest FileSlice exists");
      assertTrue(testTable.baseFileExists(p2, "40", file3P2), "Latest FileSlice exists");
      assertTrue(testTable.baseFileExists(p2, "40", file4P2), "Latest FileSlice exists");
    } finally {
      testTable.close();
    }
  }

  /**
   * Common test method for validating pending compactions.
   *
   * @param config             Hoodie Write Config
   * @param expNumFilesDeleted Number of files deleted
   */
  private void testPendingCompactions(HoodieWriteConfig config, int expNumFilesDeleted,
                                      int expNumFilesUnderCompactionDeleted, boolean retryFailure) throws Exception {
    HoodieTableMetaClient metaClient =
        HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);

    final String partition = "2016/03/15";
    String timePrefix = "00000000000";
    Map<String, String> expFileIdToPendingCompaction = new HashMap<String, String>() {
      {
        put("fileId2", timePrefix + "004");
        put("fileId3", timePrefix + "006");
        put("fileId4", timePrefix + "008");
        put("fileId5", timePrefix + "010");
      }
    };
    Map<String, String> fileIdToLatestInstantBeforeCompaction = new HashMap<String, String>() {
      {
        put("fileId1", timePrefix + "000");
        put("fileId2", timePrefix + "000");
        put("fileId3", timePrefix + "001");
        put("fileId4", timePrefix + "003");
        put("fileId5", timePrefix + "005");
        put("fileId6", timePrefix + "009");
        put("fileId7", timePrefix + "013");
      }
    };

    // Generate 7 file-groups. First one has only one slice and no pending compaction. File Slices (2 - 5) has
    // multiple versions with pending compaction. File Slices (6 - 7) have multiple file-slices but not under
    // compactions
    // FileIds 2-5 will be under compaction
    // reload because table configs could have been updated
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    try {

      testTable.withPartitionMetaFiles(partition);

      // add file partition "part_1"
      String file1P1 = "fileId1";
      String file2P1 = "fileId2";
      String file3P1 = "fileId3";
      String file4P1 = "fileId4";
      String file5P1 = "fileId5";
      String file6P1 = "fileId6";
      String file7P1 = "fileId7";

      Map<String, List<String>> part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file1P1, file2P1, file3P1, file4P1, file5P1, file6P1, file7P1));
      // all 7 fileIds
      commitWithMdt(timePrefix + "000", part1ToFileId, testTable, config, true, true);
      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file3P1, file4P1, file5P1, file6P1, file7P1));
      // fileIds 3 to 7
      commitWithMdt(timePrefix + "001", part1ToFileId, testTable, config, true, true);
      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file4P1, file5P1, file6P1, file7P1));
      // fileIds 4 to 7
      commitWithMdt(timePrefix + "003", part1ToFileId, testTable, config, true, true);

      // add compaction
      testTable.addRequestedCompaction(timePrefix + "004", new FileSlice(partition, timePrefix + "000", file2P1));

      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file2P1));
      commitWithMdt(timePrefix + "005", part1ToFileId, testTable, config, false, true);

      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file5P1, file6P1, file7P1));
      commitWithMdt(timePrefix + "0055", part1ToFileId, testTable, config, true, true);

      testTable.addRequestedCompaction(timePrefix + "006", new FileSlice(partition, timePrefix + "001", file3P1));

      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file3P1));
      commitWithMdt(timePrefix + "007", part1ToFileId, testTable, config, false, true);

      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file6P1, file7P1));
      commitWithMdt(timePrefix + "0075", part1ToFileId, testTable, config, true, true);

      testTable.addRequestedCompaction(timePrefix + "008", new FileSlice(partition, timePrefix + "003", file4P1));

      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file4P1));
      commitWithMdt(timePrefix + "009", part1ToFileId, testTable, config, false, true);

      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file6P1, file7P1));
      commitWithMdt(timePrefix + "0095", part1ToFileId, testTable, config, true, true);

      testTable.addRequestedCompaction(timePrefix + "010", new FileSlice(partition, timePrefix + "005", file5P1));

      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file5P1));
      commitWithMdt(timePrefix + "011", part1ToFileId, testTable, config, false, true);

      part1ToFileId = new HashMap<>();
      part1ToFileId.put(partition, Arrays.asList(file7P1));
      commitWithMdt(timePrefix + "013", part1ToFileId, testTable, config, true, true);

      // Clean now
      metaClient = HoodieTableMetaClient.reload(metaClient);
      List<HoodieCleanStat> hoodieCleanStats = runCleaner(config, 14, true);

      // Test for safety
      final HoodieTableMetaClient newMetaClient = HoodieTableMetaClient.reload(metaClient);
      final HoodieTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

      expFileIdToPendingCompaction.forEach((fileId, value) -> {
        String baseInstantForCompaction = fileIdToLatestInstantBeforeCompaction.get(fileId);
        Option<FileSlice> fileSliceForCompaction = Option.fromJavaOptional(hoodieTable.getSliceView()
            .getLatestFileSlicesBeforeOrOn(partition, baseInstantForCompaction,
                true)
            .filter(fs -> fs.getFileId().equals(fileId)).findFirst());
        assertTrue(fileSliceForCompaction.isPresent(), "Base Instant for Compaction must be preserved");
        assertTrue(fileSliceForCompaction.get().getBaseFile().isPresent(), "FileSlice has data-file");
        assertEquals(2, fileSliceForCompaction.get().getLogFiles().count(), "FileSlice has log-files");
      });

      // Test for progress (Did we clean some files ?)
      long numFilesUnderCompactionDeleted = hoodieCleanStats.stream()
          .flatMap(cleanStat -> convertPathToFileIdWithCommitTime(newMetaClient, cleanStat.getDeletePathPatterns())
              .map(fileIdWithCommitTime -> {
                if (expFileIdToPendingCompaction.containsKey(fileIdWithCommitTime.getKey())) {
                  assertTrue(compareTimestamps(
                          fileIdToLatestInstantBeforeCompaction.get(fileIdWithCommitTime.getKey()),
                          GREATER_THAN, fileIdWithCommitTime.getValue()),
                      "Deleted instant time must be less than pending compaction");
                  return true;
                }
                return false;
              })).filter(x -> x).count();
      long numDeleted =
          hoodieCleanStats.stream().mapToLong(cleanStat -> cleanStat.getDeletePathPatterns().size()).sum();
      // Tighter check for regression
      assertEquals(expNumFilesDeleted, numDeleted, "Correct number of files deleted");
      assertEquals(expNumFilesUnderCompactionDeleted, numFilesUnderCompactionDeleted,
          "Correct number of files under compaction deleted");
    } finally {
      testTable.close();
    }
  }

  private Stream<Pair<String, String>> convertPathToFileIdWithCommitTime(final HoodieTableMetaClient metaClient,
                                                                         List<String> paths) {
    Predicate<String> roFilePredicate =
        path -> path.contains(metaClient.getTableConfig().getBaseFileFormat().getFileExtension());
    Predicate<String> rtFilePredicate =
        path -> path.contains(metaClient.getTableConfig().getLogFileFormat().getFileExtension());
    Stream<Pair<String, String>> stream1 = paths.stream().filter(roFilePredicate).map(fullPath -> {
      String fileName = Paths.get(fullPath).getFileName().toString();
      return Pair.of(FSUtils.getFileId(fileName), FSUtils.getCommitTime(fileName));
    });
    Stream<Pair<String, String>> stream2 = paths.stream().filter(rtFilePredicate).map(path -> Pair.of(HadoopFSUtils.getFileIdFromLogPath(new Path(path)),
        FSUtils.getDeltaCommitTimeFromLogPath(new StoragePath(path))));
    return Stream.concat(stream1, stream2);
  }

  private Pair<String, HoodieCleanerPlan> scheduleCleaning(SparkRDDWriteClient<?> client) throws IOException {
    Option<String> cleanInstant = client.scheduleTableService(Option.empty(), TableServiceType.CLEAN);
    HoodieCleanerPlan cleanPlan = metaClient.reloadActiveTimeline().readCleanerPlan(metaClient.createNewInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanInstant.get()));
    return Pair.of(cleanInstant.orElse(null), cleanPlan);
  }
}
