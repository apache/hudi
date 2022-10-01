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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.bootstrap.TestBootstrapIndex;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataMigrator;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanMigrator;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV1MigrationHandler;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.action.clean.CleanPlanner;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple3;

import static org.apache.hudi.HoodieTestCommitGenerator.getBaseFilename;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeIncrementalCommitTimes;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
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
public class TestCleaner extends HoodieClientTestBase {

  private static final int BIG_BATCH_INSERT_SIZE = 500;
  private static final Logger LOG = LogManager.getLogger(TestCleaner.class);

  /**
   * Helper method to do first batch of insert for clean by versions/commits tests.
   *
   * @param cfg Hoodie Write Config
   * @param client Hoodie Client
   * @param recordGenFunction Function to generate records for insertion
   * @param insertFn Insertion API for testing
   * @throws Exception in case of error
   */
  private Pair<String, JavaRDD<WriteStatus>> insertFirstBigBatchForClientCleanerTest(HoodieWriteConfig cfg, SparkRDDWriteClient client,
      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      HoodieCleaningPolicy cleaningPolicy) throws Exception {

    /*
     * do a big insert (this is basically same as insert part of upsert, just adding it here so we can catch breakages
     * in insert(), if the implementation diverges.)
     */
    String newCommitTime = client.startCommit();

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, BIG_BATCH_INSERT_SIZE);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 5);

    JavaRDD<WriteStatus> statuses = insertFn.apply(client, writeRecords, newCommitTime);
    // Verify there are no errors
    assertNoWriteErrors(statuses.collect());
    // verify that there is a commit
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
    assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(), "Expecting a single commit.");
    // Should have 100 records in table (check using Index), all in locations marked at commit
    HoodieTable table = HoodieSparkTable.create(client.getConfig(), context, metaClient);

    if (client.getConfig().getBoolean(HoodieWriteConfig.AUTO_COMMIT_ENABLE)) {
      assertFalse(table.getCompletedCommitsTimeline().empty());
    }
    // We no longer write empty cleaner plans when there is nothing to be cleaned.
    assertTrue(table.getCompletedCleanTimeline().empty());

    if (client.getConfig().getBoolean(HoodieWriteConfig.AUTO_COMMIT_ENABLE)) {
      HoodieIndex index = SparkHoodieIndexFactory.createIndex(cfg);
      List<HoodieRecord> taggedRecords = tagLocation(index, jsc.parallelize(records, 1), table).collect();
      checkTaggedRecords(taggedRecords, newCommitTime);
    }
    return Pair.of(newCommitTime, statuses);
  }

  /**
   * Helper method to do first batch of insert for clean by versions/commits tests.
   *
   * @param cfg Hoodie Write Config
   * @param client Hoodie Client
   * @param recordGenFunction Function to generate records for insertion
   * @param insertFn Insertion API for testing
   * @throws Exception in case of error
   */
  private Pair<String, JavaRDD<WriteStatus>> insertFirstFailedBigBatchForClientCleanerTest(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                                       Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                                       Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
                                                       HoodieCleaningPolicy cleaningPolicy) throws Exception {

    /*
     * do a big insert (this is basically same as insert part of upsert, just adding it here so we can catch breakages
     * in insert(), if the implementation diverges.)
     */
    String newCommitTime = client.startCommit();

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, BIG_BATCH_INSERT_SIZE);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 5);

    JavaRDD<WriteStatus> statuses = insertFn.apply(client, writeRecords, newCommitTime);
    // Verify there are no errors
    assertNoWriteErrors(statuses.collect());
    // Don't invoke commit to simulate failed write
    client.getHeartbeatClient().stop(newCommitTime);
    return Pair.of(newCommitTime, statuses);
  }

  /**
   * Test Clean-By-Versions using insert/upsert API.
   */
  @Test
  public void testInsertAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(SparkRDDWriteClient::insert, SparkRDDWriteClient::upsert, false);
  }

  /**
   * Test Clean-Failed-Writes when Cleaning policy is by VERSIONS using insert/upsert API.
   */
  @Test
  public void testInsertAndCleanFailedWritesByVersions() throws Exception {
    testInsertAndCleanFailedWritesByVersions(SparkRDDWriteClient::insert, false);
  }

  /**
   * Test Clean-By-Versions using prepped versions of insert/upsert API.
   */
  @Test
  public void testInsertPreppedAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(SparkRDDWriteClient::insertPreppedRecords, SparkRDDWriteClient::upsertPreppedRecords,
        true);
  }

  /**
   * Test Clean-By-Versions using bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(SparkRDDWriteClient::bulkInsert, SparkRDDWriteClient::upsert, false);
  }

  /**
   * Test Clean-By-Versions using prepped versions of bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertPreppedAndCleanByVersions() throws Exception {
    testInsertAndCleanByVersions(
        (client, recordRDD, instantTime) -> client.bulkInsertPreppedRecords(recordRDD, instantTime, Option.empty()),
        SparkRDDWriteClient::upsertPreppedRecords, true);
  }


  /**
   * Tests no more than 1 clean is scheduled if hoodie.clean.allow.multiple config is set to false.
   */
  @Test
  public void testMultiClean() {
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
        String newCommitTime = "00" + index;
        List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 1, partition);
        client.startCommitWithTime(newCommitTime);
        client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      }
    }

    // mimic failed/leftover clean by scheduling a clean but not performing it
    cleanInstantTime = "00" + index++;
    HoodieTable table = HoodieSparkTable.create(writeConfig, context);
    Option<HoodieCleanerPlan> cleanPlan = table.scheduleCleaning(context, cleanInstantTime, Option.empty());
    assertEquals(cleanPlan.get().getFilePathsToBeDeletedPerPartition().get(partition).size(), 1);
    assertEquals(metaClient.reloadActiveTimeline().getCleanerTimeline().filterInflightsAndRequested().countInstants(), 1);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, writeConfig)) {
      // Next commit. This is required so that there is an additional file version to clean.
      String newCommitTime = "00" + index++;
      List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 1, partition);
      client.startCommitWithTime(newCommitTime);
      client.insert(jsc.parallelize(records, 1), newCommitTime).collect();

      // Try to schedule another clean
      String newCleanInstantTime = "00" + index++;
      HoodieCleanMetadata cleanMetadata = client.clean(newCleanInstantTime);
      // When hoodie.clean.allow.multiple is set to false, a new clean action should not be scheduled.
      // The existing requested clean should complete execution.
      assertNotNull(cleanMetadata);
      assertTrue(metaClient.reloadActiveTimeline().getCleanerTimeline()
          .filterCompletedInstants().containsInstant(cleanInstantTime));
      assertFalse(metaClient.getActiveTimeline().getCleanerTimeline()
          .containsInstant(newCleanInstantTime));

      // 1 file cleaned
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getSuccessDeleteFiles().size(), 1);
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getFailedDeleteFiles().size(), 0);
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns().size(), 1);

      // Now that there is no requested or inflight clean instant, a new clean action can be scheduled
      cleanMetadata = client.clean(newCleanInstantTime);
      assertNotNull(cleanMetadata);
      assertTrue(metaClient.reloadActiveTimeline().getCleanerTimeline()
          .containsInstant(newCleanInstantTime));

      // 1 file cleaned
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getSuccessDeleteFiles().size(), 1);
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getFailedDeleteFiles().size(), 0);
      assertEquals(cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns().size(), 1);
    }
  }

  /**
   * Test Helper for Cleaning by versions logic from HoodieWriteClient API perspective.
   *
   * @param insertFn Insert API to be tested
   * @param upsertFn Upsert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   *        record generation to also tag the regards (de-dupe is implicit as we use unique record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testInsertAndCleanByVersions(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> upsertFn, boolean isPreppedAPI)
      throws Exception {
    int maxVersions = 2; // keep upto 2 versions for each file
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
            .retainFileVersions(maxVersions).build())
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1).withDeleteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      final Function2<List<HoodieRecord>, String, Integer> recordInsertGenWrappedFunction =
          generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateInserts);

      final Function2<List<HoodieRecord>, String, Integer> recordUpsertGenWrappedFunction =
          generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateUniqueUpdates);

      insertFirstBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
          HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS);

      Map<HoodieFileGroupId, FileSlice> compactionFileIdToLatestFileSlice = new HashMap<>();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
      for (String partitionPath : dataGen.getPartitionPaths()) {
        TableFileSystemView fsView = table.getFileSystemView();
        Option<Boolean> added = Option.fromJavaOptional(fsView.getAllFileGroups(partitionPath).findFirst().map(fg -> {
          fg.getLatestFileSlice().map(fs -> compactionFileIdToLatestFileSlice.put(fg.getFileGroupId(), fs));
          return true;
        }));
        if (added.isPresent()) {
          // Select only one file-group for compaction
          break;
        }
      }

      // Create workload with selected file-slices
      List<Pair<String, FileSlice>> partitionFileSlicePairs = compactionFileIdToLatestFileSlice.entrySet().stream()
          .map(e -> Pair.of(e.getKey().getPartitionPath(), e.getValue())).collect(Collectors.toList());
      HoodieCompactionPlan compactionPlan =
          CompactionUtils.buildFromFileSlices(partitionFileSlicePairs, Option.empty(), Option.empty());
      List<String> instantTimes = makeIncrementalCommitTimes(9, 1, 10);
      String compactionTime = instantTimes.get(0);
      table.getActiveTimeline().saveToCompactionRequested(
          new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionTime),
          TimelineMetadataUtils.serializeCompactionPlan(compactionPlan));

      instantTimes = instantTimes.subList(1, instantTimes.size());
      // Keep doing some writes and clean inline. Make sure we have expected number of files
      // remaining.
      for (String newInstantTime : instantTimes) {
        try {
          client.startCommitWithTime(newInstantTime);
          List<HoodieRecord> records = recordUpsertGenWrappedFunction.apply(newInstantTime, 100);

          List<WriteStatus> statuses = upsertFn.apply(client, jsc.parallelize(records, 1), newInstantTime).collect();
          // Verify there are no errors
          assertNoWriteErrors(statuses);

          metaClient = HoodieTableMetaClient.reload(metaClient);
          table = HoodieSparkTable.create(getConfig(), context, metaClient);
          HoodieTimeline timeline = table.getMetaClient().getCommitsTimeline();

          TableFileSystemView fsView = table.getFileSystemView();
          // Need to ensure the following
          for (String partitionPath : dataGen.getPartitionPaths()) {
            // compute all the versions of all files, from time 0
            HashMap<String, TreeSet<String>> fileIdToVersions = new HashMap<>();
            for (HoodieInstant entry : timeline.getInstants().collect(Collectors.toList())) {
              HoodieCommitMetadata commitMetadata =
                  HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(entry).get(), HoodieCommitMetadata.class);

              for (HoodieWriteStat wstat : commitMetadata.getWriteStats(partitionPath)) {
                if (!fileIdToVersions.containsKey(wstat.getFileId())) {
                  fileIdToVersions.put(wstat.getFileId(), new TreeSet<>());
                }
                fileIdToVersions.get(wstat.getFileId()).add(FSUtils.getCommitTime(new Path(wstat.getPath()).getName()));
              }
            }

            List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());

            for (HoodieFileGroup fileGroup : fileGroups) {
              if (compactionFileIdToLatestFileSlice.containsKey(fileGroup.getFileGroupId())) {
                // Ensure latest file-slice selected for compaction is retained
                Option<HoodieBaseFile> dataFileForCompactionPresent =
                    Option.fromJavaOptional(fileGroup.getAllBaseFiles().filter(df -> {
                      return compactionFileIdToLatestFileSlice.get(fileGroup.getFileGroupId()).getBaseInstantTime()
                          .equals(df.getCommitTime());
                    }).findAny());
                assertTrue(dataFileForCompactionPresent.isPresent(),
                    "Data File selected for compaction is retained");
              } else {
                // file has no more than max versions
                String fileId = fileGroup.getFileGroupId().getFileId();
                List<HoodieBaseFile> dataFiles = fileGroup.getAllBaseFiles().collect(Collectors.toList());

                assertTrue(dataFiles.size() <= maxVersions,
                    "fileId " + fileId + " has more than " + maxVersions + " versions");

                // Each file, has the latest N versions (i.e cleaning gets rid of older versions)
                List<String> commitedVersions = new ArrayList<>(fileIdToVersions.get(fileId));
                for (int i = 0; i < dataFiles.size(); i++) {
                  assertEquals((dataFiles.get(i)).getCommitTime(),
                      commitedVersions.get(commitedVersions.size() - 1 - i),
                      "File " + fileId + " does not have latest versions on commits" + commitedVersions);
                }
              }
            }
          }
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
  }

  /**
   * Test Clean-By-Commits using insert/upsert API.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInsertAndCleanByCommits(boolean isAsync) throws Exception {
    testInsertAndCleanByCommits(SparkRDDWriteClient::insert, SparkRDDWriteClient::upsert, false, isAsync);
  }

  /**
   * Test Clean-By-Commits using insert/upsert API.
   */
  @Test
  public void testFailedInsertAndCleanByCommits() throws Exception {
    testFailedInsertAndCleanByCommits(SparkRDDWriteClient::insert, false);
  }

  /**
   * Test Clean-By-Commits using prepped version of insert/upsert API.
   */
  @Test
  public void testInsertPreppedAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(SparkRDDWriteClient::insertPreppedRecords, SparkRDDWriteClient::upsertPreppedRecords,
        true, false);
  }

  /**
   * Test Clean-By-Commits using prepped versions of bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertPreppedAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(
        (client, recordRDD, instantTime) -> client.bulkInsertPreppedRecords(recordRDD, instantTime, Option.empty()),
        SparkRDDWriteClient::upsertPreppedRecords, true, false);
  }

  /**
   * Test Clean-By-Commits using bulk-insert/upsert API.
   */
  @Test
  public void testBulkInsertAndCleanByCommits() throws Exception {
    testInsertAndCleanByCommits(SparkRDDWriteClient::bulkInsert, SparkRDDWriteClient::upsert, false, false);
  }

  /**
   * Test Helper for Cleaning by versions logic from HoodieWriteClient API perspective.
   *
   * @param insertFn Insert API to be tested
   * @param upsertFn Upsert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   *        record generation to also tag the regards (de-dupe is implicit as we use unique record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testInsertAndCleanByCommits(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> upsertFn, boolean isPreppedAPI, boolean isAsync)
      throws Exception {
    int maxCommits = 3; // keep upto 3 commits from the past
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).withAsyncClean(isAsync).retainCommits(maxCommits).build())
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1).withDeleteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    final Function2<List<HoodieRecord>, String, Integer> recordInsertGenWrappedFunction =
        generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateInserts);

    final Function2<List<HoodieRecord>, String, Integer> recordUpsertGenWrappedFunction =
        generateWrapRecordsFn(isPreppedAPI, cfg, dataGen::generateUniqueUpdates);

    insertFirstBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS);

    // Keep doing some writes and clean inline. Make sure we have expected number of files remaining.
    for (int i = 0; i < 8; i++) {
      String newCommitTime = makeNewCommitTime();
      try {
        client.startCommitWithTime(newCommitTime);
        List<HoodieRecord> records = recordUpsertGenWrappedFunction.apply(newCommitTime, 100);

        List<WriteStatus> statuses = upsertFn.apply(client, jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metaClient = HoodieTableMetaClient.reload(metaClient);
        HoodieTable table1 = HoodieSparkTable.create(cfg, context, metaClient);
        HoodieTimeline activeTimeline = table1.getCompletedCommitsTimeline();
        HoodieInstant lastInstant = activeTimeline.lastInstant().get();
        if (cfg.getBoolean(HoodieCleanConfig.ASYNC_CLEAN)) {
          activeTimeline = activeTimeline.findInstantsBefore(lastInstant.getTimestamp());
        }
        // NOTE: See CleanPlanner#getFilesToCleanKeepingLatestCommits. We explicitly keep one commit before earliest
        // commit
        Option<HoodieInstant> earliestRetainedCommit = activeTimeline.nthFromLastInstant(maxCommits);
        Set<HoodieInstant> acceptableCommits = activeTimeline.getInstants().collect(Collectors.toSet());
        if (earliestRetainedCommit.isPresent()) {
          acceptableCommits
              .removeAll(activeTimeline.findInstantsInRange("000", earliestRetainedCommit.get().getTimestamp())
                  .getInstants().collect(Collectors.toSet()));
          acceptableCommits.add(earliestRetainedCommit.get());
        }

        TableFileSystemView fsView = table1.getFileSystemView();
        // Need to ensure the following
        for (String partitionPath : dataGen.getPartitionPaths()) {
          List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
          for (HoodieFileGroup fileGroup : fileGroups) {
            Set<String> commitTimes = new HashSet<>();
            fileGroup.getAllBaseFiles().forEach(value -> {
              LOG.debug("Data File - " + value);
              commitTimes.add(value.getCommitTime());
            });
            if (cfg.getBoolean(HoodieCleanConfig.ASYNC_CLEAN)) {
              commitTimes.remove(lastInstant.getTimestamp());
            }
            assertEquals(acceptableCommits.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toSet()), commitTimes,
                "Only contain acceptable versions of file should be present");
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  /**
   * Test Helper for Cleaning failed commits by commits logic from HoodieWriteClient API perspective.
   *
   * @param insertFn Insert API to be tested
   * @param isPreppedAPI Flag to indicate if a prepped-version is used. If true, a wrapper function will be used during
   *        record generation to also tag the regards (de-dupe is implicit as we use uniq record-gen APIs)
   * @throws Exception in case of errors
   */
  private void testFailedInsertAndCleanByCommits(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertFn, boolean isPreppedAPI)
      throws Exception {
    int maxCommits = 3; // keep upto 3 commits from the past
    HoodieWriteConfig cfg = getConfigBuilder()
        .withAutoCommit(false)
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

    Pair<String, JavaRDD<WriteStatus>> result = insertFirstBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS);
    client.commit(result.getLeft(), result.getRight());

    HoodieTable table = HoodieSparkTable.create(client.getConfig(), context, metaClient);
    assertTrue(table.getCompletedCleanTimeline().empty());

    insertFirstFailedBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS);

    insertFirstFailedBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS);

    Pair<String, JavaRDD<WriteStatus>> ret =
        insertFirstFailedBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
        HoodieCleaningPolicy.KEEP_LATEST_COMMITS);
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
    HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeAvroMetadata(
        timeline.getInstantDetails(rollBackInstantForFailedCommit.get()).get(), HoodieRollbackMetadata.class);
    // Rollback of one of the failed writes should have deleted 3 files
    assertEquals(3, rollbackMetadata.getTotalFilesDeleted());
  }

  /**
   * Helper to run cleaner and collect Clean Stats.
   *
   * @param config HoodieWriteConfig
   */
  protected List<HoodieCleanStat> runCleaner(HoodieWriteConfig config) throws IOException {
    return runCleaner(config, false, false, 1, false);
  }

  protected List<HoodieCleanStat> runCleanerWithInstantFormat(HoodieWriteConfig config, boolean needInstantInHudiFormat) throws IOException {
    return runCleaner(config, false, false, 1, needInstantInHudiFormat);
  }

  protected List<HoodieCleanStat> runCleaner(HoodieWriteConfig config, int firstCommitSequence, boolean needInstantInHudiFormat) throws IOException {
    return runCleaner(config, false, false, firstCommitSequence, needInstantInHudiFormat);
  }

  protected List<HoodieCleanStat> runCleaner(HoodieWriteConfig config, boolean simulateRetryFailure) throws IOException {
    return runCleaner(config, simulateRetryFailure, false, 1, false);
  }

  protected List<HoodieCleanStat> runCleaner(
      HoodieWriteConfig config, boolean simulateRetryFailure, boolean simulateMetadataFailure) throws IOException {
    return runCleaner(config, simulateRetryFailure, simulateMetadataFailure, 1, false);
  }

  /**
   * Helper to run cleaner and collect Clean Stats.
   *
   * @param config HoodieWriteConfig
   */
  protected List<HoodieCleanStat> runCleaner(
      HoodieWriteConfig config, boolean simulateRetryFailure, boolean simulateMetadataFailure,
      Integer firstCommitSequence, boolean needInstantInHudiFormat) throws IOException {
    SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(config);
    String cleanInstantTs = needInstantInHudiFormat ? makeNewCommitTime(firstCommitSequence, "%014d") : makeNewCommitTime(firstCommitSequence, "%09d");
    HoodieCleanMetadata cleanMetadata1 = writeClient.clean(cleanInstantTs);

    if (null == cleanMetadata1) {
      return new ArrayList<>();
    }

    if (simulateRetryFailure) {
      HoodieInstant completedCleanInstant = new HoodieInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, cleanInstantTs);
      HoodieCleanMetadata metadata = CleanerUtils.getCleanerMetadata(metaClient, completedCleanInstant);
      metadata.getPartitionMetadata().values().forEach(p -> {
        String dirPath = metaClient.getBasePath() + "/" + p.getPartitionPath();
        p.getSuccessDeleteFiles().forEach(p2 -> {
          try {
            metaClient.getFs().create(new Path(dirPath, p2), true).close();
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
        });
      });
      metaClient.reloadActiveTimeline().revertToInflight(completedCleanInstant);

      if (config.isMetadataTableEnabled() && simulateMetadataFailure) {
        // Simulate the failure of corresponding instant in the metadata table
        HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
            .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath()))
            .setConf(metaClient.getHadoopConf())
            .build();
        HoodieInstant deltaCommit = new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, cleanInstantTs);
        metadataMetaClient.reloadActiveTimeline().revertToInflight(deltaCommit);
      }

      // retry clean operation again
      writeClient.clean();
      final HoodieCleanMetadata retriedCleanMetadata = CleanerUtils.getCleanerMetadata(HoodieTableMetaClient.reload(metaClient), completedCleanInstant);
      cleanMetadata1.getPartitionMetadata().keySet().forEach(k -> {
        HoodieCleanPartitionMetadata p1 = cleanMetadata1.getPartitionMetadata().get(k);
        HoodieCleanPartitionMetadata p2 = retriedCleanMetadata.getPartitionMetadata().get(k);
        assertEquals(p1.getDeletePathPatterns(), p2.getDeletePathPatterns());
        assertEquals(p1.getSuccessDeleteFiles(), p2.getSuccessDeleteFiles());
        assertEquals(p1.getFailedDeleteFiles(), p2.getFailedDeleteFiles());
        assertEquals(p1.getPartitionPath(), p2.getPartitionPath());
        assertEquals(k, p1.getPartitionPath());
      });
    }

    Map<String, HoodieCleanStat> cleanStatMap = cleanMetadata1.getPartitionMetadata().values().stream()
        .map(x -> new HoodieCleanStat.Builder().withPartitionPath(x.getPartitionPath())
            .withFailedDeletes(x.getFailedDeleteFiles()).withSuccessfulDeletes(x.getSuccessDeleteFiles())
            .withPolicy(HoodieCleaningPolicy.valueOf(x.getPolicy())).withDeletePathPattern(x.getDeletePathPatterns())
            .withEarliestCommitRetained(Option.ofNullable(cleanMetadata1.getEarliestCommitToRetain() != null
                ? new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "000")
                : null))
            .build())
        .collect(Collectors.toMap(HoodieCleanStat::getPartitionPath, x -> x));
    cleanMetadata1.getBootstrapPartitionMetadata().values().forEach(x -> {
      HoodieCleanStat s = cleanStatMap.get(x.getPartitionPath());
      cleanStatMap.put(x.getPartitionPath(), new HoodieCleanStat.Builder().withPartitionPath(x.getPartitionPath())
          .withFailedDeletes(s.getFailedDeleteFiles()).withSuccessfulDeletes(s.getSuccessDeleteFiles())
          .withPolicy(HoodieCleaningPolicy.valueOf(x.getPolicy())).withDeletePathPattern(s.getDeletePathPatterns())
          .withEarliestCommitRetained(Option.ofNullable(s.getEarliestCommitToRetain())
              .map(y -> new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, y)))
          .withSuccessfulDeleteBootstrapBaseFiles(x.getSuccessDeleteFiles())
          .withFailedDeleteBootstrapBaseFiles(x.getFailedDeleteFiles())
          .withDeleteBootstrapBasePathPatterns(x.getDeletePathPatterns()).build());
    });
    return new ArrayList<>(cleanStatMap.values());
  }

  @Test
  public void testCleanEmptyInstants() throws Exception {
    HoodieWriteConfig config =
            HoodieWriteConfig.newBuilder()
                    .withPath(basePath)
                    .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
                    .withCleanConfig(HoodieCleanConfig.newBuilder()
                        .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).build())
                    .build();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    int commitCount = 20;
    int cleanCount = 20;

    int startInstant = 1;

    for (int i = 0; i < cleanCount; i++, startInstant++) {
      String commitTime = makeNewCommitTime(startInstant, "%09d");
      createEmptyCleanMetadata(commitTime + "", false);
    }

    int instantClean = startInstant;

    for (int i = 0; i < commitCount; i++, startInstant++) {
      String commitTime = makeNewCommitTime(startInstant, "%09d");
      HoodieTestTable.of(metaClient).addCommit(commitTime);
    }

    List<HoodieCleanStat> cleanStats = runCleaner(config);
    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();

    assertEquals(0, cleanStats.size(), "Must not clean any files");
    assertEquals(1, timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).filterInflightsAndRequested().countInstants());
    assertEquals(0, timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).filterInflights().countInstants());
    assertEquals(--cleanCount, timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).filterCompletedInstants().countInstants());
    assertTrue(timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).filterInflightsAndRequested().containsInstant(makeNewCommitTime(--instantClean, "%09d")));

    cleanStats = runCleaner(config);
    timeline = metaClient.reloadActiveTimeline();

    assertEquals(0, cleanStats.size(), "Must not clean any files");
    assertEquals(1, timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).filterInflightsAndRequested().countInstants());
    assertEquals(0, timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).filterInflights().countInstants());
    assertEquals(--cleanCount, timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).filterCompletedInstants().countInstants());
    assertTrue(timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).filterInflightsAndRequested().containsInstant(makeNewCommitTime(--instantClean, "%09d")));
  }
  
  @Test
  public void testCleanWithReplaceCommits() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .withAssumeDatePartitioning(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(2).build())
        .build();

    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, config, context);
    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);
    String p0 = "2020/01/01";
    String p1 = "2020/01/02";

    // make 1 commit, with 1 file per partition
    String file1P0C0 = UUID.randomUUID().toString();
    String file1P1C0 = UUID.randomUUID().toString();
    testTable.addInflightCommit("00000000000001").withBaseFilesInPartition(p0, file1P0C0).withBaseFilesInPartition(p1, file1P1C0);

    HoodieCommitMetadata commitMetadata = generateCommitMetadata("00000000000001",
        Collections.unmodifiableMap(new HashMap<String, List<String>>() {
          {
            put(p0, CollectionUtils.createImmutableList(file1P0C0));
            put(p1, CollectionUtils.createImmutableList(file1P1C0));
          }
        })
    );
    metadataWriter.update(commitMetadata, "00000000000001", false);
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "00000000000001"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

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
    testTable.addReplaceCommit("00000000000002", Option.of(replaceMetadata.getKey()), Option.empty(), replaceMetadata.getValue());

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
    testTable.addReplaceCommit("00000000000003", Option.of(replaceMetadata.getKey()), Option.empty(), replaceMetadata.getValue());

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
    testTable.addReplaceCommit("00000000000004", Option.of(replaceMetadata.getKey()), Option.empty(), replaceMetadata.getValue());

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
    testTable.addReplaceCommit("00000000000006", Option.of(replaceMetadata.getKey()), Option.empty(), replaceMetadata.getValue());

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
      writeStat.setPath(partition + "/" + getBaseFilename(instantTime, newFileId));
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

    String partition1 = DEFAULT_PARTITION_PATHS[0];
    String partition2 = DEFAULT_PARTITION_PATHS[1];

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
        Arrays.asList(cleanStat1, cleanStat2)
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

    String partition1 = DEFAULT_PARTITION_PATHS[0];
    String partition2 = DEFAULT_PARTITION_PATHS[1];

    String extension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    String fileName1 = "data1_1_000" + extension;
    String fileName2 = "data2_1_000" + extension;

    Map<String, List<String>> filesToBeCleanedPerPartition = new HashMap<>();
    filesToBeCleanedPerPartition.put(partition1, Arrays.asList(fileName1));
    filesToBeCleanedPerPartition.put(partition2, Arrays.asList(fileName2));

    HoodieCleanerPlan version1Plan =
        HoodieCleanerPlan.newBuilder().setEarliestInstantToRetain(HoodieActionInstant.newBuilder()
            .setAction(HoodieTimeline.COMMIT_ACTION)
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
    assertEquals(new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), partition1), fileName1).toString(),
        version2Plan.getFilePathsToBeDeletedPerPartition().get(partition1).get(0).getFilePath());
    assertEquals(new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), partition2), fileName2).toString(),
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
   * Generate Bootstrap index, bootstrap base file and corresponding metaClient.
   * @return Partition to BootstrapFileMapping Map
   * @throws IOException
   */
  protected Map<String, List<BootstrapFileMapping>> generateBootstrapIndexAndSourceData(String... partitions) throws IOException {
    // create bootstrap source data path
    java.nio.file.Path sourcePath = tempDir.resolve("data");
    java.nio.file.Files.createDirectories(sourcePath);
    assertTrue(new File(sourcePath.toString()).exists());

    // recreate metaClient with Bootstrap base path
    metaClient = HoodieTestUtils.init(basePath, getTableType(), sourcePath.toString(), true);

    // generate bootstrap index
    Map<String, List<BootstrapFileMapping>> bootstrapMapping = TestBootstrapIndex.generateBootstrapIndex(metaClient, sourcePath.toString(),
        partitions, 1);

    for (Map.Entry<String, List<BootstrapFileMapping>> entry : bootstrapMapping.entrySet()) {
      new File(sourcePath.toString() + "/" + entry.getKey()).mkdirs();
      assertTrue(new File(entry.getValue().get(0).getBootstrapFileStatus().getPath().getUri()).createNewFile());
    }
    return bootstrapMapping;
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
    table.getActiveTimeline().transitionRequestedToInflight(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "001"), Option.empty());
    metaClient.reloadActiveTimeline();
    HoodieInstant rollbackInstant = new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001");
    table.scheduleRollback(context, "002", rollbackInstant, false, config.getBoolean(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE));
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
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build())
        .build();

    // Make a commit, although there are no partitionPaths.
    // Example use-case of this is when a client wants to create a table
    // with just some commit metadata, but no data/partitionPaths.
    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, config, context);
    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);
    testTable.doWriteOperation("001", WriteOperationType.INSERT, Collections.emptyList(), 1);

    metaClient = HoodieTableMetaClient.reload(metaClient);

    List<HoodieCleanStat> hoodieCleanStatsOne = runCleaner(config);
    assertTrue(hoodieCleanStatsOne.isEmpty(), "HoodieCleanStats should be empty for a table with empty partitionPaths");
  }

  /**
   * Test Keep Latest Commits when there are pending compactions.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testKeepLatestCommitsWithPendingCompactions(boolean isAsync) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).withAsyncClean(isAsync).retainCommits(2).build())
        .build();
    // Deletions:
    // . FileId Base Logs Total Retained Commits
    // FileId7 5 10 15 009, 011
    // FileId6 5 10 15 009
    // FileId5 3 6 9 005
    // FileId4 2 4 6 003
    // FileId3 1 2 3 001
    // FileId2 0 0 0 000
    // FileId1 0 0 0 000
    testPendingCompactions(config, 48, 18, false);
  }

  /**
   * Test Keep Latest Versions when there are pending compactions.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testKeepLatestVersionsWithPendingCompactions(boolean retryFailure) throws Exception {
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(2).build())
            .build();
    // Deletions:
    // . FileId Base Logs Total Retained Commits
    // FileId7 5 10 15 009, 011
    // FileId6 4 8 12 007, 009
    // FileId5 2 4 6 003 005
    // FileId4 1 2 3 001, 003
    // FileId3 0 0 0 000, 001
    // FileId2 0 0 0 000
    // FileId1 0 0 0 000
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
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS).retainFileVersions(1).build())
            .build();

    String commitTime = makeNewCommitTime(1, "%09d");
    List<String> cleanerFileNames = Arrays.asList(
        HoodieTimeline.makeRequestedCleanerFileName(commitTime),
        HoodieTimeline.makeInflightCleanerFileName(commitTime));
    for (String f : cleanerFileNames) {
      Path commitFile = new Path(Paths
          .get(metaClient.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME, f).toString());
      try (FSDataOutputStream os = metaClient.getFs().create(commitFile, true)) {
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
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .withAssumeDatePartitioning(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(2).build())
        .build();

    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, config, context);
    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);
    String p0 = "2020/01/01";
    String p1 = "2020/01/02";

    // make 1 commit, with 1 file per partition
    String file1P0C0 = UUID.randomUUID().toString();
    String file1P1C0 = UUID.randomUUID().toString();
    testTable.addInflightCommit("00000000000001").withBaseFilesInPartition(p0, file1P0C0).withBaseFilesInPartition(p1, file1P1C0);

    HoodieCommitMetadata commitMetadata = generateCommitMetadata("00000000000001",
        Collections.unmodifiableMap(new HashMap<String, List<String>>() {
          {
            put(p0, CollectionUtils.createImmutableList(file1P0C0));
            put(p1, CollectionUtils.createImmutableList(file1P1C0));
          }
        })
    );
    metadataWriter.update(commitMetadata, "00000000000001", false);
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "00000000000001"),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    metaClient = HoodieTableMetaClient.reload(metaClient);

    // make next replacecommit, with 1 clustering operation. logically delete p0. No change to p1
    // notice that clustering generates empty inflight commit files
    Map<String, String> partitionAndFileId002 = testTable.forReplaceCommit("00000000000002").getFileIdsWithBaseFilesInPartitions(p0);
    String file2P0C1 = partitionAndFileId002.get(p0);
    Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> replaceMetadata =
        generateReplaceCommitMetadata("00000000000002", p0, file1P0C0, file2P0C1);
    testTable.addReplaceCommit("00000000000002", Option.of(replaceMetadata.getKey()), Option.empty(), replaceMetadata.getValue());

    // make next replacecommit, with 1 clustering operation. Replace data in p1. No change to p0
    // notice that clustering generates empty inflight commit files
    Map<String, String> partitionAndFileId003 = testTable.forReplaceCommit("00000000000003").getFileIdsWithBaseFilesInPartitions(p1);
    String file3P1C2 = partitionAndFileId003.get(p1);
    replaceMetadata = generateReplaceCommitMetadata("00000000000003", p1, file1P1C0, file3P1C2);
    testTable.addReplaceCommit("00000000000003", Option.of(replaceMetadata.getKey()), Option.empty(), replaceMetadata.getValue());

    // make next replacecommit, with 1 clustering operation. Replace data in p0 again
    // notice that clustering generates empty inflight commit files
    Map<String, String> partitionAndFileId004 = testTable.forReplaceCommit("00000000000004").getFileIdsWithBaseFilesInPartitions(p0);
    String file4P0C3 = partitionAndFileId004.get(p0);
    replaceMetadata = generateReplaceCommitMetadata("00000000000004", p0, file2P0C1, file4P0C3);
    testTable.addReplaceCommit("00000000000004", Option.of(replaceMetadata.getKey()), Option.empty(), replaceMetadata.getValue());

    // run cleaner with failures
    List<HoodieCleanStat> hoodieCleanStats = runCleaner(config, true, simulateMetadataFailure, 5, true);
    assertTrue(testTable.baseFileExists(p0, "00000000000004", file4P0C3));
    assertTrue(testTable.baseFileExists(p0, "00000000000002", file2P0C1));
    assertTrue(testTable.baseFileExists(p1, "00000000000003", file3P1C2));
    assertFalse(testTable.baseFileExists(p0, "00000000000001", file1P0C0));
    //file1P1C0 still stays because its not replaced until 3 and its the only version available
    assertTrue(testTable.baseFileExists(p1, "00000000000001", file1P1C0));
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
        .withAutoCommit(false)
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

      Pair<String, JavaRDD<WriteStatus>> result = insertFirstBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
          HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS);

      client.commit(result.getLeft(), result.getRight());

      HoodieTable table = HoodieSparkTable.create(client.getConfig(), context, metaClient);

      assertTrue(table.getCompletedCleanTimeline().empty());

      insertFirstFailedBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
          HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS);

      insertFirstFailedBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
          HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS);

      Pair<String, JavaRDD<WriteStatus>> ret =
          insertFirstFailedBigBatchForClientCleanerTest(cfg, client, recordInsertGenWrappedFunction, insertFn,
          HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS);

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
      HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeAvroMetadata(
          timeline.getInstantDetails(rollBackInstantForFailedCommit.get()).get(), HoodieRollbackMetadata.class);
      // Rollback of one of the failed writes should have deleted 3 files
      assertEquals(3, rollbackMetadata.getTotalFilesDeleted());
    }
  }

  /**
   * Common test method for validating pending compactions.
   *
   * @param config Hoodie Write Config
   * @param expNumFilesDeleted Number of files deleted
   */
  private void testPendingCompactions(HoodieWriteConfig config, int expNumFilesDeleted,
      int expNumFilesUnderCompactionDeleted, boolean retryFailure) throws Exception {
    HoodieTableMetaClient metaClient =
        HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    final String partition = "2016/03/15";
    Map<String, String> expFileIdToPendingCompaction = new HashMap<String, String>() {
      {
        put("fileId2", "004");
        put("fileId3", "006");
        put("fileId4", "008");
        put("fileId5", "010");
      }
    };
    Map<String, String> fileIdToLatestInstantBeforeCompaction = new HashMap<String, String>() {
      {
        put("fileId1", "000");
        put("fileId2", "000");
        put("fileId3", "001");
        put("fileId4", "003");
        put("fileId5", "005");
        put("fileId6", "009");
        put("fileId7", "011");
      }
    };

    // Generate 7 file-groups. First one has only one slice and no pending compaction. File Slices (2 - 5) has
    // multiple versions with pending compaction. File Slices (6 - 7) have multiple file-slices but not under
    // compactions
    // FileIds 2-5 will be under compaction
    HoodieTestTable.of(metaClient)
        .addCommit("000")
        .withBaseFilesInPartition(partition, "fileId1", "fileId2", "fileId3", "fileId4", "fileId5", "fileId6", "fileId7")
        .withLogFile(partition, "fileId1", 1, 2)
        .withLogFile(partition, "fileId2", 1, 2)
        .withLogFile(partition, "fileId3", 1, 2)
        .withLogFile(partition, "fileId4", 1, 2)
        .withLogFile(partition, "fileId5", 1, 2)
        .withLogFile(partition, "fileId6", 1, 2)
        .withLogFile(partition, "fileId7", 1, 2)
        .addCommit("001")
        .withBaseFilesInPartition(partition, "fileId3", "fileId4", "fileId5", "fileId6", "fileId7")
        .withLogFile(partition, "fileId3", 1, 2)
        .withLogFile(partition, "fileId4", 1, 2)
        .withLogFile(partition, "fileId5", 1, 2)
        .withLogFile(partition, "fileId6", 1, 2)
        .withLogFile(partition, "fileId7", 1, 2)
        .addCommit("003")
        .withBaseFilesInPartition(partition, "fileId4", "fileId5", "fileId6", "fileId7")
        .withLogFile(partition, "fileId4", 1, 2)
        .withLogFile(partition, "fileId5", 1, 2)
        .withLogFile(partition, "fileId6", 1, 2)
        .withLogFile(partition, "fileId7", 1, 2)
        .addRequestedCompaction("004", new FileSlice(partition, "000", "fileId2"))
        .withLogFile(partition, "fileId2", 1, 2)
        .addCommit("005")
        .withBaseFilesInPartition(partition, "fileId5", "fileId6", "fileId7")
        .withLogFile(partition, "fileId5", 1, 2)
        .withLogFile(partition, "fileId6", 1, 2)
        .withLogFile(partition, "fileId7", 1, 2)
        .addRequestedCompaction("006", new FileSlice(partition, "001", "fileId3"))
        .withLogFile(partition, "fileId3", 1, 2)
        .addCommit("007")
        .withBaseFilesInPartition(partition, "fileId6", "fileId7")
        .withLogFile(partition, "fileId6", 1, 2)
        .withLogFile(partition, "fileId7", 1, 2)
        .addRequestedCompaction("008", new FileSlice(partition, "003", "fileId4"))
        .withLogFile(partition, "fileId4", 1, 2)
        .addCommit("009")
        .withBaseFilesInPartition(partition, "fileId6", "fileId7")
        .withLogFile(partition, "fileId6", 1, 2)
        .withLogFile(partition, "fileId7", 1, 2)
        .addRequestedCompaction("010", new FileSlice(partition, "005", "fileId5"))
        .withLogFile(partition, "fileId5", 1, 2)
        .addCommit("011")
        .withBaseFilesInPartition(partition, "fileId7")
        .withLogFile(partition, "fileId7", 1, 2)
        .addCommit("013");

    // Clean now
    metaClient = HoodieTableMetaClient.reload(metaClient);
    List<HoodieCleanStat> hoodieCleanStats = runCleaner(config, retryFailure);

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
            assertTrue(HoodieTimeline.compareTimestamps(
                fileIdToLatestInstantBeforeCompaction.get(fileIdWithCommitTime.getKey()),
                HoodieTimeline.GREATER_THAN, fileIdWithCommitTime.getValue()),
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
    Stream<Pair<String, String>> stream2 = paths.stream().filter(rtFilePredicate).map(path -> {
      return Pair.of(FSUtils.getFileIdFromLogPath(new Path(path)),
          FSUtils.getBaseCommitTimeFromLogPath(new Path(path)));
    });
    return Stream.concat(stream1, stream2);
  }

  protected static HoodieCommitMetadata generateCommitMetadata(
      String instantTime, Map<String, List<String>> partitionToFilePaths) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestTable.PHONY_TABLE_SCHEMA);
    partitionToFilePaths.forEach((partitionPath, fileList) -> fileList.forEach(f -> {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(partitionPath);
      writeStat.setPath(partitionPath + "/" + getBaseFilename(instantTime, f));
      writeStat.setFileId(f);
      writeStat.setTotalWriteBytes(1);
      writeStat.setFileSizeInBytes(1);
      metadata.addWriteStat(partitionPath, writeStat);
    }));
    return metadata;
  }
}
