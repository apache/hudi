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

package org.apache.hudi;

import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.TestRawTripPayload.MetadataMergeWriteStatus;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base Class providing setup/cleanup and utility methods for testing Hoodie Client facing tests.
 */
public class TestHoodieClientBase extends HoodieClientTestHarness {

  protected static Logger logger = LogManager.getLogger(TestHoodieClientBase.class);

  @Before
  public void setUp() throws Exception {
    initResources();
  }

  @After
  public void tearDown() throws Exception {
    cleanupResources();
  }

  protected HoodieCleanClient getHoodieCleanClient(HoodieWriteConfig cfg) {
    return new HoodieCleanClient(jsc, cfg, new HoodieMetrics(cfg, cfg.getTableName()));
  }

  protected HoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    return getHoodieWriteClient(cfg, false);
  }

  protected HoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg, boolean rollbackInflightCommit) {
    return getHoodieWriteClient(cfg, rollbackInflightCommit, HoodieIndex.createIndex(cfg, jsc));
  }

  protected HoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg, boolean rollbackInflightCommit,
      HoodieIndex index) {
    return new HoodieWriteClient(jsc, cfg, rollbackInflightCommit, index);
  }

  protected HoodieReadClient getHoodieReadClient(String basePath) {
    return new HoodieReadClient(jsc, basePath);
  }

  /**
   * Get Default HoodieWriteConfig for tests.
   *
   * @return Default Hoodie Write Config for tests
   */
  protected HoodieWriteConfig getConfig() {
    return getConfigBuilder().build();
  }


  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  HoodieWriteConfig.Builder getConfigBuilder() {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.BLOOM).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  protected HoodieTable getHoodieTable(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    ((SyncableFileSystemView) (table.getRTFileSystemView())).reset();
    return table;
  }

  /**
   * Assert no failures in writing hoodie files.
   *
   * @param statuses List of Write Status
   */
  static void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse("Errors found in write of " + status.getFileId(), status.hasErrors());
    }
  }

  /**
   * Ensure presence of partition meta-data at known depth.
   *
   * @param partitionPaths Partition paths to check
   * @param fs File System
   * @throws IOException in case of error
   */
  void assertPartitionMetadata(String[] partitionPaths, FileSystem fs) throws IOException {
    for (String partitionPath : partitionPaths) {
      assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(fs, new Path(basePath, partitionPath)));
      HoodiePartitionMetadata pmeta = new HoodiePartitionMetadata(fs, new Path(basePath, partitionPath));
      pmeta.readFromFS();
      Assert.assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_DEPTH, pmeta.getPartitionDepth());
    }
  }

  /**
   * Ensure records have location field set.
   *
   * @param taggedRecords Tagged Records
   * @param commitTime Commit Timestamp
   */
  void checkTaggedRecords(List<HoodieRecord> taggedRecords, String commitTime) {
    for (HoodieRecord rec : taggedRecords) {
      assertTrue("Record " + rec + " found with no location.", rec.isCurrentLocationKnown());
      assertEquals("All records should have commit time " + commitTime + ", since updates were made",
          rec.getCurrentLocation().getInstantTime(), commitTime);
    }
  }

  /**
   * Assert that there is no duplicate key at the partition level.
   *
   * @param records List of Hoodie records
   */
  void assertNodupesWithinPartition(List<HoodieRecord> records) {
    Map<String, Set<String>> partitionToKeys = new HashMap<>();
    for (HoodieRecord r : records) {
      String key = r.getRecordKey();
      String partitionPath = r.getPartitionPath();
      if (!partitionToKeys.containsKey(partitionPath)) {
        partitionToKeys.put(partitionPath, new HashSet<>());
      }
      assertTrue("key " + key + " is duplicate within partition " + partitionPath,
          !partitionToKeys.get(partitionPath).contains(key));
      partitionToKeys.get(partitionPath).add(key);
    }
  }

  /**
   * Helper to generate records generation function for testing Prepped version of API. Prepped APIs expect the records
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by record-generation function itself.
   *
   * @param writeConfig Hoodie Write Config
   * @param recordGenFunction Records Generation function
   * @return Wrapped function
   */
  private Function2<List<HoodieRecord>, String, Integer> wrapRecordsGenFunctionForPreppedCalls(
      final HoodieWriteConfig writeConfig, final Function2<List<HoodieRecord>, String, Integer> recordGenFunction) {
    return (commit, numRecords) -> {
      final HoodieIndex index = HoodieIndex.createIndex(writeConfig, jsc);
      List<HoodieRecord> records = recordGenFunction.apply(commit, numRecords);
      final HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath, true);
      HoodieTable table = HoodieTable.getHoodieTable(metaClient, writeConfig, jsc);
      JavaRDD<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), jsc, table);
      return taggedRecords.collect();
    };
  }

  /**
   * Helper to generate delete keys generation function for testing Prepped version of API. Prepped APIs expect the keys
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by key-generation function itself.
   *
   * @param writeConfig Hoodie Write Config
   * @param keyGenFunction Keys Generation function
   * @return Wrapped function
   */
  private Function2<List<HoodieKey>, String, Integer> wrapDeleteKeysGenFunctionForPreppedCalls(
      final HoodieWriteConfig writeConfig, final Function2<List<HoodieKey>, String, Integer> keyGenFunction) {
    return (commit, numRecords) -> {
      final HoodieIndex index = HoodieIndex.createIndex(writeConfig, jsc);
      List<HoodieKey> records = keyGenFunction.apply(commit, numRecords);
      final HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath, true);
      HoodieTable table = HoodieTable.getHoodieTable(metaClient, writeConfig, jsc);
      JavaRDD<HoodieRecord> recordsToDelete = jsc.parallelize(records, 1)
          .map(key -> new HoodieRecord(key, new EmptyHoodieRecordPayload()));
      JavaRDD<HoodieRecord> taggedRecords = index.tagLocation(recordsToDelete, jsc, table);
      return taggedRecords.map(record -> record.getKey()).collect();
    };
  }

  /**
   * Generate wrapper for record generation function for testing Prepped APIs.
   *
   * @param isPreppedAPI Flag to indicate if this is for testing prepped-version of APIs
   * @param writeConfig Hoodie Write Config
   * @param wrapped Actual Records Generation function
   * @return Wrapped Function
   */
  Function2<List<HoodieRecord>, String, Integer> generateWrapRecordsFn(boolean isPreppedAPI,
      HoodieWriteConfig writeConfig, Function2<List<HoodieRecord>, String, Integer> wrapped) {
    if (isPreppedAPI) {
      return wrapRecordsGenFunctionForPreppedCalls(writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  /**
   * Generate wrapper for delete key generation function for testing Prepped APIs.
   *
   * @param isPreppedAPI Flag to indicate if this is for testing prepped-version of APIs
   * @param writeConfig Hoodie Write Config
   * @param wrapped Actual Records Generation function
   * @return Wrapped Function
   */
  Function2<List<HoodieKey>, String, Integer> generateWrapDeleteKeysFn(boolean isPreppedAPI,
      HoodieWriteConfig writeConfig, Function2<List<HoodieKey>, String, Integer> wrapped) {
    if (isPreppedAPI) {
      return wrapDeleteKeysGenFunctionForPreppedCalls(writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  /**
   * Helper to insert first batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig Hoodie Write Config
   * @param client Hoodie Write Client
   * @param newCommitTime New Commit Timestamp to be used
   * @param initCommitTime Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn Write Function to be used for insertion
   * @param isPreppedAPI Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  JavaRDD<WriteStatus> insertFirstBatch(HoodieWriteConfig writeConfig, HoodieWriteClient client, String newCommitTime,
      String initCommitTime, int numRecordsInThisCommit,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateInserts);

    return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
        recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expRecordsInThisCommit, 1);
  }

  /**
   * Helper to upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig Hoodie Write Config
   * @param client Hoodie Write Client
   * @param newCommitTime New Commit Timestamp to be used
   * @param prevCommitTime Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn Write Function to be used for upsert
   * @param isPreppedAPI Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords Expected number of records when scanned
   * @param expTotalCommits Expected number of commits (including this commit)
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  JavaRDD<WriteStatus> updateBatch(HoodieWriteConfig writeConfig, HoodieWriteClient client, String newCommitTime,
      String prevCommitTime, Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
      int numRecordsInThisCommit,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueUpdates);

    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits);
  }

  /**
   * Helper to delete batch of keys and do regular assertions on the state after successful completion.
   *
   * @param writeConfig Hoodie Write Config
   * @param client Hoodie Write Client
   * @param newCommitTime New Commit Timestamp to be used
   * @param prevCommitTime Commit Timestamp used in previous commit
   * @param initCommitTime Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param deleteFn Delete Function to be used for deletes
   * @param isPreppedAPI Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords Expected number of records when scanned
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  JavaRDD<WriteStatus> deleteBatch(HoodieWriteConfig writeConfig, HoodieWriteClient client, String newCommitTime,
      String prevCommitTime, String initCommitTime,
      int numRecordsInThisCommit,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieKey>, String> deleteFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords) throws Exception {
    final Function2<List<HoodieKey>, String, Integer> keyGenFunction =
        generateWrapDeleteKeysFn(isPreppedAPI, writeConfig, dataGen::generateUniqueDeletes);

    return deleteBatch(client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit,
        keyGenFunction,
        deleteFn, assertForCommit, expRecordsInThisCommit, expTotalRecords);
  }

  /**
   * Helper to insert/upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param client Hoodie Write Client
   * @param newCommitTime New Commit Timestamp to be used
   * @param prevCommitTime Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param recordGenFunction Records Generation Function
   * @param writeFn Write Function to be used for upsert
   * @param assertForCommit Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords Expected number of records when scanned
   * @param expTotalCommits Expected number of commits (including this commit)
   * @throws Exception in case of error
   */
  JavaRDD<WriteStatus> writeBatch(HoodieWriteClient client, String newCommitTime, String prevCommitTime,
      Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits) throws Exception {

    // Write 1 (only inserts)
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    JavaRDD<WriteStatus> result = writeFn.apply(client, writeRecords, newCommitTime);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    // check the partition metadata is written out
    assertPartitionMetadata(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, fs);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();

    if (assertForCommit) {
      assertEquals("Expecting " + expTotalCommits + " commits.", expTotalCommits,
          timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants());
      Assert.assertEquals("Latest commit should be " + newCommitTime, newCommitTime,
          timeline.lastInstant().get().getTimestamp());
      assertEquals("Must contain " + expRecordsInThisCommit + " records", expRecordsInThisCommit,
          HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count());

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals("Must contain " + expTotalRecords + " records", expTotalRecords,
          HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count());

      // Check that the incremental consumption from prevCommitTime
      assertEquals("Incremental consumption from " + prevCommitTime + " should give all records in latest commit",
          HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
          HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, prevCommitTime).count());
      if (commitTimesBetweenPrevAndNew.isPresent()) {
        commitTimesBetweenPrevAndNew.get().forEach(ct -> {
          assertEquals("Incremental consumption from " + ct + " should give all records in latest commit",
              HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
              HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, ct).count());
        });
      }
    }
    return result;
  }

  /**
   * Helper to delete batch of hoodie keys and do regular assertions on the state after successful completion.
   *
   * @param client Hoodie Write Client
   * @param newCommitTime New Commit Timestamp to be used
   * @param prevCommitTime Commit Timestamp used in previous commit
   * @param initCommitTime Begin Timestamp (usually "000")
   * @param keyGenFunction Key Generation function
   * @param deleteFn Write Function to be used for delete
   * @param assertForCommit Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords Expected number of records when scanned
   * @throws Exception in case of error
   */
  JavaRDD<WriteStatus> deleteBatch(HoodieWriteClient client, String newCommitTime, String prevCommitTime,
      String initCommitTime, int numRecordsInThisCommit,
      Function2<List<HoodieKey>, String, Integer> keyGenFunction,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieKey>, String> deleteFn,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords) throws Exception {

    // Delete 1 (only deletes)
    client.startCommitWithTime(newCommitTime);

    List<HoodieKey> keysToDelete = keyGenFunction.apply(newCommitTime, numRecordsInThisCommit);
    JavaRDD<HoodieKey> deleteRecords = jsc.parallelize(keysToDelete, 1);

    JavaRDD<WriteStatus> result = deleteFn.apply(client, deleteRecords, newCommitTime);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    // check the partition metadata is written out
    assertPartitionMetadata(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, fs);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();

    if (assertForCommit) {
      assertEquals("Expecting 3 commits.", 3,
          timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants());
      Assert.assertEquals("Latest commit should be " + newCommitTime, newCommitTime,
          timeline.lastInstant().get().getTimestamp());
      assertEquals("Must contain " + expRecordsInThisCommit + " records", expRecordsInThisCommit,
          HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count());

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals("Must contain " + expTotalRecords + " records", expTotalRecords,
          HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count());

      // Check that the incremental consumption from prevCommitTime
      assertEquals("Incremental consumption from " + prevCommitTime + " should give no records in latest commit,"
              + " since it is a delete operation",
          HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
          HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, prevCommitTime).count());
    }
    return result;
  }

  /**
   * Get Cleaner state corresponding to a partition path.
   *
   * @param hoodieCleanStatsTwo List of Clean Stats
   * @param partitionPath Partition path for filtering
   * @return Cleaner state corresponding to partition path
   */
  HoodieCleanStat getCleanStat(List<HoodieCleanStat> hoodieCleanStatsTwo, String partitionPath) {
    return hoodieCleanStatsTwo.stream().filter(e -> e.getPartitionPath().equals(partitionPath)).findFirst().orElse(null);
  }

  /**
   * Utility to simulate commit touching files in a partition.
   *
   * @param files List of file-Ids to be touched
   * @param partitionPath Partition
   * @param commitTime Commit Timestamp
   * @throws IOException in case of error
   */
  void updateAllFilesInPartition(List<String> files, String partitionPath, String commitTime) throws IOException {
    for (String fileId : files) {
      HoodieTestUtils.createDataFile(basePath, partitionPath, commitTime, fileId);
    }
  }

  /**
   * Helper methods to create new data files in a partition.
   *
   * @param partitionPath Partition
   * @param commitTime Commit Timestamp
   * @param numFiles Number of files to be added
   * @return Created files
   * @throws IOException in case of error
   */
  List<String> createFilesInPartition(String partitionPath, String commitTime, int numFiles) throws IOException {
    List<String> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      files.add(HoodieTestUtils.createNewDataFile(basePath, partitionPath, commitTime));
    }
    return files;
  }

  // Functional Interfaces for passing lambda and Hoodie Write API contexts

  @FunctionalInterface
  interface Function2<R, T1, T2> {

    R apply(T1 v1, T2 v2) throws IOException;
  }

  @FunctionalInterface
  interface Function3<R, T1, T2, T3> {

    R apply(T1 v1, T2 v2, T3 v3) throws IOException;
  }

}
