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

package org.apache.hudi.testutils;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.table.HoodieSparkTable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base Class providing setup/cleanup and utility methods for testing Hoodie Client facing tests.
 */
public class HoodieClientTestBase extends HoodieClientTestHarness {

  protected static final Logger LOG = LogManager.getLogger(HoodieClientTestBase.class);

  @BeforeEach
  public void setUp() throws Exception {
    initResources();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  /**
   * Get Default HoodieWriteConfig for tests.
   *
   * @return Default Hoodie Write Config for tests
   */
  public HoodieWriteConfig getConfig() {
    return getConfigBuilder().build();
  }

  public HoodieWriteConfig getConfig(IndexType indexType) {
    return getConfigBuilder(indexType).build();
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder() {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, IndexType.BLOOM, cleaningPolicy);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(IndexType indexType) {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, indexType, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    return getConfigBuilder(schemaStr, IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, IndexType indexType) {
    return getConfigBuilder(schemaStr, indexType, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, IndexType indexType,
                                                    HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withFailedWritesCleaningPolicy(cleaningPolicy)
            .compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(timelineServicePort)
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  public HoodieSparkTable getHoodieTable(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    HoodieSparkTable table = HoodieSparkTable.create(config, context, metaClient);
    ((SyncableFileSystemView) (table.getSliceView())).reset();
    return table;
  }

  public void assertPartitionMetadataForRecords(List<HoodieRecord> inputRecords, FileSystem fs) throws IOException {
    Set<String> partitionPathSet = inputRecords.stream()
        .map(HoodieRecord::getPartitionPath)
        .collect(Collectors.toSet());
    assertPartitionMetadata(partitionPathSet.stream().toArray(String[]::new), fs);
  }

  public void assertPartitionMetadataForKeys(List<HoodieKey> inputKeys, FileSystem fs) throws IOException {
    Set<String> partitionPathSet = inputKeys.stream()
        .map(HoodieKey::getPartitionPath)
        .collect(Collectors.toSet());
    assertPartitionMetadata(partitionPathSet.stream().toArray(String[]::new), fs);
  }

  /**
   * Ensure presence of partition meta-data at known depth.
   *
   * @param partitionPaths Partition paths to check
   * @param fs File System
   * @throws IOException in case of error
   */
  public void assertPartitionMetadata(String[] partitionPaths, FileSystem fs) throws IOException {
    for (String partitionPath : partitionPaths) {
      assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(fs, new Path(basePath, partitionPath)));
      HoodiePartitionMetadata pmeta = new HoodiePartitionMetadata(fs, new Path(basePath, partitionPath));
      pmeta.readFromFS();
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_DEPTH, pmeta.getPartitionDepth());
    }
  }

  /**
   * Ensure records have location field set.
   *
   * @param taggedRecords Tagged Records
   * @param instantTime Commit Timestamp
   */
  public void checkTaggedRecords(List<HoodieRecord> taggedRecords, String instantTime) {
    for (HoodieRecord rec : taggedRecords) {
      assertTrue(rec.isCurrentLocationKnown(), "Record " + rec + " found with no location.");
      assertEquals(rec.getCurrentLocation().getInstantTime(), instantTime,
          "All records should have commit time " + instantTime + ", since updates were made");
    }
  }

  /**
   * Assert that there is no duplicate key at the partition level.
   *
   * @param records List of Hoodie records
   */
  public void assertNodupesWithinPartition(List<HoodieRecord<RawTripTestPayload>> records) {
    Map<String, Set<String>> partitionToKeys = new HashMap<>();
    for (HoodieRecord r : records) {
      String key = r.getRecordKey();
      String partitionPath = r.getPartitionPath();
      if (!partitionToKeys.containsKey(partitionPath)) {
        partitionToKeys.put(partitionPath, new HashSet<>());
      }
      assertFalse(partitionToKeys.get(partitionPath).contains(key), "key " + key + " is duplicate within partition " + partitionPath);
      partitionToKeys.get(partitionPath).add(key);
    }
  }

  /**
   * Helper to generate records generation function for testing Prepped version of API. Prepped APIs expect the records
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by record-generation function itself.
   *
   * @param writeConfig       Hoodie Write Config
   * @param recordGenFunction Records Generation function
   * @return Wrapped function
   */
  private Function2<List<HoodieRecord>, String, Integer> wrapRecordsGenFunctionForPreppedCalls(
      final HoodieWriteConfig writeConfig, final Function2<List<HoodieRecord>, String, Integer> recordGenFunction) {
    return (commit, numRecords) -> {
      final HoodieIndex index = SparkHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieRecord> records = recordGenFunction.apply(commit, numRecords);
      final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
      HoodieSparkTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
      JavaRDD<HoodieRecord> taggedRecords = tagLocation(index, jsc.parallelize(records, 1), table);
      return taggedRecords.collect();
    };
  }

  private Function3<List<HoodieRecord>, String, Integer, String> wrapRecordsGenFunctionForPreppedCalls(
      final HoodieWriteConfig writeConfig, final Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction) {
    return (commit, numRecords, partition) -> {
      final HoodieIndex index = SparkHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieRecord> records = recordGenFunction.apply(commit, numRecords, partition);
      final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
      HoodieSparkTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
      JavaRDD<HoodieRecord> taggedRecords = tagLocation(index, jsc.parallelize(records, 1), table);
      return taggedRecords.collect();
    };
  }

  /**
   * Helper to generate delete keys generation function for testing Prepped version of API. Prepped APIs expect the keys
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by key-generation function itself.
   *
   * @param writeConfig    Hoodie Write Config
   * @param keyGenFunction Keys Generation function
   * @return Wrapped function
   */
  private Function<Integer, List<HoodieKey>> wrapDeleteKeysGenFunctionForPreppedCalls(
      final HoodieWriteConfig writeConfig, final Function<Integer, List<HoodieKey>> keyGenFunction) {
    return (numRecords) -> {
      final HoodieIndex index = SparkHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieKey> records = keyGenFunction.apply(numRecords);
      final HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
      HoodieSparkTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
      JavaRDD<HoodieRecord> recordsToDelete = jsc.parallelize(records, 1)
          .map(key -> new HoodieAvroRecord(key, new EmptyHoodieRecordPayload()));
      JavaRDD<HoodieRecord> taggedRecords = tagLocation(index, recordsToDelete, table);
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
  public Function2<List<HoodieRecord>, String, Integer> generateWrapRecordsFn(boolean isPreppedAPI,
      HoodieWriteConfig writeConfig,
      Function2<List<HoodieRecord>, String, Integer> wrapped) {
    if (isPreppedAPI) {
      return wrapRecordsGenFunctionForPreppedCalls(writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  public Function3<List<HoodieRecord>, String, Integer, String> generateWrapRecordsForPartitionFn(boolean isPreppedAPI,
      HoodieWriteConfig writeConfig, Function3<List<HoodieRecord>, String, Integer, String> wrapped) {
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
  public Function<Integer, List<HoodieKey>> generateWrapDeleteKeysFn(boolean isPreppedAPI,
      HoodieWriteConfig writeConfig, Function<Integer, List<HoodieKey>> wrapped) {
    if (isPreppedAPI) {
      return wrapDeleteKeysGenFunctionForPreppedCalls(writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  public JavaRDD<WriteStatus> insertFirstBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                               String initCommitTime, int numRecordsInThisCommit,
                                               Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                               boolean assertForCommit, int expRecordsInThisCommit) throws Exception {
    return insertFirstBatch(writeConfig, client, newCommitTime, initCommitTime, numRecordsInThisCommit, writeFn, isPreppedAPI, assertForCommit, expRecordsInThisCommit, true);
  }

  /**
   * Helper to insert first batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn                Write Function to be used for insertion
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> insertFirstBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
      String initCommitTime, int numRecordsInThisCommit,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit, boolean filterForCommitTimeWithAssert) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateInserts);

    return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
        recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expRecordsInThisCommit, 1, false, filterForCommitTimeWithAssert);
  }

  /**
   * Helper to insert another batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn                Write Function to be used for insertion
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords        Expected number of records when scanned
   * @param expTotalCommits        Expected number of commits (including this commit)
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> insertBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
      String initCommitTime, int numRecordsInThisCommit,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, Option<String> partition) throws Exception {

    if (partition.isPresent()) {
      final Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction =
          generateWrapRecordsForPartitionFn(isPreppedAPI, writeConfig, dataGen::generateInsertsForPartition);

      return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
          recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, false,
          partition.get());
    } else {
      final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
          generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateInserts);

      return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
          recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, false);
    }
  }

  public JavaRDD<WriteStatus> updateBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                          String prevCommitTime, Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                          int numRecordsInThisCommit,
                                          Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                          boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits) throws Exception {
    return updateBatch(writeConfig, client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, writeFn,
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, true);
  }

  /**
   * Helper to upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig                  Hoodie Write Config
   * @param client                       Hoodie Write Client
   * @param newCommitTime                New Commit Timestamp to be used
   * @param prevCommitTime               Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime               Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit       Number of records to be added in the new commit
   * @param writeFn                      Write Function to be used for upsert
   * @param isPreppedAPI                 Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit              Enable Assertion of Writes
   * @param expRecordsInThisCommit       Expected number of records in this commit
   * @param expTotalRecords              Expected number of records when scanned
   * @param expTotalCommits              Expected number of commits (including this commit)
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> updateBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                          String prevCommitTime, Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                          int numRecordsInThisCommit,
                                          Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                          boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                          boolean filterForCommitTimeWithAssert) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueUpdates);

    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, false, filterForCommitTimeWithAssert);
  }

  public JavaRDD<WriteStatus> deleteBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                          String prevCommitTime, String initCommitTime,
                                          int numRecordsInThisCommit,
                                          Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieKey>, String> deleteFn, boolean isPreppedAPI,
                                          boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords) throws Exception {
    return deleteBatch(writeConfig, client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit, deleteFn, isPreppedAPI,
        assertForCommit, expRecordsInThisCommit, expTotalRecords, true);
  }

  /**
   * Helper to delete batch of keys and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param prevCommitTime         Commit Timestamp used in previous commit
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param deleteFn               Delete Function to be used for deletes
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords        Expected number of records when scanned
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> deleteBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
      String prevCommitTime, String initCommitTime,
      int numRecordsInThisCommit,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieKey>, String> deleteFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, boolean filterForCommitTimeWithAssert) throws Exception {
    final Function<Integer, List<HoodieKey>> keyGenFunction =
        generateWrapDeleteKeysFn(isPreppedAPI, writeConfig, dataGen::generateUniqueDeletes);

    return deleteBatch(client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit,
        keyGenFunction,
        deleteFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, filterForCommitTimeWithAssert);
  }

  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, boolean doCommit) throws Exception {
    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, doCommit, true);
  }

  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                         boolean doCommit, String partition) throws Exception {
    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, doCommit, true, partition);
  }

  /**
   * Helper to insert/upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param client                       Hoodie Write Client
   * @param newCommitTime                New Commit Timestamp to be used
   * @param prevCommitTime               Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime               Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit       Number of records to be added in the new commit
   * @param recordGenFunction            Records Generation Function
   * @param writeFn                      Write Function to be used for upsert
   * @param assertForCommit              Enable Assertion of Writes
   * @param expRecordsInThisCommit       Expected number of records in this commit
   * @param expTotalRecords              Expected number of records when scanned
   * @param expTotalCommits              Expected number of commits (including this commit)
   * @param doCommit
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, boolean doCommit,
                                         boolean filterForCommitTimeWithAssert) throws Exception {

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit);
    return writeBatchHelper(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, records, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, doCommit, filterForCommitTimeWithAssert);
  }

  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, boolean doCommit,
                                         boolean filterForCommitTimeWithAssert,
                                         String partition) throws Exception {

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit, partition);
    return writeBatchHelper(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, records, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, doCommit, filterForCommitTimeWithAssert);
  }

  private JavaRDD<WriteStatus> writeBatchHelper(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                                Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                                int numRecordsInThisCommit, List<HoodieRecord> records,
                                                Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                                boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                                int expTotalCommits, boolean doCommit, boolean filterForCommitTimeWithAssert) throws IOException {
    // Write 1 (only inserts)
    client.startCommitWithTime(newCommitTime);

    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    JavaRDD<WriteStatus> result = writeFn.apply(client, writeRecords, newCommitTime);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    if (doCommit) {
      client.commit(newCommitTime, result);
    }
    // check the partition metadata is written out
    assertPartitionMetadataForRecords(records, fs);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();

    if (assertForCommit) {
      assertEquals(expTotalCommits, timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants(),
          "Expecting " + expTotalCommits + " commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().getTimestamp(),
          "Latest commit should be " + newCommitTime);
      if (filterForCommitTimeWithAssert) { // when meta cols are disabled, we can't really do per commit assertion.
        assertEquals(expRecordsInThisCommit, HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
            "Must contain " + expRecordsInThisCommit + " records");
      }

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals(expTotalRecords, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
          "Must contain " + expTotalRecords + " records");

      if (filterForCommitTimeWithAssert) {
        // Check that the incremental consumption from prevCommitTime
        assertEquals(HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
            HoodieClientTestUtils.countRecordsOptionallySince(jsc, basePath, sqlContext, timeline, Option.of(prevCommitTime)),
            "Incremental consumption from " + prevCommitTime + " should give all records in latest commit");
        if (commitTimesBetweenPrevAndNew.isPresent()) {
          commitTimesBetweenPrevAndNew.get().forEach(ct -> {
            assertEquals(HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
                HoodieClientTestUtils.countRecordsOptionallySince(jsc, basePath, sqlContext, timeline, Option.of(ct)),
                "Incremental consumption from " + ct + " should give all records in latest commit");
          });
        }
      }
    }
    return result;
  }

  /**
   * Helper to delete batch of hoodie keys and do regular assertions on the state after successful completion.
   *
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param prevCommitTime         Commit Timestamp used in previous commit
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param keyGenFunction         Key Generation function
   * @param deleteFn               Write Function to be used for delete
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords        Expected number of records when scanned
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> deleteBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
      String initCommitTime, int numRecordsInThisCommit,
      Function<Integer, List<HoodieKey>> keyGenFunction,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieKey>, String> deleteFn,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, boolean filerForCommitTimeWithAssert) throws Exception {

    // Delete 1 (only deletes)
    client.startCommitWithTime(newCommitTime);

    List<HoodieKey> keysToDelete = keyGenFunction.apply(numRecordsInThisCommit);
    JavaRDD<HoodieKey> deleteRecords = jsc.parallelize(keysToDelete, 1);

    JavaRDD<WriteStatus> result = deleteFn.apply(client, deleteRecords, newCommitTime);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    // check the partition metadata is written out
    assertPartitionMetadataForKeys(keysToDelete, fs);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();

    if (assertForCommit) {
      assertEquals(3, timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants(),
          "Expecting 3 commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().getTimestamp(),
          "Latest commit should be " + newCommitTime);
      if (filerForCommitTimeWithAssert) { // if meta cols are disabled, we can't do assertion based on assertion time
        assertEquals(expRecordsInThisCommit, HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
            "Must contain " + expRecordsInThisCommit + " records");
      }

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals(expTotalRecords, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
          "Must contain " + expTotalRecords + " records");

      if (filerForCommitTimeWithAssert) {
        // Check that the incremental consumption from prevCommitTime
        assertEquals(HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime).count(),
            HoodieClientTestUtils.countRecordsOptionallySince(jsc, basePath, sqlContext, timeline, Option.of(prevCommitTime)),
            "Incremental consumption from " + prevCommitTime + " should give no records in latest commit,"
                + " since it is a delete operation");
      }
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
  public HoodieCleanStat getCleanStat(List<HoodieCleanStat> hoodieCleanStatsTwo, String partitionPath) {
    return hoodieCleanStatsTwo.stream().filter(e -> e.getPartitionPath().equals(partitionPath)).findFirst().orElse(null);
  }

  // Functional Interfaces for passing lambda and Hoodie Write API contexts

  @FunctionalInterface
  public interface Function2<R, T1, T2> {

    R apply(T1 v1, T2 v2) throws IOException;
  }

  @FunctionalInterface
  public interface Function3<R, T1, T2, T3> {

    R apply(T1 v1, T2 v2, T3 v3) throws IOException;
  }

}
