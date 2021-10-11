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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.List;

import static java.util.UUID.randomUUID;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base Class providing setup/cleanup and utility methods for testing Hoodie Client facing tests.
 */
public class HoodieClientTestBase extends HoodieClientTestHarness {

  protected static final Logger LOG = LogManager.getLogger(HoodieClientTestBase.class);

  private static final String FILE_ID = randomUUID().toString();

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

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder() {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    return getConfigBuilder(schemaStr, IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(
      String schemaStr,
      IndexType indexType,
      HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(schemaStr)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(
            ConsistencyGuardConfig.newBuilder()
                .withConsistencyCheckEnabled(true)
                .build())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withFailedWritesCleaningPolicy(cleaningPolicy)
                .compactionSmallFileSize(1024 * 1024)
                .build())
        .withStorageConfig(
            HoodieStorageConfig.newBuilder()
                .hfileMaxFileSize(1024 * 1024)
                .parquetMaxFileSize(1024 * 1024)
                .orcMaxFileSize(1024 * 1024)
                .build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withFileSystemViewConfig(
            FileSystemViewStorageConfig.newBuilder()
                .withEnableBackupForRemoteFileSystemView(
                    false) // Fail test if problem connecting to timeline-server
                .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE)
                .build());
  }

  public void assertPartitionMetadataForRecords(List<HoodieRecord> inputRecords, FileSystem fs)
      throws IOException {
    assertPartitionMetadata(
        inputRecords.stream()
            .map(HoodieRecord::getPartitionPath)
            .distinct()
            .toArray(String[]::new),
        fs);
  }

  /**
   * Ensure presence of partition meta-data at known depth.
   *
   * @param partitionPaths Partition paths to check
   * @param fs             File System
   * @throws IOException in case of error
   */
  public void assertPartitionMetadata(String[] partitionPaths, FileSystem fs) throws IOException {
    for (String partitionPath : partitionPaths) {
      assertTrue(
          HoodiePartitionMetadata.hasPartitionMetadata(
              fs, new Path(basePath, partitionPath)));
      HoodiePartitionMetadata pmeta =
          new HoodiePartitionMetadata(fs, new Path(basePath, partitionPath));
      pmeta.readFromFS();
      assertEquals(
          HoodieTestDataGenerator.DEFAULT_PARTITION_DEPTH, pmeta.getPartitionDepth());
    }
  }

  /**
   * Helper to generate insert records generation function for testing Prepped version of API.
   * Prepped APIs expect the records to be already de-duped and have location set. This wrapper
   * takes care of record-location setting. Uniqueness is guaranteed by record-generation function
   * itself.
   *
   * @param recordGenFunction Records Generation function
   * @return Wrapped function
   */
  private Function2<List<HoodieRecord>, String, Integer> generateWrapInsertRecordsFn(
      final Function2<List<HoodieRecord>, String, Integer> recordGenFunction) {
    return (commit, numRecords) -> {
      final List<HoodieRecord> records = recordGenFunction.apply(commit, numRecords);
      records.forEach(r -> r.setCurrentLocation(new HoodieRecordLocation("I", FILE_ID)));
      return records;
    };
  }

  /**
   * Helper to generate upsert records generation function for testing Prepped version of API.
   * Prepped APIs expect the records to be already de-duped and have location set. This wrapper
   * takes care of record-location setting. Uniqueness is guaranteed by record-generation function
   * itself.
   *
   * @param recordGenFunction Records Generation function
   * @return Wrapped function
   */
  private Function2<List<HoodieRecord>, String, Integer> generateWrapUpsertRecordsFn(
      final Function2<List<HoodieRecord>, String, Integer> recordGenFunction) {
    return (commit, numRecords) -> {
      final List<HoodieRecord> records = recordGenFunction.apply(commit, numRecords);
      records.forEach(r -> r.setCurrentLocation(new HoodieRecordLocation(commit, FILE_ID)));
      return records;
    };
  }

  /**
   * Helper to insert first batch of records and do regular assertions on the state after
   * successful completion.
   *
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn                Write Function to be used for insertion
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public List<WriteStatus> insertFirstBatch(
      HoodieFlinkWriteClient client,
      String newCommitTime,
      int numRecordsInThisCommit,
      Function3<List<WriteStatus>, HoodieFlinkWriteClient, List<HoodieRecord>, String>
          writeFn)
      throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapInsertRecordsFn(dataGen::generateInserts);
    return writeBatch(
        client, newCommitTime, numRecordsInThisCommit, recordGenFunction, writeFn);
  }

  /**
   * Helper to upsert batch of records and do regular assertions on the state after successful
   * completion.
   *
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn                Write Function to be used for upsert
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public List<WriteStatus> updateBatch(
      HoodieFlinkWriteClient client,
      String newCommitTime,
      int numRecordsInThisCommit,
      Function3<List<WriteStatus>, HoodieFlinkWriteClient, List<HoodieRecord>, String>
          writeFn)
      throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapUpsertRecordsFn(dataGen::generateUniqueUpdates);
    return writeBatch(
        client, newCommitTime, numRecordsInThisCommit, recordGenFunction, writeFn);
  }

  /**
   * Helper to insert/upsert batch of records and do regular assertions on the state after
   * successful completion.
   *
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param recordGenFunction      Records Generation Function
   * @param writeFn                Write Function to be used for upsert
   * @throws Exception in case of error
   */
  public List<WriteStatus> writeBatch(
      HoodieFlinkWriteClient client,
      String newCommitTime,
      int numRecordsInThisCommit,
      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
      Function3<List<WriteStatus>, HoodieFlinkWriteClient, List<HoodieRecord>, String>
          writeFn)
      throws Exception {
    // Write 1 (only inserts)
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> writeRecords =
        recordGenFunction.apply(newCommitTime, numRecordsInThisCommit);
    List<WriteStatus> statuses = writeFn.apply(client, writeRecords, newCommitTime);
    assertNoWriteErrors(statuses);
    // Check the partition metadata is written out
    assertPartitionMetadataForRecords(writeRecords, fs);
    // Transition REQUESTED to INFLIGHT state
    client.transitionRequestedToInflight(HoodieActiveTimeline.COMMIT_ACTION, newCommitTime);
    return statuses;
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
