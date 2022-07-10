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

package org.apache.hudi.client;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.transaction.AsyncTimelineMarkerConflictResolutionStrategy;
import org.apache.hudi.client.transaction.SimpleDirectMarkerConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieClientMultiWriterWithEarlyConflictDetection extends HoodieClientTestBase {

  public void setUpMORTestTable() throws IOException {
    cleanupResources();
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initFileSystem();
    fs.mkdirs(new Path(basePath));
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, HoodieFileFormat.PARQUET);
    initTestDataGenerator();
  }

  /**
   * Test multi-writers with early conflict detect enable, including
   *    1. MOR + Direct marker
   *    2. COW + Direct marker
   *    3. MOR + Timeline server based marker
   *    4. COW + Timeline server based marker
   *
   *  ---|---------|--------------------|--------------------------------------|-------------------------> time
   * init 001
   *               002 start writing
   *                                    003 start which has conflict with 002
   *                                    and failed soon
   *                                                                           002 commit successfully
   * @param tableType
   * @param markerType
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("configParams")
  public void testHoodieClientBasicMultiWriterWithEarlyConflictDetection(String tableType, String markerType) throws Exception {
    if (tableType.equalsIgnoreCase(HoodieTableType.MERGE_ON_READ.name())) {
      setUpMORTestTable();
    }
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");

    HoodieWriteConfig writeConfig = buildWriteConfigForEarlyConflictDetect(markerType, properties);
    // Create the first commit
    final String nextCommitTime1 = "001";
    createCommitWithInserts(writeConfig, getHoodieWriteClient(writeConfig), "000", nextCommitTime1, 2000, true);

    final SparkRDDWriteClient client2 = getHoodieWriteClient(writeConfig);
    final SparkRDDWriteClient client3 = getHoodieWriteClient(writeConfig);

    final String nextCommitTime2 = "002";
    final JavaRDD<WriteStatus> writeStatusList2 = startCommitForUpdate(writeConfig, client2, nextCommitTime2, 1000);
    final String nextCommitTime3 = "003";

    assertThrows(SparkException.class, () -> {
      final JavaRDD<WriteStatus> writeStatusList3 = startCommitForUpdate(writeConfig, client3, nextCommitTime3, 1000);
      client3.commit(nextCommitTime3, writeStatusList3);
    }, "Early conflict detected but cannot resolve conflicts for overlapping writes");
    assertDoesNotThrow(() -> {
      client2.commit(nextCommitTime2, writeStatusList2);
    });

    List<String> completedInstant = metaClient.reloadActiveTimeline().getCommitsTimeline()
            .filterCompletedInstants().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());

    assertEquals(2, completedInstant.size());
    assertTrue(completedInstant.contains(nextCommitTime1));
    assertTrue(completedInstant.contains(nextCommitTime2));
  }

  private void createCommitWithInserts(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                           String prevCommitTime, String newCommitTime, int numRecords,
                                           boolean doCommit) throws Exception {
    // Finish first base commit
    JavaRDD<WriteStatus> result = insertFirstBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, SparkRDDWriteClient::insert,
            false, false, numRecords);
    if (doCommit) {
      assertTrue(client.commit(newCommitTime, result), "Commit should succeed");
    }
  }

  /**
   * Start the commit for an update operation with given number of records
   *
   * @param writeConfig   - Write config
   * @param writeClient   - Write client for starting the commit
   * @param newCommitTime - Commit time for the update
   * @param numRecords    - Number of records to update
   * @return RDD of write status from the update
   * @throws Exception
   */
  private JavaRDD<WriteStatus> startCommitForUpdate(HoodieWriteConfig writeConfig, SparkRDDWriteClient writeClient,
                                                    String newCommitTime, int numRecords) throws Exception {
    // Start the new commit
    writeClient.startCommitWithTime(newCommitTime);

    // Prepare update records
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(false, writeConfig, dataGen::generateUniqueUpdates);
    final List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecords);
    final JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Write updates
    Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn = SparkRDDWriteClient::upsert;
    JavaRDD<WriteStatus> result = writeFn.apply(writeClient, writeRecords, newCommitTime);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);
    return result;
  }

  public static Stream<Arguments> configParams() {
    Object[][] data =
            new Object[][] {{"COPY_ON_WRITE", MarkerType.TIMELINE_SERVER_BASED.name()}, {"MERGE_ON_READ", MarkerType.DIRECT.name()},
                {"MERGE_ON_READ", MarkerType.TIMELINE_SERVER_BASED.name()}, {"COPY_ON_WRITE", MarkerType.DIRECT.name()}};
    return Stream.of(data).map(Arguments::of);
  }

  private HoodieWriteConfig buildWriteConfigForEarlyConflictDetect(String markerType, Properties properties) {
    if (markerType.equalsIgnoreCase(MarkerType.DIRECT.name())) {
      return getConfigBuilder()
              .withHeartbeatIntervalInMs(3600 * 1000)
              .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
                      .withStorageType(FileSystemViewStorageType.MEMORY)
                      .withSecondaryStorageType(FileSystemViewStorageType.MEMORY).build())
              .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                      .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
                      .withAutoArchive(false).withAutoClean(false).build())
              .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
              .withMarkersType(MarkerType.DIRECT.name())
              .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
                      .withEarlyConflictDetectionEnable(true)
                      .withConflictResolutionStrategy(SimpleDirectMarkerConflictResolutionStrategy.class.getName())
                      .withMarkerConflictCheckerBatchInterval(0)
                      .withMarkerConflictCheckerPeriod(100)
                      .build()).withAutoCommit(false).withProperties(properties).build();
    } else {
      return getConfigBuilder()
              .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(20 * 1024).build())
              .withHeartbeatIntervalInMs(3600 * 1000)
              .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
                      .withStorageType(FileSystemViewStorageType.MEMORY)
                      .withSecondaryStorageType(FileSystemViewStorageType.MEMORY).build())
              .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                      .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
                      .withAutoArchive(false).withAutoClean(false).build())
              .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
              .withMarkersType(MarkerType.TIMELINE_SERVER_BASED.name())
              .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
                      .withEarlyConflictDetectionEnable(true)
                      .withConflictResolutionStrategy(AsyncTimelineMarkerConflictResolutionStrategy.class.getName())
                      .withMarkerConflictCheckerBatchInterval(0)
                      .withMarkerConflictCheckerPeriod(100)
                      .build()).withAutoCommit(false).withProperties(properties).build();
    }
  }
}
