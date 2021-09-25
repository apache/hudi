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

import org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieClientMultiWriter extends HoodieClientTestBase {

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

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testHoodieClientBasicMultiWriter(HoodieTableType tableType) throws Exception {
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
    }
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(FileSystemBasedLockProviderTestClass.class)
            .build()).withAutoCommit(false).withProperties(properties).build();
    // Create the first commit
    createCommitWithInserts(cfg, getHoodieWriteClient(cfg), "000", "001", 200);
    try {
      ExecutorService executors = Executors.newFixedThreadPool(2);
      SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
      SparkRDDWriteClient client2 = getHoodieWriteClient(cfg);
      Future future1 = executors.submit(() -> {
        String newCommitTime = "004";
        int numRecords = 100;
        String commitTimeBetweenPrevAndNew = "002";
        try {
          createCommitWithUpserts(cfg, client1, "002", commitTimeBetweenPrevAndNew, newCommitTime, numRecords);
        } catch (Exception e1) {
          Assertions.assertTrue(e1 instanceof HoodieWriteConflictException);
          throw new RuntimeException(e1);
        }
      });
      Future future2 = executors.submit(() -> {
        String newCommitTime = "005";
        int numRecords = 100;
        String commitTimeBetweenPrevAndNew = "002";
        try {
          createCommitWithUpserts(cfg, client2, "002", commitTimeBetweenPrevAndNew, newCommitTime, numRecords);
        } catch (Exception e2) {
          Assertions.assertTrue(e2 instanceof HoodieWriteConflictException);
          throw new RuntimeException(e2);
        }
      });
      future1.get();
      future2.get();
      Assertions.fail("Should not reach here, this means concurrent writes were handled incorrectly");
    } catch (Exception e) {
      // Expected to fail due to overlapping commits
    }
  }

  @Disabled
  public void testMultiWriterWithAsyncTableServicesWithConflictCOW() throws Exception {
    testMultiWriterWithAsyncTableServicesWithConflict(HoodieTableType.COPY_ON_WRITE);
  }

  @Test
  public void testMultiWriterWithAsyncTableServicesWithConflictMOR() throws Exception {
    testMultiWriterWithAsyncTableServicesWithConflict(HoodieTableType.MERGE_ON_READ);
  }

  private void testMultiWriterWithAsyncTableServicesWithConflict(HoodieTableType tableType) throws Exception {
    // create inserts X 1
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
      LOG.warn("TEST_LOG. testMultiWriterWithAsyncTableServicesWithConflict for MOR " + basePath);
    } else {
      LOG.warn("TEST_LOG. testMultiWriterWithAsyncTableServicesWithConflict for COW " + basePath);
    }
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath);
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "3");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "5000");
    //properties.setProperty(HoodieMetadataConfig.ENABLE.key(), "false");
    // Disabling embedded timeline server, it doesn't work with multiwriter
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withAutoClean(false)
            .withInlineCompaction(false).withAsyncClean(true)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .withEmbeddedTimelineServerEnabled(false)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(
            FileSystemViewStorageType.MEMORY).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClusteringNumCommits(1).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        //.withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(FileSystemBasedLockProviderTestClass.class)
        //  .build())
        .withAutoCommit(false).withProperties(properties).build();

    Set<String> validInstants = new HashSet<>();
    // Create the first commit with inserts
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    createCommitWithInserts(cfg, client, "000", "001", 200);
    validInstants.add("001");
    LOG.warn("TEST_LOG. commit 001 complete ================ " + tableType.name());
    // Create 2 commits with upserts
    createCommitWithUpserts(cfg, client, "001", "000", "002", 100);
    LOG.warn("TEST_LOG. commit 002 complete ================ " + tableType.name());
    createCommitWithUpserts(cfg, client, "002", "000", "003", 100);
    LOG.warn("TEST_LOG. commit 003 complete ================ " + tableType.name());
    validInstants.add("002");
    validInstants.add("003");
    ExecutorService executors = Executors.newFixedThreadPool(2);
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg);
    // Create upserts, schedule cleaning, schedule compaction in parallel
    Future future1 = executors.submit(() -> {
      String newCommitTime = "004";
      int numRecords = 100;
      String commitTimeBetweenPrevAndNew = "002";
      try {
        createCommitWithUpserts(cfg, client1, "003", commitTimeBetweenPrevAndNew, newCommitTime, numRecords);
        if (tableType == HoodieTableType.MERGE_ON_READ) {
          LOG.warn("TEST_LOG. commit 004 completed by client1111 ---------------------- " + tableType.name());
          Assertions.fail("Conflicts not handled correctly");
        } else {
          LOG.warn("TEST_LOG. commit 004 completed by client1111" + tableType.name());
        }
        validInstants.add("004");
      } catch (Exception e1) {
        if (tableType == HoodieTableType.MERGE_ON_READ) {
          LOG.warn("TEST_LOG. commit 004 failed due to conflict w/ MOR table" + tableType.name());
          Assertions.assertTrue(e1 instanceof HoodieWriteConflictException);
        } else {
          LOG.warn("TEST_LOG. commit 004 failed due to conflict w/ COW table --------------- should not have happened" + tableType.name());
        }
      }
    });
    Future future2 = executors.submit(() -> {
      try {
        client2.scheduleTableService("005", Option.empty(), TableServiceType.COMPACT);
        LOG.warn("TEST_LOG. scheduling compaction 005 succeeded " + tableType.name());
      } catch (Exception e2) {
        if (tableType == HoodieTableType.MERGE_ON_READ) {
          LOG.warn("TEST_LOG. Compaction 005 w/ MOR should not throw exception --------------- " + tableType.name());
          throw new RuntimeException(e2);
        } else {
          LOG.warn("TEST_LOG. Compaction 005 w/ COW should not throw exception --------------- " + tableType.name() + " " + e2.getMessage());
        }
      }
    });
    Future future3 = executors.submit(() -> {
      try {
        client2.scheduleTableService("006", Option.empty(), TableServiceType.CLEAN);
        LOG.warn("TEST_LOG. table service clean 006 succeeded " + tableType.name());
      } catch (Exception e2) {
        LOG.warn("TEST_LOG. CLEAN 006 should not throw exception --------------- " + tableType.name());
        throw new RuntimeException(e2);
      }
    });
    future1.get();
    future2.get();
    future3.get();

    LOG.warn("TEST_LOG. Step 222222 " + tableType.name());
    metaClient.reloadActiveTimeline();
    List<String> completedInstants0 = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    List<String> allHeartBeats = client1.getHeartbeatClient().getAllExistingHeartbeatInstants();
    LOG.warn("TEST_LOG. all heart beats for client 1 " + Arrays.toString(allHeartBeats.toArray()));
    LOG.warn("TEST_LOG. completed instants " + tableType.name() + " = " + Arrays.toString(completedInstants0.toArray()));
    // Create inserts, run cleaning, run compaction in parallel
    future1 = executors.submit(() -> {
      String newCommitTime = "007";
      int numRecords = 100;
      try {
        createCommitWithInserts(cfg, client1, tableType == HoodieTableType.COPY_ON_WRITE ? "004" : "003", newCommitTime, numRecords);
        LOG.warn("TEST_LOG. Commit 007 completed client111 " + tableType.name());
        validInstants.add("007");
      } catch (Exception e1) {
        LOG.warn("TEST_LOG. Commit 007 failed --------------- " + tableType.name() + " " + e1.getMessage() + ", cause " + e1.getCause());
        throw new RuntimeException(e1);
      }
    });
    LOG.warn("TEST_LOG. should auto completed " + cfg.shouldAutoCommit() + tableType.name());
    future2 = executors.submit(() -> {
      try {
        JavaRDD<WriteStatus> writeStatusJavaRDD = (JavaRDD<WriteStatus>) client2.compact("005");
        LOG.warn("TEST_LOG. Compacted for 005. Going to commit compaction " + tableType.name());
        client2.commitCompaction("005", writeStatusJavaRDD, Option.empty());
        LOG.warn("TEST_LOG. Compaction complete for 005 " + tableType.name());
        validInstants.add("005");
      } catch (Exception e2) {
        if (tableType == HoodieTableType.MERGE_ON_READ) {
          LOG.warn("TEST_LOG. Compaction 005 failed for MOR ------------- " + tableType.name());
          throw new RuntimeException(e2);
        } else {
          LOG.warn("TEST_LOG. Compaction 005 failed for COW --------------- " + tableType.name());
        }
      }
    });
    future3 = executors.submit(() -> {
      try {
        client2.clean("006", false);
        LOG.warn("TEST_LOG. Clean 006 complete " + tableType.name());
        validInstants.add("006");
      } catch (Exception e2) {
        LOG.warn("TEST_LOG. Clean failed -------------- " + tableType.name());
        throw new RuntimeException(e2);
      }
    });
    future1.get();
    future2.get();
    future3.get();
    allHeartBeats = client1.getHeartbeatClient().getAllExistingHeartbeatInstants();
    LOG.warn("TEST_LOG. all heart beats for client 1 " + tableType.name() + " = " + Arrays.toString(allHeartBeats.toArray()));
    LOG.warn("TEST_LOG. Step 33333 " + tableType.name());
    List<String> completedInstants = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    LOG.warn("TEST_LOG. completed instants " + tableType + " = " + Arrays.toString(completedInstants.toArray()));
    Assertions.assertTrue(validInstants.containsAll(completedInstants));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testHoodieClientMultiWriterWithClustering(HoodieTableType tableType) throws Exception {
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
    }
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClusteringNumCommits(1).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(FileSystemBasedLockProviderTestClass.class)
            .build()).withAutoCommit(false).withProperties(properties).build();
    // Create the first commit
    createCommitWithInserts(cfg, getHoodieWriteClient(cfg), "000", "001", 200);
    // Start another inflight commit
    String newCommitTime = "003";
    int numRecords = 100;
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    String commitTimeBetweenPrevAndNew = "002";
    JavaRDD<WriteStatus> result1 = updateBatch(cfg, client1, newCommitTime, "001",
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2);
    // Start and finish another commit while the previous writer for commit 003 is running
    newCommitTime = "004";
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg);
    JavaRDD<WriteStatus> result2 = updateBatch(cfg, client2, newCommitTime, "001",
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2);
    client2.commit(newCommitTime, result2);
    // Schedule and run clustering while previous writer for commit 003 is running
    SparkRDDWriteClient client3 = getHoodieWriteClient(cfg);
    // schedule clustering
    Option<String> clusterInstant = client3.scheduleTableService(Option.empty(), TableServiceType.CLUSTER);
    client3.cluster(clusterInstant.get(), true);
    // Attempt to commit the inflight commit 003
    try {
      client1.commit("003", result1);
      Assertions.fail("Should have thrown a concurrent conflict exception");
    } catch (Exception e) {
      // Expected
    }
  }

  private void createCommitWithInserts(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                       String prevCommitTime, String newCommitTime, int numRecords) throws Exception {
    // Finish first base commmit
    JavaRDD<WriteStatus> result = insertFirstBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, SparkRDDWriteClient::bulkInsert,
        false, false, numRecords, false);
    assertTrue(client.commit(newCommitTime, result), "Commit should succeed");
  }

  private void createCommitWithUpserts(HoodieWriteConfig cfg, SparkRDDWriteClient client, String prevCommit,
                                       String commitTimeBetweenPrevAndNew, String newCommitTime, int numRecords)
      throws Exception {
    JavaRDD<WriteStatus> result = updateBatch(cfg, client, newCommitTime, prevCommit,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2);
    client.commit(newCommitTime, result);
  }

}
