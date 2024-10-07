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

import org.apache.hudi.client.transaction.PreferWriterConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMultiWriterWithPreferWriterIngestion extends HoodieClientTestBase {

  public void setUpMORTestTable() throws IOException {
    cleanupResources();
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initHoodieStorage();
    storage.createDirectory(new StoragePath(basePath));
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ,
        HoodieFileFormat.PARQUET);
    initTestDataGenerator();
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"MERGE_ON_READ"})
  public void testMultiWriterWithAsyncTableServicesWithConflict(HoodieTableType tableType) throws Exception {
    // create inserts X 1
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
    }
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    // Disabling embedded timeline server, it doesn't work with multiwriter
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAutoClean(false).withAsyncClean(true)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withEmbeddedTimelineServerEnabled(false)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(
            FileSystemViewStorageType.MEMORY).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClusteringNumCommits(1).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new PreferWriterConflictResolutionStrategy())
            .build()).withAutoCommit(false).withProperties(properties).build();
    Set<String> validInstants = new HashSet<>();
    // Create the first commit with inserts
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    String instantTime1 = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithInserts(cfg, client, "000", instantTime1, 200);
    validInstants.add(instantTime1);
    // Create 2 commits with upserts
    String instantTime2 = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithUpserts(cfg, client, instantTime1, "000", instantTime2, 100);
    String instantTime3 = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithUpserts(cfg, client, instantTime2, "000", instantTime3, 100);
    validInstants.add(instantTime2);
    validInstants.add(instantTime3);
    ExecutorService executors = Executors.newFixedThreadPool(2);
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg);
    // Create upserts, schedule cleaning, schedule compaction in parallel
    Future future1 = executors.submit(() -> {
      int numRecords = 100;
      String commitTimeBetweenPrevAndNew = instantTime2;
      try {
        int counter = 0;
        while (counter++ < 5) { // we can't really time which writer triggers first and which one triggers later. So, lets add few rounds of retries.
          // For both COW and MOR table types the commit should not be blocked, since we are giving preference to ingestion.
          try {
            String instant4 = HoodieActiveTimeline.createNewInstantTime();
            createCommitWithUpserts(cfg, client1, instantTime3, commitTimeBetweenPrevAndNew, instant4, numRecords);
            validInstants.add(instant4);
            break;
          } catch (IllegalArgumentException e) {
            if (!e.getMessage().toString().contains("Found later commit time")) {
              throw e;
            }
          }
        }
      } catch (Exception e1) {
        throw new RuntimeException(e1);
      }
    });
    AtomicReference<String> instant5Ref = new AtomicReference<>();
    Future future2 = executors.submit(() -> {
      try {
        int counter = 0;
        while (counter++ < 5) { // we can't really time which writer triggers first and which one triggers later. So, lets add few rounds of retries.
          try {
            String instant5 = HoodieActiveTimeline.createNewInstantTime();
            client2.scheduleTableService(instant5, Option.empty(), TableServiceType.COMPACT);
            instant5Ref.set(instant5);
            break;
          } catch (IllegalArgumentException e) {
            if (!e.getMessage().toString().contains("Found later commit time")) {
              throw e;
            }
          }
        }
      } catch (Exception e2) {
        if (tableType == HoodieTableType.MERGE_ON_READ) {
          throw new RuntimeException(e2);
        }
      }
    });
    AtomicReference<String> instant6Ref = new AtomicReference<>();
    Future future3 = executors.submit(() -> {
      int counter = 0;
      while (counter++ < 5) { // we can't really time which writer triggers first and which one triggers later. So, lets add few rounds of retries.
        try {
          String instant6 = HoodieActiveTimeline.createNewInstantTime();
          client2.scheduleTableService(instant6, Option.empty(), TableServiceType.CLEAN);
          instant6Ref.set(instant6);
          break;
        } catch (IllegalArgumentException e) {
          if (!e.getMessage().toString().contains("Found later commit time")) {
            throw e;
          }
        } catch (Exception e2) {
          throw new RuntimeException(e2);
        }
      }
    });
    future1.get();
    future2.get();
    future3.get();
    // Create inserts, run cleaning, run compaction in parallel
    future1 = executors.submit(() -> {
      int numRecords = 100;
      int counter = 0;
      while (counter++ < 5) { // we can't really time which writer triggers first and which one triggers later. So, lets add few rounds of retries.
        try {
          String instant7 = HoodieActiveTimeline.createNewInstantTime();
          createCommitWithInserts(cfg, client1, instantTime3, instant7, numRecords);
          validInstants.add(instant7);
          break;
        } catch (IllegalArgumentException e) {
          if (!e.getMessage().toString().contains("Found later commit time")) {
            throw e;
          }
        } catch (Exception e1) {
          throw new RuntimeException(e1);
        }
      }
    });
    future2 = executors.submit(() -> {
      int counter = 0;
      while (counter++ < 5) { // we can't really time which writer triggers first and which one triggers later. So, lets add few rounds of retries.
        try {
          HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client2.compact(instant5Ref.get());
          client2.commitCompaction(instant5Ref.get(), compactionMetadata.getCommitMetadata().get(), Option.empty());
          validInstants.add(instant5Ref.get());
          break;
        } catch (IllegalArgumentException e) {
          if (!e.getMessage().toString().contains("Found later commit time")) {
            throw e;
          }
        } catch (Exception e2) {
          if (tableType == HoodieTableType.MERGE_ON_READ) {
            Assertions.assertTrue(e2 instanceof HoodieWriteConflictException);
          }
        }
      }
    });
    future3 = executors.submit(() -> {
      int counter = 0;
      while (counter++ < 5) { // we can't really time which writer triggers first and which one triggers later. So, lets add few rounds of retries.
        try {
          client2.clean(instant6Ref.get(), false);
          validInstants.add(instant6Ref.get());
          break;
        } catch (IllegalArgumentException e) {
          if (!e.getMessage().toString().contains("Found later commit time")) {
            throw e;
          }
        } catch (Exception e2) {
          throw new RuntimeException(e2);
        }
      }
    });
    future1.get();
    future2.get();
    future3.get();
    Set<String> completedInstants = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toSet());
    Assertions.assertTrue(validInstants.containsAll(completedInstants));
  }

  private String getNextCommitTime() {
    return HoodieActiveTimeline.createNewInstantTime();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testHoodieClientMultiWriterWithClustering(HoodieTableType tableType) throws Exception {
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
    }
    Properties properties = new Properties();
    // Use RDD API to perform clustering (TODO: Fix row-writer API)
    properties.put("hoodie.datasource.write.row.writer.enable", String.valueOf(false));
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    HoodieWriteConfig cfg = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClusteringNumCommits(1).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new PreferWriterConflictResolutionStrategy())
            .build()).withAutoCommit(false).withProperties(properties).build();
    // Create the first commit
    String instant1 = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithInserts(cfg, getHoodieWriteClient(cfg), "000", instant1, 200);
    // Start another inflight commit
    String instant2 = HoodieActiveTimeline.createNewInstantTime();
    int numRecords = 100;
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);

    JavaRDD<WriteStatus> result1 = updateBatch(cfg, client1, instant2, instant1,
        Option.of(Arrays.asList(instant1)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2);
    // Start and finish another commit while the previous writer for commit 003 is running
    String instant3 = HoodieActiveTimeline.createNewInstantTime();
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg);
    JavaRDD<WriteStatus> result2 = updateBatch(cfg, client2, instant3, instant1,
        Option.of(Arrays.asList(instant1)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2);
    client2.commit(instant3, result2);
    // Schedule and run clustering while previous writer for commit 003 is running
    SparkRDDWriteClient client3 = getHoodieWriteClient(cfg);
    // schedule clustering
    Option<String> clusterInstant = client3.scheduleTableService(Option.empty(), TableServiceType.CLUSTER);

    // Since instant 2 is still in inflight the clustering commit should fail with HoodieWriteConflictException exception.
    assertThrows(HoodieClusteringException.class, () -> client3.cluster(clusterInstant.get(), true));
  }

  private void createCommitWithInserts(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                       String prevCommitTime, String newCommitTime, int numRecords) throws Exception {
    // Finish first base commmit
    JavaRDD<WriteStatus> result = insertFirstBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, SparkRDDWriteClient::bulkInsert,
        false, false, numRecords);
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
