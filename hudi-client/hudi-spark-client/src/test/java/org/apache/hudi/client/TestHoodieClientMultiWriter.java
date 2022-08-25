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

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHoodieClientMultiWriter extends HoodieClientTestBase {

  private Properties lockProperties = null;

  @BeforeEach
  public void setup() throws IOException {
    if (lockProperties == null) {
      lockProperties = new Properties();
      lockProperties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
      lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
      lockProperties.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "1");
      lockProperties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
      lockProperties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");
      lockProperties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "3");
    }
  }

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

  private static final List<Class> LOCK_PROVIDER_CLASSES = Arrays.asList(
        InProcessLockProvider.class,
        FileSystemBasedLockProvider.class);

  private static Iterable<Object[]> providerClassAndTableType() {
    List<Object[]> opts = new ArrayList<>();
    for (Object providerClass : LOCK_PROVIDER_CLASSES) {
      opts.add(new Object[] {HoodieTableType.COPY_ON_WRITE, providerClass});
      opts.add(new Object[] {HoodieTableType.MERGE_ON_READ, providerClass});
    }
    return opts;
  }

  @ParameterizedTest
  @MethodSource("providerClassAndTableType")
  public void testHoodieClientBasicMultiWriter(HoodieTableType tableType, Class providerClass) throws Exception {
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
    }
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");

    HoodieWriteConfig writeConfig = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .withAutoArchive(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(providerClass)
            .build()).withAutoCommit(false).withProperties(lockProperties).build();

    // Create the first commit
    createCommitWithInserts(writeConfig, getHoodieWriteClient(writeConfig), "000", "001", 200, true);

    final int threadCount = 2;
    final ExecutorService executors = Executors.newFixedThreadPool(2);
    final SparkRDDWriteClient client1 = getHoodieWriteClient(writeConfig);
    final SparkRDDWriteClient client2 = getHoodieWriteClient(writeConfig);

    final CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount);
    final AtomicBoolean writer1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    Future future1 = executors.submit(() -> {
      try {
        final String nextCommitTime = "002";
        final JavaRDD<WriteStatus> writeStatusList = startCommitForUpdate(writeConfig, client1, nextCommitTime, 100);

        // Wait for the 2nd writer to start the commit
        cyclicBarrier.await(60, TimeUnit.SECONDS);

        // Commit the update before the 2nd writer
        assertDoesNotThrow(() -> {
          client1.commit(nextCommitTime, writeStatusList);
        });

        // Signal the 2nd writer to go ahead for his commit
        cyclicBarrier.await(60, TimeUnit.SECONDS);
        writer1Completed.set(true);
      } catch (Exception e) {
        writer1Completed.set(false);
      }
    });

    Future future2 = executors.submit(() -> {
      try {
        final String nextCommitTime = "003";

        // Wait for the 1st writer to make progress with the commit
        cyclicBarrier.await(60, TimeUnit.SECONDS);
        final JavaRDD<WriteStatus> writeStatusList = startCommitForUpdate(writeConfig, client2, nextCommitTime, 100);

        // Wait for the 1st writer to complete the commit
        cyclicBarrier.await(60, TimeUnit.SECONDS);
        assertThrows(HoodieWriteConflictException.class, () -> {
          client2.commit(nextCommitTime, writeStatusList);
        });
        writer2Completed.set(true);
      } catch (Exception e) {
        writer2Completed.set(false);
      }
    });

    future1.get();
    future2.get();

    // both should have been completed successfully. I mean, we already assert for conflict for writer2 at L155.
    assertTrue(writer1Completed.get() && writer2Completed.get());
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testMultiWriterWithInsertsToDistinctPartitions(HoodieTableType tableType) throws Exception {
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
    }

    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "3000");
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "20");

    HoodieWriteConfig cfg = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(2)
            .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .withAutoCommit(false)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withProperties(lockProperties)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.REMOTE_FIRST)
            .withSecondaryStorageType(FileSystemViewStorageType.MEMORY).build())
        .build();

    // Create the first commit
    SparkRDDWriteClient<?> client = getHoodieWriteClient(cfg);
    createCommitWithInsertsForPartition(cfg, client, "000", "001", 100, "2016/03/01");

    int numConcurrentWriters = 5;
    ExecutorService executors = Executors.newFixedThreadPool(numConcurrentWriters);

    List<Future<?>> futures = new ArrayList<>(numConcurrentWriters);
    for (int loop = 0; loop < numConcurrentWriters; loop++) {
      String newCommitTime = "00" + (loop + 2);
      String partition = "2016/03/0" + (loop + 2);
      futures.add(executors.submit(() -> {
        try {
          SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg);
          createCommitWithInsertsForPartition(cfg, writeClient, "001", newCommitTime, 100, partition);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    futures.forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Count down the latch and await for all the needed threads to join.
   *
   * @param latch          - Count down latch
   * @param waitTimeMillis - Max wait time in millis for waiting
   */
  private void latchCountDownAndWait(CountDownLatch latch, long waitTimeMillis) {
    latch.countDown();
    try {
      latch.await(waitTimeMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      //
    }
  }

  @ParameterizedTest
  @MethodSource("providerClassAndTableType")
  public void testMultiWriterWithAsyncTableServicesWithConflict(HoodieTableType tableType, Class providerClass) throws Exception {
    // create inserts X 1
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
    }
    // Disabling embedded timeline server, it doesn't work with multiwriter
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAutoClean(false)
            .withAsyncClean(true)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .withEmbeddedTimelineServerEnabled(false)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(
            FileSystemViewStorageType.MEMORY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(providerClass)
            .build()).withAutoCommit(false).withProperties(lockProperties);
    Set<String> validInstants = new HashSet<>();
    // Create the first commit with inserts
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    createCommitWithInserts(cfg, client, "000", "001", 200, true);
    validInstants.add("001");
    // Create 2 commits with upserts
    createCommitWithUpserts(cfg, client, "001", "000", "002", 100);
    createCommitWithUpserts(cfg, client, "002", "000", "003", 100);
    validInstants.add("002");
    validInstants.add("003");

    // Three clients running actions in parallel
    final int threadCount = 3;
    final CountDownLatch scheduleCountDownLatch = new CountDownLatch(threadCount);
    final ExecutorService executors = Executors.newFixedThreadPool(threadCount);

    // Write config with clustering enabled
    final HoodieWriteConfig cfg2 = writeConfigBuilder
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClustering(true)
            .withInlineClusteringNumCommits(1)
            .build())
        .build();
    final SparkRDDWriteClient client1 = getHoodieWriteClient(cfg2);
    final SparkRDDWriteClient client2 = getHoodieWriteClient(cfg);
    final SparkRDDWriteClient client3 = getHoodieWriteClient(cfg);

    // Create upserts, schedule cleaning, schedule compaction in parallel
    Future future1 = executors.submit(() -> {
      final String newCommitTime = "004";
      final int numRecords = 100;
      final String commitTimeBetweenPrevAndNew = "002";

      // We want the upsert to go through only after the compaction
      // and cleaning schedule completion. So, waiting on latch here.
      latchCountDownAndWait(scheduleCountDownLatch, 30000);
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        // Since the compaction already went in, this upsert has
        // to fail
        assertThrows(IllegalArgumentException.class, () -> {
          createCommitWithUpserts(cfg, client1, "003", commitTimeBetweenPrevAndNew, newCommitTime, numRecords);
        });
      } else {
        // We don't have the compaction for COW and so this upsert
        // has to pass
        assertDoesNotThrow(() -> {
          createCommitWithUpserts(cfg, client1, "003", commitTimeBetweenPrevAndNew, newCommitTime, numRecords);
        });
        validInstants.add(newCommitTime);
      }
    });

    Future future2 = executors.submit(() -> {
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        assertDoesNotThrow(() -> {
          client2.scheduleTableService("005", Option.empty(), TableServiceType.COMPACT);
        });
      }
      latchCountDownAndWait(scheduleCountDownLatch, 30000);
    });

    Future future3 = executors.submit(() -> {
      assertDoesNotThrow(() -> {
        latchCountDownAndWait(scheduleCountDownLatch, 30000);
        client3.scheduleTableService("006", Option.empty(), TableServiceType.CLEAN);
      });
    });
    future1.get();
    future2.get();
    future3.get();

    CountDownLatch runCountDownLatch = new CountDownLatch(threadCount);
    // Create inserts, run cleaning, run compaction in parallel
    future1 = executors.submit(() -> {
      final String newCommitTime = "007";
      final int numRecords = 100;
      latchCountDownAndWait(runCountDownLatch, 30000);
      assertDoesNotThrow(() -> {
        createCommitWithInserts(cfg, client1, "003", newCommitTime, numRecords, true);
        validInstants.add("007");
      });
    });

    future2 = executors.submit(() -> {
      latchCountDownAndWait(runCountDownLatch, 30000);
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        assertDoesNotThrow(() -> {
          HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata =  client2.compact("005");
          client2.commitCompaction("005", compactionMetadata.getCommitMetadata().get(), Option.empty());
          validInstants.add("005");
        });
      }
    });

    future3 = executors.submit(() -> {
      latchCountDownAndWait(runCountDownLatch, 30000);
      assertDoesNotThrow(() -> {
        client3.clean("006", false);
        validInstants.add("006");
      });
    });
    future1.get();
    future2.get();
    future3.get();

    validInstants.addAll(
        metaClient.reloadActiveTimeline().getCompletedReplaceTimeline()
            .filterCompletedInstants().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toSet()));
    Set<String> completedInstants = metaClient.reloadActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toSet());
    assertTrue(validInstants.containsAll(completedInstants));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testHoodieClientMultiWriterWithClustering(HoodieTableType tableType) throws Exception {
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      setUpMORTestTable();
    }
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .build()).withAutoCommit(false).withProperties(properties);
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    HoodieWriteConfig cfg2 = writeConfigBuilder.build();
    HoodieWriteConfig cfg3 = writeConfigBuilder
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClustering(true).withInlineClusteringNumCommits(1).build())
        .build();

    // Create the first commit
    createCommitWithInserts(cfg, getHoodieWriteClient(cfg), "000", "001", 200, true);
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
    JavaRDD<WriteStatus> result2 = updateBatch(cfg2, client2, newCommitTime, "001",
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2);
    client2.commit(newCommitTime, result2);
    // Schedule and run clustering while previous writer for commit 003 is running
    SparkRDDWriteClient client3 = getHoodieWriteClient(cfg3);
    // schedule clustering
    Option<String> clusterInstant = client3.scheduleTableService(Option.empty(), TableServiceType.CLUSTER);
    assertTrue(clusterInstant.isPresent());
    // Attempt to commit the inflight commit 003
    try {
      client1.commit("003", result1);
      fail("Should have thrown a concurrent conflict exception");
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testHoodieClientMultiWriterAutoCommitForConflict() throws Exception {
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "100");
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .build()).withAutoCommit(true).withProperties(lockProperties);
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    HoodieWriteConfig cfg2 = writeConfigBuilder.build();

    // Create the first commit
    createCommitWithInserts(cfg, getHoodieWriteClient(cfg), "000", "001", 5000, false);
    // Start another inflight commit
    String newCommitTime1 = "003";
    String newCommitTime2 = "004";
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg2);

    List<HoodieRecord> updates1 = dataGen.generateUpdates(newCommitTime1, 5000);
    List<HoodieRecord> updates2 = dataGen.generateUpdates(newCommitTime2, 5000);

    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(updates1, 4);
    JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(updates2, 4);

    runConcurrentAndAssert(writeRecords1, writeRecords2, client1, client2, SparkRDDWriteClient::upsert, true);
  }

  private void runConcurrentAndAssert(JavaRDD<HoodieRecord> writeRecords1, JavaRDD<HoodieRecord> writeRecords2,
                                      SparkRDDWriteClient client1, SparkRDDWriteClient client2,
                                      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                      boolean assertForConflict) throws ExecutionException, InterruptedException {

    CountDownLatch runCountDownLatch = new CountDownLatch(2);
    final ExecutorService executors = Executors.newFixedThreadPool(2);
    String newCommitTime1 = "003";
    String newCommitTime2 = "004";

    AtomicBoolean client1Succeeded = new AtomicBoolean(true);
    AtomicBoolean client2Succeeded = new AtomicBoolean(true);

    Future future1 = executors.submit(() -> {
          try {
            ingestBatch(writeFn, client1, newCommitTime1, writeRecords1, runCountDownLatch);
          } catch (IOException e) {
            LOG.error("IOException thrown " + e.getMessage());
          } catch (InterruptedException e) {
            LOG.error("Interrupted Exception thrown " + e.getMessage());
          } catch (Exception e) {
            client1Succeeded.set(false);
          }
        }
    );

    Future future2 = executors.submit(() -> {
          try {
            ingestBatch(writeFn, client2, newCommitTime2, writeRecords2, runCountDownLatch);
          } catch (IOException e) {
            LOG.error("IOException thrown " + e.getMessage());
          } catch (InterruptedException e) {
            LOG.error("Interrupted Exception thrown " + e.getMessage());
          } catch (Exception e) {
            client2Succeeded.set(false);
          }
        }
    );

    future1.get();
    future2.get();
    if (assertForConflict) {
      assertFalse(client1Succeeded.get() && client2Succeeded.get());
      assertTrue(client1Succeeded.get() || client2Succeeded.get());
    } else {
      assertTrue(client2Succeeded.get() && client1Succeeded.get());
    }
  }

  @Test
  public void testHoodieClientMultiWriterAutoCommitNonConflict() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "100");
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .build()).withAutoCommit(true).withProperties(properties);
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    HoodieWriteConfig cfg2 = writeConfigBuilder.build();

    // Create the first commit
    createCommitWithInserts(cfg, getHoodieWriteClient(cfg), "000", "001", 200, false);
    // Start another inflight commit
    String newCommitTime1 = "003";
    String newCommitTime2 = "004";
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg2);

    List<HoodieRecord> updates1 = dataGen.generateInserts(newCommitTime1, 200);
    List<HoodieRecord> updates2 = dataGen.generateInserts(newCommitTime2, 200);

    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(updates1, 1);
    JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(updates2, 1);

    runConcurrentAndAssert(writeRecords1, writeRecords2, client1, client2, SparkRDDWriteClient::bulkInsert, false);
  }

  private void ingestBatch(Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                           SparkRDDWriteClient writeClient, String commitTime, JavaRDD<HoodieRecord> records,
                           CountDownLatch countDownLatch) throws IOException, InterruptedException {
    writeClient.startCommitWithTime(commitTime);
    countDownLatch.countDown();
    countDownLatch.await();
    JavaRDD<WriteStatus> statusJavaRDD = writeFn.apply(writeClient, records, commitTime);
    statusJavaRDD.collect();
  }

  private void createCommitWithInsertsForPartition(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                                   String prevCommitTime, String newCommitTime, int numRecords,
                                                   String partition) throws Exception {
    JavaRDD<WriteStatus> result = insertBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, SparkRDDWriteClient::insert,
        false, false, numRecords, numRecords, 1, Option.of(partition));
    assertTrue(client.commit(newCommitTime, result), "Commit should succeed");
  }

  private void createCommitWithInserts(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                       String prevCommitTime, String newCommitTime, int numRecords,
                                       boolean doCommit) throws Exception {
    // Finish first base commit
    JavaRDD<WriteStatus> result = insertFirstBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, SparkRDDWriteClient::bulkInsert,
        false, false, numRecords);
    if (doCommit) {
      assertTrue(client.commit(newCommitTime, result), "Commit should succeed");
    }
  }

  private void createCommitWithUpserts(HoodieWriteConfig cfg, SparkRDDWriteClient client, String prevCommit,
                                       String commitTimeBetweenPrevAndNew, String newCommitTime, int numRecords)
      throws Exception {
    JavaRDD<WriteStatus> result = updateBatch(cfg, client, newCommitTime, prevCommit,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2);
    client.commit(newCommitTime, result);
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
}
