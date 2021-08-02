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

package org.apache.hudi.client.functional;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieMetadataMetrics;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngrade;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag("functional")
public class TestHoodieBackedMetadata extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestHoodieBackedMetadata.class);

  @TempDir
  public java.nio.file.Path tempFolder;

  private String metadataTableBasePath;

  private HoodieTableType tableType;

  public void init(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    initPath();
    initSparkContexts("TestHoodieMetadata");
    initFileSystem();
    fs.mkdirs(new Path(basePath));
    initMetaClient(tableType);
    initTestDataGenerator();
    metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);

  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  /**
   * Metadata Table bootstrap scenarios.
   */
  @Test
  public void testMetadataTableBootstrap() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Metadata table should not exist until created for the first time
    assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not exist");
    assertThrows(TableNotFoundException.class, () -> HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build());

    // Metadata table is not created if disabled by config
    String firstCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      client.startCommitWithTime(firstCommitTime);
      client.insert(jsc.parallelize(dataGen.generateInserts(firstCommitTime, 5)), firstCommitTime);
      assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not be created");
      assertThrows(TableNotFoundException.class, () -> HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build());
    }

    HoodieTableMetaClient metadataTableMetaClient;
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);

    // Create an inflight commit on the table
    String inflightCommitTime = HoodieActiveTimeline.createNewInstantTime();
    testTable.addInflightCommit(inflightCommitTime);

    // Metadata table should not be created if any non-complete instants are present
    String secondCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfigBuilder(true, true, false, HoodieFailedWritesCleaningPolicy.LAZY).build())) {
      client.startCommitWithTime(secondCommitTime);
      client.insert(jsc.emptyRDD(), secondCommitTime);

      assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not be created");
      assertThrows(TableNotFoundException.class, () -> HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build());
    }

    // Rollback the inflight commit so that the metadata table can finally be created
    testTable.removeCommit(inflightCommitTime);

    // Metadata table created when enabled by config
    String thirdCommitTime = HoodieActiveTimeline.createNewInstantTime();
    String fourthCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      client.startCommitWithTime(thirdCommitTime);
      client.insert(jsc.parallelize(dataGen.generateUpdates(thirdCommitTime, 2)), thirdCommitTime);

      metadataTableMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
      assertTrue(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not be created");
      assertTrue(metadataTableMetaClient.getActiveTimeline().countInstants() == 2); // bootstrap and thirdCommit

      client.startCommitWithTime(fourthCommitTime);
      client.insert(jsc.emptyRDD(), fourthCommitTime);
      assertTrue(metadataTableMetaClient.getActiveTimeline().countInstants() == 2); // bootstrap, thirdCommit, fourthCommit

      validateMetadata(client);
    }

    /**

    // Delete the thirdCommitTime and fourthCommitTime instants and introduce a new commit. This should trigger a rebootstrap
    // of the metadata table as un-synched instants have been "archived".
    testTable.removeCommit(inflightCommitTime);
    final String metadataTableMetaPath = metadataTableBasePath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME;
    assertTrue(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(thirdCommitTime))));
    assertTrue(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(fourthCommitTime))));
    LOG.info("=--------------------------------------------- " + new Path(metaClient.getMetaPath(), "{" + thirdCommitTime + "," + fourthCommitTime + "}.*"));
    Arrays.stream(fs.globStatus(new Path(metaClient.getMetaPath(), "{" + firstCommitTime + "," + secondCommitTime + "," + thirdCommitTime + "," + fourthCommitTime + "}.*"))).forEach(s -> {
      LOG.info("----------------------------------------------------------------------------- " + s);
      try {
        fs.delete(s.getPath(), false);
      } catch (IOException e) {
        LOG.warn("Error when deleting instant " + s + ": " + e);
      }
    });

    String fifthCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      client.startCommitWithTime(fifthCommitTime);
      client.insert(jsc.emptyRDD(), fifthCommitTime);
      validateMetadata(client);
    }

    // Delete all existing instants on dataset to simulate archiving. This should trigger a re-bootstrap of the metadata
    // table as last synched instant has been "archived".
    final String metadataTableMetaPath = metadataTableBasePath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME;
    assertTrue(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(secondCommitTime))));

    Arrays.stream(fs.listStatus(new Path(metadataTableMetaClient.getMetaPath()))).filter(status -> status.getPath().getName().matches("^\\d+\\..*"))
        .forEach(status -> {
          try {
            fs.delete(status.getPath(), false);
          } catch (IOException e) {
            LOG.warn("Error when deleting instant " + status + ": " + e);
          }
        });

    String thirdCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      client.startCommitWithTime(sixthCommitTime);
      client.insert(jsc.parallelize(dataGen.generateUpdates(sixthCommitTime, 2)), sixthCommitTime);
      assertTrue(fs.exists(new Path(metadataTableBasePath)));
      validateMetadata(client);

      // Metadata Table should not have previous delta-commits as it was re-bootstrapped
      assertFalse(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(firstCommitTime))));
      assertFalse(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(secondCommitTime))));
      assertTrue(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(thirdCommitTime))));
    }
    */
  }

  /**
   * Only valid partition directories are added to the metadata.
   */
  @Test
  public void testOnlyValidPartitionsAdded() throws Exception {
    // This test requires local file system
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Create an empty directory which is not a partition directory (lacks partition metadata)
    final String nonPartitionDirectory = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-nonpartition";
    Files.createDirectories(Paths.get(basePath, nonPartitionDirectory));

    // Three directories which are partitions but will be ignored due to filter
    final String filterDirRegex = ".*-filterDir\\d|\\..*";
    final String filteredDirectoryOne = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-filterDir1";
    final String filteredDirectoryTwo = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-filterDir2";
    final String filteredDirectoryThree = ".backups";

    // Create some commits
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.withPartitionMetaFiles("p1", "p2", filteredDirectoryOne, filteredDirectoryTwo, filteredDirectoryThree)
        .addCommit("001").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10)
        .addCommit("002").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10, 10);

    final HoodieWriteConfig writeConfig =
            getWriteConfigBuilder(true, true, false, HoodieFailedWritesCleaningPolicy.NEVER)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withDirectoryFilterRegex(filterDirRegex).build()).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      client.startCommitWithTime("005");
      client.insert(jsc.emptyRDD(), "005");

      List<String> partitions = metadataWriter(client).metadata().getAllPartitionPaths();
      assertFalse(partitions.contains(nonPartitionDirectory),
          "Must not contain the non-partition " + nonPartitionDirectory);
      assertTrue(partitions.contains("p1"), "Must contain partition p1");
      assertTrue(partitions.contains("p2"), "Must contain partition p2");

      assertFalse(partitions.contains(filteredDirectoryOne),
          "Must not contain the filtered directory " + filteredDirectoryOne);
      assertFalse(partitions.contains(filteredDirectoryTwo),
          "Must not contain the filtered directory " + filteredDirectoryTwo);
      assertFalse(partitions.contains(filteredDirectoryThree),
          "Must not contain the filtered directory " + filteredDirectoryThree);

      FileStatus[] statuses = metadata(client).getAllFilesInPartition(new Path(basePath, "p1"));
      assertEquals(2, statuses.length);
      statuses = metadata(client).getAllFilesInPartition(new Path(basePath, "p2"));
      assertEquals(5, statuses.length);
      Map<String, FileStatus[]> partitionsToFilesMap = metadata(client).getAllFilesInPartitions(
          Arrays.asList(basePath + "/p1", basePath + "/p2"));
      assertEquals(2, partitionsToFilesMap.size());
      assertEquals(2, partitionsToFilesMap.get(basePath + "/p1").length);
      assertEquals(5, partitionsToFilesMap.get(basePath + "/p2").length);
    }
  }

  /**
   * Test various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testTableOperations(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {

      // Write 1 (Bulk insert)
      String newCommitTime = "001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 2 (inserts)
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);
      validateMetadata(client);

      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 3 (updates)
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 4 (updates and inserts)
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "005";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client);
      }

      // Write 5 (updates and inserts)
      newCommitTime = "006";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "007";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client);
      }

      // Deletes
      newCommitTime = "008";
      records = dataGen.generateDeletes(newCommitTime, 10);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      client.delete(deleteKeys, newCommitTime);
      validateMetadata(client);

      // Clean
      newCommitTime = "009";
      client.clean(newCommitTime);
      validateMetadata(client);

      // Restore
      client.restoreToInstant("006");
      validateMetadata(client);
    }
  }

  /**
   * Test multi-writer on metadata table with optimistic concurrency.
   */
  @Test
  public void testMetadataMultiWriter() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(FileSystemBasedLockProviderTestClass.class).build())
        .withProperties(properties)
        .build();

    ExecutorService executors = Executors.newFixedThreadPool(dataGen.getPartitionPaths().length);
    // Create clients in advance
    SparkRDDWriteClient[] writeClients = new SparkRDDWriteClient[dataGen.getPartitionPaths().length];
    for (int i = 0; i < dataGen.getPartitionPaths().length; ++i) {
      writeClients[i] = new SparkRDDWriteClient(engineContext, writeConfig);
    }

    // Parallel commits for separate partitions
    List<Future> futures = new LinkedList<>();
    for (int i = 0; i < dataGen.getPartitionPaths().length; ++i) {
      final int index = i;
      String newCommitTime = "00" + (index + 1);
      Future future = executors.submit(() -> {
        List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 100, dataGen.getPartitionPaths()[index]);
        writeClients[index].startCommitWithTime(newCommitTime);
        List<WriteStatus> writeStatuses = writeClients[index].insert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(writeStatuses);
      });
      futures.add(future);
    }

    // Wait for all commits to complete
    for (Future future : futures) {
      future.get();
    }

    // Ensure all commits were synced to the Metadata Table
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
    assertEquals(metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().countInstants(), 4);
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "001")));
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "002")));
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "003")));

    // Compaction may occur if the commits completed in order
    assertTrue(metadataMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().countInstants() <= 1);

    // Validation
    validateMetadata(writeClients[0]);
  }

  /**
   * Test rollback of various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testRollbackOperations(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // Write 1 (Bulk insert)
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 2 (inserts) + Rollback of inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      validateMetadata(client);

      // Write 3 (updates) + Rollback of updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 20);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      validateMetadata(client);

      // Rollback of updates and inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      validateMetadata(client);

      // Rollback of Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client);
      }

      // Rollback of Deletes
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateDeletes(newCommitTime, 10);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      writeStatuses = client.delete(deleteKeys, newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      validateMetadata(client);

      // Rollback of Clean
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.clean(newCommitTime);
      validateMetadata(client);
      client.rollback(newCommitTime);
      // TODO client.syncTableMetadata();
      validateMetadata(client);
    }

    // Rollback of partial commits
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(false, true, false).withRollbackUsingMarkers(false).build())) {
      // Write updates and inserts
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.rollback(newCommitTime);
      // TODO client.syncTableMetadata();
      validateMetadata(client);
    }

    // Marker based rollback of partial commits
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(false, true, false).withRollbackUsingMarkers(true).build())) {
      // Write updates and inserts
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.rollback(newCommitTime);
      // TODO client.syncTableMetadata();
      validateMetadata(client);
    }
  }

  /**
   * Test that manual rollbacks work correctly and enough timeline history is maintained on the metadata table
   * timeline.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  @Disabled
  public void testManualRollbacks(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Setting to archive more aggressively on the Metadata Table than the Dataset
    final int maxDeltaCommitsBeforeCompaction = 4;
    final int minArchiveCommitsMetadata = 2;
    final int minArchiveCommitsDataset = 4;
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .archiveCommitsWith(minArchiveCommitsMetadata, minArchiveCommitsMetadata + 1).retainCommits(1)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(minArchiveCommitsDataset, minArchiveCommitsDataset + 1)
            .retainCommits(1).retainFileVersions(1).withAutoClean(false).withAsyncClean(true).build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, config)) {
      // Initialize table with metadata
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // Perform multiple commits
      for (int i = 1; i < 10; ++i) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        if (i == 1) {
          records = dataGen.generateInserts(newCommitTime, 5);
        } else {
          records = dataGen.generateUpdates(newCommitTime, 2);
        }
        client.startCommitWithTime(newCommitTime);
        writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(writeStatuses);
      }
      validateMetadata(client);

      // We can only rollback those commits whose deltacommit have not been archived yet.
      int numRollbacks = 0;
      boolean exceptionRaised = false;

      metaClient.reloadActiveTimeline();
      List<HoodieInstant> allInstants = metaClient.getCommitsAndCompactionTimeline().getReverseOrderedInstants()
          .collect(Collectors.toList());
      for (HoodieInstant instantToRollback : allInstants) {
        try {
          client.rollback(instantToRollback.getTimestamp());
          ++numRollbacks;
        } catch (HoodieMetadataException e) {
          exceptionRaised = true;
          break;
        }
      }
      validateMetadata(client);

      assertTrue(exceptionRaised, "Rollback of archived instants should fail");
      // Since each rollback also creates a deltacommit, we can only support rolling back of half of the original
      // instants present before rollback started.
      assertTrue(numRollbacks >= Math.max(minArchiveCommitsDataset, minArchiveCommitsMetadata) / 2,
          "Rollbacks of non archived instants should work");
    }
  }

  /**
   * Test sync of table operations.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  @Disabled
  public void testSync(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    String newCommitTime;
    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;

    // Initial commits without metadata table enabled
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.startCommitWithTime(newCommitTime);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.startCommitWithTime(newCommitTime);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
    }

    // Enable metadata table so it initialized by listing from file system
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 5);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      validateMetadata(client);
      // TODO assertTrue(metadata(client).isInSync());
    }

    // Various table operations without metadata table enabled
    String restoreToInstant;
    String inflightActionTimestamp;
    String beforeInflightActionTimestamp;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      // updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // updates and inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
      }

      // Savepoint
      restoreToInstant = newCommitTime;
      if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
        client.savepoint("hoodie", "metadata test");
      }

      // Record a timestamp for creating an inflight instance for sync testing
      inflightActionTimestamp = HoodieActiveTimeline.createNewInstantTime();
      beforeInflightActionTimestamp = newCommitTime;

      // Deletes
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateDeletes(newCommitTime, 5);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      client.delete(deleteKeys, newCommitTime);

      // Clean
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.clean(newCommitTime);

      // updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // insert overwrite to test replacecommit
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
      records = dataGen.generateInserts(newCommitTime, 5);
      HoodieWriteResult replaceResult = client.insertOverwrite(jsc.parallelize(records, 1), newCommitTime);
      writeStatuses = replaceResult.getWriteStatuses().collect();
      assertNoWriteErrors(writeStatuses);
    }

    // If there is an incomplete operation, the Metadata Table is not updated beyond that operations but the
    // in-memory merge should consider all the completed operations.
    Path inflightCleanPath = new Path(metaClient.getMetaPath(), HoodieTimeline.makeInflightCleanerFileName(inflightActionTimestamp));
    fs.create(inflightCleanPath).close();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // Restore cannot be done until the metadata table is in sync. See HUDI-1502 for details
      // TODO client.syncTableMetadata();

      // Table should sync only before the inflightActionTimestamp
      HoodieBackedTableMetadataWriter writer =
          (HoodieBackedTableMetadataWriter) SparkHoodieBackedTableMetadataWriter.create(hadoopConf, client.getConfig(), context);
      assertEquals(writer.getLatestSyncedInstantTime().get(), beforeInflightActionTimestamp);

      // Reader should sync to all the completed instants
      HoodieTableMetadata metadata  = HoodieTableMetadata.create(context, client.getConfig().getMetadataConfig(),
          client.getConfig().getBasePath(), FileSystemViewStorageConfig.FILESYSTEM_VIEW_SPILLABLE_DIR.defaultValue());
      // TODO assertEquals(metadata.getSyncedInstantTimeForReader().get(), newCommitTime);

      // Remove the inflight instance holding back table sync
      fs.delete(inflightCleanPath, false);

      writer =
          (HoodieBackedTableMetadataWriter)SparkHoodieBackedTableMetadataWriter.create(hadoopConf, client.getConfig(), context);
      assertEquals(writer.getLatestSyncedInstantTime().get(), newCommitTime);

      // Reader should sync to all the completed instants
      metadata  = HoodieTableMetadata.create(context, client.getConfig().getMetadataConfig(),
          client.getConfig().getBasePath(), FileSystemViewStorageConfig.FILESYSTEM_VIEW_SPILLABLE_DIR.defaultValue());
      assertEquals(metadata.getSyncedInstantTime().get(), newCommitTime);
    }

    // Enable metadata table and ensure it is synced
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      client.restoreToInstant(restoreToInstant);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);

      validateMetadata(client);
    }
  }

  /**
   * Ensure that the reader only reads completed instants.
   * @throws IOException
   */
  @Test
  public void testReader() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;
    String[] commitTimestamps = {HoodieActiveTimeline.createNewInstantTime(), HoodieActiveTimeline.createNewInstantTime(),
        HoodieActiveTimeline.createNewInstantTime(), HoodieActiveTimeline.createNewInstantTime()};

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      for (int i = 0; i < commitTimestamps.length; ++i) {
        records = dataGen.generateInserts(commitTimestamps[i], 5);
        client.startCommitWithTime(commitTimestamps[i]);
        writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamps[i]).collect();
        assertNoWriteErrors(writeStatuses);
      }

      // Ensure we can see files from each commit
      Set<String> timelineTimestamps = getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
      assertEquals(timelineTimestamps.size(), commitTimestamps.length);
      for (int i = 0; i < commitTimestamps.length; ++i) {
        assertTrue(timelineTimestamps.contains(commitTimestamps[i]));
      }

      // mark each commit as incomplete and ensure files are not seen
      for (int i = 0; i < commitTimestamps.length; ++i) {
        FileCreateUtils.deleteCommit(basePath, commitTimestamps[i]);
        timelineTimestamps = getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
        assertEquals(timelineTimestamps.size(), commitTimestamps.length - 1);
        for (int j = 0; j < commitTimestamps.length; ++j) {
          assertTrue(j == i || timelineTimestamps.contains(commitTimestamps[j]));
        }
        FileCreateUtils.createCommit(basePath, commitTimestamps[i]);
      }

      // Test multiple incomplete commits
      FileCreateUtils.deleteCommit(basePath, commitTimestamps[0]);
      FileCreateUtils.deleteCommit(basePath, commitTimestamps[2]);
      timelineTimestamps = getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
      assertEquals(timelineTimestamps.size(), commitTimestamps.length - 2);
      for (int j = 0; j < commitTimestamps.length; ++j) {
        assertTrue(j == 0 || j == 2 || timelineTimestamps.contains(commitTimestamps[j]));
      }

      // Test no completed commits
      for (int i = 0; i < commitTimestamps.length; ++i) {
        FileCreateUtils.deleteCommit(basePath, commitTimestamps[i]);
      }
      timelineTimestamps = getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
      assertEquals(timelineTimestamps.size(), 0);
    }
  }

  /**
   * Instants on Metadata Table should be archived as per config but we always keep atlest the number of instants
   * as on the dataset.
   *
   * Metadata Table should be automatically compacted as per config.
   */
  @Test
  public void testCleaningArchivingAndCompaction() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    final int maxDeltaCommitsBeforeCompaction = 3;
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .archiveCommitsWith(40, 60).retainCommits(1)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 4)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.NEVER)
            .retainCommits(1).retainFileVersions(1).withAutoClean(true).withAsyncClean(false).build())
        .build();

    List<HoodieRecord> records;
    String newCommitTime;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, config)) {
      // Some initial commits so compaction is not triggered.
      // 1 deltacommit will be from bootstrap. So we can perform maxDeltaCommitsBeforeCompaction - 2 more commits before
      // compaction will be attempted.
      for (int i = 0; i < maxDeltaCommitsBeforeCompaction - 2; ++i) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        records = dataGen.generateInserts(newCommitTime, 5);
        client.startCommitWithTime(newCommitTime);
        client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      }

      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
      HoodieTableMetaClient datasetMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(config.getBasePath()).build();

      // There should not be any compaction yet and we have not performed more than maxDeltaCommitsBeforeCompaction
      // deltacommits (1 will be due to bootstrap)
      HoodieActiveTimeline metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitTimeline().filterCompletedInstants().countInstants(), 0);
      assertEquals(metadataTimeline.getCommitsTimeline().filterCompletedInstants().countInstants(), maxDeltaCommitsBeforeCompaction - 1);
      assertEquals(datasetMetaClient.getArchivedTimeline().reload().countInstants(), 0);

      // Next commit will initiate a compaction
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.startCommitWithTime(newCommitTime);
      client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitTimeline().filterCompletedInstants().countInstants(), 1);
      assertEquals(metadataTimeline.getCommitsTimeline().filterCompletedInstants().countInstants(), maxDeltaCommitsBeforeCompaction + 1);
      assertEquals(datasetMetaClient.getArchivedTimeline().reload().countInstants(), 0);

      // More than maxDeltaCommitsBeforeCompaction commits
      String inflightCommitTime = newCommitTime;
      for (int i = 0; i < maxDeltaCommitsBeforeCompaction + 1; ++i) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        records = dataGen.generateInserts(newCommitTime, 5);
        client.startCommitWithTime(newCommitTime);
        client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
        if (i == 0) {
          // Mark this commit inflight so compactions dont take place
          FileCreateUtils.deleteCommit(basePath, newCommitTime);
          FileCreateUtils.createInflightCommit(basePath, newCommitTime);
          inflightCommitTime = newCommitTime;
        }
      }

      // Ensure no more compactions took place due to the leftover inflight commit
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitTimeline().filterCompletedInstants().countInstants(), 1);
      assertEquals(metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants(), 2 * maxDeltaCommitsBeforeCompaction + 4 /* cleans */);

      // Complete commit
      FileCreateUtils.createCommit(basePath, inflightCommitTime);

      // Next commit should lead to compaction
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.startCommitWithTime(newCommitTime);
      client.insert(jsc.parallelize(records, 1), newCommitTime).collect();

      // Ensure compactions took place
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitTimeline().filterCompletedInstants().countInstants(), 2);
      assertEquals(metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants(), 2 * maxDeltaCommitsBeforeCompaction + 1 + 5 /* cleans */);

      assertTrue(datasetMetaClient.getArchivedTimeline().reload().countInstants() > 0);

      validateMetadata(client);
    }

    /*
    // ensure archiving has happened
    long numDataCompletedInstants = datasetMetaClient.getActiveTimeline().filterCompletedInstants().countInstants();
    long numDeltaCommits = metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants();
    assertTrue(numDeltaCommits < numDataCompletedInstants, "Must have less delta commits than total completed instants on data timeline.");
    */
  }

  @Test
  public void testUpgradeDowngrade() throws IOException {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Perform a commit. This should bootstrap the metadata table with latest version.
    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;
    String commitTimestamp = HoodieActiveTimeline.createNewInstantTime();
    HoodieWriteConfig writeConfig = getWriteConfig(true, true);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      records = dataGen.generateInserts(commitTimestamp, 5);
      client.startCommitWithTime(commitTimestamp);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamp).collect();
      assertNoWriteErrors(writeStatuses);
    }

    // Metadata table should have been bootstrapped
    assertTrue(fs.exists(new Path(metadataTableBasePath)), "Metadata table should exist");
    FileStatus oldStatus = fs.getFileStatus(new Path(metadataTableBasePath));

    // set hoodie.table.version to 1 in hoodie.properties file
    changeTableVersion(HoodieTableVersion.ONE);

    // With next commit the table should be deleted (as part of upgrade)
    commitTimestamp = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      records = dataGen.generateInserts(commitTimestamp, 5);
      client.startCommitWithTime(commitTimestamp);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamp).collect();
      assertNoWriteErrors(writeStatuses);
    }
    assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not exist");

    // With next commit the table should be re-bootstrapped (currently in the constructor. To be changed)
    commitTimestamp = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      records = dataGen.generateInserts(commitTimestamp, 5);
      client.startCommitWithTime(commitTimestamp);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamp).collect();
      assertNoWriteErrors(writeStatuses);
    }

    initMetaClient();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(), HoodieTableVersion.TWO.versionCode());
    assertTrue(fs.exists(new Path(metadataTableBasePath)), "Metadata table should exist");
    FileStatus newStatus = fs.getFileStatus(new Path(metadataTableBasePath));
    assertTrue(oldStatus.getModificationTime() < newStatus.getModificationTime());

    // Test downgrade by running the downgrader
    new SparkUpgradeDowngrade(metaClient, writeConfig, context).run(metaClient, HoodieTableVersion.ONE, writeConfig, context, null);

    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(), HoodieTableVersion.ONE.versionCode());
    assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not exist");
  }

  /**
   * Test various error scenarios.
   */
  @Test
  public void testErrorCases() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // TESTCASE: If commit on the metadata table succeeds but fails on the dataset, then on next init the metadata table
    // should be rolled back to last valid commit.
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 5);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // There is no way to simulate failed commit on the main dataset, hence we simply delete the completed
      // instant so that only the inflight is left over.
      String commitInstantFileName = HoodieTimeline.makeCommitFileName(newCommitTime);
      assertTrue(fs.delete(new Path(basePath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME,
          commitInstantFileName), false));
    }

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      String newCommitTime = client.startCommit();
      // Next insert
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 5);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // Post rollback commit and metadata should be valid
      validateMetadata(client);
    }
  }

  /**
   * Test non-partitioned datasets.
   */
  //@Test
  public void testNonPartitioned() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    HoodieTestDataGenerator nonPartitionedGenerator = new HoodieTestDataGenerator(new String[] {""});
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // Write 1 (Bulk insert)
      String newCommitTime = "001";
      List<HoodieRecord> records = nonPartitionedGenerator.generateInserts(newCommitTime, 10);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      validateMetadata(client);

      List<String> metadataPartitions = metadata(client).getAllPartitionPaths();
      assertTrue(metadataPartitions.contains(""), "Must contain empty partition");
    }
  }

  /**
   * Test various metrics published by metadata table.
   */
  @Test
  public void testMetadataMetrics() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfigBuilder(true, true, true).build())) {
      // Write
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      Registry metricsRegistry = Registry.getRegistry("HoodieMetadata");
      assertTrue(metricsRegistry.getAllCounts().containsKey(HoodieMetadataMetrics.INITIALIZE_STR + ".count"));
      assertTrue(metricsRegistry.getAllCounts().containsKey(HoodieMetadataMetrics.INITIALIZE_STR + ".totalDuration"));
      assertTrue(metricsRegistry.getAllCounts().get(HoodieMetadataMetrics.INITIALIZE_STR + ".count") >= 1L);
      final String prefix = MetadataPartitionType.FILES.partitionPath() + ".";
      assertTrue(metricsRegistry.getAllCounts().containsKey(prefix + HoodieMetadataMetrics.STAT_COUNT_BASE_FILES));
      assertTrue(metricsRegistry.getAllCounts().containsKey(prefix + HoodieMetadataMetrics.STAT_COUNT_LOG_FILES));
      assertTrue(metricsRegistry.getAllCounts().containsKey(prefix + HoodieMetadataMetrics.STAT_TOTAL_BASE_FILE_SIZE));
      assertTrue(metricsRegistry.getAllCounts().containsKey(prefix + HoodieMetadataMetrics.STAT_TOTAL_LOG_FILE_SIZE));
    }
  }

  /**
   * Validate the metadata tables contents to ensure it matches what is on the file system.
   */
  private void validateMetadata(SparkRDDWriteClient testClient) throws IOException {
    HoodieWriteConfig config = testClient.getConfig();

    SparkRDDWriteClient client;
    if (config.isEmbeddedTimelineServerEnabled()) {
      testClient.close();
      client = new SparkRDDWriteClient(testClient.getEngineContext(), testClient.getConfig());
    } else {
      client = testClient;
    }

    HoodieTableMetadata tableMetadata = metadata(client);
    assertNotNull(tableMetadata, "MetadataReader should have been initialized");
    if (!config.useFileListingMetadata()) {
      return;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Partitions should match
    FileSystemBackedTableMetadata fsBackedTableMetadata = new FileSystemBackedTableMetadata(engineContext,
        new SerializableConfiguration(hadoopConf), config.getBasePath(), config.shouldAssumeDatePartitioning());
    List<String> fsPartitions = fsBackedTableMetadata.getAllPartitionPaths();
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertTrue(fsPartitions.equals(metadataPartitions), "Partitions should match");

    // Files within each partition should match
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(config, engineContext);
    TableFileSystemView tableView = table.getHoodieView();
    List<String> fullPartitionPaths = fsPartitions.stream().map(partition -> basePath + "/" + partition).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = tableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    assertEquals(fsPartitions.size(), partitionToFilesMap.size());

    fsPartitions.forEach(partition -> {
      try {
        Path partitionPath;
        if (partition.equals("")) {
          // Should be the non-partitioned case
          partitionPath = new Path(basePath);
        } else {
          partitionPath = new Path(basePath, partition);
        }
        FileStatus[] fsStatuses = FSUtils.getAllDataFilesInPartition(fs, partitionPath);
        FileStatus[] metaStatuses = tableMetadata.getAllFilesInPartition(partitionPath);
        List<String> fsFileNames = Arrays.stream(fsStatuses)
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        List<String> metadataFilenames = Arrays.stream(metaStatuses)
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        Collections.sort(fsFileNames);
        Collections.sort(metadataFilenames);

        assertEquals(fsStatuses.length, partitionToFilesMap.get(basePath + "/" + partition).length);

        // File sizes should be valid
        Arrays.stream(metaStatuses).forEach(s -> assertTrue(s.getLen() > 0));

        // Block sizes should be valid
        Arrays.stream(metaStatuses).forEach(s -> assertTrue(s.getBlockSize() > 0));
        List<Long> fsBlockSizes = Arrays.stream(fsStatuses).map(FileStatus::getBlockSize).collect(Collectors.toList());
        Collections.sort(fsBlockSizes);
        List<Long> metadataBlockSizes = Arrays.stream(metaStatuses).map(FileStatus::getBlockSize).collect(Collectors.toList());
        Collections.sort(metadataBlockSizes);
        assertEquals(fsBlockSizes, metadataBlockSizes);

        if ((fsFileNames.size() != metadataFilenames.size()) || (!fsFileNames.equals(metadataFilenames))) {
          LOG.info("*** File system listing = " + Arrays.toString(fsFileNames.toArray()));
          LOG.info("*** Metadata listing = " + Arrays.toString(metadataFilenames.toArray()));

          for (String fileName : fsFileNames) {
            if (!metadataFilenames.contains(fileName)) {
              LOG.error(partition + "FsFilename " + fileName + " not found in Meta data");
            }
          }
          for (String fileName : metadataFilenames) {
            if (!fsFileNames.contains(fileName)) {
              LOG.error(partition + "Metadata file " + fileName + " not found in original FS");
            }
          }
        }

        assertEquals(fsFileNames.size(), metadataFilenames.size(), "Files within partition " + partition + " should match");
        assertTrue(fsFileNames.equals(metadataFilenames), "Files within partition " + partition + " should match");

        // FileSystemView should expose the same data
        List<HoodieFileGroup> fileGroups = tableView.getAllFileGroups(partition).collect(Collectors.toList());
        fileGroups.addAll(tableView.getAllReplacedFileGroups(partition).collect(Collectors.toList()));

        fileGroups.forEach(g -> LogManager.getLogger(TestHoodieBackedMetadata.class).info(g));
        fileGroups.forEach(g -> g.getAllBaseFiles().forEach(b -> LogManager.getLogger(TestHoodieBackedMetadata.class).info(b)));
        fileGroups.forEach(g -> g.getAllFileSlices().forEach(s -> LogManager.getLogger(TestHoodieBackedMetadata.class).info(s)));

        long numFiles = fileGroups.stream()
            .mapToLong(g -> g.getAllBaseFiles().count() + g.getAllFileSlices().mapToLong(s -> s.getLogFiles().count()).sum())
            .sum();
        assertEquals(metadataFilenames.size(), numFiles);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        assertTrue(false, "Exception should not be raised: " + e);
      }
    });

    HoodieBackedTableMetadataWriter metadataWriter = metadataWriter(client);
    assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

    // Validate write config for metadata table
    HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
    assertFalse(metadataWriteConfig.useFileListingMetadata(), "No metadata table for metadata table");

    // Metadata table should be in sync with the dataset
    //TODO assertTrue(metadata(client).isInSync());
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();

    // Metadata table is MOR
    assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

    // Metadata table is HFile format
    assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
        "Metadata Table base file format should be HFile");

    // Metadata table has a fixed number of partitions
    // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
    // in the .hoodie folder.
    List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, HoodieTableMetadata.getMetadataTableBasePath(basePath),
        false, false);
    assertEquals(MetadataPartitionType.values().length, metadataTablePartitions.size());

    // Metadata table should automatically compact and clean
    // versions are +1 as autoclean / compaction happens end of commits
    int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metadataMetaClient, metadataMetaClient.getActiveTimeline());
    metadataTablePartitions.forEach(partition -> {
      List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());
      assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).count() <= 1, "Should have a single latest base file");
      assertTrue(latestSlices.size() <= 1, "Should have a single latest file slice");
      assertTrue(latestSlices.size() <= numFileVersions, "Should limit file slice to "
          + numFileVersions + " but was " + latestSlices.size());
    });

    LOG.info("Validation time=" + timer.endTimer());
  }

  /**
   * Returns the list of all files in the dataset by iterating over the metadata table.
   * @throws IOException
   * @throws IllegalArgumentException
   */
  private List<Path> getAllFiles(HoodieTableMetadata metadata) throws Exception {
    List<Path> allfiles = new LinkedList<>();
    for (String partition : metadata.getAllPartitionPaths()) {
      for (FileStatus status : metadata.getAllFilesInPartition(new Path(basePath, partition))) {
        allfiles.add(status.getPath());
      }
    }

    return allfiles;
  }

  private HoodieBackedTableMetadataWriter metadataWriter(SparkRDDWriteClient client) {
    return (HoodieBackedTableMetadataWriter) SparkHoodieBackedTableMetadataWriter
        .create(hadoopConf, client.getConfig(), new HoodieSparkEngineContext(jsc));
  }

  private HoodieTableMetadata metadata(SparkRDDWriteClient client) {
    HoodieWriteConfig clientConfig = client.getConfig();
    return HoodieTableMetadata.create(client.getEngineContext(), clientConfig.getMetadataConfig(), clientConfig.getBasePath(),
        clientConfig.getSpillableMapBasePath());
  }

  // TODO: this can be moved to TestHarness after merge from master
  private void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors(), "Errors found in write of " + status.getFileId());
    }
  }

  private HoodieWriteConfig getWriteConfig(boolean autoCommit, boolean useFileListingMetadata) {
    return getWriteConfigBuilder(autoCommit, useFileListingMetadata, false).build();
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(boolean autoCommit, boolean useFileListingMetadata, boolean enableMetrics) {
    return getWriteConfigBuilder(autoCommit, useFileListingMetadata, enableMetrics, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(boolean autoCommit, boolean useFileListingMetadata, boolean enableMetrics,
      HoodieFailedWritesCleaningPolicy failedWritesPolicy) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withDeleteParallelism(2).withRollbackParallelism(2).withFinalizeWriteParallelism(2)
        .withAutoCommit(autoCommit)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1)
            .withFailedWritesCleaningPolicy(failedWritesPolicy)
            .withAutoClean(false).retainCommits(1).retainFileVersions(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(useFileListingMetadata)
            .enableMetrics(enableMetrics).build())
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(enableMetrics)
            .withExecutorMetrics(true).usePrefix("unit-test").build());
  }

  private void changeTableVersion(HoodieTableVersion version) throws IOException {
    metaClient.getTableConfig().setTableVersion(version);
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream os = metaClient.getFs().create(propertyFile)) {
      metaClient.getTableConfig().getProps().store(os, "");
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }
}
