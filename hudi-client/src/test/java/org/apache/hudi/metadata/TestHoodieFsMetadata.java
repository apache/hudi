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

package org.apache.hudi.metadata;

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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieMetadataConfig;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestHoodieFsMetadata extends HoodieClientTestHarness {
  private static final Logger LOG = LogManager.getLogger(TestHoodieFsMetadata.class);

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
    initMetaClient();
    initTestDataGenerator();
    metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  /**
   * Metadata Table should not be created unless it is enabled in config.
   */
  @Test
  public void testDefaultNoMetadataTable() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);

    // Metadata table should not exist until created for the first time
    assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not exist");
    assertThrows(TableNotFoundException.class, () -> new HoodieTableMetaClient(hadoopConf, metadataTableBasePath));

    // Metadata table is not created if disabled by config
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, false))) {
      client.startCommitWithTime("001");
      assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not be created");
      assertThrows(TableNotFoundException.class, () -> new HoodieTableMetaClient(hadoopConf, metadataTableBasePath));
    }

    // Metadata table created when enabled by config
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true), true)) {
      client.startCommitWithTime("001");
      assertTrue(fs.exists(new Path(metadataTableBasePath)));
      validateMetadata(client);
    }
  }

  /**
   * Only valid partition directories are added to the metadata.
   */
  @Test
  public void testOnlyValidPartitionsAdded() throws Exception {
    // This test requires local file system
    init(HoodieTableType.COPY_ON_WRITE);

    // Create an empty directory which is not a partition directory (lacks partition metadata)
    final String nonPartitionDirectory = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-nonpartition";
    Files.createDirectories(Paths.get(basePath, nonPartitionDirectory));

    // Create some commits
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.withPartitionMetaFiles("p1", "p2")
             .addCommit("001").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10)
             .addCommit("002").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10, 10)
             .addInflightCommit("003").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10);

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
      client.startCommitWithTime("005");

      List<String> partitions = metadataWriter(client).metadata().getAllPartitionPaths();
      assertFalse(partitions.contains(nonPartitionDirectory),
          "Must not contain the non-partition " + nonPartitionDirectory);
      assertTrue(partitions.contains("p1"), "Must contain partition p1");
      assertTrue(partitions.contains("p2"), "Must contain partition p2");

      FileStatus[] statuses = metadata(client).getAllFilesInPartition(new Path(basePath, "p1"));
      assertTrue(statuses.length == 2);
      statuses = metadata(client).getAllFilesInPartition(new Path(basePath, "p2"));
      assertTrue(statuses.length == 5);
    }
  }

  /**
   * Test various table operations sync to Metadata Table correctly.
   */
  //@ParameterizedTest
  //@EnumSource(HoodieTableType.class)
  //public void testTableOperations(HoodieTableType tableType) throws Exception {
  public void testTableOperations() throws Exception {
    //FIXME(metadata): This is broken for MOR, until HUDI-1434 is fixed
    init(HoodieTableType.COPY_ON_WRITE);

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {

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
   * Test rollback of various table operations sync to Metadata Table correctly.
   */
  //@ParameterizedTest
  //@EnumSource(HoodieTableType.class)
  //public void testRollbackOperations(HoodieTableType tableType) throws Exception {
  public void testRollbackOperations() throws Exception {
    //FIXME(metadata): This is broken for MOR, until HUDI-1434 is fixed
    init(HoodieTableType.COPY_ON_WRITE);

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
      // Write 1 (Bulk insert)
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Rollback of inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);

      // Rollback of updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 20);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);

      // Rollback of updates and inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
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
      client.syncTableMetadata();
      validateMetadata(client);

      // Rollback of Clean
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.clean(newCommitTime);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);

    }

    // Rollback of partial commits
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc,
        getWriteConfigBuilder(false, true, false).withRollbackUsingMarkers(false).build())) {
      // Write updates and inserts
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);
    }

    // Marker based rollback of partial commits
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc,
        getWriteConfigBuilder(false, true, false).withRollbackUsingMarkers(true).build())) {
      // Write updates and inserts
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);
    }

  }

  /**
   * Test sync of table operations.
   */
  //@ParameterizedTest
  //@EnumSource(HoodieTableType.class)
  //public void testSync(HoodieTableType tableType) throws Exception {
  public void testSync() throws Exception {
    //FIXME(metadata): This is broken for MOR, until HUDI-1434 is fixed
    init(HoodieTableType.COPY_ON_WRITE);

    String newCommitTime;
    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;

    // Initial commits without metadata table enabled
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, false))) {
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
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
      // inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 5);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      validateMetadata(client);
      assertTrue(metadata(client).isInSync());
    }

    // Various table operations without metadata table enabled
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, false))) {
      // updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertFalse(metadata(client).isInSync());

      // updates and inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertFalse(metadata(client).isInSync());

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        assertFalse(metadata(client).isInSync());
      }

      // Savepoint
      String savepointInstant = newCommitTime;
      if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
        client.savepoint("hoodie", "metadata test");
        assertFalse(metadata(client).isInSync());
      }

      // Deletes
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateDeletes(newCommitTime, 5);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      client.delete(deleteKeys, newCommitTime);
      assertFalse(metadata(client).isInSync());

      // Clean
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.clean(newCommitTime);
      assertFalse(metadata(client).isInSync());

      // updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertFalse(metadata(client).isInSync());

      client.restoreToInstant(savepointInstant);
      assertFalse(metadata(client).isInSync());
    }


    // Enable metadata table and ensure it is synced
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);

      validateMetadata(client);
      assertTrue(metadata(client).isInSync());
    }

  }

  /**
   * Instants on Metadata Table should be archived as per config.
   * Metadata Table should be automatically compacted as per config.
   */
  @ParameterizedTest
  @ValueSource(booleans =  {false})
  public void testCleaningArchivingAndCompaction(boolean asyncClean) throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);

    final int maxDeltaCommitsBeforeCompaction = 4;
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .archiveCommitsWith(6, 8).retainCommits(1)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 3)
            .retainCommits(1).retainFileVersions(1).withAutoClean(true).withAsyncClean(asyncClean).build())
        .build();

    List<HoodieRecord> records;
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, config)) {
      for (int i = 1; i < 10; ++i) {
        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        if (i == 1) {
          records = dataGen.generateInserts(newCommitTime, 5);
        } else {
          records = dataGen.generateUpdates(newCommitTime, 2);
        }
        client.startCommitWithTime(newCommitTime);
        List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(writeStatuses);
        validateMetadata(client);
      }
    }

    HoodieTableMetaClient metadataMetaClient = new HoodieTableMetaClient(hadoopConf, metadataTableBasePath);
    HoodieActiveTimeline metadataTimeline = metadataMetaClient.getActiveTimeline();
    // check that there are 2 compactions.
    assertEquals(2, metadataTimeline.getCommitTimeline().filterCompletedInstants().countInstants());
    // check that cleaning has happened twice, once after each compaction.
    assertEquals(2, metadataTimeline.getCleanerTimeline().filterCompletedInstants().countInstants());
    // ensure archiving has happened
    List<HoodieInstant> instants = metadataTimeline.getCommitsAndCompactionTimeline()
        .getInstants().collect(Collectors.toList());
    Collections.reverse(instants);
    long numDeltaCommits = instants.stream().filter(instant -> instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)).count();
    assertEquals(6, numDeltaCommits);
  }

  /**
   * Test various error scenarios.
   */
  @Test
  public void testErrorCases() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);

    // TESTCASE: If commit on the metadata table succeeds but fails on the dataset, then on next init the metadata table
    // should be rolled back to last valid commit.
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true), true)) {
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

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true), true)) {
      // Start the next commit which will rollback the previous one and also should update the metadata table by
      // updating it with HoodieRollbackMetadata.
      String newCommitTime = client.startCommit();

      // Dangling commit but metadata should be valid at this time
      validateMetadata(client);

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
  @Test
  public void testNonPartitioned() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);

    HoodieTestDataGenerator nonPartitionedGenerator = new HoodieTestDataGenerator(new String[] {""});
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
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

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfigBuilder(true, true, true).build())) {
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
      assertEquals(metricsRegistry.getAllCounts().get(HoodieMetadataMetrics.INITIALIZE_STR + ".count"), 1L);
      assertTrue(metricsRegistry.getAllCounts().containsKey("basefile.size"));
      assertTrue(metricsRegistry.getAllCounts().containsKey("logfile.size"));
      assertTrue(metricsRegistry.getAllCounts().containsKey("basefile.count"));
      assertTrue(metricsRegistry.getAllCounts().containsKey("logfile.count"));
    }
  }

  /**
   * Validate the metadata tables contents to ensure it matches what is on the file system.
   *
   * @throws IOException
   */
  private void validateMetadata(HoodieWriteClient client) throws IOException {
    HoodieWriteConfig config = client.getConfig();
    HoodieBackedTableMetadataWriter metadataWriter = metadataWriter(client);
    assertNotNull(metadataWriter, "MetadataWriter should have been initialized");
    if (!config.useFileListingMetadata()) {
      return;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();

    // Validate write config for metadata table
    HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
    assertFalse(metadataWriteConfig.useFileListingMetadata(), "No metadata table for metadata table");
    assertFalse(metadataWriteConfig.getFileListingMetadataVerify(), "No verify for metadata table");

    // Metadata table should be in sync with the dataset
    assertTrue(metadata(client).isInSync());

    // Partitions should match
    List<String> fsPartitions = FSUtils.getAllFoldersWithPartitionMetaFile(fs, basePath);
    List<String> metadataPartitions = metadataWriter.metadata().getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertTrue(fsPartitions.equals(metadataPartitions), "Partitions should match");

    // Files within each partition should match
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(config, hadoopConf);
    TableFileSystemView tableView = table.getHoodieView();
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
        FileStatus[] metaStatuses = metadataWriter.metadata().getAllFilesInPartition(partitionPath);
        List<String> fsFileNames = Arrays.stream(fsStatuses)
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        List<String> metadataFilenames = Arrays.stream(metaStatuses)
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        Collections.sort(fsFileNames);
        Collections.sort(metadataFilenames);

        // File sizes should be valid
        Arrays.stream(metaStatuses).forEach(s -> assertTrue(s.getLen() > 0));

        if ((fsFileNames.size() != metadataFilenames.size()) || (!fsFileNames.equals(metadataFilenames))) {
          LOG.info("*** File system listing = " + Arrays.toString(fsFileNames.toArray()));
          LOG.info("*** Metadata listing = " + Arrays.toString(metadataFilenames.toArray()));
        }
        assertEquals(fsFileNames.size(), metadataFilenames.size(), "Files within partition " + partition + " should match");
        assertTrue(fsFileNames.equals(metadataFilenames), "Files within partition " + partition + " should match");

        // FileSystemView should expose the same data
        List<HoodieFileGroup> fileGroups = tableView.getAllFileGroups(partition).collect(Collectors.toList());

        fileGroups.forEach(g -> LogManager.getLogger(TestHoodieFsMetadata.class).info(g));
        fileGroups.forEach(g -> g.getAllBaseFiles().forEach(b -> LogManager.getLogger(TestHoodieFsMetadata.class).info(b)));
        fileGroups.forEach(g -> g.getAllFileSlices().forEach(s -> LogManager.getLogger(TestHoodieFsMetadata.class).info(s)));

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

    HoodieTableMetaClient metadataMetaClient = new HoodieTableMetaClient(hadoopConf, metadataTableBasePath);

    // Metadata table should be in sync with the dataset
    assertTrue(metadataWriter.metadata().isInSync());

    // Metadata table is MOR
    assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

    // Metadata table is HFile format
    assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
        "Metadata Table base file format should be HFile");

    // Metadata table has a fixed number of partitions
    // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
    // in the .hoodie folder.
    List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(fs, HoodieTableMetadata.getMetadataTableBasePath(basePath),
        false);
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

  private HoodieBackedTableMetadataWriter metadataWriter(HoodieWriteClient client) {
    return (HoodieBackedTableMetadataWriter) HoodieTableMetadataWriter.create(hadoopConf, client.getConfig(), jsc);
  }

  private HoodieBackedTableMetadata metadata(HoodieWriteClient client) {
    HoodieWriteConfig clientConfig = client.getConfig();
    return (HoodieBackedTableMetadata) HoodieTableMetadata.create(hadoopConf, clientConfig.getBasePath(), clientConfig.getSpillableMapBasePath(),
        clientConfig.useFileListingMetadata(), clientConfig.getFileListingMetadataVerify(), false, clientConfig.shouldAssumeDatePartitioning());
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
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withDeleteParallelism(2).withRollbackParallelism(2).withFinalizeWriteParallelism(2)
        .withAutoCommit(autoCommit).withAssumeDatePartitioning(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1)
            .withAutoClean(false).retainCommits(1).retainFileVersions(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(false).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(useFileListingMetadata).build())
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(enableMetrics)
                           .withExecutorMetrics(true).usePrefix("unit-test").build());
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }
}
