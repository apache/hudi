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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.ClientUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
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
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
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
import org.junit.jupiter.params.provider.EnumSource;

public class TestHoodieMetadata extends HoodieClientTestHarness {
  private static final Logger LOG = LogManager.getLogger(TestHoodieMetadata.class);

  @TempDir
  public java.nio.file.Path tempFolder;

  private String metadataTableBasePath;

  public void init() throws IOException {
    init(HoodieTableType.MERGE_ON_READ);
  }

  public void init(HoodieTableType tableType) throws IOException {
    initDFS();
    initSparkContexts("TestHoodieMetadata");
    hadoopConf.addResource(dfs.getConf());
    initPath();
    dfs.mkdirs(new Path(basePath));
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, tableType);
    initTestDataGenerator();

    metadataTableBasePath = HoodieMetadata.getMetadataTableBasePath(basePath);
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
    init();

    // Metadata table should not exist until created for the first time
    assertFalse(dfs.exists(new Path(metadataTableBasePath)), "Metadata table should not exist");
    assertThrows(TableNotFoundException.class, () -> new HoodieTableMetaClient(hadoopConf, metadataTableBasePath));

    // Metadata table is not created if disabled by config
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, false))) {
      client.startCommitWithTime("001");
      assertFalse(dfs.exists(new Path(metadataTableBasePath)), "Metadata table should not be created");
      assertThrows(TableNotFoundException.class, () -> new HoodieTableMetaClient(hadoopConf, metadataTableBasePath));
    }

    // Metadata table created when enabled by config
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true), true)) {
      client.startCommitWithTime("001");
      assertTrue(dfs.exists(new Path(metadataTableBasePath)));
      validateMetadata(client.getConfig());
    }
  }

  /**
   * Only valid partition directories are added to the metadata.
   */
  @Test
  public void testOnlyValidPartitionsAdded() throws Exception {
    init();

    HoodieTestDataGenerator.writePartitionMetadata(dfs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, basePath);

    // Create an empty directory which is not a partition directory (lacks partition metadata)
    final String nonPartitionDirectory = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-nonpartition";
    final Path nonPartitionPath = new Path(basePath, nonPartitionDirectory);
    dfs.mkdirs(nonPartitionPath);

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
      client.startCommitWithTime("001");
      validateMetadata(client.getConfig());

      List<String> partitions = HoodieMetadata.getAllPartitionPaths(dfs, basePath, true);
      assertFalse(partitions.contains(nonPartitionDirectory),
          "Must not contain the non-partition " + nonPartitionDirectory);
    }
  }

  /**
   * Test various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testTableOperations(HoodieTableType tableType) throws Exception {
    init(tableType);

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {

      // Write 1 (Bulk insert)
      String newCommitTime = "001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      validateMetadata(client.getConfig());

      // Write 2 (inserts)
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);
      validateMetadata(client.getConfig());

      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());

      // Write 3 (updates)
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());

      // Write 4 (updates and inserts)
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "005";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client.getConfig());
      }

      // Write 5 (updates and inserts)
      newCommitTime = "006";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "007";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client.getConfig());
      }

      // Deletes
      newCommitTime = "008";
      records = dataGen.generateDeletes(newCommitTime, 10);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      client.delete(deleteKeys, newCommitTime);
      validateMetadata(client.getConfig());

      // Clean
      newCommitTime = "009";
      client.clean(newCommitTime);
      validateMetadata(client.getConfig());

      // Restore
      client.restoreToInstant("006");
      validateMetadata(client.getConfig());
    }
  }

  /**
   * Test rollback of various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testRollbackOperations(HoodieTableType tableType) throws Exception {
    init(tableType);

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
      // Write 1 (Bulk insert)
      String newCommitTime = "001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      validateMetadata(client.getConfig());

      // Rollback of inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());
      client.rollback(newCommitTime);
      validateMetadata(client.getConfig());

      // Rollback of updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 20);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());
      client.rollback(newCommitTime);
      validateMetadata(client.getConfig());

      // Rollback of updates and inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());
      client.rollback(newCommitTime);
      validateMetadata(client.getConfig());

      // Rollback of Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "005";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client.getConfig());
      }

      // Rollback of Deletes
      newCommitTime = "008";
      records = dataGen.generateDeletes(newCommitTime, 10);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      writeStatuses = client.delete(deleteKeys, newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());
      client.rollback(newCommitTime);
      validateMetadata(client.getConfig());

      // Rollback of Clean
      newCommitTime = "009";
      client.clean(newCommitTime);
      validateMetadata(client.getConfig());
      client.rollback(newCommitTime);
      validateMetadata(client.getConfig());

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
      validateMetadata(client.getConfig());
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
      validateMetadata(client.getConfig());
    }

  }

  /**
   * Test sync of table operations.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testSync(HoodieTableType tableType) throws Exception {
    init(tableType);

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

      validateMetadata(client.getConfig());
      assertTrue(HoodieMetadata.isInSync(basePath));
      assertTrue(HoodieMetadata.findInstantsToSync(hadoopConf, basePath).isEmpty());
    }

    // Various table operations without metadata table enabled
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, false))) {
      // updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertFalse(HoodieMetadata.isInSync(basePath));

      // updates and inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertFalse(HoodieMetadata.isInSync(basePath));

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        assertFalse(HoodieMetadata.isInSync(basePath));
      }

      // Savepoint
      String savepointInstant = newCommitTime;
      if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
        client.savepoint("hoodie", "metadata test");
        assertFalse(HoodieMetadata.isInSync(basePath));
      }

      // Deletes
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateDeletes(newCommitTime, 5);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      client.delete(deleteKeys, newCommitTime);
      assertFalse(HoodieMetadata.isInSync(basePath));

      // Clean
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.clean(newCommitTime);
      assertFalse(HoodieMetadata.isInSync(basePath));

      // updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertFalse(HoodieMetadata.isInSync(basePath));

      // TODO: This is broken for MOR due to rollback issue. Fix testRollbackOperations first.
      // Restore
      //client.restoreToInstant(savepointInstant);
      //assertFalse(HoodieMetadata.isInSync());
    }

    assertFalse(HoodieMetadata.findInstantsToSync(hadoopConf, basePath).isEmpty());

    // Enable metadata table and ensure it is synced
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);

      validateMetadata(client.getConfig());
      assertTrue(HoodieMetadata.isInSync(basePath));
    }

    assertTrue(HoodieMetadata.findInstantsToSync(hadoopConf, basePath).isEmpty());
  }

  /**
   * Instants on Metadata Table should be archived as per config.
   * Metadata Table should be automatically compacted as per config.
   */
  @Test
  public void testArchivingAndCompaction() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);

    final int maxDeltaCommitsBeforeCompaction = 6;
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false)
        .withMetadataCompactionConfig(HoodieCompactionConfig.newBuilder()
            .archiveCommitsWith(2, 4).retainCommits(1).retainFileVersions(1).withAutoClean(true)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
            .withInlineCompaction(true).withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 3)
            .retainCommits(1).retainFileVersions(1).withAutoClean(false).build())
        .build();
    List<HoodieRecord> records;
    HoodieTableMetaClient metaClient = ClientUtils.createMetaClient(jsc.hadoopConfiguration(), config, true);

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
        validateMetadata(client.getConfig());

        // Inline compaction is enabled so metadata table should be compacted as required
        HoodieTableMetaClient metadataMetaClient = new HoodieTableMetaClient(hadoopConf, metadataTableBasePath);
        HoodieTimeline metadataTimeline = metadataMetaClient.getActiveTimeline();
        List<HoodieInstant> instants = metadataTimeline.getCommitsAndCompactionTimeline()
            .getInstants().collect(Collectors.toList());
        Collections.reverse(instants);
        int numDeltaCommits = 0;
        for (HoodieInstant instant : instants) {
          if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION)) {
            break;
          }
          if (instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
            ++numDeltaCommits;
          }
        }

        assertTrue(numDeltaCommits <= (maxDeltaCommitsBeforeCompaction + 1), "Inline compaction should occur");

        // No archive until there is a compaction on the metadata table
        List<HoodieInstant> archivedInstants = metaClient.getArchivedTimeline().reload()
            .getInstants().collect(Collectors.toList());
        Option<HoodieInstant> lastCompaction = metadataTimeline.filterCompletedInstants()
            .filter(instant -> instant.getAction().equals(HoodieTimeline.COMMIT_ACTION)).lastInstant();
        archivedInstants.forEach(instant -> {
          assertTrue(HoodieTimeline.compareTimestamps(instant.getTimestamp(),
              HoodieTimeline.LESSER_THAN_OR_EQUALS, lastCompaction.get().getTimestamp()));
          assertTrue(lastCompaction.isPresent());
        });
      }
    }
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
      validateMetadata(client.getConfig());

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 5);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // There is no way to simulate failed commit on the main dataset, hence we simply delete the completed
      // instant so that only the inflight is left over.
      String commitInstantFileName = HoodieTimeline.makeCommitFileName(newCommitTime);
      assertTrue(dfs.delete(new Path(basePath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME,
          commitInstantFileName), false));
    }

    // Simulates a new process which will re-initialize the metadata table
    HoodieMetadata.remove(basePath);

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true), true)) {
      // Start the next commit which will rollback the previous one and also should update the metadata table by
      // updating it with HoodieRollbackMetadata.
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);

      // Dangling commit but metadata should be valid at this time
      validateMetadata(client.getConfig());

      // Next insert
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 5);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // Post rollback commit and metadata should be valid
      validateMetadata(client.getConfig());
    }
  }

  /**
   * Test non-partitioned datasets.
   */
  @Test
  public void testNonPartitioned() throws Exception {
    init();

    HoodieTestDataGenerator nonPartitionedGenerator = new HoodieTestDataGenerator(new String[] {""});
    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc, getWriteConfig(true, true))) {
      // Write 1 (Bulk insert)
      String newCommitTime = "001";
      List<HoodieRecord> records = nonPartitionedGenerator.generateInserts(newCommitTime, 10);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      validateMetadata(client.getConfig());
    }
  }

  /**
   * Test various metrics published by metadata table.
   */
  @Test
  public void testMetadataMetrics() throws Exception {
    init();

    try (HoodieWriteClient client = new HoodieWriteClient<>(jsc,
        getWriteConfigBuilder(true, true, true).withFileListingMetadataVerify(true).build())) {
      // Write
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client.getConfig());

      // The empty partition is saved with a special name
      List<String> metadataPartitions = HoodieMetadata.getAllPartitionPaths(dfs, basePath, false);
      assertTrue(metadataPartitions.contains(HoodieMetadataCommon.NON_PARTITIONED_NAME), "Must contain empty partition");
    }
  }

  /**
   * Validate the metadata tables contents to ensure it matches what is on the file system.
   *
   * @throws IOException
   */
  private void validateMetadata(HoodieWriteConfig config) throws IOException {
    long t1 = System.currentTimeMillis();
    // Validate write config for metadata table
    HoodieWriteConfig metadataWriteConfig = HoodieMetadata.getWriteConfig(basePath);
    assertFalse(metadataWriteConfig.useFileListingMetadata(), "No metadata table for metadata table");
    assertFalse(metadataWriteConfig.getFileListingMetadataVerify(), "No verify for metadata table");

    // Metadata table should be in sync with the dataset
    assertTrue(HoodieMetadata.isInSync(basePath));

    // Partitions should match
    List<String> fsPartitions = FSUtils.getAllFoldersWithPartitionMetaFile(dfs, basePath);
    List<String> metadataPartitions = HoodieMetadata.getAllPartitionPaths(dfs, basePath, false);

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertTrue(fsPartitions.equals(metadataPartitions), "Partitions should match");

    // Files within each partition should match
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, hadoopConf);
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
        FileStatus[] fsStatuses = FSUtils.getAllDataFilesInPartition(dfs, partitionPath);
        FileStatus[] metaStatuses = HoodieMetadata.getAllFilesInPartition(hadoopConf, basePath, partitionPath);
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

        fileGroups.stream().forEach(g -> LogManager.getLogger(TestHoodieMetadata.class).info(g));
        fileGroups.stream().forEach(g -> g.getAllBaseFiles().forEach(b -> LogManager.getLogger(TestHoodieMetadata.class).info(b)));
        fileGroups.stream().forEach(g -> g.getAllFileSlices().forEach(s -> LogManager.getLogger(TestHoodieMetadata.class).info(s)));

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
    assertTrue(HoodieMetadata.isInSync(basePath));

    // Metadata table is MOR
    assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

    // Metadata table is HFile format
    assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
        "Metadata Table base file format should be HFile");

    // Metadata table has a fixed number of partitions
    // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
    // in the .hoodie folder.
    List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(dfs, HoodieMetadata.getMetadataTableBasePath(basePath), false);
    assertEquals(HoodieMetadataImpl.METADATA_ALL_PARTITIONS.length, metadataTablePartitions.size());

    // Metadata table should automatically compact and clean
    // versions are +1 as autoclean / compaction happens end of commits
    int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metadataMetaClient,
        metadataMetaClient.getActiveTimeline());
    metadataTablePartitions.forEach(partition -> {
      assertTrue(fsView.getLatestBaseFiles(partition).count() <= 1, "Should have a single latest base file");
      assertTrue(fsView.getLatestFileSlices(partition).count() <= 1, "Should have a single latest file slice");
      if (fsView.getLatestFileSlices(partition).findFirst().isPresent()) {
        assertTrue(fsView.getLatestFileSlices(partition).findFirst().get().getLogFiles().count() <= numFileVersions,
            "Should limit files to num versions configured");
      }

      assertTrue(fsView.getAllFileSlices(partition).count() <= numFileVersions, "Should limit file slice to "
          + numFileVersions + " but was " + fsView.getAllFileSlices(partition).count());
    });

    LOG.info("Validation time=" + (System.currentTimeMillis() - t1));
  }

  // TODO: this can be moved to TestHarness after merge from master
  private void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors(), "Errors found in write of " + status.getFileId());
    }
  }

  private HoodieWriteConfig getWriteConfig() {
    return getWriteConfig(true, true);
  }

  private HoodieWriteConfig getWriteConfig(boolean autoCommit, boolean useFileListingMetadata) {
    return getWriteConfigBuilder(autoCommit, useFileListingMetadata, false).build();
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(boolean autoCommit, boolean useFileListingMetadata,
                                                          boolean enableMetrics) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withDeleteParallelism(2).withRollbackParallelism(2).withFinalizeWriteParallelism(2)
        .withAutoCommit(autoCommit).withAssumeDatePartitioning(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1)
            .withAutoClean(false).retainCommits(1).retainFileVersions(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(false).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withUseFileListingMetadata(useFileListingMetadata)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().withReporterType("CONSOLE").on(enableMetrics)
                           .usePrefix("unit-test").build());
  }
}
