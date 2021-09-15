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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieMetadataMetrics;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieBackedMetadata extends HoodieClientTestHarness {

  private static HoodieTestTable testTable;
  private String metadataTableBasePath;
  private HoodieTableType tableType;
  private HoodieWriteConfig writeConfig;

  public void init(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    initPath();
    initSparkContexts("TestHoodieMetadata");
    initFileSystem();
    fs.mkdirs(new Path(basePath));
    initMetaClient(tableType);
    initTestDataGenerator();
    metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    writeConfig = getWriteConfig(true, true);
    testTable = HoodieTestTable.of(metaClient);
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  public static List<Arguments> bootstrapAndTableOperationTestArgs() {
    return Arrays.asList(
        Arguments.of(COPY_ON_WRITE, true),
        Arguments.of(COPY_ON_WRITE, false),
        Arguments.of(MERGE_ON_READ, true),
        Arguments.of(MERGE_ON_READ, false)
    );
  }

  /**
   * Metadata Table bootstrap scenarios.
   */
  @ParameterizedTest
  @MethodSource("bootstrapAndTableOperationTestArgs")
  public void testMetadataTableBootstrap(HoodieTableType tableType, boolean addRollback) throws Exception {
    init(tableType);
    // bootstrap with few commits
    testTable.doWriteOperation("001", WriteOperationType.INSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 2, true);
    testTable.doWriteOperation("002", WriteOperationType.INSERT, Arrays.asList("p1", "p2"), 2, true);
    syncAndValidate(testTable);

    if (addRollback) {
      // trigger an UPSERT that will be rolled back
      testTable.doWriteOperation("003", WriteOperationType.UPSERT,
          Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 2);
      syncTableMetadata(writeConfig);
      // rollback last commit
      testTable = testTable.doRollback("003", "004");
      syncAndValidate(testTable);
    }

    testTable.doWriteOperation("005", WriteOperationType.INSERT, Arrays.asList("p1", "p2"), 4);
    syncAndValidate(testTable);

    // trigger an upsert and validate
    testTable.doWriteOperation("006", WriteOperationType.UPSERT, Collections.singletonList("p3"),
        Arrays.asList("p1", "p2", "p3"), 4);
    syncAndValidate(testTable);
  }

  /**
   * Only valid partition directories are added to the metadata.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testOnlyValidPartitionsAdded(HoodieTableType tableType) throws Exception {
    // This test requires local file system
    init(tableType);
    // Create an empty directory which is not a partition directory (lacks partition metadata)
    final String nonPartitionDirectory = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-nonpartition";
    Files.createDirectories(Paths.get(basePath, nonPartitionDirectory));

    // Three directories which are partitions but will be ignored due to filter
    final String filterDirRegex = ".*-filterDir\\d|\\..*";
    final String filteredDirectoryOne = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-filterDir1";
    final String filteredDirectoryTwo = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-filterDir2";
    final String filteredDirectoryThree = ".backups";

    // Create some commits
    testTable.withPartitionMetaFiles("p1", "p2", filteredDirectoryOne, filteredDirectoryTwo, filteredDirectoryThree)
        .addCommit("001").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10)
        .addCommit("002").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10, 10);

    writeConfig = getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.NEVER, true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withDirectoryFilterRegex(filterDirRegex).build()).build();
    testTable.doWriteOperation("003", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 1, true);
    syncTableMetadata(writeConfig);

    List<String> partitions = metadataWriter(writeConfig).metadata().getAllPartitionPaths();
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

    FileStatus[] statuses = metadata(writeConfig, context).getAllFilesInPartition(new Path(basePath, "p1"));
    assertEquals(3, statuses.length);
    statuses = metadata(writeConfig, context).getAllFilesInPartition(new Path(basePath, "p2"));
    assertEquals(6, statuses.length);
    Map<String, FileStatus[]> partitionsToFilesMap = metadata(writeConfig, context).getAllFilesInPartitions(
        Arrays.asList(basePath + "/p1", basePath + "/p2"));
    assertEquals(2, partitionsToFilesMap.size());
    assertEquals(3, partitionsToFilesMap.get(basePath + "/p1").length);
    assertEquals(6, partitionsToFilesMap.get(basePath + "/p2").length);
  }

  /**
   * Test various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @MethodSource("bootstrapAndTableOperationTestArgs")
  public void testTableOperations(HoodieTableType tableType, boolean doNotSyncFewCommits) throws Exception {
    init(tableType);
    // bootstrap w/ 2 commits
    bootstrapMetadata(testTable);

    // trigger an upsert
    testTable.doWriteOperation("003", WriteOperationType.UPSERT, Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 3);
    syncAndValidate(testTable);

    // trigger compaction
    if (MERGE_ON_READ.equals(tableType)) {
      testTable = testTable.doCompaction("004", Arrays.asList("p1", "p2"));
      syncAndValidate(testTable);
    }

    // trigger an upsert
    testTable.doWriteOperation("005", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    if (doNotSyncFewCommits) {
      syncAndValidate(testTable, Collections.emptyList(), true, false, true);
    }

    // trigger clean
    testTable.doClean("006", Collections.singletonList("001"));
    if (doNotSyncFewCommits) {
      syncAndValidate(testTable, Collections.emptyList(), true, false, false);
    }

    // trigger delete
    testTable.doWriteOperation("007", WriteOperationType.DELETE, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    syncAndValidate(testTable, Collections.emptyList(), true, true, false);
  }

  /**
   * Tests rollback of a commit with metadata enabled.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testRollbackOperations(HoodieTableType tableType) throws Exception {
    init(tableType);
    // bootstrap w/ 2 commits
    bootstrapMetadata(testTable);

    // trigger an upsert
    testTable.doWriteOperation("003", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2"), 2);
    syncAndValidate(testTable);

    // trigger a commit and rollback
    testTable.doWriteOperation("004", WriteOperationType.UPSERT, Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 3);
    syncTableMetadata(writeConfig);
    // rollback last commit
    testTable = testTable.doRollback("004", "005");
    syncAndValidate(testTable);

    // trigger few upserts and validate
    for (int i = 6; i < 10; i++) {
      testTable.doWriteOperation("00" + i, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    }
    syncAndValidate(testTable);

    testTable.doWriteOperation("010", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 3);
    syncAndValidate(testTable);

    // rollback last commit. sync and validate.
    testTable.doRollback("010", "011");
    syncTableMetadata(writeConfig);
  }

  /**
   * Test that manual rollbacks work correctly and enough timeline history is maintained on the metadata table
   * timeline.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testManualRollbacks(HoodieTableType tableType) throws Exception {
    init(tableType);
    bootstrapMetadata(testTable);

    // Setting to archive more aggressively on the Metadata Table than the Dataset
    final int maxDeltaCommitsBeforeCompaction = 4;
    final int minArchiveCommitsMetadata = 2;
    final int minArchiveCommitsDataset = 4;
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .archiveCommitsWith(minArchiveCommitsMetadata, minArchiveCommitsMetadata + 1).retainCommits(1)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(minArchiveCommitsDataset, minArchiveCommitsDataset + 1)
            .retainCommits(1).retainFileVersions(1).withAutoClean(false).withAsyncClean(true).build())
        .build();
    for (int i = 3; i < 10; i++) {
      if (i == 3) {
        testTable.doWriteOperation("00" + i, WriteOperationType.UPSERT, Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 2);
        syncTableMetadata(writeConfig);
      } else {
        testTable.doWriteOperation("00" + i, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
      }
    }
    syncAndValidate(testTable);

    // We can only rollback those commits whose deltacommit have not been archived yet.
    int numRollbacks = 0;
    boolean exceptionRaised = false;

    List<HoodieInstant> allInstants = metaClient.reloadActiveTimeline().getCommitsTimeline().getReverseOrderedInstants().collect(Collectors.toList());
    for (HoodieInstant instantToRollback : allInstants) {
      try {
        testTable.doRollback(instantToRollback.getTimestamp(), String.valueOf(Time.now()));
        syncTableMetadata(writeConfig);
        ++numRollbacks;
      } catch (HoodieMetadataException e) {
        exceptionRaised = true;
        break;
      }
    }

    assertTrue(exceptionRaised, "Rollback of archived instants should fail");
    // Since each rollback also creates a deltacommit, we can only support rolling back of half of the original
    // instants present before rollback started.
    assertTrue(numRollbacks >= Math.max(minArchiveCommitsDataset, minArchiveCommitsMetadata) / 2,
        "Rollbacks of non archived instants should work");
  }

  /**
   * Test sync of table operations.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  @Disabled
  public void testSync(HoodieTableType tableType) throws Exception {
    init(tableType);
    // Initial commits without metadata table enabled
    writeConfig = getWriteConfigBuilder(true, false, false).build();
    testTable.doWriteOperation("001", WriteOperationType.BULK_INSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 1);
    testTable.doWriteOperation("002", WriteOperationType.BULK_INSERT, Arrays.asList("p1", "p2"), 1);
    // Enable metadata table so it initialized by listing from file system
    testTable.doWriteOperation("003", WriteOperationType.INSERT, Arrays.asList("p1", "p2"), 1);
    syncAndValidate(testTable, Collections.emptyList(), true, true, true);
    // Various table operations without metadata table enabled
    testTable.doWriteOperation("004", WriteOperationType.UPSERT, Arrays.asList("p1", "p2"), 1);
    testTable.doWriteOperation("005", WriteOperationType.UPSERT, Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 3);
    syncAndValidate(testTable);

    // trigger compaction
    if (MERGE_ON_READ.equals(tableType)) {
      testTable = testTable.doCompaction("006", Arrays.asList("p1", "p2"));
      syncAndValidate(testTable);
    }

    // trigger an upsert
    testTable.doWriteOperation("007", WriteOperationType.UPSERT, Arrays.asList("p1", "p2", "p3"), 2);
    syncAndValidate(testTable, Collections.emptyList(), true, false, true);

    // savepoint
    if (COPY_ON_WRITE.equals(tableType)) {
      testTable.doSavepoint("007");
      syncTableMetadata(writeConfig);
      assertTrue(metadata(writeConfig, context).isInSync());
    }

    // trigger delete
    testTable.doWriteOperation("008", WriteOperationType.DELETE, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    syncAndValidate(testTable, Collections.emptyList(), true, true, false);

    // trigger clean
    testTable.doClean("009", Arrays.asList("001", "002"));
    syncAndValidate(testTable, Collections.emptyList(), true, false, false);

    // trigger another upsert
    testTable.doWriteOperation("010", WriteOperationType.UPSERT, Arrays.asList("p1", "p2", "p3"), 2);
    syncAndValidate(testTable, Collections.emptyList(), true, false, false);

    // trigger clustering
    testTable.doCluster("011", new HashMap<>());
    syncAndValidate(testTable, Collections.emptyList(), true, true, false);

    // If there is an inflight operation, the Metadata Table is not updated beyond that operations but the
    // in-memory merge should consider all the completed operations.
    HoodieCommitMetadata inflightCommitMeta = testTable.doWriteOperation("012", WriteOperationType.UPSERT, Collections.emptyList(),
        Arrays.asList("p1", "p2", "p3"), 2, false, true);
    // trigger upsert
    testTable.doWriteOperation("013", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    // testTable validation will fetch only files pertaining to completed commits. So, validateMetadata() will skip files for 006
    // while validating against actual metadata table.
    syncAndValidate(testTable, Collections.singletonList("012"), writeConfig.isMetadataTableEnabled(), writeConfig.getMetadataConfig().enableSync(), false);
    // Remove the inflight instance holding back table sync
    testTable.moveInflightCommitToComplete("012", inflightCommitMeta);
    syncTableMetadata(writeConfig);
    // A regular commit should get synced
    testTable.doWriteOperation("014", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    syncAndValidate(testTable, Collections.emptyList(), true, true, false);

    /* TODO: Restore to savepoint, enable metadata table and ensure it is synced
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      client.restoreToInstant(restoreToInstant);
      assertFalse(metadata(client).isInSync());

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      client.syncTableMetadata();

      validateMetadata(client);
      assertTrue(metadata(client).isInSync());
    }*/
  }

  /**
   * Instants on Metadata Table should be archived as per config but we always keep atlest the number of instants
   * as on the dataset. Metadata Table should be automatically compacted as per config.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testCleaningArchivingAndCompaction(HoodieTableType tableType) throws Exception {
    init(tableType);
    bootstrapMetadata(testTable);

    final int maxDeltaCommitsBeforeCompaction = 4;
    final int minArchiveLimit = 4;
    final int maxArchiveLimit = 6;
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .archiveCommitsWith(minArchiveLimit - 2, maxArchiveLimit - 2).retainCommits(1)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(minArchiveLimit, maxArchiveLimit)
            .retainCommits(1).retainFileVersions(1).withAutoClean(true).withAsyncClean(true).build())
        .build();
    for (int i = 3; i < 10; i++) {
      if (i == 3) {
        testTable.doWriteOperation("00" + i, WriteOperationType.UPSERT, Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 2);
        syncTableMetadata(writeConfig);
      } else {
        testTable.doWriteOperation("00" + i, WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
      }
    }
    syncAndValidate(testTable);

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
    HoodieTableMetaClient datasetMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(writeConfig.getBasePath()).build();
    HoodieActiveTimeline metadataTimeline = metadataMetaClient.getActiveTimeline();
    // check that there are compactions.
    assertTrue(metadataTimeline.getCommitTimeline().filterCompletedInstants().countInstants() > 0);
    // check that cleaning has, once after each compaction.
    assertTrue(metadataTimeline.getCleanerTimeline().filterCompletedInstants().countInstants() > 0);
    // ensure archiving has happened
    long numDataCompletedInstants = datasetMetaClient.getActiveTimeline().filterCompletedInstants().countInstants();
    long numDeltaCommits = metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants();
    assertTrue(numDeltaCommits >= minArchiveLimit);
    assertTrue(numDeltaCommits < numDataCompletedInstants, "Must have less delta commits than total completed instants on data timeline.");
  }

  /**
   * Test various error scenarios.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testErrorCases(HoodieTableType tableType) throws Exception {
    init(tableType);
    // TESTCASE: If commit on the metadata table succeeds but fails on the dataset, then on next init the metadata table
    // should be rolled back to last valid commit.
    testTable.doWriteOperation("001", WriteOperationType.UPSERT, Arrays.asList("p1", "p2"),
        Arrays.asList("p1", "p2"), 1);
    syncAndValidate(testTable);
    testTable.doWriteOperation("002", WriteOperationType.BULK_INSERT, Collections.emptyList(),
        Arrays.asList("p1", "p2"), 1);
    syncAndValidate(testTable);
    // There is no way to simulate failed commit on the main dataset, hence we simply delete the completed
    // instant so that only the inflight is left over.
    String commitInstantFileName = HoodieTimeline.makeCommitFileName("002");
    assertTrue(fs.delete(new Path(basePath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME,
        commitInstantFileName), false));
    // Next upsert
    testTable.doWriteOperation("003", WriteOperationType.UPSERT, Collections.emptyList(),
        Arrays.asList("p1", "p2"), 1);
    // Post rollback commit and metadata should be valid
    syncTableMetadata(writeConfig);
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
    HoodieActiveTimeline timeline = metadataMetaClient.getActiveTimeline();
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "001")));
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "002")));
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "003")));
  }

  /**
   * Test non-partitioned datasets.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testNonPartitioned(HoodieTableType tableType) throws Exception {
    init(tableType);
    // Non-partitioned bulk insert
    testTable.doWriteOperation("001", WriteOperationType.BULK_INSERT, Collections.emptyList(), 1);
    syncTableMetadata(writeConfig);
    List<String> metadataPartitions = metadata(writeConfig, context).getAllPartitionPaths();
    assertTrue(metadataPartitions.isEmpty(), "Must contain empty partition");
  }

  /**
   * Test various metrics published by metadata table.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataMetrics(HoodieTableType tableType) throws Exception {
    init(tableType);
    writeConfig = getWriteConfigBuilder(true, true, true).build();
    testTable.doWriteOperation(HoodieActiveTimeline.createNewInstantTime(), WriteOperationType.INSERT, Arrays.asList("p1", "p2"),
        Arrays.asList("p1", "p2"), 2, true);
    syncTableMetadata(writeConfig);
    Registry metricsRegistry = Registry.getRegistry("HoodieMetadata");
    assertTrue(metricsRegistry.getAllCounts().containsKey(HoodieMetadataMetrics.INITIALIZE_STR + ".count"));
    assertTrue(metricsRegistry.getAllCounts().containsKey(HoodieMetadataMetrics.INITIALIZE_STR + ".totalDuration"));
    assertTrue(metricsRegistry.getAllCounts().get(HoodieMetadataMetrics.INITIALIZE_STR + ".count") >= 1L);
    assertTrue(metricsRegistry.getAllCounts().containsKey("basefile.size"));
    assertTrue(metricsRegistry.getAllCounts().containsKey("logfile.size"));
    assertTrue(metricsRegistry.getAllCounts().containsKey("basefile.count"));
    assertTrue(metricsRegistry.getAllCounts().containsKey("logfile.count"));
  }

  /**
   * Test when reading from metadata table which is out of sync with dataset that results are still consistent.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataOutOfSync(HoodieTableType tableType) throws Exception {
    init(tableType);
    testTable.doWriteOperation("001", WriteOperationType.BULK_INSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 1);
    // Enable metadata so table is initialized but do not sync
    syncAndValidate(testTable, Collections.emptyList(), true, false, false);
    // Perform an insert and upsert
    testTable.doWriteOperation("002", WriteOperationType.INSERT, Arrays.asList("p1", "p2"), 1);
    testTable.doWriteOperation("003", WriteOperationType.UPSERT, Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 1);
    // Run compaction for MOR table
    if (MERGE_ON_READ.equals(tableType)) {
      testTable = testTable.doCompaction("004", Arrays.asList("p1", "p2"));
    }
    assertFalse(metadata(writeConfig, context).isInSync());
    testTable.doWriteOperation("005", WriteOperationType.UPSERT, Arrays.asList("p1", "p2", "p3"), 1);
    if (MERGE_ON_READ.equals(tableType)) {
      testTable = testTable.doCompaction("006", Arrays.asList("p1", "p2"));
    }
    testTable.doClean("007", Collections.singletonList("001"));
    /* TODO: Perform restore with metadata disabled
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      client.restoreToInstant("004");
    }*/
    assertFalse(metadata(writeConfig, context).isInSync());
    syncAndValidate(testTable, Collections.emptyList(), true, true, true);
  }

  /**
   * Test that failure to perform deltacommit on the metadata table does not lead to missed sync.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetdataTableCommitFailure(HoodieTableType tableType) throws Exception {
    init(tableType);
    testTable.doWriteOperation("001", WriteOperationType.INSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"), 2, true);
    syncTableMetadata(writeConfig);
    testTable.doWriteOperation("002", WriteOperationType.INSERT, Arrays.asList("p1", "p2"), 2, true);
    syncTableMetadata(writeConfig);

    // At this time both commits 001 and 002 must be synced to the metadata table
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
    HoodieActiveTimeline timeline = metadataMetaClient.getActiveTimeline();
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "001")));
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "002")));

    // Delete the 002 deltacommit completed instant to make it inflight
    FileCreateUtils.deleteDeltaCommit(metadataTableBasePath, "002");
    timeline = metadataMetaClient.reloadActiveTimeline();
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "001")));
    assertTrue(timeline.containsInstant(new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, "002")));

    // In this commit deltacommit "002" will be rolled back and attempted again.
    testTable.doWriteOperation("003", WriteOperationType.BULK_INSERT, Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 2);
    syncTableMetadata(writeConfig);

    timeline = metadataMetaClient.reloadActiveTimeline();
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "001")));
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "002")));
    assertTrue(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "003")));
    assertEquals(1, timeline.getRollbackTimeline().countInstants());
  }

  /**
   * Tests that if timeline has an inflight commit midway, metadata syncs only completed commits (including later to inflight commit).
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testInFlightCommit(HoodieTableType tableType) throws Exception {
    init(tableType);
    // bootstrap w/ 2 commits
    bootstrapMetadata(testTable);

    // trigger an upsert
    testTable.doWriteOperation("003", WriteOperationType.UPSERT, Collections.singletonList("p3"), Arrays.asList("p1", "p2", "p3"), 3);
    syncAndValidate(testTable);

    // trigger an upsert
    testTable.doWriteOperation("005", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    syncAndValidate(testTable);

    // create an inflight commit.
    HoodieCommitMetadata inflightCommitMeta = testTable.doWriteOperation("006", WriteOperationType.UPSERT, Collections.emptyList(),
        Arrays.asList("p1", "p2", "p3"), 2, false, true);

    // trigger upsert
    testTable.doWriteOperation("007", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    // testTable validation will fetch only files pertaining to completed commits. So, validateMetadata() will skip files for 006
    // while validating against actual metadata table.
    syncAndValidate(testTable, Collections.singletonList("006"), writeConfig.isMetadataTableEnabled(), writeConfig.getMetadataConfig().enableSync(), false);

    // Remove the inflight instance holding back table sync
    testTable.moveInflightCommitToComplete("006", inflightCommitMeta);
    syncTableMetadata(writeConfig);

    // A regular commit should get synced
    testTable.doWriteOperation("008", WriteOperationType.UPSERT, Collections.emptyList(), Arrays.asList("p1", "p2", "p3"), 2);
    syncAndValidate(testTable);
  }

  private void bootstrapMetadata(HoodieTestTable testTable) throws Exception {
    testTable.doWriteOperation("001", WriteOperationType.INSERT, Arrays.asList("p1", "p2"), Arrays.asList("p1", "p2"),
        2, true);
    testTable.doWriteOperation("002", WriteOperationType.INSERT, Arrays.asList("p1", "p2"),
        2, true);
    syncAndValidate(testTable);
  }

  private void syncAndValidate(HoodieTestTable testTable) throws IOException {
    syncAndValidate(testTable, Collections.emptyList(), writeConfig.isMetadataTableEnabled(), writeConfig.getMetadataConfig().enableSync(), true);
  }

  private void syncAndValidate(HoodieTestTable testTable, List<String> inflightCommits, boolean shouldEnableMetadata, boolean shouldEnableMetadataSync, boolean shouldValidateMetadata)
      throws IOException {
    writeConfig.getMetadataConfig().setValue(HoodieMetadataConfig.ENABLE, String.valueOf(shouldEnableMetadata));
    writeConfig.getMetadataConfig().setValue(HoodieMetadataConfig.SYNC_ENABLE, String.valueOf(shouldEnableMetadataSync));
    writeConfig.getMetadataConfig().setValue(HoodieMetadataConfig.VALIDATE_ENABLE, String.valueOf(shouldValidateMetadata));
    syncTableMetadata(writeConfig);
    validateMetadata(testTable, inflightCommits, writeConfig, metadataTableBasePath);
  }

  private HoodieWriteConfig getWriteConfig(boolean autoCommit, boolean useFileListingMetadata) {
    return getWriteConfigBuilder(autoCommit, useFileListingMetadata, false).build();
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(boolean autoCommit, boolean useFileListingMetadata, boolean enableMetrics) {
    return getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, autoCommit, useFileListingMetadata, enableMetrics);
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy policy, boolean autoCommit, boolean useFileListingMetadata, boolean enableMetrics) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withDeleteParallelism(2).withRollbackParallelism(2).withFinalizeWriteParallelism(2)
        .withAutoCommit(autoCommit)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1)
            .withFailedWritesCleaningPolicy(policy)
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
            .withExecutorMetrics(true).build())
        .withMetricsGraphiteConfig(HoodieMetricsGraphiteConfig.newBuilder()
            .usePrefix("unit-test").build());
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }
}
