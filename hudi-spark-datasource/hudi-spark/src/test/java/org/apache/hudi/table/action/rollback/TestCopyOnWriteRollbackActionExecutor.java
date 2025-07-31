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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.ClusteringTestUtils;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;

public class TestCopyOnWriteRollbackActionExecutor extends HoodieClientRollbackTestBase {
  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initHoodieStorage();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testCopyOnWriteRollbackActionExecutorForFileListingAsGenerateFile() throws Exception {
    final String p1 = "2015/03/16";
    final String p2 = "2015/03/17";
    final String p3 = "2016/03/15";
    // Let's create some commit files and base files
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(p1, p2, p3)
        .addCommit("001")
        .withBaseFilesInPartition(p1, "id11").getLeft()
        .withBaseFilesInPartition(p2, "id12").getLeft()
        .withLogFile(p1, "id11", 3).getLeft()
        .addCommit("002")
        .withBaseFilesInPartition(p1, "id21").getLeft()
        .withBaseFilesInPartition(p2, "id22").getLeft();

    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(false).build();
    HoodieTable table = this.getHoodieTable(metaClient, writeConfig);
    HoodieInstant needRollBackInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, "002");
    String rollbackInstant = "003";
    // execute CopyOnWriteRollbackActionExecutor with filelisting mode
    BaseRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, rollbackInstant, needRollBackInstant, false,
            table.getConfig().shouldRollbackUsingMarkers(), false);
    HoodieRollbackPlan rollbackPlan = (HoodieRollbackPlan) copyOnWriteRollbackPlanActionExecutor.execute().get();
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(context, table.getConfig(), table, "003", needRollBackInstant, true,
        false);
    List<HoodieRollbackStat> hoodieRollbackStats = copyOnWriteRollbackActionExecutor.executeRollback(rollbackPlan);

    // assert hoodieRollbackStats
    // Rollbacks requests for p1/id21 and p2/id22 only
    assertEquals(hoodieRollbackStats.size(), 2);
    for (HoodieRollbackStat stat : hoodieRollbackStats) {
      switch (stat.getPartitionPath()) {
        case p1:
          assertEquals(1, stat.getSuccessDeleteFiles().size());
          assertEquals(0, stat.getFailedDeleteFiles().size());
          assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
          assertEquals(testTable.forCommit("002").getBaseFilePath(p1, "id21").toString(),
              this.storage.getScheme() + ":" + stat.getSuccessDeleteFiles().get(0));
          break;
        case p2:
          assertEquals(1, stat.getSuccessDeleteFiles().size());
          assertEquals(0, stat.getFailedDeleteFiles().size());
          assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
          assertEquals(testTable.forCommit("002").getBaseFilePath(p2, "id22").toString(),
              this.storage.getScheme() + ":" + stat.getSuccessDeleteFiles().get(0));
          break;
        case p3:
          assertEquals(0, stat.getSuccessDeleteFiles().size());
          assertEquals(0, stat.getFailedDeleteFiles().size());
          assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
          break;
        default:
          fail("Unexpected partition: " + stat.getPartitionPath());
      }
    }

    assertTrue(testTable.inflightCommitExists("001"));
    assertTrue(testTable.commitExists("001"));
    assertTrue(testTable.baseFileExists(p1, "001", "id11"));
    assertTrue(testTable.baseFileExists(p2, "001", "id12"));
    // Note that executeRollback() does not delete inflight instant files
    // The deletion is done in finishRollback() called by runRollback()
    assertTrue(testTable.inflightCommitExists("002"));
    assertFalse(testTable.commitExists("002"));
    assertFalse(testTable.baseFileExists(p1, "002", "id21"));
    assertFalse(testTable.baseFileExists(p2, "002", "id22"));
  }

  @Test
  public void testListBasedRollbackStrategy() throws Exception {
    //just generate two partitions
    dataGen = new HoodieTestDataGenerator(
        new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH,
            DEFAULT_THIRD_PARTITION_PATH});
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(false).build();
    // 1. prepare data
    HoodieTestDataGenerator.writePartitionMetadataDeprecated(
        storage, new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH},
        basePath);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 3);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      List<WriteStatus> statusList = client.upsert(writeRecords, newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      newCommitTime = "002";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, records);
      statusList = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      context = new HoodieSparkEngineContext(jsc);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = this.getHoodieTable(metaClient, cfg);
      HoodieInstant needRollBackInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, "002");
      String rollbackInstant = "003";

      ListingBasedRollbackStrategy rollbackStrategy = new ListingBasedRollbackStrategy(table, context, table.getConfig(), rollbackInstant, false);
      List<HoodieRollbackRequest> rollBackRequests = rollbackStrategy.getRollbackRequests(needRollBackInstant);

      HoodieRollbackRequest rollbackRequest = rollBackRequests.stream().filter(entry -> entry.getPartitionPath().equals(DEFAULT_FIRST_PARTITION_PATH)).findFirst().get();

      FileSystem fs = Mockito.mock(FileSystem.class);
      MockitoAnnotations.initMocks(this);

      // mock to throw exception when fs.exists() is invoked
      Mockito.when(fs.exists(any()))
          .thenThrow(new IOException("Failing exists call for " + rollbackRequest.getFilesToBeDeleted().get(0)));

      rollbackStrategy = new ListingBasedRollbackStrategy(table, context, cfg, rollbackInstant, false);
      List<HoodieRollbackRequest> rollBackRequestsUpdated = rollbackStrategy.getRollbackRequests(needRollBackInstant);

      assertEquals(rollBackRequests, rollBackRequestsUpdated);
    }
  }

  // Verify that rollback works with replacecommit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCopyOnWriteRollbackWithReplaceCommits(boolean isUsingMarkers) throws IOException, InterruptedException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder()
        .withRollbackUsingMarkers(isUsingMarkers)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteTimelineClientRetry(true).build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      this.insertOverwriteCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers, client);
      HoodieTable table = this.getHoodieTable(metaClient, cfg);
      performRollbackAndValidate(isUsingMarkers, cfg, table, firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCopyOnWriteRollbackActionExecutor(boolean isUsingMarkers) throws IOException, InterruptedException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(isUsingMarkers).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      this.twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers, client);
      metaClient.reloadActiveTimeline();
      HoodieTable table = this.getHoodieTable(metaClient, cfg);
      performRollbackAndValidate(isUsingMarkers, cfg, table, firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices);
    }
  }

  /**
   * Test rollback when large number of files are to be rolled back.
   */
  @Test
  public void testRollbackScale() throws Exception {
    final String p1 = "2015/03/16";
    final String p2 = "2015/03/17";
    final String p3 = "2016/03/15";

    // File lengths for files that will be part of commit 003 to be rolled back
    final int largeCommitNumFiles = 10000;
    int[] fileLengths = IntStream.range(10, 10 + largeCommitNumFiles).toArray();

    // Let's create some commit files and parquet files.
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(p1, p2, p3)
        .addCommit("001")
        .withBaseFilesInPartition(p1, "id11").getLeft()
        .withBaseFilesInPartition(p2, "id12").getLeft()
        .withLogFile(p1, "id11", 3).getLeft()
        .addCommit("002")
        .withBaseFilesInPartition(p1, "id21").getLeft()
        .withBaseFilesInPartition(p2, "id22").getLeft()
        .addCommit("003")
        .withBaseFilesInPartition(p3, fileLengths);

    HoodieTable table = this.getHoodieTable(metaClient, getConfigBuilder().withRollbackUsingMarkers(false).build());
    HoodieInstant needRollBackInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, "003");

    // Schedule rollback
    BaseRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, "003", needRollBackInstant, false,
            table.getConfig().shouldRollbackUsingMarkers(), false);
    HoodieRollbackPlan hoodieRollbackPlan = (HoodieRollbackPlan) copyOnWriteRollbackPlanActionExecutor.execute().get();

    // execute CopyOnWriteRollbackActionExecutor with filelisting mode
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(context, table.getConfig(), table, "003", needRollBackInstant, true, true);
    copyOnWriteRollbackActionExecutor.doRollbackAndGetStats(hoodieRollbackPlan);

    // All files must have been rolled back
    assertEquals(largeCommitNumFiles, hoodieRollbackPlan.getRollbackRequests().stream().mapToInt(r -> r.getFilesToBeDeleted().size()).sum());
    assertEquals(largeCommitNumFiles, hoodieRollbackPlan.getRollbackRequests().stream().filter(r -> !r.getFilesToBeDeleted().isEmpty()).count());
  }

  private void performRollbackAndValidate(boolean isUsingMarkers, HoodieWriteConfig cfg, HoodieTable table,
                                          List<FileSlice> firstPartitionCommit2FileSlices,
                                          List<FileSlice> secondPartitionCommit2FileSlices) throws IOException, InterruptedException {
    //2. rollback
    HoodieInstant commitInstant;
    if (isUsingMarkers) {
      commitInstant = table.getActiveTimeline().getCommitAndReplaceTimeline().filterInflights().lastInstant().get();
    } else {
      commitInstant = table.getCompletedCommitTimeline().lastInstant().get();
    }

    BaseRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, "003", commitInstant, false,
            table.getConfig().shouldRollbackUsingMarkers(), false);
    HoodieRollbackPlan hoodieRollbackPlan = (HoodieRollbackPlan) copyOnWriteRollbackPlanActionExecutor.execute().get();
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(context, cfg, table, "003", commitInstant, false,
        false);
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = copyOnWriteRollbackActionExecutor.execute().getPartitionMetadata();

    //3. assert the rollback stat
    assertEquals(2, rollbackMetadata.size());
    for (Map.Entry<String, HoodieRollbackPartitionMetadata> entry : rollbackMetadata.entrySet()) {
      HoodieRollbackPartitionMetadata meta = entry.getValue();
      assertTrue(meta.getFailedDeleteFiles() == null
          || meta.getFailedDeleteFiles().size() == 0);
      assertTrue(meta.getSuccessDeleteFiles() == null
          || meta.getSuccessDeleteFiles().size() == 1);
    }

    //4. assert filegroup after rollback, and compare to the rollbackstat
    // assert the first partition file group and file slice
    List<HoodieFileGroup> firstPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileGroups.size());
    List<FileSlice> firstPartitionRollBack1FileSlices = firstPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileSlices.size());

    firstPartitionCommit2FileSlices.removeAll(firstPartitionRollBack1FileSlices);
    assertEquals(1, firstPartitionCommit2FileSlices.size());
    assertEquals(firstPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
        this.storage.getScheme() + ":"
            + rollbackMetadata.get(DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().get(0));


    // assert the second partition file group and file slice
    List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileGroups.size());
    List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileSlices.size());

    // assert the second partition rollback file is equals rollBack1SecondPartitionStat
    secondPartitionCommit2FileSlices.removeAll(secondPartitionRollBack1FileSlices);
    assertEquals(1, secondPartitionCommit2FileSlices.size());
    assertEquals(secondPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
        this.storage.getScheme() + ":"
            + rollbackMetadata.get(DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().get(0));

    assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), table, commitInstant.requestedTime()).doesMarkerDirExist());
  }

  @Test
  public void testRollbackBackup() throws Exception {
    final String p1 = "2015/03/16";
    final String p2 = "2015/03/17";
    final String p3 = "2016/03/15";
    // Let's create some commit files and parquet files
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(p1, p2, p3)
        .addCommit("001")
        .withBaseFilesInPartition(p1, "id11").getLeft()
        .withBaseFilesInPartition(p2, "id12").getLeft()
        .withLogFile(p1, "id11", 3).getLeft()
        .addCommit("002")
        .withBaseFilesInPartition(p1, "id21").getLeft()
        .withBaseFilesInPartition(p2, "id22").getLeft();

    // we are using test table infra. So, col stats are not populated.
    HoodieTable table =
        this.getHoodieTable(metaClient, getConfigBuilder().withRollbackBackupEnabled(true)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMetadataIndexColumnStats(false).build())
            .build());
    HoodieInstant needRollBackInstant = HoodieTestUtils.getCompleteInstant(
        metaClient.getStorage(), metaClient.getTimelinePath(),
        "002", COMMIT_ACTION);

    // Create the rollback plan and perform the rollback
    BaseRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, "003",
            needRollBackInstant, false,
            table.getConfig().shouldRollbackUsingMarkers(), false);
    copyOnWriteRollbackPlanActionExecutor.execute();

    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor =
        new CopyOnWriteRollbackActionExecutor(context, table.getConfig(), table, "003",
            needRollBackInstant, true, false);
    copyOnWriteRollbackActionExecutor.execute();

    // Completed and inflight instants should have been backed up
    StoragePath backupDir = new StoragePath(
        metaClient.getMetaPath(), table.getConfig().getRollbackBackupDirectory());
    assertTrue(storage.exists(new StoragePath(
        backupDir, INSTANT_FILE_NAME_GENERATOR.getFileName(needRollBackInstant))));
    assertTrue(storage.exists(new StoragePath(
        backupDir, testTable.getInflightCommitFilePath("002").getName())));
  }

  /**
   * Throw an exception when rolling back inflight ingestion commit,
   * when there is a completed commit with greater timestamp.
   */
  @Test
  public void testRollbackForMultiwriter() throws Exception {
    final String p1 = "2015/03/16";
    final String p2 = "2015/03/17";
    final String p3 = "2016/03/15";
    // Let's create some commit files and parquet files
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(p1, p2, p3)
        .addCommit("001")
        .withBaseFilesInPartition(p1, "id11").getLeft()
        .withBaseFilesInPartition(p2, "id12").getLeft()
        .withLogFile(p1, "id11", 3).getLeft()
        .addCommit("002")
        .withBaseFilesInPartition(p1, "id21").getLeft()
        .withBaseFilesInPartition(p2, "id22").getLeft()
        .addInflightCommit("003")
        .withBaseFilesInPartition(p1, "id31").getLeft()
        .addCommit("004");

    HoodieTable table = this.getHoodieTable(metaClient, getConfigBuilder().build());
    HoodieInstant needRollBackInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, COMMIT_ACTION, "003");

    // execute CopyOnWriteRollbackActionExecutor with filelisting mode
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor =
        new CopyOnWriteRollbackActionExecutor(context, table.getConfig(), table, "003", needRollBackInstant, true, true);
    assertThrows(HoodieRollbackException.class, () -> copyOnWriteRollbackActionExecutor.execute());
  }

  /**
   * This method tests rollback of completed ingestion commits and replacecommit inflight files
   * when there is another replacecommit with greater timestamp already present in the timeline.
   */
  @Test
  public void testRollbackWhenReplaceCommitIsPresent() throws Exception {

    // insert data
    HoodieWriteConfig writeConfig = getConfigBuilder().build();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);

    // Create a base commit.
    int numRecords = 200;
    String firstCommit = WriteClientTestUtils.createNewInstantTime();
    String partitionStr = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    dataGen = new HoodieTestDataGenerator(new String[]{HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH,
        DEFAULT_THIRD_PARTITION_PATH});
    writeBatch(writeClient, firstCommit, "000", Option.of(Arrays.asList("000")), "000",
        numRecords, dataGen::generateInserts, SparkRDDWriteClient::insert, true, numRecords, numRecords,
        1, INSTANT_GENERATOR);

    // Create second commit.
    String secondCommit = WriteClientTestUtils.createNewInstantTime();
    writeBatch(writeClient, secondCommit, firstCommit, Option.of(Arrays.asList(firstCommit)), "000", 100,
        dataGen::generateInserts, SparkRDDWriteClient::insert, true, 100, 300,
        2, INSTANT_GENERATOR);

    // Create completed clustering commit
    Properties properties = new Properties();
    properties.put("hoodie.datasource.write.row.writer.enable", String.valueOf(false));
    // not incremental related UTs, here just disable incremental, allowed continuous scheduling of two full clustering to simplify testing
    properties.put("hoodie.table.services.incremental.enabled", String.valueOf(false));
    properties.put("hoodie.clustering.plan.strategy.partition.selected", DEFAULT_FIRST_PARTITION_PATH);
    properties.put("hoodie.clustering.plan.partition.filter.mode","SELECTED_PARTITIONS");
    SparkRDDWriteClient clusteringClient = getHoodieWriteClient(
        ClusteringTestUtils.getClusteringConfig(basePath, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, properties));

    // Save an older instant for us to run clustering.
    // Now execute clustering on the saved instant and do not allow it to commit.
    String clusteringInstant1 = ClusteringTestUtils.runClustering(clusteringClient, false, false);
    clusteringClient.close();

    properties.put("hoodie.clustering.plan.strategy.partition.selected", DEFAULT_SECOND_PARTITION_PATH);
    clusteringClient = getHoodieWriteClient(
        ClusteringTestUtils.getClusteringConfig(basePath, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, properties));
    // Create completed clustering commit
    ClusteringTestUtils.runClustering(clusteringClient, false, true);
    clusteringClient.close();

    HoodieTable table = this.getHoodieTable(metaClient, getConfigBuilder().build());
    HoodieInstant needRollBackInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, secondCommit);

    properties.put("hoodie.clustering.plan.strategy.partition.selected", DEFAULT_FIRST_PARTITION_PATH);
    clusteringClient = getHoodieWriteClient(
        ClusteringTestUtils.getClusteringConfig(basePath, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, properties));

    // Schedule rollback
    String rollbackInstant = WriteClientTestUtils.createNewInstantTime();
    BaseRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, rollbackInstant, needRollBackInstant, false,
            !table.getConfig().shouldRollbackUsingMarkers(), false);
    copyOnWriteRollbackPlanActionExecutor.execute().get();

    // execute CopyOnWriteRollbackActionExecutor with filelisting mode
    final CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(
        context, table.getConfig(), table, rollbackInstant, needRollBackInstant, true, false, true);
    assertThrows(HoodieRollbackException.class, copyOnWriteRollbackActionExecutor::execute);

    // Schedule rollback for incomplete clustering instant.
    needRollBackInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, clusteringInstant1);
    rollbackInstant = WriteClientTestUtils.createNewInstantTime();
    copyOnWriteRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, rollbackInstant, needRollBackInstant, false,
            table.getConfig().shouldRollbackUsingMarkers(), false);
    HoodieRollbackPlan rollbackPlan = (HoodieRollbackPlan) copyOnWriteRollbackPlanActionExecutor.execute().get();

    // execute CopyOnWriteRollbackActionExecutor with filelisting mode
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutorForClustering = new CopyOnWriteRollbackActionExecutor(
        context, table.getConfig(), table, rollbackInstant, needRollBackInstant, true, false, true);
    copyOnWriteRollbackActionExecutorForClustering.execute();
  }
}
