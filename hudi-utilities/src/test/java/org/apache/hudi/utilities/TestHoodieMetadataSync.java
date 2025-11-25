/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.config.HoodieCleanConfig.CLEANER_COMMITS_RETAINED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMetadataSync extends SparkClientFunctionalTestHarness implements SparkProvider {

  static final String PARTITION_PATH_1 = "2020";
  static final String PARTITION_PATH_2 = "2021";
  static final String PARTITION_PATH_3 = "2023";
  static final String TABLE_NAME = "testing";

  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator(0L);
  private HoodieTableMetaClient sourceMetaClient1;
  private HoodieTableMetaClient sourceMetaClient2;

  String sourcePath1;
  String sourcePath2;
  String targetPath;

  @BeforeEach
  public void init() throws IOException {
    // Initialize test data dirs
    sourcePath1 = Paths.get(basePath(), "source1").toString();
    sourcePath2 = Paths.get(basePath(), "source2").toString();
    targetPath = Paths.get(basePath(), "target").toString();
  }

  private HoodieWriteConfig getHoodieWriteConfig(String basePath) {
    Properties props = new Properties();
    props.put("hoodie.metadata.compact.max.delta.commits", "3");
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(2)
        .forTable(TABLE_NAME)
        .withProps(props)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build();
  }

  @AfterAll
  public static void cleanup() {
    DATA_GENERATOR.close();
  }

  @Test
  void testHoodieMetadataSync() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});

    triggerNCommitsToSource(writeConfig1, PARTITION_PATH_1, 1, Option.of(dataGen));
    triggerNUpdatesToSource(writeConfig1, 1, 5, dataGen);

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
    triggerNCommitsToSource(writeConfig2, PARTITION_PATH_2, 1, Option.of(dataGen));
    triggerNUpdatesToSource(writeConfig2, 1, 5, dataGen);

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath);

    assertDataFromSourcesToTarget(singletonList(sourcePath1), targetPath, true);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath);

    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
  }

  @Test
  void testHoodieMetadataSyncWithBulkInsert() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});

    triggerNCommitsToSource(writeConfig1, PARTITION_PATH_1, 1, Option.of(dataGen));
    triggerNUpdatesToSource(writeConfig1, 1, 5, dataGen);

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
    triggerNCommitsToSource(writeConfig2, PARTITION_PATH_2, 1, Option.of(dataGen));
    triggerNBulkInsertsToSource(writeConfig2, PARTITION_PATH_2, 1, 5, Option.of(dataGen));

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath);

    assertDataFromSourcesToTarget(singletonList(sourcePath1), targetPath, true);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath);

    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
  }

  @Test
  void testHoodieMetadataSyncWithClustering() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});

    triggerNCommitsToSource(writeConfig1, PARTITION_PATH_1, 1, Option.of(dataGen));
    triggerNUpdatesToSource(writeConfig1, 1, 5, dataGen);

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_2});
    triggerNCommitsToSource(writeConfig2, PARTITION_PATH_2, 1, Option.of(dataGen));
    triggerNBulkInsertsToSource(writeConfig2, PARTITION_PATH_2, 1, 5, Option.of(dataGen));

    //perform clustering
    triggerClustering(sourcePath1);

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath);

    assertDataFromSourcesToTarget(singletonList(sourcePath1), targetPath, true);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath);

    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHoodieMetadataSyncWithInsertOverwrite(boolean performaBootstrap) throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);
    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    // Trigger Insert Overwrite Commits to source table 1
    triggerNInsertOverwriteToSource(writeConfig1, sourceMetaClient1, PARTITION_PATH_1, 2);

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, performaBootstrap);

    // validate commit on target table
    assertDataFromSourcesToTarget(singletonList(sourcePath1), targetPath, true);

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);
    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    // trigger Insert Overwrite commits to source table 2
    triggerNInsertOverwriteToSource(writeConfig2, sourceMetaClient2, PARTITION_PATH_2, 2);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath, performaBootstrap);

    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
  }

  @Test
  void testHoodieMetadataSyncWithDeletePartitions() throws Exception {
    Properties props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    triggerNCommitsToSource(writeConfig1, PARTITION_PATH_1, 2);
    triggerDeletePartition(writeConfig1, singletonList(PARTITION_PATH_1));

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    triggerNCommitsToSource(getHoodieWriteConfig(sourcePath2), PARTITION_PATH_2, 2);

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);

    assertDataFromSourcesToTarget(singletonList(sourcePath1), targetPath, true);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath, false);

    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
  }

  @Test
  void testHoodieMetadataSyncWithDelete() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH_1});

    triggerNCommitsToSource(getHoodieWriteConfig(sourcePath1), PARTITION_PATH_1, 2, Option.of(dataGen));
    triggerNDeletesToSource(getHoodieWriteConfig(sourcePath1), 2, 5, dataGen);

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);
    triggerNCommitsToSource(getHoodieWriteConfig(sourcePath2), PARTITION_PATH_2, 1);

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath);

    assertDataFromSourcesToTarget(singletonList(sourcePath1), targetPath, true);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath);

    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
  }

  @Test
  void testHoodieMetadataSyncWithClean() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    HoodieWriteConfig writeConfig1 = getHoodieWriteConfig(sourcePath1);
    triggerNCommitsToSource(writeConfig1, PARTITION_PATH_1, 5);

    HoodieWriteConfig cleanConfig1 = getHoodieCleanConfig(sourcePath1);
    triggerCleanToSource(cleanConfig1);

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);

    HoodieWriteConfig writeConfig2 = getHoodieWriteConfig(sourcePath2);
    triggerNCommitsToSource(writeConfig2, PARTITION_PATH_2, 5);

    HoodieWriteConfig cleanConfig2 = getHoodieCleanConfig(sourcePath2);
    triggerCleanToSource(cleanConfig2);

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath);
    assertDataFromSourcesToTarget(singletonList(sourcePath1), targetPath, true);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList(PARTITION_PATH_1), false);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath);
    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
    assertFSV(asList(sourcePath1, sourcePath2), targetPath, asList(PARTITION_PATH_1, PARTITION_PATH_2), false);
  }

  @Test
  void testHoodieMetadataSyncWithMDTTableServices() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    for (int i = 0; i < 5; i++) {
      HoodieTestTable testTable = HoodieTestTable.of(sourceMetaClient1);

      // add 1st completed commit to source table
      String instant = HoodieActiveTimeline.createNewInstantTime();
      testTable.addCommit(instant, Option.of(testTable.doWriteOperation(instant, INSERT, singletonList("p1"),
          singletonList("p1"), 2, false, true)));
    }

    List<String> configs = new ArrayList<>();
    configs.add("hoodie.keep.min.commits=2");
    configs.add("hoodie.keep.max.commits=3");
    configs.add("hoodie.metadata.compact.max.delta.commits=5");
    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false, true, configs);

    HoodieTableMetaClient targetTableMetaClient = HoodieTableMetaClient.builder().setBasePath(targetPath)
        .setConf(hadoopConf()).build();

    assertFalse(targetTableMetaClient.getArchivedTimeline().getInstants().isEmpty());

    String metadataPath = targetPath + "/.hoodie/metadata";
    HoodieTableMetaClient targetMetadataTableMetaClient = HoodieTableMetaClient.builder().setBasePath(metadataPath)
        .setConf(hadoopConf()).build();

    List<HoodieInstant> instants = targetMetadataTableMetaClient.getActiveTimeline().getCommitsAndCompactionTimeline().getInstants();
    // assert metadata table timeline contains a compaction action
    assertTrue(instants.stream().anyMatch(instant -> instant.getAction().equals(HoodieTimeline.COMMIT_ACTION)));
    List<HoodieInstant> archivedInstants = targetMetadataTableMetaClient.getArchivedTimeline().getInstants();
    // validate archived timeline is not empty
    assertFalse(archivedInstants.isEmpty());
  }

  @Test
  void testHoodieMetadataSync_Bootstrap() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);

    // write data to 1st table
    triggerNCommitsToSource(getHoodieWriteConfig(sourcePath1), PARTITION_PATH_1, 2);

    triggerClustering(sourcePath1);

    // write data to 1st table after clustering
    triggerNCommitsToSource(getHoodieWriteConfig(sourcePath1), PARTITION_PATH_1, 1);

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, true);
    assertDataFromSourcesToTarget(singletonList(sourcePath1), targetPath, true);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList(PARTITION_PATH_1), true);

    // write data to 2nd table
    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);
    triggerNCommitsToSource(getHoodieWriteConfig(sourcePath2), PARTITION_PATH_2, 1);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath, true);
    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList(PARTITION_PATH_1), true);

    triggerNCommitsToSource(getHoodieWriteConfig(sourcePath2), PARTITION_PATH_2, 1);

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath);
    assertDataFromSourcesToTarget(asList(sourcePath1, sourcePath2), targetPath, true);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList(PARTITION_PATH_1), true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHoodieMetadataSync_PendingInstantsWithNoNewInstantsInTimeline(boolean performBootstrap) throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);
    HoodieTestTable testTable = HoodieTestTable.of(sourceMetaClient1);

    // add 1st completed commit to source table
    String instant1 = "100";
    testTable.addCommit(instant1, Option.of(testTable.doWriteOperation(instant1, INSERT, singletonList("p1"),
        singletonList("p1"), 2, false, true)));

    // add inflight commit to source table after 100
    String instant2 = "105";
    testTable.addInflightCommit(instant2);

    // add completed commit after 105
    String instant3 = "110";
    testTable.addCommit(instant3, Option.of(testTable.doWriteOperation(instant3, INSERT, emptyList(),
        singletonList("p1"), 2, false, true)));

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, performBootstrap);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), performBootstrap);

    // move inflight commit 105 to completed state
    testTable.moveInflightCommitToComplete(instant2, testTable.doWriteOperation(instant2, INSERT, emptyList(),
        singletonList("p1"), 2, false, true));

    // it should pick up the commit 105 and sync to target table
    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), performBootstrap);

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);
    HoodieTestTable testTable2 = HoodieTestTable.of(sourceMetaClient2);

    String instant4 = "105";
    testTable2.addCommit(instant4, Option.of(testTable2.doWriteOperation(instant4, INSERT, singletonList("p2"),
        singletonList("p2"), 2, false, true)));

    syncMetadata(sourcePath2, sourceMetaClient2, targetPath, performBootstrap);
    assertFSV(asList(sourcePath1, sourcePath2), targetPath, asList("p1", "p2"), true);
  }

  @Test
  void testHoodieMetadataSync_PendingInstantStuckForMultipleSyncs() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);
    HoodieTestTable testTable = HoodieTestTable.of(sourceMetaClient1);

    // add 1st completed commit to source table
    String instant1 = "100";
    testTable.addCommit(instant1, Option.of(testTable.doWriteOperation(instant1, INSERT, singletonList("p1"),
        singletonList("p1"), 2, false, true)));

    // add inflight commit to source table after 100
    String instant2 = "105";
    testTable.addInflightCommit(instant2);

    String instant3 = "110";
    testTable.addInflightCommit(instant3);

    // add completed commit after 105
    String instant4 = "115";
    testTable.addCommit(instant4, Option.of(testTable.doWriteOperation(instant4, INSERT, emptyList(),
        singletonList("p1"), 2, false, true)));

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), true);

    // move inflight commit 105 to completed state
    testTable.moveInflightCommitToComplete(instant2, testTable.doWriteOperation(instant2, INSERT, emptyList(),
        singletonList("p1"), 2, false, true));

    // it should pick up the commit 105 and sync to target table
    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), false);

    // move inflight commit 110 to completed state
    testTable.moveInflightCommitToComplete(instant3, testTable.doWriteOperation(instant3, INSERT, emptyList(),
        singletonList("p1"), 2, false, true));

    // Add new commit to the timeline
    String instant5 = "120";
    testTable.addCommit(instant5, Option.of(testTable.doWriteOperation(instant5, INSERT, emptyList(),
        singletonList("p1"), 2, false, true)));

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), false);
  }

  @Test
  void testHoodieMetadataSync_RollbackPendingInstantInSyncMetadata() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);
    HoodieTestTable testTable = HoodieTestTable.of(sourceMetaClient1);

    // add inflight commit to source table after 100
    String instant1 = "100";
    testTable.addRequestedCommit(instant1);

    // add completed commit after 105
    String instant2 = "105";
    testTable.addCommit(instant2, Option.of(testTable.doWriteOperation(instant2, INSERT, singletonList("p1"),
        singletonList("p1"), 2, false, true)));

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), true);

    String rollbackInstant = "110";
    HoodieRollbackMetadata rollbackMetadata = getHoodieRollbackMetadata(instant2);
    HoodieRollbackPlan rollbackPlan = getHoodieRollbackPlan(instant2);
    testTable.addRollback(rollbackInstant, rollbackMetadata, rollbackPlan);

    testTable.addRollbackCompleted(rollbackInstant, rollbackMetadata, false);
    FileCreateUtils.deleteRequestedCommit(sourceMetaClient1.getBasePathV2().toUri().getPath(), instant2);

    String instant3 = "115";
    testTable.addCommit(instant3, Option.of(testTable.doWriteOperation(instant3, INSERT, emptyList(),
        singletonList("p1"), 2, false, true)));

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), false);
  }

  private HoodieRollbackPlan getHoodieRollbackPlan(String commitToRollback) {

    HoodieRollbackPlan rollbackPlan = new HoodieRollbackPlan();

    // Instant to rollback (commit + action)
    rollbackPlan.setInstantToRollback(
        new HoodieInstantInfo(commitToRollback, HoodieTimeline.COMMIT_ACTION)
    );

    // No file-level delete actions for now
    rollbackPlan.setRollbackRequests(Collections.emptyList());

    // Version = 1 (matches metadata)
    rollbackPlan.setVersion(1);

    return rollbackPlan;
  }

  private HoodieRollbackMetadata getHoodieRollbackMetadata(String commitToRollback) {
    HoodieRollbackMetadata rollbackMetadata = new HoodieRollbackMetadata();

    rollbackMetadata.setStartRollbackTime(commitToRollback);
    rollbackMetadata.setTimeTakenInMillis(0L);
    rollbackMetadata.setTotalFilesDeleted(0);

    // Single commit being rolled back
    rollbackMetadata.setCommitsRollback(Collections.singletonList(commitToRollback));

    // Empty partition metadata for now
    rollbackMetadata.setPartitionMetadata(Collections.emptyMap());

    // Instant info
    rollbackMetadata.setInstantsRollback(Collections.singletonList(
        new HoodieInstantInfo(commitToRollback, HoodieTimeline.COMMIT_ACTION)
    ));

    // Schema default = 1 but set it anyway for clarity
    rollbackMetadata.setVersion(1);

    return rollbackMetadata;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHoodieMetadataSync_SameInstantsOnDifferentSourceTables(boolean performBootstrap) throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);
    HoodieTestTable testTable = HoodieTestTable.of(sourceMetaClient1);

    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);
    HoodieTestTable testTable2 = HoodieTestTable.of(sourceMetaClient2);

    String instant1 = "100";
    testTable.addCommit(instant1, Option.of(testTable.doWriteOperation(instant1, INSERT, singletonList("p1"),
        singletonList("p1"), 2, false, true)));

    String instant2 = "105";
    testTable.addCommit(instant2, Option.of(testTable.doWriteOperation(instant1, INSERT, singletonList("p2"),
        singletonList("p2"), 2, false, true)));

    String instant3 = "100";
    testTable2.addCommit(instant3, Option.of(testTable2.doWriteOperation(instant1, INSERT, singletonList("p3"),
        singletonList("p3"), 2, false, true)));


    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, performBootstrap);
    syncMetadata(sourcePath2, sourceMetaClient2, targetPath, performBootstrap);

    assertFSV(asList(sourcePath1, sourcePath2), targetPath, asList("p1", "p2", "p3"), true);
  }

  @Test
  void testMetadataSync_RollbackPendingInstantOnTargetTableAndMetadataTable() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);
    HoodieTestTable testTable = HoodieTestTable.of(sourceMetaClient1);

    String instant1 = HoodieActiveTimeline.createNewInstantTime();
    testTable.addCommit(instant1, Option.of(testTable.doWriteOperation(instant1, INSERT, singletonList("p1"),
        singletonList("p1"), 2, false, true)));

    // bootstrap target table
    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), true);

    HoodieTableMetaClient targetTableMetaClient = HoodieTableMetaClient.builder().setBasePath(targetPath)
        .setConf(hadoopConf()).build();
    HoodieTableMetadataWriter writer = SparkHoodieBackedTableMetadataWriter.create(hadoopConf(), getHoodieWriteConfig(targetPath), context());
    HoodieTestTable targetTestTable = HoodieMetadataTestTable.of(targetTableMetaClient, writer, Option.of(context()));

    // add pending instant on target table
    String pendingInstant = HoodieActiveTimeline.createNewInstantTime();
    targetTestTable.addInflightCommit(pendingInstant);

    // move pending instant to target table on metadata table
    ((HoodieMetadataTestTable) targetTestTable).moveInflightCommitToComplete(pendingInstant, testTable.doWriteOperation(pendingInstant, INSERT, emptyList(),
        singletonList("p1"), 2, false, true), false, true);

    // validate pending instant is present in the timeline
    assertTrue(targetTableMetaClient.reloadActiveTimeline().getInstants().stream().anyMatch(instant -> instant.getTimestamp().equals(pendingInstant)));

    String metadataPath = targetPath + "/.hoodie/metadata";
    HoodieTableMetaClient targetMetadataTableMetaClient = HoodieTableMetaClient.builder().setBasePath(metadataPath)
        .setConf(hadoopConf()).build();

    // validate metadata table timeline contains a completed commit corresponding to pending commit on data table
    assertTrue(targetMetadataTableMetaClient.getActiveTimeline().filterCompletedInstants().containsInstant(pendingInstant));

    String instant2 = HoodieActiveTimeline.createNewInstantTime();
    testTable.addCommit(instant2, Option.of(testTable.doWriteOperation(instant2, INSERT, emptyList(),
        singletonList("p1"), 2, false, true)));

    syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false);

    assertFalse(targetTableMetaClient.reloadActiveTimeline().getInstants().stream().anyMatch(instant -> instant.getTimestamp().equals(pendingInstant)));
    assertFalse(targetMetadataTableMetaClient.reloadActiveTimeline().containsInstant(pendingInstant));
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), true);
  }

  @Test
  void testMetadataSync_MultiSyncParallel() throws Exception {
    Properties props = getTableProps();
    sourceMetaClient1 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath1, props);
    sourceMetaClient2 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath2, props);
    HoodieTestTable testTable = HoodieTestTable.of(sourceMetaClient1);

    // Create two commits in source table
    String instant1 = HoodieActiveTimeline.createNewInstantTime();
    testTable.addCommit(instant1, Option.of(testTable.doWriteOperation(
        instant1, INSERT, singletonList("p1"), singletonList("p1"), 2, false, true)));

    HoodieTestTable testTable2 = HoodieTestTable.of(sourceMetaClient2);
    String instant2 = HoodieActiveTimeline.createNewInstantTime();
    testTable2.addCommit(instant2, Option.of(testTable2.doWriteOperation(
        instant2, INSERT, emptyList(), singletonList("p2"), 2, false, true)));


    String sourcePath3 = Paths.get(basePath(), "source2").toString();
    HoodieTableMetaClient sourceMetaClient3 = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf(), sourcePath3, props);

    HoodieTestTable testTable3 = HoodieTestTable.of(sourceMetaClient3);
    String instant3 = HoodieActiveTimeline.createNewInstantTime();
    testTable3.addCommit(instant3, Option.of(testTable3.doWriteOperation(
        instant3, INSERT, emptyList(), singletonList("p3"), 2, false, true)));

    ExecutorService executor = Executors.newFixedThreadPool(2);

    Future<?> sync1 = executor.submit(() -> syncMetadata(sourcePath1, sourceMetaClient1, targetPath, false));
    Future<?> sync2 = executor.submit(() -> syncMetadata(sourcePath2, sourceMetaClient2, targetPath, false));
    Future<?> sync3 = executor.submit(() -> syncMetadata(sourcePath3, sourceMetaClient3, targetPath, false));

    // Ensure both sync tasks finish successfully
    sync1.get(60, TimeUnit.SECONDS);
    sync2.get(60, TimeUnit.SECONDS);
    sync3.get(60, TimeUnit.SECONDS);

    executor.shutdown();

    // Validate target is fully synced
    assertFSV(singletonList(sourcePath1), targetPath, singletonList("p1"), false);
  }

  private void syncMetadata(String sourcePath, HoodieTableMetaClient sourceMetaClient, String targetPath) {
    syncMetadata(sourcePath, sourceMetaClient, targetPath, false);
  }

  private void syncMetadata(String sourcePath, HoodieTableMetaClient sourceMetaClient, String targetPath, boolean bootstrap) {
    syncMetadata(sourcePath, sourceMetaClient, targetPath, bootstrap, false);
  }

  private void syncMetadata(String sourcePath, HoodieTableMetaClient sourceMetaClient, String targetPath, boolean bootstrap, boolean performTableMaintenance) {
    syncMetadata(sourcePath, sourceMetaClient, targetPath, bootstrap, performTableMaintenance, Collections.emptyList());
  }

  private void syncMetadata(String sourcePath, HoodieTableMetaClient sourceMetaClient, String targetPath, boolean bootstrap,
                            boolean performTableMaintenance, List<String> configs) {
    try {
      HoodieMetadataSync.Config cfg = new HoodieMetadataSync.Config();
      cfg.sourceBasePath = sourcePath;
      cfg.targetBasePath = targetPath;
      cfg.commitToSync = sourceMetaClient.reloadActiveTimeline().lastInstant().get().getTimestamp();
      cfg.targetTableName = TABLE_NAME;
      cfg.sparkMaster = "local[2]";
      cfg.sparkMemory = "1g";
      cfg.boostrap = bootstrap;
      cfg.performTableMaintenance = performTableMaintenance;
      cfg.configs = configs;
      HoodieMetadataSync metadataSync = new HoodieMetadataSync(jsc(), cfg);
      metadataSync.run();
    } catch (Exception e) {
      throw new RuntimeException("Sync run failed ", e);
    }
  }

  private void triggerNCommitsToSource(HoodieWriteConfig writeConfig, String partitionPath, int numCommits) throws IOException {
    triggerNCommitsToSource(writeConfig, partitionPath, numCommits, Option.empty());
  }

  private void triggerDeletePartition(HoodieWriteConfig writeConfig, List<String> partitions) throws IOException {
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      String instant = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, sourceMetaClient1);
      writeClient.deletePartitions(partitions, instant);
    }
  }

  private void triggerNCommitsToSource(HoodieWriteConfig writeConfig, String partitionPath, int numCommits, Option<HoodieTestDataGenerator> dataGeneratorOption) throws IOException {
    HoodieTestDataGenerator dataGen = dataGeneratorOption.orElseGet(() -> new HoodieTestDataGenerator(new String[] {partitionPath}));
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      for (int i = 0; i < numCommits; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insert(dataset, instant);
      }
    }
  }

  private void triggerNBulkInsertsToSource(HoodieWriteConfig writeConfig, String partitionPath, int numCommits, int numRecords, Option<HoodieTestDataGenerator> dataGeneratorOption)
      throws IOException {
    HoodieTestDataGenerator dataGen = dataGeneratorOption.orElseGet(() -> new HoodieTestDataGenerator(new String[] {partitionPath}));
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      for (int i = 0; i < numCommits; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> updates = dataGen.generateInserts(instant, numRecords);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(updates);
        writeClient.bulkInsert(dataset, instant);
      }
    }
  }

  private void triggerNInsertOverwriteToSource(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient, String partitionPath, int numCommits) throws IOException {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {partitionPath});
    for (int i = 0; i < numCommits; i++) {
      try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
        String instant2 = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
        List<HoodieRecord> records = dataGen.generateInserts(instant2, 10);
        JavaRDD<HoodieRecord> dataset = jsc().parallelize(records);
        writeClient.insertOverwrite(dataset, instant2);
      }
    }
  }

  private void triggerNDeletesToSource(HoodieWriteConfig writeConfig, int numCommits, int numDeletes, HoodieTestDataGenerator dataGen) throws IOException {
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      for (int i = 0; i < numCommits; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> deletes = dataGen.generateDeletes(instant, numDeletes);
        List<HoodieKey> keys = deletes.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
        writeClient.delete(jsc().parallelize(keys), instant);
      }
    }
  }

  private void triggerNUpdatesToSource(HoodieWriteConfig writeConfig, int numCommits, int numUpdates, HoodieTestDataGenerator dataGen) throws IOException {
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig)) {
      for (int i = 0; i < numCommits; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> deletes = dataGen.generateUpdates(instant, numUpdates);
        List<HoodieKey> keys = deletes.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
        writeClient.delete(jsc().parallelize(keys), instant);
      }
    }
  }

  private void triggerClustering(String basePath) {
    HoodieClusteringJob.Config clusterConfig = buildHoodieClusteringUtilConfig(basePath, true, "scheduleAndExecute", false);
    HoodieClusteringJob clusteringJob = new HoodieClusteringJob(jsc(), clusterConfig);
    clusteringJob.cluster(0);
  }

  private void triggerCleanToSource(HoodieWriteConfig writeConfig) throws IOException {
    try (SparkRDDWriteClient cleanClient = getHoodieWriteClient(writeConfig)) {
      cleanClient.clean();
    }
  }

  private void assertDataFromSourcesToTarget(List<String> sourcePaths, String targetPath, boolean doMatch) {
    spark().sqlContext().clearCache();
    for (int i = 0; i < sourcePaths.size(); i++) {
      spark().read().format("hudi").load(sourcePaths.get(i)).registerTempTable("srcTable" + (i + 1));
    }
    spark().read().format("hudi").option("hoodie.metadata.enable", "true")
        .load(targetPath).registerTempTable("tgtTable1");

    Dataset<Row> tempSrcDf = spark().sql("select * from srcTable1").drop("city_to_state");
    tempSrcDf.cache();
    Dataset<Row> srcDf = tempSrcDf;
    if (sourcePaths.size() > 1) {
      for (int i = 1; i < sourcePaths.size(); i++) {
        Dataset<Row> tempSrcDf1 = spark().sql("select * from srcTable" + (i + 1)).drop("city_to_state");
        tempSrcDf1.cache();
        srcDf = srcDf.union(tempSrcDf1);
      }
    }
    Dataset<Row> tgtDf = spark().sql("select * from tgtTable1").drop("city_to_state");
    assertEquals(srcDf.schema(), tgtDf.schema());
    if (doMatch) {
      assertTrue(srcDf.except(tgtDf).isEmpty() && tgtDf.except(srcDf).isEmpty());
    } else {
      assertFalse(srcDf.except(tgtDf).isEmpty() && tgtDf.except(srcDf).isEmpty());
    }
  }

  private Properties getTableProps() {
    return HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(new Properties())
        .build();
  }

  private void assertFSV(List<String> sourcePaths, String targetPath, List<String> partitions, boolean matchLatestFileGroups) {
    HoodieTableMetaClient targetTableMetaClient = HoodieTableMetaClient.builder().setBasePath(targetPath)
        .setConf(hadoopConf()).build();

    HoodieTableFileSystemView targetFSV = FileSystemViewManager.createInMemoryFileSystemView(
        context(), targetTableMetaClient,
        HoodieMetadataConfig.newBuilder().enable(true).build()
    );

    List<HoodieTableFileSystemView> sourceFSVs = sourcePaths.stream()
        .map(sourcePath -> {
          HoodieTableMetaClient sourceMetaClient = HoodieTableMetaClient.builder().setBasePath(sourcePath)
              .setConf(hadoopConf()).build();
          return FileSystemViewManager.createInMemoryFileSystemView(
              context(), sourceMetaClient,
              HoodieMetadataConfig.newBuilder().enable(false).build());
        }).collect(Collectors.toList());

    targetFSV.loadAllPartitions();
    Set<Path> targetPartitions = new HashSet<>(targetFSV.getPartitionPaths());
    // ToDo - validate full partition paths
    assertEquals(partitions.size(), targetPartitions.size());

    partitions.forEach(partition -> {
      Set<HoodieBaseFile> sourceBaseFiles = new HashSet<>();
      sourceFSVs.forEach(fsv ->
          sourceBaseFiles.addAll(
              matchLatestFileGroups ? fsv.getLatestBaseFiles(partition).collect(Collectors.toSet())
                  : fsv.getAllBaseFiles(partition).collect(Collectors.toSet())
          )
      );

      Set<HoodieBaseFile> targetBaseFiles = matchLatestFileGroups ? targetFSV.getLatestBaseFiles(partition).collect(Collectors.toSet())
          : targetFSV.getAllBaseFiles(partition).collect(Collectors.toSet());

      assertBaseFileListEquality(sourceBaseFiles, targetBaseFiles);
    });
  }

  static void assertBaseFileListEquality(Set<HoodieBaseFile> baseFileList1, Set<HoodieBaseFile> baseFileList2) {
    assertEquals(baseFileList1.size(), baseFileList2.size());
    Map<String, HoodieBaseFile> fileNameToBaseFileMap1 = new HashMap<>();
    baseFileList1.forEach(entry -> {
      fileNameToBaseFileMap1.put(entry.getFileName(), entry);
    });
    Map<String, HoodieBaseFile> fileNameToBaseFileMap2 = new HashMap<>();
    baseFileList2.forEach(entry -> {
      fileNameToBaseFileMap2.put(entry.getFileName(), entry);
    });
    fileNameToBaseFileMap1.entrySet().forEach((kv) -> {
      assertTrue(fileNameToBaseFileMap2.containsKey(kv.getKey()));
      assertBaseFileEquality(kv.getValue(), fileNameToBaseFileMap2.get(kv.getKey()));
    });
  }

  static void assertBaseFileEquality(HoodieBaseFile baseFile1, HoodieBaseFile baseFile2) {
    assertEquals(baseFile1.getFileName(), baseFile2.getFileName());
    assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
    assertEquals(baseFile1.getFileLen(), baseFile2.getFileLen());
    assertEquals(baseFile1.getFileSize(), baseFile2.getFileSize());
  }

  private HoodieClusteringJob.Config buildHoodieClusteringUtilConfig(String basePath, boolean runSchedule, String runningMode, boolean isAutoClean) {
    HoodieClusteringJob.Config config = new HoodieClusteringJob.Config();
    config.basePath = basePath;
    config.runSchedule = runSchedule;
    config.runningMode = runningMode;
    config.configs.add("hoodie.metadata.enable=false");
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.AUTO_CLEAN.key(), isAutoClean));
    config.configs.add(String.format("%s=%s", CLEANER_COMMITS_RETAINED.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), 1));
    return config;
  }

  private HoodieWriteConfig getHoodieCleanConfig(String basePath) {
    Properties props = new Properties();
    props.put(CLEANER_COMMITS_RETAINED.key(), "2");
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(2)
        .withCleanConfig(
            HoodieCleanConfig.newBuilder()
                .retainCommits(2)
                .withAsyncClean(false)
                .build())
        .forTable(TABLE_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withProps(props)
        .build();
  }
}
