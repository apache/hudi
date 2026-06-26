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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkSingleFileSortPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkSingleFileSortExecutionStrategy;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for INSERT_OVERWRITE, INSERT_OVERWRITE_TABLE, and DELETE_PARTITION operations
 * when there are pending clustering operations on the file groups being replaced.
 */
public class TestInsertOverwriteWithClustering extends HoodieClientTestBase {

  private HoodieTestDataGenerator dataGen;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    dataGen = new HoodieTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private HoodieClusteringConfig.Builder baseClusteringConfigBuilder(boolean rollbackPendingClustering) {
    return HoodieClusteringConfig.newBuilder()
        .withClusteringPlanStrategyClass(SparkSingleFileSortPlanStrategy.class.getName())
        .withClusteringExecutionStrategyClass(SparkSingleFileSortExecutionStrategy.class.getName())
        .withClusteringMaxNumGroups(10)
        .withRollbackPendingClustering(rollbackPendingClustering);
  }

  private HoodieWriteConfig.Builder getConfigBuilder(boolean rollbackPendingClustering) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withRollbackParallelism(2)
        .withClusteringConfig(baseClusteringConfigBuilder(rollbackPendingClustering).build());
  }

  private HoodieWriteConfig.Builder getConfigBuilderWithPartitionFilter(boolean rollbackPendingClustering, String partitionFilter) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withRollbackParallelism(2)
        .withClusteringConfig(baseClusteringConfigBuilder(rollbackPendingClustering)
            .withClusteringPartitionSelected(partitionFilter)
            .build());
  }

  /**
   * Test that INSERT_OVERWRITE operation throws an exception when file groups
   * to be replaced conflict with pending clustering.
   */
  @Test
  public void testStaticInsertOverwriteWithPendingClusteringRejectsUpdate() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Initial insert to create some data
    String instant1 = nextInstant();
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 100);
    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 2);
    commitInsert(client, writeRecords1, instant1);

    // Get partition path from the first record
    String partitionPath = records1.get(0).getPartitionPath();

    // Step 2: Schedule clustering for the partition
    Option<String> clusteringInstantOpt = client.scheduleClustering(Option.empty());
    assertTrue(clusteringInstantOpt.isPresent(), "Expected clustering to be scheduled but returned empty");
    String clusteringInstant = clusteringInstantOpt.get();

    // Verify clustering is pending
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTimeline pendingReplaceTimeline = metaClient.getActiveTimeline()
        .filterPendingClusteringTimeline();
    assertEquals(1, pendingReplaceTimeline.countInstants());

    // Get file groups involved in pending clustering
    Set<HoodieFileGroupId> pendingClusteringFileGroups = ClusteringUtils
        .getAllFileGroupsInPendingClusteringPlans(metaClient).keySet();
    assertFalse(pendingClusteringFileGroups.isEmpty());

    // Step 3: Perform static INSERT_OVERWRITE on the same partition
    // This should throw HoodieUpsertException because file groups to be replaced
    // conflict with pending clustering
    String instant3 = nextInstant();
    List<HoodieRecord> records3 = dataGen.generateInsertsForPartition(instant3, 50, partitionPath);
    JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 2);

    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieUpsertException upsertException = assertThrows(HoodieUpsertException.class, () ->
        commitInsertOverwrite(client, writeRecords3, instant3)
    );
    assertTrue(upsertException.getCause().getMessage().contains("Not allowed to update the clustering file group"));

    // Verify clustering is still pending (NOT rolled back)
    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    pendingReplaceTimeline = metaClient.getActiveTimeline().filterPendingClusteringTimeline();
    assertEquals(1, pendingReplaceTimeline.countInstants(),
        "Pending clustering should remain pending after failed INSERT_OVERWRITE");
    assertTrue(pendingReplaceTimeline.containsInstant(clusteringInstant));

    // Verify the INSERT_OVERWRITE did NOT complete
    HoodieTimeline completedReplaceTimeline = metaClient.getCommitTimeline()
        .filterCompletedInstants();
    assertFalse(completedReplaceTimeline.containsInstant(instant3));
  }

  /**
   * Test that dynamic INSERT_OVERWRITE_TABLE operation gets aborted when it overlaps w/ pending clustering.
   */
  @Test
  public void testDynamicInsertOverwriteWithPendingClustering() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(true).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Initial insert to create data in multiple partitions
    String instant1 = nextInstant();
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 100);
    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 2);
    commitInsert(client, writeRecords1, instant1);

    // Get partitions that have data
    Set<String> partitionsWithData = records1.stream()
        .map(HoodieRecord::getPartitionPath)
        .collect(Collectors.toSet());

    // Step 2: Schedule clustering
    Option<String> clusteringInstantOpt = client.scheduleClustering(Option.empty());
    assertTrue(clusteringInstantOpt.isPresent(), "Expected clustering to be scheduled but returned empty");
    String clusteringInstant = clusteringInstantOpt.get();

    // Verify clustering is pending
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    assertEquals(1, metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants());

    // Get file groups involved in pending clustering
    Set<HoodieFileGroupId> pendingClusteringFileGroups = ClusteringUtils
        .getAllFileGroupsInPendingClusteringPlans(metaClient).keySet();
    assertFalse(pendingClusteringFileGroups.isEmpty());

    // Step 3: Perform dynamic INSERT_OVERWRITE_TABLE on overlapping partitions
    String instant3 = nextInstant();
    String targetPartition = partitionsWithData.iterator().next();
    List<HoodieRecord> records3 = dataGen.generateInsertsForPartition(instant3, 50, targetPartition);
    JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 2);
    HoodieUpsertException upsertException = assertThrows(HoodieUpsertException.class, () ->
        commitInsertOverwrite(client, writeRecords3, instant3)
    );
    assertTrue(upsertException.getCause().getMessage().contains("Not allowed to update the clustering file group"));

    // Verify clustering was never rolled back.
    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTimeline pendingReplaceTimeline = metaClient.getActiveTimeline().filterPendingClusteringTimeline();
    assertTrue(pendingReplaceTimeline.containsInstant(clusteringInstant),
        "Pending clustering should not be rolled back");

    // Verify the INSERT_OVERWRITE did NOT complete
    HoodieTimeline completedReplaceTimeline = metaClient.getCommitTimeline()
        .filterCompletedInstants();
    assertFalse(completedReplaceTimeline.containsInstant(instant3));
  }

  /**
   * Test that DELETE_PARTITION operation succeeds when pending clustering does not overlap.
   */
  @Test
  public void testDeletePartitionWithNonOverlappingPendingClustering() throws Exception {
    HoodieWriteConfig config = getConfigBuilderWithPartitionFilter(false, "partition1").build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: insert into partition1
    String instant1 = nextInstant();
    List<HoodieRecord> records1 = dataGen.generateInsertsForPartition(instant1, 100, "partition1");
    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 2);
    commitInsert(client, writeRecords1, instant1);

    String instant2 = nextInstant();
    List<HoodieRecord> records2 = dataGen.generateInsertsForPartition(instant2, 100, "partition2");
    JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(records2, 2);
    commitInsert(client, writeRecords2, instant2);

    // Step 3: Schedule clustering for the partition1
    Option<String> clusteringInstantOpt = client.scheduleClustering(Option.empty());
    assertTrue(clusteringInstantOpt.isPresent(), "Expected clustering to be scheduled but returned empty");
    String clusteringInstant = clusteringInstantOpt.get();

    // Verify clustering is pending
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    assertEquals(1, metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants());

    // Get file groups involved in pending clustering
    Set<HoodieFileGroupId> pendingClusteringFileGroups = ClusteringUtils
        .getAllFileGroupsInPendingClusteringPlans(metaClient).keySet();
    assertFalse(pendingClusteringFileGroups.isEmpty());

    client.close();
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);

    // Step 3: Delete the partition2
    String instant3 = nextInstant();
    commitDeletePartitions(client2, Arrays.asList("partition2"), instant3);

    // Verify clustering is still pending (NOT rolled back)
    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTimeline pendingReplaceTimeline = metaClient.getActiveTimeline().filterPendingClusteringTimeline();
    assertTrue(pendingReplaceTimeline.containsInstant(clusteringInstant),
        "Pending clustering should remain pending after successful DELETE_PARTITION");

    // Verify the DELETE_PARTITION is completed
    HoodieTimeline completedReplaceTimeline = metaClient.getCommitTimeline()
        .filterCompletedInstants();
    assertTrue(completedReplaceTimeline.containsInstant(instant3));
  }

  /**
   * Test getFileGroupsBeingReplaced method in SparkInsertOverwriteCommitActionExecutor
   * for static INSERT_OVERWRITE scenario.
   */
  @Test
  public void testGetFileGroupsBeingReplacedForStaticOverwrite() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(true).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Initial insert to create some data
    String instant1 = nextInstant();
    String partitionPath = "2023/01/01";
    List<HoodieRecord> records1 = dataGen.generateInsertsForPartition(instant1, 100, partitionPath);
    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 2);
    commitInsert(client, writeRecords1, instant1);

    // Step 2: Get the file groups in the partition
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    List<String> existingFileIds = table.getSliceView()
        .getLatestFileSlices(partitionPath)
        .map(fileSlice -> fileSlice.getFileId())
        .collect(Collectors.toList());
    assertFalse(existingFileIds.isEmpty(), "Should have at least one file group");

    // Step 3: Create INSERT_OVERWRITE executor and test getFileGroupsBeingReplaced
    String instant2 = nextInstant();
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(instant2, 10, partitionPath);
    HoodieData<HoodieRecord> inputRecords = HoodieJavaRDD.of(jsc.parallelize(records, 1));

    SparkInsertOverwriteCommitActionExecutor executor =
        new SparkInsertOverwriteCommitActionExecutor(
            context, config, table, instant2, inputRecords);

    // Invoke the method - it's protected so we test it indirectly through the workflow
    Set<HoodieFileGroupId> fileGroupsBeingReplaced = executor.getFileGroupsBeingReplaced(inputRecords);

    // Verify that the file groups in the partition are identified as being replaced
    assertEquals(existingFileIds.size(), fileGroupsBeingReplaced.size(),
        "Should identify all existing file groups in the partition as being replaced");
    assertTrue(fileGroupsBeingReplaced.stream()
        .allMatch(fg -> fg.getPartitionPath().equals(partitionPath)),
        "All identified file groups should be in the target partition");
  }

  /**
   * Test that INSERT_OVERWRITE on a non-overlapping partition succeeds
   * even when there is pending clustering on a different partition.
   */
  @Test
  public void testInsertOverwriteNonOverlappingPartitionWithPendingClustering() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(true)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSingleFileSortPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(SparkSingleFileSortExecutionStrategy.class.getName())
            .withClusteringMaxNumGroups(10)
            .withRollbackPendingClustering(true)
            .withClusteringPartitionSelected("partition1")
            .build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Insert data into partition1
    String instant1 = nextInstant();
    List<HoodieRecord> records1 = dataGen.generateInsertsForPartition(instant1, 100, "partition1");
    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 2);
    commitInsert(client, writeRecords1, instant1);

    String instant2 = nextInstant();
    List<HoodieRecord> records2 = dataGen.generateInsertsForPartition(instant1, 100, "partition2");
    JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(records2, 2);
    commitInsert(client, writeRecords2, instant2);

    // Step 2: Schedule clustering for partition1
    Option<String> clusteringInstantOpt = client.scheduleClustering(Option.empty());
    assertTrue(clusteringInstantOpt.isPresent(), "Expected clustering to be scheduled but returned empty");
    String clusteringInstant = clusteringInstantOpt.get();

    // Verify clustering is pending
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    assertEquals(1, metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants());

    // Step 3: Perform INSERT_OVERWRITE on partition2 (non-overlapping)
    String instant3 = nextInstant();
    List<HoodieRecord> records3 = dataGen.generateInsertsForPartition(instant3, 50, "partition2");
    JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 2);
    commitInsertOverwrite(client, writeRecords3, instant3);

    // Verify clustering was NOT rolled back (no overlap)
    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTimeline pendingReplaceTimeline = metaClient.getActiveTimeline().filterPendingClusteringTimeline();
    assertEquals(1, pendingReplaceTimeline.countInstants(),
        "Pending clustering should NOT be rolled back for non-overlapping partition");

    // Verify the INSERT_OVERWRITE completed successfully
    HoodieTimeline completedReplaceTimeline = metaClient.getCommitTimeline()
        .filterCompletedInstants();
    assertTrue(completedReplaceTimeline.containsInstant(instant3));
  }

  /**
   * Test dynamic INSERT_OVERWRITE that determines partitions from input records.
   * If input records target a partition with pending clustering, it should detect the conflict and abort.
   */
  @Test
  public void testDynamicInsertOverwriteDetectsOverlap() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Insert data into multiple partitions
    String instant1 = nextInstant();
    List<HoodieRecord> partition1Records = dataGen.generateInsertsForPartition(instant1, 100, "2023/01/01");
    List<HoodieRecord> partition2Records = dataGen.generateInsertsForPartition(instant1, 100, "2023/01/02");
    List<HoodieRecord> partition3Records = dataGen.generateInsertsForPartition(instant1, 100, "2023/01/03");
    List<HoodieRecord> allRecords = new ArrayList<>();
    allRecords.addAll(partition1Records);
    allRecords.addAll(partition2Records);
    allRecords.addAll(partition3Records);
    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(allRecords, 2);
    commitInsert(client, writeRecords1, instant1);

    // Step 2: Schedule clustering
    Option<String> clusteringInstantOpt = client.scheduleClustering(Option.empty());
    assertTrue(clusteringInstantOpt.isPresent(), "Expected clustering to be scheduled but returned empty");
    String clusteringInstant = clusteringInstantOpt.get();

    // Verify clustering is pending
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    assertEquals(1, metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants());

    // Get partitions in clustering
    Set<HoodieFileGroupId> clusteringFileGroups = ClusteringUtils
        .getAllFileGroupsInPendingClusteringPlans(metaClient).keySet();
    Set<String> clusteringPartitions = clusteringFileGroups.stream()
        .map(HoodieFileGroupId::getPartitionPath)
        .collect(Collectors.toSet());
    assertFalse(clusteringPartitions.isEmpty());

    // Step 3: Perform dynamic INSERT_OVERWRITE_TABLE with records in overlapping partition
    // This should fail
    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    String instant3 = nextInstant();
    String targetPartition = clusteringPartitions.iterator().next();
    List<HoodieRecord> records3 = dataGen.generateInsertsForPartition(instant3, 50, targetPartition);
    JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 2);
    HoodieUpsertException upsertException = assertThrows(HoodieUpsertException.class, () ->
        commitInsertOverwrite(client, writeRecords3, instant3)
    );
    assertTrue(upsertException.getCause().getMessage().contains("Not allowed to update the clustering file group"));

    // Verify clustering is still pending (NOT rolled back)
    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTimeline pendingReplaceTimeline = metaClient.getActiveTimeline().filterPendingClusteringTimeline();
    assertEquals(1, pendingReplaceTimeline.countInstants(),
        "Pending clustering should remain pending after failed INSERT_OVERWRITE_TABLE");
    assertTrue(pendingReplaceTimeline.containsInstant(clusteringInstant));

    // Verify the INSERT_OVERWRITE_TABLE did NOT complete
    HoodieTimeline completedReplaceTimeline = metaClient.getCommitTimeline()
        .filterCompletedInstants();
    assertFalse(completedReplaceTimeline.containsInstant(instant3));
  }

  /**
   * Test multiple concurrent INSERT_OVERWRITE operations on different partitions
   * with one partition having pending clustering.
   */
  @Test
  public void testMultipleInsertOverwriteWithSelectiveOverlap() throws Exception {
    HoodieWriteConfig config = getConfigBuilderWithPartitionFilter(false,"2023/01/01,2023/01/02").build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Insert data into three partitions
    String instant1 = nextInstant();
    List<HoodieRecord> partition1Records = dataGen.generateInsertsForPartition(instant1, 100, "2023/01/01");
    List<HoodieRecord> partition2Records = dataGen.generateInsertsForPartition(instant1, 100, "2023/01/02");
    List<HoodieRecord> partition3Records = dataGen.generateInsertsForPartition(instant1, 100, "2023/01/03");
    List<HoodieRecord> allRecords = new ArrayList<>();
    allRecords.addAll(partition1Records);
    allRecords.addAll(partition2Records);
    allRecords.addAll(partition3Records);
    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(allRecords, 2);
    commitInsert(client, writeRecords1, instant1);

    // Step 2: Schedule clustering (will cluster partition1 and partition2)
    Option<String> clusteringInstantOpt = client.scheduleClustering(Option.empty());
    assertTrue(clusteringInstantOpt.isPresent(), "Expected clustering to be scheduled but returned empty");
    String clusteringInstant = clusteringInstantOpt.get();

    // Verify clustering is pending
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    assertEquals(1, metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants());

    client.close();
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);

    // Step 3: Perform INSERT_OVERWRITE on partition3 (no overlap) - should succeed without rollback
    String instant3 = nextInstant();
    List<HoodieRecord> records3 = dataGen.generateInsertsForPartition(instant3, 50, "2023/01/03");
    JavaRDD<HoodieRecord> writeRecords3 = jsc.parallelize(records3, 2);
    commitInsertOverwrite(client2, writeRecords3, instant3);

    // Verify clustering is still pending (no rollback for non-overlapping partition)
    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    assertEquals(1, metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants(),
        "Clustering should still be pending after INSERT_OVERWRITE on non-overlapping partition");

    client2.close();
    SparkRDDWriteClient client3 = getHoodieWriteClient(config);
    // Step 4: Now perform INSERT_OVERWRITE on partition1 (with overlap) - should fail
    String instant4 = nextInstant();
    List<HoodieRecord> records4 = dataGen.generateInsertsForPartition(instant4, 50, "2023/01/01");
    JavaRDD<HoodieRecord> writeRecords4 = jsc.parallelize(records4, 2);
    HoodieUpsertException upsertException = assertThrows(HoodieUpsertException.class, () ->
        commitInsertOverwrite(client3, writeRecords4, instant4)
    );
    assertTrue(upsertException.getCause().getMessage().contains("Not allowed to update the clustering file group"));

    // Verify clustering is still pending (NOT rolled back)
    metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTimeline pendingReplaceTimeline = metaClient.getActiveTimeline().filterPendingClusteringTimeline();
    assertTrue(pendingReplaceTimeline.containsInstant(clusteringInstant), "Pending clustering should remain pending after failed INSERT_OVERWRITE");

    // Verify the INSERT_OVERWRITE did NOT complete
    HoodieTimeline completedReplaceTimeline = metaClient.getCommitTimeline()
        .filterCompletedInstants();
    assertFalse(completedReplaceTimeline.containsInstant(instant4));
  }

  /**
   * Test getPartitionToReplacedFileIds for static INSERT_OVERWRITE.
   * Static overwrite uses configured partition paths, not input records.
   */
  @Test
  public void testGetPartitionToReplacedFileIdsForStaticOverwrite() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Insert data into multiple partitions
    String instant1 = nextInstant();
    List<HoodieRecord> partition1Records = dataGen.generateInsertsForPartition(instant1, 100, "2023/01/01");
    List<HoodieRecord> partition2Records = dataGen.generateInsertsForPartition(instant1, 100, "2023/01/02");
    List<HoodieRecord> allRecords = new ArrayList<>();
    allRecords.addAll(partition1Records);
    allRecords.addAll(partition2Records);
    commitInsert(client, jsc.parallelize(allRecords, 2), instant1);

    // Insert more data to create multiple file groups per partition
    String instant2 = nextInstant();
    List<HoodieRecord> moreRecords1 = dataGen.generateInsertsForPartition(instant2, 50, "2023/01/01");
    List<HoodieRecord> moreRecords2 = dataGen.generateInsertsForPartition(instant2, 50, "2023/01/02");
    List<HoodieRecord> moreRecords = new ArrayList<>();
    moreRecords.addAll(moreRecords1);
    moreRecords.addAll(moreRecords2);
    commitInsert(client, jsc.parallelize(moreRecords, 2), instant2);

    // Get existing file IDs before overwrite
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    List<String> partition1FileIds = table.getSliceView()
        .getLatestFileSlices("2023/01/01")
        .map(slice -> slice.getFileId())
        .collect(Collectors.toList());
    List<String> partition2FileIds = table.getSliceView()
        .getLatestFileSlices("2023/01/02")
        .map(slice -> slice.getFileId())
        .collect(Collectors.toList());

    assertFalse(partition1FileIds.isEmpty(), "Partition 2023/01/01 should have file groups");
    assertFalse(partition2FileIds.isEmpty(), "Partition 2023/01/02 should have file groups");

    // Step 2: Perform static INSERT_OVERWRITE on partition 2023/01/01 only
    String instant3 = nextInstant();
    List<HoodieRecord> overwriteRecords = dataGen.generateInsertsForPartition(instant3, 30, "2023/01/01");
    commitInsertOverwrite(client, jsc.parallelize(overwriteRecords, 2), instant3);

    // Step 3: Verify replaced file IDs - should only include partition 2023/01/01
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant instant3Instant = metaClient.getActiveTimeline().filterCompletedInstants()
        .filter(i -> i.requestedTime().equals(instant3)).firstInstant().get();
    HoodieReplaceCommitMetadata replaceMetadata = metaClient.getActiveTimeline()
        .readReplaceCommitMetadata(instant3Instant);

    Map<String, List<String>> partitionToReplacedFileIds = replaceMetadata.getPartitionToReplaceFileIds();

    // Verify partition 2023/01/01 is in replaced file IDs
    assertTrue(partitionToReplacedFileIds.containsKey("2023/01/01"),
        "Partition 2023/01/01 should be in replaced file IDs");

    // Verify all file IDs from partition 2023/01/01 are marked as replaced
    List<String> replacedFileIds = partitionToReplacedFileIds.get("2023/01/01");
    assertEquals(partition1FileIds.size(), replacedFileIds.size(),
        "All file IDs from partition 2023/01/01 should be marked as replaced");
    assertTrue(replacedFileIds.containsAll(partition1FileIds),
        "Replaced file IDs should match original file IDs in partition 2023/01/01");

    // Verify partition 2023/01/02 is NOT in replaced file IDs (was not overwritten)
    assertFalse(partitionToReplacedFileIds.containsKey("2023/01/02"),
        "Partition 2023/01/02 should NOT be in replaced file IDs");
  }

  /**
   * Test getPartitionToReplacedFileIds for dynamic INSERT_OVERWRITE.
   * Dynamic overwrite determines partitions from input records.
   */
  @Test
  public void testGetPartitionToReplacedFileIdsForDynamicOverwrite() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Insert data into three partitions
    String instant1 = nextInstant();
    List<HoodieRecord> partition1Records = dataGen.generateInsertsForPartition(instant1, 50, "2023/01/01");
    List<HoodieRecord> partition2Records = dataGen.generateInsertsForPartition(instant1, 50, "2023/01/02");
    List<HoodieRecord> partition3Records = dataGen.generateInsertsForPartition(instant1, 50, "2023/01/03");
    List<HoodieRecord> allRecords = new ArrayList<>();
    allRecords.addAll(partition1Records);
    allRecords.addAll(partition2Records);
    allRecords.addAll(partition3Records);
    commitInsert(client, jsc.parallelize(allRecords, 2), instant1);

    // Get existing file IDs
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    List<String> partition1FileIds = table.getSliceView()
        .getLatestFileSlices("2023/01/01")
        .map(slice -> slice.getFileId())
        .collect(Collectors.toList());
    List<String> partition2FileIds = table.getSliceView()
        .getLatestFileSlices("2023/01/02")
        .map(slice -> slice.getFileId())
        .collect(Collectors.toList());

    // Step 2: Perform dynamic INSERT_OVERWRITE with records for partitions 01/01 and 01/02
    String instant2 = nextInstant();
    List<HoodieRecord> overwriteRecords = new ArrayList<>();
    overwriteRecords.addAll(dataGen.generateInsertsForPartition(instant2, 30, "2023/01/01"));
    overwriteRecords.addAll(dataGen.generateInsertsForPartition(instant2, 30, "2023/01/02"));
    commitInsertOverwrite(client, jsc.parallelize(overwriteRecords, 2), instant2);

    // Step 3: Verify replaced file IDs include both partitions
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant instant2Instant = metaClient.getActiveTimeline().filterCompletedInstants()
        .filter(i -> i.requestedTime().equals(instant2)).firstInstant().get();
    HoodieReplaceCommitMetadata replaceMetadata = metaClient.getActiveTimeline()
        .readReplaceCommitMetadata(instant2Instant);

    Map<String, List<String>> partitionToReplacedFileIds = replaceMetadata.getPartitionToReplaceFileIds();

    // Verify both partitions are in replaced file IDs
    assertTrue(partitionToReplacedFileIds.containsKey("2023/01/01"),
        "Partition 2023/01/01 should be in replaced file IDs");
    assertTrue(partitionToReplacedFileIds.containsKey("2023/01/02"),
        "Partition 2023/01/02 should be in replaced file IDs");

    // Verify file IDs match
    assertEquals(partition1FileIds.size(), partitionToReplacedFileIds.get("2023/01/01").size());
    assertEquals(partition2FileIds.size(), partitionToReplacedFileIds.get("2023/01/02").size());

    // Verify partition 2023/01/03 is NOT in replaced file IDs
    assertFalse(partitionToReplacedFileIds.containsKey("2023/01/03"),
        "Partition 2023/01/03 should NOT be in replaced file IDs");
  }

  /**
   * Test getPartitionToReplacedFileIds when overwriting a partition with multiple file groups.
   */
  @Test
  public void testGetPartitionToReplacedFileIdsWithMultipleFileGroups() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Create multiple file groups in a single partition through multiple inserts
    String partitionPath = "2023/01/01";
    for (int i = 1; i <= 3; i++) {
      String instant = nextInstant();
      List<HoodieRecord> records = dataGen.generateInsertsForPartition(instant, 50, partitionPath);
      commitBulkInsert(client, jsc.parallelize(records, 2), instant);
    }

    // Get all file IDs in the partition
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    List<String> existingFileIds = table.getSliceView()
        .getLatestFileSlices(partitionPath)
        .map(slice -> slice.getFileId())
        .distinct()
        .collect(Collectors.toList());

    assertTrue(existingFileIds.size() >= 2,
        "Should have multiple file groups in partition from multiple inserts");

    // Step 2: Perform INSERT_OVERWRITE
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String overwriteInstant = nextInstant();
    List<HoodieRecord> overwriteRecords = dataGen.generateInsertsForPartition(overwriteInstant, 100, partitionPath);
    commitInsertOverwrite(client, jsc.parallelize(overwriteRecords, 2), overwriteInstant);

    // Step 3: Verify all file groups are marked as replaced
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant overwriteInstantObj = metaClient.getActiveTimeline().filterCompletedInstants()
        .filter(i -> i.requestedTime().equals(overwriteInstant)).firstInstant().get();
    HoodieReplaceCommitMetadata replaceMetadata = metaClient.getActiveTimeline()
        .readReplaceCommitMetadata(overwriteInstantObj);

    Map<String, List<String>> partitionToReplacedFileIds = replaceMetadata.getPartitionToReplaceFileIds();

    assertTrue(partitionToReplacedFileIds.containsKey(partitionPath));
    List<String> replacedFileIds = partitionToReplacedFileIds.get(partitionPath);

    // All existing file IDs should be marked as replaced
    assertEquals(existingFileIds.size(), replacedFileIds.size(),
        "All file groups should be marked as replaced");
    assertTrue(replacedFileIds.containsAll(existingFileIds),
        "Replaced file IDs should include all original file groups");
  }

  /**
   * Test getPartitionToReplacedFileIds when overwriting an empty partition.
   */
  @Test
  public void testGetPartitionToReplacedFileIdsForEmptyPartition() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Insert data into partition1
    String instant1 = nextInstant();
    String partition1 = "2023/01/01";
    List<HoodieRecord> records1 = dataGen.generateInsertsForPartition(instant1, 100, partition1);
    commitInsert(client, jsc.parallelize(records1, 2), instant1);

    // Step 2: Perform INSERT_OVERWRITE on a different partition (empty partition2)
    String instant2 = nextInstant();
    String partition2 = "2023/01/02";
    List<HoodieRecord> overwriteRecords = dataGen.generateInsertsForPartition(instant2, 50, partition2);
    commitInsertOverwrite(client, jsc.parallelize(overwriteRecords, 2), instant2);

    // Step 3: Verify partition2 is in replaced file IDs even though it was empty
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieInstant instant2Instant = metaClient.getActiveTimeline().filterCompletedInstants()
        .filter(i -> i.requestedTime().equals(instant2)).firstInstant().get();
    HoodieReplaceCommitMetadata replaceMetadata = metaClient.getActiveTimeline()
        .readReplaceCommitMetadata(instant2Instant);
    Map<String, List<String>> partitionToReplacedFileIds = replaceMetadata.getPartitionToReplaceFileIds();

    // partition2 should be present with empty list (no existing files to replace)
    assertTrue(partitionToReplacedFileIds.containsKey(partition2),
        "Empty partition should still be in replaced file IDs map");
    assertTrue(partitionToReplacedFileIds.get(partition2).isEmpty(),
        "Empty partition should have empty list of replaced file IDs");

    // partition1 should NOT be in replaced file IDs
    assertFalse(partitionToReplacedFileIds.containsKey(partition1),
        "Non-overwritten partition should not be in replaced file IDs");
  }

  /**
   * Test getPartitionToReplacedFileIds validates actual file IDs, not just counts.
   * Ensures the specific file IDs returned match the file groups that existed.
   */
  @Test
  public void testGetPartitionToReplacedFileIdsValidatesActualFileIds() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Step 1: Insert data
    String instant1 = nextInstant();
    String partitionPath = "2023/01/01";
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(instant1, 100, partitionPath);
    commitInsert(client, jsc.parallelize(records, 2), instant1);

    // Step 2: Insert more data to create additional file groups
    String instant2 = nextInstant();
    List<HoodieRecord> moreRecords = dataGen.generateInsertsForPartition(instant2, 50, partitionPath);
    commitInsert(client, jsc.parallelize(moreRecords, 2), instant2);

    // Capture exact file IDs before overwrite
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(this.metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    Set<String> expectedFileIds = table.getSliceView()
        .getLatestFileSlices(partitionPath)
        .map(slice -> slice.getFileId())
        .collect(Collectors.toSet());

    assertFalse(expectedFileIds.isEmpty(), "Should have file groups before overwrite");

    // Step 3: Perform INSERT_OVERWRITE
    String instant3 = nextInstant();
    List<HoodieRecord> overwriteRecords = dataGen.generateInsertsForPartition(instant3, 75, partitionPath);
    commitInsertOverwrite(client, jsc.parallelize(overwriteRecords, 2), instant3);

    // Step 4: Validate exact file IDs in replaced list
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant instant3Instant = metaClient.getActiveTimeline().filterCompletedInstants()
        .filter(i -> i.requestedTime().equals(instant3)).firstInstant().get();
    HoodieReplaceCommitMetadata replaceMetadata = metaClient.getActiveTimeline()
        .readReplaceCommitMetadata(instant3Instant);
    Map<String, List<String>> partitionToReplacedFileIds = replaceMetadata.getPartitionToReplaceFileIds();

    Set<String> actualReplacedFileIds = new HashSet<>(partitionToReplacedFileIds.get(partitionPath));

    // Validate exact file IDs, not just counts
    assertEquals(expectedFileIds, actualReplacedFileIds,
        "Replaced file IDs should exactly match the file groups that existed before overwrite");
  }

  // Hudi instant times are millisecond-resolution; sleep briefly between generations so
  // back-to-back instants in a single test method are strictly ordered.
  private String nextInstant() throws InterruptedException {
    Thread.sleep(2);
    return WriteClientTestUtils.createNewInstantTime();
  }

  private JavaRDD<WriteStatus> commitInsert(SparkRDDWriteClient client, JavaRDD<HoodieRecord> records, String instantTime) {
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    JavaRDD<WriteStatus> writeStatuses = client.insert(records, instantTime);
    List<WriteStatus> statusList = writeStatuses.collect();
    client.commit(instantTime, jsc.parallelize(statusList, 1));
    return writeStatuses;
  }

  private JavaRDD<WriteStatus> commitBulkInsert(SparkRDDWriteClient client, JavaRDD<HoodieRecord> records, String instantTime) {
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    JavaRDD<WriteStatus> writeStatuses = client.bulkInsert(records, instantTime);
    List<WriteStatus> statusList = writeStatuses.collect();
    client.commit(instantTime, jsc.parallelize(statusList, 1));
    return writeStatuses;
  }

  private HoodieWriteResult commitInsertOverwrite(SparkRDDWriteClient client, JavaRDD<HoodieRecord> records, String instantTime) {
    WriteClientTestUtils.startCommitWithTime(client, instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
    HoodieWriteResult result = client.insertOverwrite(records, instantTime);
    List<WriteStatus> statusList = result.getWriteStatuses().collect();
    client.commit(instantTime, jsc.parallelize(statusList, 1), Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,
        result.getPartitionToReplaceFileIds());
    return result;
  }

  private HoodieWriteResult commitDeletePartitions(SparkRDDWriteClient client, List<String> partitions, String instantTime) {
    WriteClientTestUtils.startCommitWithTime(client, instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
    HoodieWriteResult result = client.deletePartitions(partitions, instantTime);
    List<WriteStatus> statusList = result.getWriteStatuses().collect();
    client.commit(instantTime, jsc.parallelize(statusList, 1), Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,
        result.getPartitionToReplaceFileIds());
    return result;
  }
}
