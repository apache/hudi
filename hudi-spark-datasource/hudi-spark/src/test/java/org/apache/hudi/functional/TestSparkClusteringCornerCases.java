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

package org.apache.hudi.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestSparkClusteringCornerCases extends HoodieClientTestBase {

  private static Stream<Arguments> testClusteringWithPendingIngestionInDifferentPartition() {
    // tableType, enableRowWriter
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, true),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false)
    );
  }

  /**
   * Test clustering succeeds when there's a pending ingestion in a different partition.
   * This test uses a 2MB max parquet file size and generates enough data to create multiple file groups.
   * Clustering is configured to cluster only 1 file group (max groups = 1, max bytes per group = 2MB).
   *
   * Scenario:
   * 1. Ingest data into partition1 with 5000 records (complete) - creates multiple file groups
   * 2. Schedule clustering on partition1 (but don't execute yet) - will cluster only 1 file group
   * 3. Start ingestion into partition2 (but don't complete it - leave it inflight)
   * 4. Execute the clustering that was scheduled earlier (clustering instant time < pending ingestion instant)
   * 5. Clustering should succeed because there's no partition overlap
   * 6. Complete the pending ingestion in partition2
   *
   * The test is parameterized with table type (COW/MOR) and row writer enabled/disabled.
   */
  @ParameterizedTest
  @MethodSource
  void testClusteringWithPendingIngestionInDifferentPartition(HoodieTableType tableType, boolean enableRowWriter) throws IOException {
    // Use specific partition data generators
    HoodieTestDataGenerator dataGenPartition1 = new HoodieTestDataGenerator(
        new String[] {HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH});
    HoodieTestDataGenerator dataGenPartition2 = new HoodieTestDataGenerator(
        new String[] {HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH});

    Properties props = getPropertiesForKeyGen(true);

    // Set max parquet file size to 2MB to create multiple file groups
    props.setProperty("hoodie.parquet.max.file.size", String.valueOf(2 * 1024 * 1024));

    // Configure clustering to cluster only 1 file group
    // Set max groups to 1 and target file size to ensure only 1 file group is clustered
    props.setProperty("hoodie.clustering.plan.strategy.max.num.groups", "1");
    props.setProperty("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(2 * 1024 * 1024));
    props.setProperty("hoodie.clustering.plan.strategy.max.bytes.per.group", String.valueOf(1 * 1024 * 1024));

    HoodieWriteConfig.Builder configBuilder = getConfigBuilder()
        .withProperties(props)
        .withAutoCommit(false);

    // Configure row writer based on parameter
    if (enableRowWriter) {
      props.setProperty("hoodie.datasource.write.row.writer.enable", "true");
    } else {
      props.setProperty("hoodie.datasource.write.row.writer.enable", "false");
    }
    configBuilder.withProperties(props);

    HoodieWriteConfig hoodieWriteConfig = configBuilder.build();
    initMetaClient(tableType, props);

    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      // Step 1: Ingest data into partition1 and complete it
      // Generate more records to create multiple file groups with 2MB max file size
      String firstInstant = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> recordsPartition1 = dataGenPartition1.generateInserts(firstInstant, 5000);
      writeData(client, firstInstant, recordsPartition1, true);

      // Verify we have 1 completed commit
      metaClient.reloadActiveTimeline();
      assertEquals(1, metaClient.getActiveTimeline().filterCompletedInstants().countInstants());

      // Verify data was written to exactly one partition (partition1) with multiple file groups
      HoodieInstant hoodieInstant = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieCommitMetadata.class);
      assertEquals(1, commitMetadata.getPartitionToWriteStats().size(),
          "Data should have been written to exactly 1 partition");
      Assertions.assertTrue(commitMetadata.getPartitionToWriteStats()
              .containsKey(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH),
          "Data should have been written to partition1");
      long numFileGroupsBeforeClustering = commitMetadata.getPartitionToWriteStats()
          .get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
          .stream()
          .map(stat -> stat.getFileId())
          .distinct()
          .count();
      Assertions.assertTrue(numFileGroupsBeforeClustering > 1,
          "Should have created multiple file groups in partition1 with 2MB max file size, but got: " + numFileGroupsBeforeClustering);

      // Step 2: Schedule clustering on partition1 (but don't execute yet)
      String clusteringInstant = HoodieActiveTimeline.createNewInstantTime();
      client.scheduleClusteringAtInstant(clusteringInstant, Option.empty());

      // Verify clustering is scheduled (pending)
      metaClient.reloadActiveTimeline();
      List<HoodieInstant> pendingClusteringInstants = metaClient.getActiveTimeline()
          .filterPendingReplaceTimeline()
          .getInstants();
      assertEquals(1, pendingClusteringInstants.size(), "Should have exactly one pending clustering instant");
      assertEquals(clusteringInstant, pendingClusteringInstants.get(0).getTimestamp());

      // Step 3: Start ingestion into partition2 but do not complete it
      // The ingestion instant will be greater than the clustering instant
      String secondInstant = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> recordsPartition2 = dataGenPartition2.generateInserts(secondInstant, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(recordsPartition2, 2);

      client.startCommitWithTime(secondInstant);
      List<WriteStatus> writeStatuses = client.insert(recordsRDD, secondInstant).collect();
      assertNoWriteErrors(writeStatuses);
      // Note: NOT calling client.commit() to leave the commit in inflight state

      // Verify we have one pending commit (ingestion)
      metaClient.reloadActiveTimeline();
      List<HoodieInstant> inflightCommits = metaClient.getActiveTimeline()
          .filterPendingExcludingCompaction()
          .getInstants();
      // Should have 2 pending instants: 1 clustering (replace) + 1 commit (ingestion)
      assertEquals(2, inflightCommits.size(), "Should have 2 pending instants (1 clustering + 1 ingestion)");

      // Verify the clustering instant is less than the pending ingestion instant
      Assertions.assertTrue(Long.parseLong(clusteringInstant) < Long.parseLong(secondInstant),
          "Clustering instant should be less than pending ingestion instant");

      // Step 4: Execute the clustering that was scheduled earlier
      // This should succeed even though there's a pending ingestion in a different partition
      client.cluster(clusteringInstant, true);

      // Verify clustering succeeded
      metaClient.reloadActiveTimeline();
      List<HoodieInstant> completedClustering = metaClient.getActiveTimeline()
          .getCompletedReplaceTimeline()
          .getInstants();
      assertEquals(1, completedClustering.size(), "Should have exactly one completed clustering commit");
      assertEquals(clusteringInstant, completedClustering.get(0).getTimestamp());

      // Verify the pending ingestion commit is still there
      metaClient.reloadActiveTimeline();
      List<HoodieInstant> stillPendingCommits = metaClient.getActiveTimeline()
          .filterPendingExcludingCompaction()
          .getInstants();
      assertEquals(1, stillPendingCommits.size(), "Pending ingestion commit should still exist after clustering");
      assertEquals(secondInstant, stillPendingCommits.get(0).getTimestamp());

      // Step 5: Complete the pending ingestion in partition2
      client.commit(secondInstant, jsc.parallelize(writeStatuses, 1));

      // Verify both commits are now complete
      metaClient.reloadActiveTimeline();
      assertEquals(0, metaClient.getActiveTimeline().filterPendingExcludingCompaction().countInstants(),
          "No pending commits should remain");

      // Verify we have the expected timeline: 2 commits + 1 replace (clustering)
      assertEquals(3, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants(),
          "Should have 2 completed commit instants");
      assertEquals(1, metaClient.getActiveTimeline().getCompletedReplaceTimeline().countInstants(),
          "Should have 1 completed clustering instant");

      // Verify clustering metadata shows data was written only for partition1
      HoodieReplaceCommitMetadata replaceCommitMetadata = HoodieReplaceCommitMetadata.fromBytes(
          metaClient.getActiveTimeline().getInstantDetails(metaClient.getActiveTimeline().getLastClusterCommit().get()).get(),
          HoodieReplaceCommitMetadata.class
      );
      assertEquals(1, replaceCommitMetadata.getPartitionToWriteStats().size(),
          "Clustering should have written to only 1 partition");
      Assertions.assertTrue(replaceCommitMetadata.getPartitionToWriteStats()
              .containsKey(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH),
          "Clustering should have written to partition1");

      // Verify clustering only touched 1 file group (as configured by max.num.groups=1)
      long numFileGroupsClustered = replaceCommitMetadata.getPartitionToReplaceFileIds()
          .values()
          .stream()
          .mapToLong(List::size)
          .sum();
      assertEquals(1, numFileGroupsClustered,
          "Clustering should have clustered exactly 1 file group (as configured), but clustered: " + numFileGroupsClustered);

      // Verify that there were more file groups available than what was clustered
      Assertions.assertTrue(numFileGroupsBeforeClustering > numFileGroupsClustered,
          "Should have had more file groups (" + numFileGroupsBeforeClustering + ") than what was clustered (" + numFileGroupsClustered + ")");

      // Verify exact record count match between the replaced file group and clustering output
      // Get the replaced fileId from the clustering metadata
      String replacedFileId = replaceCommitMetadata.getPartitionToReplaceFileIds()
          .get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
          .get(0);

      // Find the exact record count for this fileId from the first commit
      long expectedRecordsInReplacedFileGroup = commitMetadata.getPartitionToWriteStats()
          .get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
          .stream()
          .filter(stat -> stat.getFileId().equals(replacedFileId))
          .mapToLong(stat -> stat.getNumInserts() + stat.getNumUpdateWrites())
          .sum();

      // Get the actual records processed by clustering
      long recordsProcessedByClustering = replaceCommitMetadata.getPartitionToWriteStats()
          .get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
          .stream()
          .mapToLong(stat -> stat.getNumInserts() + stat.getNumUpdateWrites())
          .sum();

      // Verify exact match
      assertEquals(expectedRecordsInReplacedFileGroup, recordsProcessedByClustering,
          "Clustering should have processed exactly the same number of records as in the replaced file group");
      Assertions.assertTrue(recordsProcessedByClustering > 0,
          "Clustering should have processed some records, but got: " + recordsProcessedByClustering);

      // Verify that there were more total records than what was clustered
      long totalRecordsInTable = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(List::stream)
          .mapToLong(stat -> stat.getNumInserts() + stat.getNumUpdateWrites())
          .sum();
      Assertions.assertTrue(recordsProcessedByClustering < totalRecordsInTable,
          "Clustering should have processed fewer records (" + recordsProcessedByClustering
              + ") than total records in the table (" + totalRecordsInTable + ")");
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private List<HoodieRecord> writeData(SparkRDDWriteClient client, String instant, List<HoodieRecord> recordList, boolean doCommitExplicitly) {
    JavaRDD records = jsc.parallelize(recordList, 2);
    client.startCommitWithTime(instant);
    List<WriteStatus> writeStatuses = client.upsert(records, instant).collect();
    assertNoWriteErrors(writeStatuses);
    if (doCommitExplicitly) {
      client.commit(instant, jsc.parallelize(writeStatuses));
    }
    return recordList;
  }
}