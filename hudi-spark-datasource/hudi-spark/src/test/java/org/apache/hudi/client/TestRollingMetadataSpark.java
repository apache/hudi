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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRollingMetadataSpark extends SparkClientFunctionalTestHarness {

  /**
   * Test that rolling metadata is carried forward across inline compaction commits on a MOR table.
   * Compaction does not call preCommit, so its commit won't have rolling metadata. But subsequent
   * regular writes should walk back past the compaction commit to find rolling metadata from earlier commits.
   */
  @Test
  public void testRollingMetadataWithInlineCompaction() throws IOException {
    // Given: A MOR table with rolling metadata and inline compaction after 2 delta commits
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ);
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("checkpoint.offset")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(true)
            .withMaxNumDeltaCommitsBeforeCompaction(2)
            .build())
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    // When: First delta commit with rolling metadata
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("checkpoint.offset", "1000");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    // When: Second delta commit (should trigger inline compaction after this)
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateUpdates(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.upsert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Verify compaction was triggered (should see a commit action on the timeline)
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants();
    boolean hasCompaction = writeTimeline.getInstantsAsStream()
        .anyMatch(i -> i.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    assertTrue(hasCompaction, "Inline compaction should have been triggered");

    // Then: Verify the compaction commit itself carries the rolling metadata
    HoodieInstant compactionInstant = writeTimeline.getInstantsAsStream()
        .filter(i -> i.getAction().equals(HoodieTimeline.COMMIT_ACTION))
        .reduce((a, b) -> b).get();
    HoodieCommitMetadata compactionMetadata = TimelineUtils.getCommitMetadata(compactionInstant, metaClient.getActiveTimeline());
    assertEquals("1000", compactionMetadata.getMetadata("checkpoint.offset"),
        "Compaction commit itself should contain rolling metadata");

    // When: Third delta commit after compaction, no rolling metadata provided
    String instant3 = client.createNewInstantTime(false);
    List<HoodieRecord> records3 = dataGen.generateInserts(instant3, 10);
    JavaRDD<HoodieRecord> writeRecords3 = jsc().parallelize(records3, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant3);
    List<WriteStatus> writeStatuses3 = client.insert(writeRecords3, instant3).collect();
    assertNoWriteErrors(writeStatuses3);
    client.commitStats(instant3, writeStatuses3.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Rolling metadata should still be found by walking back past the compaction commit
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant lastDeltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline()
        .filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata3 = TimelineUtils.getCommitMetadata(lastDeltaCommit, metaClient.getActiveTimeline());
    assertEquals("1000", metadata3.getMetadata("checkpoint.offset"),
        "Rolling metadata should be carried forward past compaction commit");

    client.close();
  }


  /**
   * Test that rolling metadata is carried forward into clustering commits.
   * With OCC enabled, clustering calls preCommit which merges rolling metadata.
   */
  @Test
  public void testRollingMetadataWithInlineClustering() throws IOException {
    // Given: A CoW table with rolling metadata and inline clustering after 1 commit
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("checkpoint.offset,checkpoint.partition")
        .withRollingMetadataTimelineLookbackCommits(1)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringMaxNumGroups(10)
            .withClusteringTargetPartitions(0)
            .withInlineClustering(true)
            .withInlineClusteringNumCommits(1)
            .build())
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    // When: First commit with rolling metadata
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 100);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("checkpoint.offset", "5000");
    extraMetadata1.put("checkpoint.partition", "p1");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    // When: Second commit with more inserts (should trigger inline clustering after commit)
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 100);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);

    Map<String, String> extraMetadata2 = new HashMap<>();
    extraMetadata2.put("checkpoint.offset", "6000");
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata2), metaClient.getCommitActionType());

    // Then: Verify clustering was triggered
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants();
    boolean hasClustering = writeTimeline.getInstantsAsStream()
        .anyMatch(i -> i.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)
            || i.getAction().equals(HoodieTimeline.CLUSTERING_ACTION));

    assertTrue(hasClustering, "Inline clustering should have been triggered");

    // Find the clustering commit and verify it has rolling metadata
    HoodieInstant clusteringInstant = writeTimeline.getInstantsAsStream()
        .filter(i -> i.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)
            || i.getAction().equals(HoodieTimeline.CLUSTERING_ACTION))
        .reduce((a, b) -> b).get();
    HoodieCommitMetadata clusteringMetadata = TimelineUtils.getCommitMetadata(clusteringInstant, metaClient.getActiveTimeline());
    assertEquals("6000", clusteringMetadata.getMetadata("checkpoint.offset"),
        "Clustering commit should carry forward latest rolling metadata");
    assertEquals("p1", clusteringMetadata.getMetadata("checkpoint.partition"),
        "Clustering commit should carry forward rolling metadata from earlier commits");

    // When: Third commit after clustering, no rolling metadata provided
    String instant3 = client.createNewInstantTime(false);
    List<HoodieRecord> records3 = dataGen.generateInserts(instant3, 10);
    JavaRDD<HoodieRecord> writeRecords3 = jsc().parallelize(records3, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant3);
    List<WriteStatus> writeStatuses3 = client.insert(writeRecords3, instant3).collect();
    assertNoWriteErrors(writeStatuses3);
    client.commitStats(instant3, writeStatuses3.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Rolling metadata should be present in the latest commit
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant lastCommit = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata3 = TimelineUtils.getCommitMetadata(lastCommit, metaClient.getActiveTimeline());
    assertEquals("6000", metadata3.getMetadata("checkpoint.offset"),
        "Rolling metadata should persist across clustering");
    assertEquals("p1", metadata3.getMetadata("checkpoint.partition"),
        "Rolling metadata should persist across clustering");

    client.close();
  }
}
