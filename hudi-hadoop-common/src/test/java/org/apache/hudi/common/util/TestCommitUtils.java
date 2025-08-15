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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieCompactionStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.util.CommitUtils.getCheckpointValueAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CommitUtils}.
 */
public class TestCommitUtils {
  private static final String SINK_CHECKPOINT_KEY = "_hudi_streaming_sink_checkpoint";
  private static final String ID1 = "id1";
  private static final String ID2 = "id2";
  private static final String ID3 = "id3";
  @TempDir
  public java.nio.file.Path tempDir;
  private HoodieTableMetaClient metaClient;

  private void init() throws IOException {
    java.nio.file.Path basePath = tempDir.resolve("dataset");
    java.nio.file.Files.createDirectories(basePath);
    String basePathStr = basePath.toAbsolutePath().toString();
    metaClient = HoodieTestUtils.init(basePathStr, HoodieTableType.MERGE_ON_READ);
  }

  @Test
  public void testCommitMetadataCreation() {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    writeStats.add(createWriteStat("p1", "f1"));
    writeStats.add(createWriteStat("p2", "f2"));
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIds = new ArrayList<>();
    replacedFileIds.add("f0");
    partitionToReplaceFileIds.put("p1", replacedFileIds);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeStats, partitionToReplaceFileIds,
        Option.empty(),
        WriteOperationType.INSERT,
        TRIP_SCHEMA,
        HoodieTimeline.DELTA_COMMIT_ACTION);

    assertFalse(commitMetadata instanceof HoodieReplaceCommitMetadata);
    assertEquals(2, commitMetadata.getPartitionToWriteStats().size());
    assertEquals("f1", commitMetadata.getPartitionToWriteStats().get("p1").get(0).getFileId());
    assertEquals("f2", commitMetadata.getPartitionToWriteStats().get("p2").get(0).getFileId());
    assertEquals(WriteOperationType.INSERT, commitMetadata.getOperationType());
    assertEquals(TRIP_SCHEMA, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY));
  }

  @Test
  public void testReplaceMetadataCreation() {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    writeStats.add(createWriteStat("p1", "f1"));
    writeStats.add(createWriteStat("p2", "f2"));

    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIds = new ArrayList<>();
    replacedFileIds.add("f0");
    partitionToReplaceFileIds.put("p1", replacedFileIds);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeStats, partitionToReplaceFileIds,
        Option.empty(),
        WriteOperationType.INSERT,
        TRIP_SCHEMA,
        REPLACE_COMMIT_ACTION);

    assertTrue(commitMetadata instanceof HoodieReplaceCommitMetadata);
    HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
    assertEquals(1, replaceCommitMetadata.getPartitionToReplaceFileIds().size());
    assertEquals("f0", replaceCommitMetadata.getPartitionToReplaceFileIds().get("p1").get(0));
    assertEquals(2, commitMetadata.getPartitionToWriteStats().size());
    assertEquals("f1", commitMetadata.getPartitionToWriteStats().get("p1").get(0).getFileId());
    assertEquals("f2", commitMetadata.getPartitionToWriteStats().get("p2").get(0).getFileId());
    assertEquals(WriteOperationType.INSERT, commitMetadata.getOperationType());
    assertEquals(TRIP_SCHEMA, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY));
  }

  @Test
  public void testGetValidCheckpointForCurrentWriter() throws IOException {
    init();
    HoodieActiveTimeline timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);

    // Deltacommit 1 completed: (id1, 3)
    addDeltaCommit(timeline, "20230913001000000", ID1, "3", true);
    // Deltacommit 2 completed: (id2, 4)
    addDeltaCommit(timeline, "20230913002000000", ID2, "4", true);
    // Deltacommit 3 completed: (id1, 5)
    addDeltaCommit(timeline, "20230913003000000", ID1, "5", true);
    // Request compaction:
    addRequestedCompaction(timeline, "20230913003800000");
    // Deltacommit 4 completed: (id2, 6)
    addDeltaCommit(timeline, "20230913004000000", ID2, "6", true);
    // Requested replacecommit (clustering):
    addRequestedReplaceCommit(timeline, "20230913004800000");
    // Deltacommit 5 inflight: (id2, 7)
    addDeltaCommit(timeline, "20230913005000000", ID2, "7", false);
    // Commit 6 completed without checkpoints (e.g., compaction that does not affect checkpointing)
    addCommit(timeline, "20230913006000000");

    timeline = timeline.reload();
    assertEquals(Option.of("5"), CommitUtils.getValidCheckpointForCurrentWriter(timeline, SINK_CHECKPOINT_KEY, ID1));
    assertEquals(Option.of("6"), CommitUtils.getValidCheckpointForCurrentWriter(timeline, SINK_CHECKPOINT_KEY, ID2));
    assertEquals(
        Option.empty(), CommitUtils.getValidCheckpointForCurrentWriter(timeline, SINK_CHECKPOINT_KEY, ID3));
  }

  private HoodieWriteStat createWriteStat(String partition, String fileId) {
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setPartitionPath(partition);
    writeStat1.setFileId(fileId);
    return writeStat1;
  }

  private void addDeltaCommit(HoodieActiveTimeline timeline,
                              String ts, String id, String batchId,
                              boolean isCompleted) throws IOException {
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, ts);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.UPSERT);
    commitMetadata.addMetadata(SINK_CHECKPOINT_KEY,
        getCheckpointValueAsString(id, batchId));
    timeline.createNewInstant(instant);
    timeline.transitionRequestedToInflight(instant, Option.of(commitMetadata));
    if (isCompleted) {
      timeline.saveAsComplete(
          INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, instant.getAction(), instant.requestedTime()),
          Option.of(commitMetadata), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    }
  }

  private void addCommit(HoodieActiveTimeline timeline,
                         String ts) throws IOException {
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, ts);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.COMPACT);
    timeline.createNewInstant(instant);
    timeline.transitionRequestedToInflight(instant, Option.of(commitMetadata));
    timeline.saveAsComplete(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, instant.getAction(), instant.requestedTime()),
        Option.of(commitMetadata), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
  }

  private void addRequestedCompaction(HoodieActiveTimeline timeline, String ts) {
    HoodieCompactionPlan compactionPlan = HoodieCompactionPlan.newBuilder()
        .setOperations(Collections.emptyList())
        .setVersion(CompactionUtils.LATEST_COMPACTION_METADATA_VERSION)
        .setStrategy(HoodieCompactionStrategy.newBuilder().build())
        .setPreserveHoodieMetadata(true)
        .build();
    timeline.saveToCompactionRequested(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, COMPACTION_ACTION, ts),
        compactionPlan
    );
  }

  private void addRequestedReplaceCommit(HoodieActiveTimeline timeline, String ts) {
    HoodieRequestedReplaceMetadata requestedReplaceMetadata =
        HoodieRequestedReplaceMetadata.newBuilder()
            .setOperationType(WriteOperationType.CLUSTER.name())
            .setExtraMetadata(Collections.emptyMap())
            .setClusteringPlan(new HoodieClusteringPlan())
            .build();
    timeline.saveToPendingClusterCommit(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, CLUSTERING_ACTION, ts),
        requestedReplaceMetadata
    );
  }
}
