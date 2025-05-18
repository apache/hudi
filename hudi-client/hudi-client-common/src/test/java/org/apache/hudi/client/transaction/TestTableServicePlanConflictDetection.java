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

package org.apache.hudi.client.transaction;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTableServicePlanConflictDetection extends HoodieCommonTestHarness {
  private final ConflictResolutionStrategy conflictResolutionStrategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
  private final String lastKnownCompletionTime = "20250101010101";
  private final String newInstantTime = "20250101010102";
  private final String partitionPath = "2025/01/01";
  private final String fileId1 = "file_id_1";
  private final String fileId2 = "file_id_2";
  private final String fileId3 = "file_id_3";
  private final String fileId4 = "file_id_4";

  @BeforeEach
  void init() throws IOException {
    initPath();
    initMetaClient();
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void newClusteringPlanConflictsWithServicePlan(boolean isClusteringPlan) {
    HoodieInstant clusteringInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, newInstantTime);
    HoodieClusteringPlan clusteringPlan = buildClusteringPlan(Stream.of(fileId1, fileId2));
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.CLUSTER.name())
        .setExtraMetadata(Collections.emptyMap())
        .setClusteringPlan(clusteringPlan)
        .build();
    metaClient.getActiveTimeline().saveToPendingClusterCommit(clusteringInstant, requestedReplaceMetadata);

    metaClient.reloadActiveTimeline();
    TableServicePlanConflictDetection conflictDetection = new TableServicePlanConflictDetection(conflictResolutionStrategy, metaClient, lastKnownCompletionTime);

    if (isClusteringPlan) {
      HoodieClusteringPlan compactionPlan = buildClusteringPlan(Stream.of(fileId1, fileId3));
      assertTrue(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    } else {
      HoodieCompactionPlan compactionPlan = buildCompactionPlan(Stream.of(fileId1, fileId3));
      assertTrue(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void newCompactionPlanConflictsWithServicePlan(boolean isClusteringPlan) {
    HoodieInstant compactionInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, newInstantTime);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant, buildCompactionPlan(Stream.of(fileId2, fileId3)));

    metaClient.reloadActiveTimeline();
    TableServicePlanConflictDetection conflictDetection = new TableServicePlanConflictDetection(conflictResolutionStrategy, metaClient, lastKnownCompletionTime);

    if (isClusteringPlan) {
      HoodieClusteringPlan compactionPlan = buildClusteringPlan(Stream.of(fileId1, fileId3));
      assertTrue(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    } else {
      HoodieCompactionPlan compactionPlan = buildCompactionPlan(Stream.of(fileId1, fileId3));
      assertTrue(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void newDeltaCommitConflictsWithServicePlan(boolean isClusteringPlan) {
    HoodieInstant commitInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, newInstantTime);
    metaClient.getActiveTimeline().createNewInstant(commitInstant);
    metaClient.getActiveTimeline().transitionRequestedToInflight(commitInstant, Option.empty());
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setFileId(fileId1);
    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setFileId(fileId2);
    commitMetadata.addWriteStat(partitionPath, writeStat1);
    commitMetadata.addWriteStat(partitionPath, writeStat2);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    commitInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newInstantTime);
    metaClient.getActiveTimeline().saveAsComplete(commitInstant, Option.of(commitMetadata));

    metaClient.reloadActiveTimeline();
    TableServicePlanConflictDetection conflictDetection = new TableServicePlanConflictDetection(conflictResolutionStrategy, metaClient, lastKnownCompletionTime);

    if (isClusteringPlan) {
      HoodieClusteringPlan compactionPlan = buildClusteringPlan(Stream.of(fileId1, fileId3));
      assertTrue(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    } else {
      HoodieCompactionPlan compactionPlan = buildCompactionPlan(Stream.of(fileId1, fileId3));
      assertTrue(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void newClusteringPlanNoConflictsWithServicePlan(boolean isClusteringPlan) {
    HoodieInstant clusteringInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, newInstantTime);
    HoodieClusteringPlan clusteringPlan = buildClusteringPlan(Stream.of(fileId1, fileId2));
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.CLUSTER.name())
        .setExtraMetadata(Collections.emptyMap())
        .setClusteringPlan(clusteringPlan)
        .build();
    metaClient.getActiveTimeline().saveToPendingClusterCommit(clusteringInstant, requestedReplaceMetadata);

    metaClient.reloadActiveTimeline();
    TableServicePlanConflictDetection conflictDetection = new TableServicePlanConflictDetection(conflictResolutionStrategy, metaClient, lastKnownCompletionTime);

    if (isClusteringPlan) {
      HoodieClusteringPlan compactionPlan = buildClusteringPlan(Stream.of(fileId3, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    } else {
      HoodieCompactionPlan compactionPlan = buildCompactionPlan(Stream.of(fileId3, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void newCompactionPlanNoConflictsWithServicePlan(boolean isClusteringPlan) {
    HoodieInstant compactionInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, newInstantTime);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant, buildCompactionPlan(Stream.of(fileId2, fileId3)));

    metaClient.reloadActiveTimeline();
    TableServicePlanConflictDetection conflictDetection = new TableServicePlanConflictDetection(conflictResolutionStrategy, metaClient, lastKnownCompletionTime);

    if (isClusteringPlan) {
      HoodieClusteringPlan compactionPlan = buildClusteringPlan(Stream.of(fileId1, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    } else {
      HoodieCompactionPlan compactionPlan = buildCompactionPlan(Stream.of(fileId1, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void newDeltaCommitNoConflictsWithServicePlan(boolean isClusteringPlan) {
    HoodieInstant commitInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, newInstantTime);
    metaClient.getActiveTimeline().createNewInstant(commitInstant);
    metaClient.getActiveTimeline().transitionRequestedToInflight(commitInstant, Option.empty());
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setFileId(fileId1);
    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setFileId(fileId2);
    commitMetadata.addWriteStat(partitionPath, writeStat1);
    commitMetadata.addWriteStat(partitionPath, writeStat2);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    commitInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newInstantTime);
    metaClient.getActiveTimeline().saveAsComplete(commitInstant, Option.of(commitMetadata));

    metaClient.reloadActiveTimeline();
    TableServicePlanConflictDetection conflictDetection = new TableServicePlanConflictDetection(conflictResolutionStrategy, metaClient, lastKnownCompletionTime);

    if (isClusteringPlan) {
      HoodieClusteringPlan compactionPlan = buildClusteringPlan(Stream.of(fileId3, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    } else {
      HoodieCompactionPlan compactionPlan = buildCompactionPlan(Stream.of(fileId3, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void noNewCommitsDuringPlanning(boolean isClusteringPlan) {
    TableServicePlanConflictDetection conflictDetection = new TableServicePlanConflictDetection(conflictResolutionStrategy, metaClient, lastKnownCompletionTime);
    if (isClusteringPlan) {
      HoodieClusteringPlan compactionPlan = buildClusteringPlan(Stream.of(fileId1, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    } else {
      HoodieCompactionPlan compactionPlan = buildCompactionPlan(Stream.of(fileId1, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void noCompletedDeltaCommitsDuringPlanning(boolean isClusteringPlan) {
    // create an inflight commit and ensure it is ignored
    HoodieInstant commitInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, newInstantTime);
    metaClient.getActiveTimeline().createNewInstant(commitInstant);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setFileId(fileId1);
    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setFileId(fileId2);
    commitMetadata.addWriteStat(partitionPath, writeStat1);
    commitMetadata.addWriteStat(partitionPath, writeStat2);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    metaClient.getActiveTimeline().transitionRequestedToInflight(commitInstant, Option.of(commitMetadata));

    TableServicePlanConflictDetection conflictDetection = new TableServicePlanConflictDetection(conflictResolutionStrategy, metaClient, lastKnownCompletionTime);
    if (isClusteringPlan) {
      HoodieClusteringPlan compactionPlan = buildClusteringPlan(Stream.of(fileId1, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    } else {
      HoodieCompactionPlan compactionPlan = buildCompactionPlan(Stream.of(fileId1, fileId4));
      assertFalse(conflictDetection.hasConflict(compactionPlan, newInstantTime));
    }
  }

  private HoodieClusteringPlan buildClusteringPlan(Stream<String> filedIds) {
    List<HoodieSliceInfo> affectedSlices = filedIds
        .map(fileId -> HoodieSliceInfo.newBuilder().setPartitionPath(partitionPath).setFileId(fileId).build())
        .collect(Collectors.toList());
    return HoodieClusteringPlan.newBuilder().setVersion(2).setInputGroups(Collections.singletonList(HoodieClusteringGroup.newBuilder().setSlices(affectedSlices).build())).build();
  }

  private HoodieCompactionPlan buildCompactionPlan(Stream<String> filedIds) {
    List<HoodieCompactionOperation> compactionOperations = filedIds
        .map(fileId -> HoodieCompactionOperation.newBuilder().setPartitionPath(partitionPath).setFileId(fileId).setBaseInstantTime("001").build())
        .collect(Collectors.toList());
    return HoodieCompactionPlan.newBuilder().setVersion(2).setOperations(compactionOperations).build();
  }
}
