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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSimpleConcurrentFileWritesConflictResolutionStrategy extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testNoConcurrentWrites() throws Exception {
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime);
    // consider commits before this are all successful

    Option<HoodieInstant> lastSuccessfulInstant = metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant();
    newInstantTime = HoodieTestTable.makeNewCommitTime();
    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, newInstantTime));

    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient.getActiveTimeline(), currentInstant.get(), lastSuccessfulInstant);
    Assertions.assertTrue(candidateInstants.count() == 0);
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime);
    // consider commits before this are all successful
    // writer 1
    createInflightCommit(HoodieTestTable.makeNewCommitTime());
    // writer 2
    createInflightCommit(HoodieTestTable.makeNewCommitTime());
    Option<HoodieInstant> lastSuccessfulInstant = metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant();
    newInstantTime = HoodieTestTable.makeNewCommitTime();
    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, newInstantTime));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient.getActiveTimeline(), currentInstant.get(), lastSuccessfulInstant);
    Assertions.assertTrue(candidateInstants.count() == 0);
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccesssfulCommit() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant);
    // writer 2 starts and finishes
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCommit(newInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    timeline = timeline.reload();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(timeline, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with writer 2
    Assertions.assertTrue(candidateInstants.size() == 1);
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, writer 1 and writer 2 should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  @Test
  public void testConcurrentWritesWithInterleavingScheduledCompaction() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant);
    // compaction 1 gets scheduled
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCompactionRequested(newInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    timeline = timeline.reload();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(timeline, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with scheduled compaction plan 1
    Assertions.assertTrue(candidateInstants.size() == 1);
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulCompaction() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant);
    // compaction 1 gets scheduled and finishes
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCompaction(newInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    timeline = timeline.reload();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(timeline, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with compaction 1
    Assertions.assertTrue(candidateInstants.size() == 1);
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  @Test
  public void testConcurrentWriteAndCompactionScheduledEarlier() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    // compaction 1 gets scheduled
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCompaction(newInstantTime);
    // consider commits before this are all successful
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    timeline = timeline.reload();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(timeline, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 should not conflict with an earlier scheduled compaction 1 with the same file ids
    Assertions.assertTrue(candidateInstants.size() == 0);
  }

  @Test
  public void testConcurrentWritesWithInterleavingScheduledCluster() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant);
    // clustering 1 gets scheduled
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createReplaceRequested(newInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    timeline = timeline.reload();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(timeline, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with scheduled compaction plan 1
    Assertions.assertTrue(candidateInstants.size() == 1);
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulCluster() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant);
    // cluster 1 gets scheduled and finishes
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createReplace(newInstantTime, WriteOperationType.CLUSTER);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    timeline = timeline.reload();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(timeline, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with cluster 1
    Assertions.assertTrue(candidateInstants.size() == 1);
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulReplace() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant);
    // replace 1 gets scheduled and finished
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createReplace(newInstantTime, WriteOperationType.INSERT_OVERWRITE);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    timeline = timeline.reload();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(timeline, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with replace 1
    Assertions.assertTrue(candidateInstants.size() == 1);
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  private void createCommit(String instantTime) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private HoodieCommitMetadata createCommitMetadata(String instantTime) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    return commitMetadata;
  }

  private void createInflightCommit(String instantTime) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";
    HoodieTestTable.of(metaClient)
        .addInflightCommit(instantTime)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createCompactionRequested(String instantTime) throws Exception {
    String fileId1 = "file-1";
    HoodieCompactionPlan compactionPlan = new HoodieCompactionPlan();
    compactionPlan.setVersion(TimelineLayoutVersion.CURR_VERSION);
    HoodieCompactionOperation operation = new HoodieCompactionOperation();
    operation.setFileId(fileId1);
    operation.setPartitionPath(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    operation.setDataFilePath("/file-1");
    operation.setDeltaFilePaths(Arrays.asList("/file-1"));
    compactionPlan.setOperations(Arrays.asList(operation));
    HoodieTestTable.of(metaClient)
        .addRequestedCompaction(instantTime, compactionPlan);
  }

  private void createCompaction(String instantTime) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.COMPACT);
    commitMetadata.setCompacted(true);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createReplaceRequested(String instantTime) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    // create replace instant to mark fileId1 as deleted
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(WriteOperationType.CLUSTER.name());
    HoodieClusteringPlan clusteringPlan = new HoodieClusteringPlan();
    HoodieClusteringGroup clusteringGroup = new HoodieClusteringGroup();
    HoodieSliceInfo sliceInfo = new HoodieSliceInfo();
    sliceInfo.setFileId(fileId1);
    sliceInfo.setPartitionPath(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    clusteringGroup.setSlices(Arrays.asList(sliceInfo));
    clusteringPlan.setInputGroups(Arrays.asList(clusteringGroup));
    requestedReplaceMetadata.setClusteringPlan(clusteringPlan);
    requestedReplaceMetadata.setVersion(TimelineLayoutVersion.CURR_VERSION);
    HoodieTestTable.of(metaClient)
        .addRequestedReplace(instantTime, Option.of(requestedReplaceMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createReplace(String instantTime, WriteOperationType writeOperationType) throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    // create replace instant to mark fileId1 as deleted
    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    Map<String, List<String>> partitionFileIds = new HashMap<>();
    partitionFileIds.put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, Arrays.asList(fileId2));
    replaceMetadata.setPartitionToReplaceFileIds(partitionFileIds);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    replaceMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    replaceMetadata.setOperationType(writeOperationType);
    // create replace instant to mark fileId1 as deleted
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(WriteOperationType.CLUSTER.name());
    HoodieClusteringPlan clusteringPlan = new HoodieClusteringPlan();
    HoodieClusteringGroup clusteringGroup = new HoodieClusteringGroup();
    HoodieSliceInfo sliceInfo = new HoodieSliceInfo();
    sliceInfo.setFileId(fileId1);
    sliceInfo.setPartitionPath(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    clusteringGroup.setSlices(Arrays.asList(sliceInfo));
    clusteringPlan.setInputGroups(Arrays.asList(clusteringGroup));
    requestedReplaceMetadata.setClusteringPlan(clusteringPlan);
    requestedReplaceMetadata.setVersion(TimelineLayoutVersion.CURR_VERSION);
    HoodieTestTable.of(metaClient)
        .addReplaceCommit(instantTime, Option.of(requestedReplaceMetadata), Option.empty(), replaceMetadata)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

}
