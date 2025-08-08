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

import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieWriteConflictException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.buildRequestedReplaceMetadata;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCluster;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createClusterInflight;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createClusterRequested;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommitMetadata;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompaction;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompactionRequested;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompleteCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompleteCompaction;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createInflightCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createPendingCluster;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createPendingCompaction;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createPendingInsertOverwrite;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createReplace;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createRequestedCommit;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;

public class TestSimpleConcurrentFileWritesConflictResolutionStrategy extends HoodieCommonTestHarness {

  @Test
  public void testNoConcurrentWrites() throws Exception {
    initMetaClient();
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime, metaClient);
    // consider commits before this are all successful

    Option<HoodieInstant> lastSuccessfulInstant = metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant();
    newInstantTime = HoodieTestTable.makeNewCommitTime();
    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, newInstantTime));

    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant);
    Assertions.assertTrue(candidateInstants.count() == 0);
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    initMetaClient();
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime, metaClient);
    // consider commits before this are all successful
    // writer 1
    createInflightCommit(HoodieTestTable.makeNewCommitTime(), metaClient);
    // writer 2
    createInflightCommit(HoodieTestTable.makeNewCommitTime(), metaClient);
    Option<HoodieInstant> lastSuccessfulInstant = metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant();
    newInstantTime = HoodieTestTable.makeNewCommitTime();
    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, newInstantTime));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant);
    Assertions.assertTrue(candidateInstants.count() == 0);
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulCommit() throws Exception {
    initMetaClient();
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // writer 2 starts and finishes
    String newInstantTime = metaClient.createNewInstantTime();
    createCommit(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with writer 2
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
  }

  @Test
  public void testConcurrentWritesWithReplaceInflightCommit() throws Exception {
    initMetaClient();
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));

    String replaceInstant = metaClient.createNewInstantTime();
    HoodieTestTable.of(metaClient).addRequestedReplace(replaceInstant, Option.empty());
    TestConflictResolutionStrategyUtil.createReplaceInflight(replaceInstant, metaClient);
    Option<HoodieInstant> lastSuccessfulInstant = Option.empty();

    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();

    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());

    // writer 1 conflicts with writer 2
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
  }

  @Test
  public void testConcurrentWritesWithClusteringInflightCommit() throws Exception {
    initMetaClient();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));

    String clusteringInstantTime = metaClient.createNewInstantTime();
    createClusterRequested(clusteringInstantTime, metaClient);
    Option<HoodieInstant> lastSuccessfulInstant = Option.empty();
    createClusterInflight(clusteringInstantTime, metaClient);

    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();

    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());

    // writer 1 conflicts with writer 2
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
  }

  @Test
  public void testConcurrentWritesWithLegacyClusteringInflightCommit() throws Exception {
    initMetaClient();
    String clusteringInstantTime = metaClient.createNewInstantTime();
    // create a replace commit with a clustering operation to mimic a commit written by a v6 writer
    HoodieTestTable.of(metaClient).addRequestedReplace(clusteringInstantTime, Option.of(buildRequestedReplaceMetadata("file-1", WriteOperationType.CLUSTER)));
    Option<HoodieInstant> lastSuccessfulInstant = Option.empty();
    HoodieTestTable.of(metaClient).addInflightReplace(clusteringInstantTime, Option.empty());

    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));

    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();

    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());

    // writer 1 conflicts with writer 2
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testConcurrentWritesWithInterleavingScheduledCompaction(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8, HoodieTableType.MERGE_ON_READ);
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled
    String newInstantTime = metaClient.createNewInstantTime();
    createCompactionRequested(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    if (preTableVersion8) {
      // writer 1 conflicts with scheduled compaction plan 1
      Assertions.assertEquals(1, candidateInstants.size());
      ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
      ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
      Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
      Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
    } else {
      // writer will not have conflicts with compaction since ordering is now based on completion time to avoid these conflicts
      Assertions.assertTrue(candidateInstants.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testConcurrentWritesWithInterleavingSuccessfulCompaction(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8, HoodieTableType.MERGE_ON_READ);
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled and finishes
    String newInstantTime = metaClient.createNewInstantTime();
    createCompaction(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    if (preTableVersion8) {
      // writer 1 conflicts with compaction 1
      Assertions.assertEquals(1, candidateInstants.size());
      ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
      ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
      Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
      Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
    } else {
      // writer will not have conflicts with compaction since ordering is now based on completion time to avoid these conflicts
      Assertions.assertTrue(candidateInstants.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testConcurrentWritesWithInterleavingInflightCompaction(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8, HoodieTableType.MERGE_ON_READ);
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // Consider commits before this are all successful.
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // Writer 1 starts.
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // Compaction 1 gets scheduled and becomes inflight.
    String newInstantTime = metaClient.createNewInstantTime();
    createPendingCompaction(newInstantTime, metaClient);

    // Writer 1 tries to commit.
    Option<HoodieInstant> currentInstant = Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, currentWriterInstant));
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();

    // Do conflict resolution.
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy =
        new SimpleConcurrentFileWritesConflictResolutionStrategy();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
        metaClient, currentInstant.get(), lastSuccessfulInstant).collect(Collectors.toList());

    if (preTableVersion8) {
      Assertions.assertEquals(1, candidateInstants.size());
      ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
      ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
      Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
      Assertions.assertThrows(
          HoodieWriteConflictException.class,
          () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
    } else {
      // Writer will not have conflicts with compaction since ordering is now based on completion time to avoid these conflicts.
      Assertions.assertTrue(candidateInstants.isEmpty());
    }
  }

  /**
   * This method is verifying if a conflict exists for already commit compaction commit with current running ingestion commit.
   */
  @Test
  public void testConcurrentWriteAndCompactionScheduledEarlier() throws Exception {
    initMetaClient(false, HoodieTableType.MERGE_ON_READ);
    createCommit(metaClient.createNewInstantTime(), metaClient);
    // compaction 1 gets scheduled
    String newInstantTime = metaClient.createNewInstantTime();
    createCompaction(newInstantTime, metaClient);
    // consider commits before this are all successful
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 should not conflict with an earlier scheduled compaction 1 with the same file ids
    Assertions.assertTrue(candidateInstants.size() == 0);
  }

  @Test
  public void testConcurrentWritesWithInterleavingScheduledCluster() throws Exception {
    initMetaClient(false, HoodieTableType.MERGE_ON_READ);
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // clustering 1 gets scheduled
    String newInstantTime = metaClient.createNewInstantTime();
    createClusterRequested(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with scheduled compaction plan 1
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulCluster() throws Exception {
    initMetaClient();
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // cluster 1 gets scheduled and finishes
    String newInstantTime = metaClient.createNewInstantTime();
    createCluster(newInstantTime, WriteOperationType.CLUSTER, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with cluster 1
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulReplace() throws Exception {
    initMetaClient();
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // replace 1 gets scheduled and finished
    String newInstantTime = metaClient.createNewInstantTime();
    createReplace(newInstantTime, WriteOperationType.INSERT_OVERWRITE, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with replace 1
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
  }

  @Test
  public void testConcurrentWritesWithPendingInsertOverwriteReplace() throws Exception {
    initMetaClient();
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // insert_overwrite 1 gets scheduled and inflighted
    String newInstantTime = metaClient.createNewInstantTime();
    createPendingInsertOverwrite(newInstantTime, WriteOperationType.INSERT_OVERWRITE, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 will not conflicts with insert_overwrite 1
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertFalse(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
  }

  // try to simulate HUDI-3355
  @Test
  public void testConcurrentWritesWithPendingInstants() throws Exception {
    initMetaClient(false, HoodieTableType.MERGE_ON_READ);
    // step1: create a pending replace/commit/compact instant: C1,C11,C12
    String newInstantTimeC1 = metaClient.createNewInstantTime();
    createPendingCluster(newInstantTimeC1, WriteOperationType.CLUSTER, metaClient);

    String newCompactionInstantTimeC11 = metaClient.createNewInstantTime();
    createPendingCompaction(newCompactionInstantTimeC11, metaClient);

    String newCommitInstantTimeC12 = metaClient.createNewInstantTime();
    createInflightCommit(newCommitInstantTimeC12, metaClient);
    // step2: create a complete commit which has no conflict with C1,C11,C12, named it as C2
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // step3: write 1 starts, which has conflict with C1,C11,C12, named it as C3
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // step4: create a requested commit, which has conflict with C3, named it as C4
    String commitC4 = metaClient.createNewInstantTime();
    createRequestedCommit(commitC4, metaClient);
    // get PendingCommit during write 1 operation
    metaClient.reloadActiveTimeline();
    Set<String> pendingInstant = TransactionUtils.getInflightAndRequestedInstants(metaClient);
    pendingInstant.remove(currentWriterInstant);
    // step5: finished pending cluster/compaction/commit operation
    createCluster(newInstantTimeC1, WriteOperationType.CLUSTER, metaClient);
    createCompleteCompaction(newCompactionInstantTimeC11, metaClient);
    createCompleteCommit(newCommitInstantTimeC12, metaClient);
    createCompleteCommit(commitC4, metaClient);

    // step6: do check
    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new SimpleConcurrentFileWritesConflictResolutionStrategy();
    // make sure c3 has conflict with C1,C11,C12,C4;
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant, "file-2");
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> completedInstantsDuringCurrentWriteOperation = TransactionUtils
            .getCompletedInstantsDuringCurrentWriteOperation(metaClient, pendingInstant).collect(Collectors.toList());
    // C1,C11,C12,C4 should be included
    Assertions.assertEquals(4, completedInstantsDuringCurrentWriteOperation.size());

    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    // check C3 has conflict with C1,C11,C12,C4
    for (HoodieInstant instant : completedInstantsDuringCurrentWriteOperation) {
      ConcurrentOperation thatCommitOperation = new ConcurrentOperation(instant, metaClient);
      Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
      if (instant.requestedTime().equals(newCompactionInstantTimeC11)) {
        Assertions.assertDoesNotThrow(() -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
      } else {
        Assertions.assertThrows(HoodieWriteConflictException.class, () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
      }
    }
  }
}
