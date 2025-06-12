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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieWriteConflictException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCluster;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommitMetadata;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompaction;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompactionRequested;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createInflightCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createReplace;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createClusterInflight;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createClusterRequested;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;

public class TestPreferWriterConflictResolutionStrategy extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testConcurrentWritesWithInterleavingScheduledCompaction() throws Exception {
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

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 does not have a conflict with scheduled compaction plan 1
    // Since, scheduled compaction plan is given lower priority compared ingestion commit.
    Assertions.assertEquals(0, candidateInstants.size());
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulCompaction() throws Exception {
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

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with compaction 1
    Assertions.assertEquals(1, candidateInstants.size());
    Assertions.assertEquals(newInstantTime, candidateInstants.get(0).requestedTime());
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
  public void testConcurrentWritesWithInterleavingCompaction() throws Exception {
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled and finishes
    String newInstantTime = metaClient.createNewInstantTime();
    createCompactionRequested(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, newInstantTime));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();
    // TODO Create method to create compactCommitMetadata
    //    HoodieCommitMetadata currentMetadata = createCommitMetadata(newInstantTime);
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with compaction 1
    Assertions.assertEquals(1, candidateInstants.size());
    Assertions.assertEquals(currentWriterInstant, candidateInstants.get(0).requestedTime());
    // TODO: Once compactCommitMetadata is created use that to verify resolveConflict method.
  }

  /**
   * This method is verifying if a conflict exists for already commit compaction commit with current running ingestion commit.
   */
  @Test
  public void testConcurrentWriteAndCompactionScheduledEarlier() throws Exception {
    createCommit(metaClient.createNewInstantTime(), metaClient);
    // consider commits before this are all successful
    // compaction 1 gets scheduled
    String newInstantTime = metaClient.createNewInstantTime();
    createCompaction(newInstantTime, metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 should not conflict with an earlier scheduled compaction 1 with the same file ids
    Assertions.assertEquals(0, candidateInstants.size());
  }

  /**
   * This method confirms that ingestion commit when completing only looks at the completed commits.
   */
  @Test
  public void testConcurrentWritesWithInterleavingScheduledCluster() throws Exception {
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
    createClusterInflight(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // Since we give preference to ingestion over clustering, there won't be a conflict with replacecommit.
    Assertions.assertEquals(0, candidateInstants.size());
  }

  /**
   * This method confirms ingestion commit failing due to already present replacecommit.
   * Here the replacecommit is allowed to commit. Ideally replacecommit cannot be committed when there is an ingestion inflight.
   * The following case can occur, during transition phase of ingestion commit from Requested to Inflight,
   * during that time replacecommit can be completed.
   */
  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulCluster() throws Exception {
    createCommit(metaClient.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = metaClient.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // clustering writer starts and complete before ingestion commit.
    String replaceWriterInstant = metaClient.createNewInstantTime();
    createCluster(replaceWriterInstant, WriteOperationType.CLUSTER, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy
        .getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant)
        .collect(Collectors.toList());
    Assertions.assertEquals(1, candidateInstants.size());
    Assertions.assertEquals(replaceWriterInstant, candidateInstants.get(0).requestedTime());
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
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

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with replace 1
    Assertions.assertEquals(1, candidateInstants.size());
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
}
