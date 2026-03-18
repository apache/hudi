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

import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
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
    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled
    String newInstantTime = WriteClientTestUtils.createNewInstantTime();
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
    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled and finishes
    String newInstantTime = WriteClientTestUtils.createNewInstantTime();
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
    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled and finishes
    String newInstantTime = WriteClientTestUtils.createNewInstantTime();
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
    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    // consider commits before this are all successful
    // compaction 1 gets scheduled
    String newInstantTime = WriteClientTestUtils.createNewInstantTime();
    createCompaction(newInstantTime, metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
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
    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // clustering 1 gets scheduled
    String newInstantTime = WriteClientTestUtils.createNewInstantTime();
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
    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // clustering writer starts and complete before ingestion commit.
    String replaceWriterInstant = WriteClientTestUtils.createNewInstantTime();
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
    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // replace 1 gets scheduled and finished
    String newInstantTime = WriteClientTestUtils.createNewInstantTime();
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

  /**
   * Confirms that when {@code hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution}
   * is enabled, clustering will detect a conflict with an ingestion .requested instant
   * that has an active heartbeat, via hasConflict/resolveConflict.
   */
  @Test
  public void testClusterConflictingWithIngestionRequestedInstantWithActiveHeartbeat() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withClusteringBlockForPendingIngestion(true)
        .withHeartbeatIntervalInMs(60 * 1000)
        .withHeartbeatTolerableMisses(2)
        .build();

    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // clustering gets scheduled and goes inflight
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createClusterRequested(currentWriterInstant, metaClient);
    createClusterInflight(currentWriterInstant, metaClient);

    // ingestion writer creates a .requested instant with active heartbeat
    String activeIngestionInstantTime = WriteClientTestUtils.createNewInstantTime();
    HoodieTestTable.of(metaClient).addRequestedCommit(activeIngestionInstantTime);
    HoodieHeartbeatClient heartbeatClient = new HoodieHeartbeatClient(
        metaClient.getStorage(), metaClient.getBasePath().toString(),
        (long) (1000 * 60), 5);
    heartbeatClient.start(activeIngestionInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();

    try {
      List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
          metaClient, currentInstant.get(), lastSuccessfulInstant, Option.of(writeConfig))
          .collect(Collectors.toList());
      // The .requested instant with active heartbeat should be returned as a candidate
      Assertions.assertEquals(1, candidateInstants.size());
      Assertions.assertEquals(activeIngestionInstantTime, candidateInstants.get(0).requestedTime());

      HoodieReplaceCommitMetadata clusteringMetadata = new HoodieReplaceCommitMetadata();
      clusteringMetadata.setOperationType(WriteOperationType.CLUSTER);
      ConcurrentOperation thisOperation = new ConcurrentOperation(currentInstant.get(), clusteringMetadata);
      ConcurrentOperation otherOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);

      // hasConflict should detect the conflict
      Assertions.assertTrue(strategy.hasConflict(thisOperation, otherOperation));

      // resolveConflict should throw with TABLE_SERVICE_VS_INGESTION category
      HoodieWriteConflictException thrown = Assertions.assertThrows(
          HoodieWriteConflictException.class,
          () -> strategy.resolveConflict(null, thisOperation, otherOperation));
      Assertions.assertTrue(thrown.getCategory().isPresent());
      Assertions.assertEquals(HoodieWriteConflictException.ConflictCategory.TABLE_SERVICE_VS_INGESTION,
          thrown.getCategory().get());
    } finally {
      heartbeatClient.stop(activeIngestionInstantTime);
      heartbeatClient.close();
    }
  }

  /**
   * Confirms that clustering does NOT fail for pending ingestion .requested instants
   * when {@code hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution}
   * is disabled (default behavior).
   */
  @Test
  public void testClusterDoesNotBlockWithoutConfigEnabled() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withClusteringBlockForPendingIngestion(false)
        .build();

    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // clustering gets scheduled and goes inflight
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createClusterRequested(currentWriterInstant, metaClient);
    createClusterInflight(currentWriterInstant, metaClient);

    // ingestion writer creates a .requested instant with active heartbeat
    String ingestionInstantTime = WriteClientTestUtils.createNewInstantTime();
    HoodieTestTable.of(metaClient).addRequestedCommit(ingestionInstantTime);
    HoodieHeartbeatClient heartbeatClient = new HoodieHeartbeatClient(
        metaClient.getStorage(), metaClient.getBasePath().toString(),
        (long) (1000 * 60), 5);
    heartbeatClient.start(ingestionInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();

    // With hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution disabled,
    // clustering should NOT fail even though there's an active heartbeat
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
        metaClient, currentInstant.get(), lastSuccessfulInstant, Option.of(writeConfig))
        .collect(Collectors.toList());
    Assertions.assertEquals(0, candidateInstants.size());

    heartbeatClient.stop(ingestionInstantTime);
    heartbeatClient.close();
  }

  /**
   * Confirms that when getCandidateInstants is called without a write config,
   * it delegates properly and uses defaults
   * ({@code hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution}
   * disabled by default).
   */
  @Test
  public void testClusterOldMethodDoesNotBlockByDefault() throws Exception {
    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // clustering gets scheduled and goes inflight
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createClusterRequested(currentWriterInstant, metaClient);
    createClusterInflight(currentWriterInstant, metaClient);

    // ingestion writer creates a .requested instant with active heartbeat
    String ingestionInstantTime = WriteClientTestUtils.createNewInstantTime();
    HoodieTestTable.of(metaClient).addRequestedCommit(ingestionInstantTime);
    HoodieHeartbeatClient heartbeatClient = new HoodieHeartbeatClient(
        metaClient.getStorage(), metaClient.getBasePath().toString(),
        (long) (1000 * 60), 5);
    heartbeatClient.start(ingestionInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();

    // Without write config, should NOT throw since the default is
    // hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution = false
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
        metaClient, currentInstant.get(), lastSuccessfulInstant)
        .collect(Collectors.toList());
    Assertions.assertEquals(0, candidateInstants.size());

    heartbeatClient.stop(ingestionInstantTime);
    heartbeatClient.close();
  }

  /**
   * Confirms that when {@code hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution}
   * is enabled and there is an inflight ingestion instant, it is returned as a candidate
   * (exercises the i.isInflight() return path in the filter).
   */
  @Test
  public void testClusterWithBlockingEnabledAndInflightIngestion() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withClusteringBlockForPendingIngestion(true)
        .withHeartbeatIntervalInMs(60 * 1000)
        .withHeartbeatTolerableMisses(2)
        .build();

    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // clustering gets scheduled and goes inflight
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createClusterRequested(currentWriterInstant, metaClient);
    createClusterInflight(currentWriterInstant, metaClient);

    // ingestion writer creates an inflight commit
    String ingestionInstantTime = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(ingestionInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();

    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
        metaClient, currentInstant.get(), lastSuccessfulInstant, Option.of(writeConfig))
        .collect(Collectors.toList());
    // The inflight ingestion instant should be returned as a candidate
    Assertions.assertEquals(1, candidateInstants.size());
    Assertions.assertEquals(ingestionInstantTime, candidateInstants.get(0).requestedTime());
  }

  /**
   * Confirms that when {@code hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution}
   * is enabled and there is both an inflight and an expired-heartbeat requested ingestion instant,
   * only the inflight is returned as a candidate.
   */
  @Test
  public void testClusterWithBlockingEnabledInflightAndExpiredRequested() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withClusteringBlockForPendingIngestion(true)
        .withHeartbeatIntervalInMs(60 * 1000)
        .withHeartbeatTolerableMisses(2)
        .build();

    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // clustering gets scheduled and goes inflight
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createClusterRequested(currentWriterInstant, metaClient);
    createClusterInflight(currentWriterInstant, metaClient);

    // expired requested ingestion instant (no heartbeat)
    String expiredInstantTime = WriteClientTestUtils.createNewInstantTime();
    HoodieTestTable.of(metaClient).addRequestedCommit(expiredInstantTime);

    // active inflight ingestion instant
    String inflightInstantTime = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(inflightInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();

    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
        metaClient, currentInstant.get(), lastSuccessfulInstant, Option.of(writeConfig))
        .collect(Collectors.toList());
    // Only the inflight should be returned; expired requested should be filtered out
    Assertions.assertEquals(1, candidateInstants.size());
    Assertions.assertEquals(inflightInstantTime, candidateInstants.get(0).requestedTime());
  }

  /**
   * Confirms that when the .requested instant has an expired heartbeat (no heartbeat file),
   * clustering does NOT treat it as a conflict even when
   * {@code hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution} is enabled.
   */
  @Test
  public void testClusterWithBlockingEnabledAndExpiredHeartbeatRequested() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withClusteringBlockForPendingIngestion(true)
        .withHeartbeatIntervalInMs(60 * 1000)
        .withHeartbeatTolerableMisses(2)
        .build();

    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // clustering gets scheduled and goes inflight
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createClusterRequested(currentWriterInstant, metaClient);
    createClusterInflight(currentWriterInstant, metaClient);

    // ingestion writer creates a .requested instant but never starts a heartbeat (simulates expired/dead writer)
    String expiredIngestionInstantTime = WriteClientTestUtils.createNewInstantTime();
    HoodieTestTable.of(metaClient).addRequestedCommit(expiredIngestionInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, currentWriterInstant));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();

    // The .requested instant with expired heartbeat should be filtered out
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
        metaClient, currentInstant.get(), lastSuccessfulInstant, Option.of(writeConfig))
        .collect(Collectors.toList());
    Assertions.assertEquals(0, candidateInstants.size());
  }

  /**
   * Confirms that compaction (non-clustering table service) when write config is provided
   * still picks up inflight ingestion instants as candidates.
   */
  @Test
  public void testCompactionWithInflightIngestionViaNewOverload() throws Exception {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withClusteringBlockForPendingIngestion(true)
        .build();

    createCommit(WriteClientTestUtils.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // writer 1 starts (inflight ingestion)
    String currentWriterInstant = WriteClientTestUtils.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // compaction gets scheduled
    String compactionInstantTime = WriteClientTestUtils.createNewInstantTime();
    createCompactionRequested(compactionInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime));
    PreferWriterConflictResolutionStrategy strategy = new PreferWriterConflictResolutionStrategy();

    // Compaction is not clustering, so .requested instants are not included even when
    // hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution is enabled
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
        metaClient, currentInstant.get(), lastSuccessfulInstant, Option.of(writeConfig))
        .collect(Collectors.toList());
    Assertions.assertEquals(1, candidateInstants.size());
    Assertions.assertEquals(currentWriterInstant, candidateInstants.get(0).requestedTime());
  }

}
