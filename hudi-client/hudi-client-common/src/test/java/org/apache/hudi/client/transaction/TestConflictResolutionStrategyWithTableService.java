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
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommitMetadata;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompaction;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompactionRequested;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCompleteCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createInflightCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createReplace;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createReplaceInflight;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createReplaceRequested;

public class TestConflictResolutionStrategyWithTableService extends HoodieCommonTestHarness {
  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanMetaClient();
  }

  private static final String SPARK_ALLOW_UPDATE_STRATEGY = "org.apache.hudi.client.clustering.update.strategy.SparkAllowUpdateStrategy";
  private static final String SPARK_REJECT_UPDATE_STRATEGY = "org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy";

  private static Stream<Arguments> additionalProps() {
    return Stream.of(
        Arguments.of(createProperties(SimpleConcurrentFileWritesConflictResolutionStrategy.class.getName(), SPARK_ALLOW_UPDATE_STRATEGY)),
        Arguments.of(createProperties(SimpleConcurrentFileWritesConflictResolutionStrategy.class.getName(), SPARK_REJECT_UPDATE_STRATEGY)),
        Arguments.of(createProperties(StateTransitionTimeBasedConflictResolutionStrategy.class.getName(), SPARK_ALLOW_UPDATE_STRATEGY)),
        Arguments.of(createProperties(StateTransitionTimeBasedConflictResolutionStrategy.class.getName(), SPARK_REJECT_UPDATE_STRATEGY))
    );
  }

  public static Properties createProperties(String conflictResolutionStrategyClassName, String updatesStrategy) {
    Properties properties = new Properties();
    properties.setProperty(HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key(), conflictResolutionStrategyClassName);
    properties.setProperty(HoodieClusteringConfig.UPDATES_STRATEGY.key(), updatesStrategy);
    return properties;
  }

  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWritesWithInterleavingScheduledCompaction(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCompactionRequested(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty()).collect(
        Collectors.toList());
    // Because scheduled compaction plan has the highest priority, writer 1 have
    // a conflict with scheduled compaction plan 1
    Assertions.assertEquals(1, candidateInstants.size());
  }

  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWritesWithInterleavingSuccessfulCompaction(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled and finishes
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    // TODO: Remove sleep stmt once the modified times issue is fixed.
    // Sleep thread for at least 1sec for consecutive commits that way they do not have two commits modified times falls on the same millisecond.
    Thread.sleep(1000);
    createCompaction(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty()).collect(
        Collectors.toList());
    // writer 1 conflict with compaction 1
    Assertions.assertEquals(1, candidateInstants.size());
  }

  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWritesWithInterleavingCompaction(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // compaction 1 gets scheduled
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCompactionRequested(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, newInstantTime));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    // TODO Create method to create compactCommitMetadata
    //    HoodieCommitMetadata currentMetadata = createCommitMetadata(newInstantTime);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty()).collect(
        Collectors.toList());
    // writer 1 not conflicts with compaction 1
    Assertions.assertEquals(0, candidateInstants.size());
  }

  /**
   * This method is verifying if a conflict exists for already commit compaction commit with current running ingestion commit.
   */
  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWriteAndCompactionScheduledEarlier(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    // compaction 1 gets scheduled
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCompactionRequested(newInstantTime, metaClient);
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty()).collect(
        Collectors.toList());
    // writer 1 should not conflict with an earlier scheduled compaction 1 with the same file ids
    Assertions.assertEquals(0, candidateInstants.size());
  }

  /**
   * This method confirms that ingestion commit when completing only looks at the completed commits.
   */
  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWritesWithInterleavingScheduledCluster(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // clustering 1 gets scheduled
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createReplaceRequested(newInstantTime, metaClient);
    createReplaceInflight(newInstantTime, WriteOperationType.CLUSTER, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty()).collect(
        Collectors.toList());
    if (props.get(HoodieClusteringConfig.UPDATES_STRATEGY.key()).equals(SPARK_ALLOW_UPDATE_STRATEGY)) {
      Assertions.assertEquals(0, candidateInstants.size());
    } else {
      Assertions.assertEquals(1, candidateInstants.size());
    }
  }

  /**
   * This method confirms ingestion commit failing due to already present replacecommit.
   * Here the replacecommit is allowed to commit. Ideally replacecommit cannot be committed when there is a ingestion inflight.
   * The following case can occur, during transition phase of ingestion commit from Requested to Inflight,
   * during that time replacecommit can be completed.
   */
  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWritesWithInterleavingSuccessfulCluster(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // TODO: Remove sleep stmt once the modified times issue is fixed.
    // Sleep thread for at least 1sec for consecutive commits that way they do not have two commits modified times falls on the same millisecond.
    Thread.sleep(1000);
    // clustering writer starts and complete before ingestion commit.
    String replaceWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createReplace(replaceWriterInstant, WriteOperationType.CLUSTER, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy
        .getCandidateInstants(metaClient, currentInstant.get(), Option.empty())
        .collect(Collectors.toList());
    Assertions.assertEquals(1, candidateInstants.size());
    Assertions.assertEquals(replaceWriterInstant, candidateInstants.get(0).getTimestamp());
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWritesWithInterleavingSuccessfulReplace(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // TODO: Remove sleep stmt once the modified times issue is fixed.
    // Sleep thread for at least 1sec for consecutive commits that way they do not have two commits modified times falls on the same millisecond.
    Thread.sleep(1000);
    // replace 1 gets scheduled and finished
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createReplace(newInstantTime, WriteOperationType.INSERT_OVERWRITE, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);

    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty()).collect(
        Collectors.toList());
    // writer 1 conflicts with replace 1
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  /**
   * Test get candidate instants, all instants are ingestion
   */
  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testGetCandidateInstantsWithIngestion(Properties props) throws Exception {
    // The whole process requires two reload active timeline, before write, pre commit
    //
    //                      start            end
    // current               <---------------->
    // instant1  <------>
    // instant2    <------------> pick
    // instant3       <--------------------------------->
    // instant4                   <---------> pick
    // instant5                      <---------------------->

    String instant1 = HoodieTestTable.makeNewCommitTime();
    createCommit(instant1, metaClient);

    String instant2 = HoodieTestTable.makeNewCommitTime();
    createInflightCommit(instant2, metaClient);

    String instant3 = HoodieTestTable.makeNewCommitTime();
    createInflightCommit(instant3, metaClient);

    String current = HoodieTestTable.makeNewCommitTime();
    // before write: reload active timeline, collect pending instants
    metaClient.reloadActiveTimeline();
    Set<String> pendingInstants = TransactionUtils.getInflightAndRequestedInstantsWithoutCurrent(metaClient, current);
    createInflightCommit(current, metaClient);

    createCompleteCommit(instant2, metaClient);

    String instant4 = HoodieTestTable.makeNewCommitTime();
    createInflightCommit(instant4, metaClient);

    String instant5 = HoodieTestTable.makeNewCommitTime();
    createInflightCommit(instant5, metaClient);

    createCompleteCommit(instant4, metaClient);

    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);

    // pre commit: reload active timeline, getCandidateInstants
    metaClient.reloadActiveTimeline();
    HoodieInstant currentInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, current);
    List<String> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant, Option.of(pendingInstants))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());

    // Since the above instants are all ingestion, current's priority is the highest
    // 2 and 4 (mandatory) are picked, 3 and 5 (optional) are excluded
    Assertions.assertEquals(2, candidateInstants.size());
    Assertions.assertTrue(candidateInstants.contains(instant2));
    Assertions.assertTrue(candidateInstants.contains(instant4));
  }

  /**
   * Test get candidate instants, current is ingestion, pending is clustering
   */
  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testGetCandidateInstantsWithIngestionToClustering(Properties props) throws Exception {
    //                                                                 start            end
    // current  (ingestion)                                             <---------------->
    // instant1 (clustering, scheduled before current start)     <------------------------------->
    // instant2 (clustering, scheduled after current start)                 <----------------------> pick if not with SparkAllowUpdateStrategy

    String instant1 = HoodieTestTable.makeNewCommitTime();
    createReplaceRequested(instant1, metaClient);

    String current = HoodieTestTable.makeNewCommitTime();
    // before write: reload active timeline, collect pending instants
    metaClient.reloadActiveTimeline();
    Set<String> pendingInstants = TransactionUtils.getInflightAndRequestedInstantsWithoutCurrent(metaClient, current);
    createInflightCommit(current, metaClient);

    String instant2 = HoodieTestTable.makeNewCommitTime();
    createReplaceRequested(instant2, metaClient);

    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);

    // pre commit: reload active timeline, getCandidateInstants
    metaClient.reloadActiveTimeline();
    HoodieInstant currentInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, current);
    List<String> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant, Option.of(pendingInstants))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());

    if (props.get(HoodieClusteringConfig.UPDATES_STRATEGY.key()).equals(SPARK_ALLOW_UPDATE_STRATEGY)) {
      Assertions.assertEquals(0, candidateInstants.size());
    } else {
      Assertions.assertEquals(1, candidateInstants.size());
      Assertions.assertTrue(candidateInstants.contains(instant2));
    }
  }

  /**
   * Test get candidate instants, current is clustering, pending is ingestion
   */
  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testGetCandidateInstantsWithClusteringToIngestion(Properties props) throws Exception {
    //                                                                 scheduled          end
    // current  (clustering)                                             <---------------->
    // instant1 (ingestion, start before clustering scheduled)     <------------------------------>   pick
    // instant2 (ingestion, start after clustering scheduled)               <-----------------------> pick

    String instant1 = HoodieTestTable.makeNewCommitTime();
    createInflightCommit(instant1, metaClient);

    String current = HoodieTestTable.makeNewCommitTime();
    // before write: reload active timeline, collect pending instants
    metaClient.reloadActiveTimeline();
    Set<String> pendingInstants = TransactionUtils.getInflightAndRequestedInstantsWithoutCurrent(metaClient, current);
    createReplaceRequested(current, metaClient);

    String instant2 = HoodieTestTable.makeNewCommitTime();
    createInflightCommit(instant2, metaClient);

    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);

    // pre commit: reload active timeline, getCandidateInstants
    metaClient.reloadActiveTimeline();
    HoodieInstant currentInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, current);
    List<String> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant, Option.of(pendingInstants))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());

    if (props.get(HoodieClusteringConfig.UPDATES_STRATEGY.key()).equals(SPARK_ALLOW_UPDATE_STRATEGY)) {
      Assertions.assertEquals(2, candidateInstants.size());
      Assertions.assertTrue(candidateInstants.contains(instant1));
      Assertions.assertTrue(candidateInstants.contains(instant2));
    } else {
      Assertions.assertEquals(0, candidateInstants.size());
    }
  }

  /**
   * Test get candidate instants, current is ingestion, pending is compaction
   */
  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testGetCandidateInstantsWithIngestionToCompaction(Properties props) throws Exception {
    //                                                                 start            end
    // current  (ingestion)                                             <---------------->
    // instant1 (compaction, scheduled before current start)     <------------------------------->
    // instant2 (compaction, scheduled after current start)                 <----------------------> pick

    String instant1 = HoodieTestTable.makeNewCommitTime();
    createCompactionRequested(instant1, metaClient);

    String current = HoodieTestTable.makeNewCommitTime();
    // before write: reload active timeline, collect pending instants
    metaClient.reloadActiveTimeline();
    Set<String> pendingInstants = TransactionUtils.getInflightAndRequestedInstantsWithoutCurrent(metaClient, current);
    createInflightCommit(current, metaClient);

    String instant2 = HoodieTestTable.makeNewCommitTime();
    createCompactionRequested(instant2, metaClient);

    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);

    // pre commit: reload active timeline, getCandidateInstants
    metaClient.reloadActiveTimeline();
    HoodieInstant currentInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, current);
    List<String> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant, Option.of(pendingInstants))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());

    Assertions.assertEquals(1, candidateInstants.size());
    Assertions.assertTrue(candidateInstants.contains(instant2));
  }
}
