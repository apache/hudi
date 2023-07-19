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
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestConflictResolutionStrategyWithBucketIndex extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanMetaClient();
  }

  private static Stream<Arguments> additionalProps() {
    return Stream.of(
        Arguments.of(createProperties(SimpleConcurrentFileWritesConflictResolutionStrategy.class.getName())),
        Arguments.of(createProperties(StateTransitionTimeBasedConflictResolutionStrategy.class.getName()))
    );
  }

  public static Properties createProperties(String conflictResolutionStrategyClassName) {
    Properties properties = new Properties();
    properties.setProperty(HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key(), conflictResolutionStrategyClassName);
    properties.setProperty(HoodieIndexConfig.INDEX_TYPE.key(), HoodieIndex.IndexType.BUCKET.name());
    return properties;
  }

  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testNoConcurrentWrites(Properties props) throws Exception {
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime);
    newInstantTime = HoodieTestTable.makeNewCommitTime();
    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, newInstantTime));

    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    metaClient.reloadActiveTimeline();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty());
    Assertions.assertEquals(0, candidateInstants.count());
  }

  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWrites(Properties props) throws Exception {
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime);
    // writer 1
    createInflightCommit(HoodieTestTable.makeNewCommitTime(), HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    // writer 2
    createInflightCommit(HoodieTestTable.makeNewCommitTime(), HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    newInstantTime = HoodieTestTable.makeNewCommitTime();
    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, newInstantTime));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    metaClient.reloadActiveTimeline();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty());
    Assertions.assertEquals(0, candidateInstants.count());
  }

  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWritesWithInterleavingSuccessfulCommit(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    // writer 2 starts and finishes
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCommit(newInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty()).collect(
        Collectors.toList());
    // writer 1 conflicts with writer 2
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    try {
      strategy.resolveConflict(thisCommitOperation, thatCommitOperation);
      Assertions.fail("Cannot reach here, writer 1 and writer 2 should have thrown a conflict");
    } catch (HoodieWriteConflictException e) {
      // expected
    }
  }

  @ParameterizedTest
  @MethodSource("additionalProps")
  public void testConcurrentWritesWithDifferentPartition(Properties props) throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime());
    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH);
    // writer 2 starts and finishes
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCommit(newInstantTime);

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    ConflictResolutionStrategy strategy = TestConflictResolutionStrategyUtil.getConflictResolutionStrategy(metaClient, props);
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH);
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), Option.empty()).collect(
        Collectors.toList());

    // there should be 1 candidate instant
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    // there should be no conflict between writer 1 and writer 2
    Assertions.assertFalse(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
  }

  private void createCommit(String instantTime) throws Exception {
    String fileId1 = "00000001-file-" + instantTime + "-1";
    String fileId2 = "00000002-file-" + instantTime + "-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId1);
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private HoodieCommitMetadata createCommitMetadata(String instantTime, String writeFileName, String partition) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(writeFileName);
    commitMetadata.addWriteStat(partition, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    return commitMetadata;
  }

  private HoodieCommitMetadata createCommitMetadata(String instantTime, String partition) {
    return createCommitMetadata(instantTime, "00000001-file-" + instantTime + "-1", partition);
  }

  private void createInflightCommit(String instantTime, String partition) throws Exception {
    String fileId1 = "00000001-file-" + instantTime + "-1";
    String fileId2 = "00000002-file-" + instantTime + "-2";
    HoodieTestTable.of(metaClient)
        .addInflightCommit(instantTime)
        .withBaseFilesInPartition(partition, fileId1, fileId2);
  }
}
