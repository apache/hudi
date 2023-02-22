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
import org.apache.hudi.exception.HoodieWriteConflictException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestBucketIndexConcurrentFileWritesConflictResolutionStrategy extends HoodieCommonTestHarness {

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

    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new BucketIndexConcurrentFileWritesConflictResolutionStrategy();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient.getActiveTimeline(), currentInstant.get(), lastSuccessfulInstant);
    Assertions.assertEquals(0, candidateInstants.count());
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
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new BucketIndexConcurrentFileWritesConflictResolutionStrategy();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient.getActiveTimeline(), currentInstant.get(), lastSuccessfulInstant);
    Assertions.assertEquals(0, candidateInstants.count());
  }

  @Test
  public void testConcurrentWritesWithInterleavingSuccessfulCommit() throws Exception {
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
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy = new BucketIndexConcurrentFileWritesConflictResolutionStrategy();
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    timeline = timeline.reload();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(timeline, currentInstant.get(), lastSuccessfulInstant).collect(
        Collectors.toList());
    // writer 1 conflicts with writer 2
    Assertions.assertEquals(1, candidateInstants.size());
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

  private void createCommit(String instantTime) throws Exception {
    String fileId1 = "00000001-file-" + instantTime + "-1";
    String fileId2 = "00000002-file-"  + instantTime + "-2";

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

  private HoodieCommitMetadata createCommitMetadata(String instantTime, String writeFileName) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(writeFileName);
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    return commitMetadata;
  }

  private HoodieCommitMetadata createCommitMetadata(String instantTime) {
    return createCommitMetadata(instantTime, "00000001-file-" + instantTime + "-1");
  }

  private void createInflightCommit(String instantTime) throws Exception {
    String fileId1 = "00000001-file-" + instantTime + "-1";
    String fileId2 = "00000002-file-" + instantTime + "-2";
    HoodieTestTable.of(metaClient)
        .addInflightCommit(instantTime)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }
}
