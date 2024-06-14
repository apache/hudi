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

package org.apache.hudi.client.transaction;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
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

import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommitMetadata;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createInflightCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createPendingCompaction;

public class TestSimpleConcurrentFileWritesConflictResolutionStrategyWithMORTable extends HoodieCommonTestHarness {
  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testConcurrentWritesWithInterleavingInflightCompaction() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // Consider commits before this are all successful.
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // Writer 1 starts.
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // Compaction 1 gets scheduled and becomes inflight.
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createPendingCompaction(newInstantTime, metaClient);

    // Writer 1 tries to commit.
    Option<HoodieInstant> currentInstant = Option.of(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, currentWriterInstant));
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant);
    metaClient.reloadActiveTimeline();

    // Do conflict resolution.
    SimpleConcurrentFileWritesConflictResolutionStrategy strategy =
        new SimpleConcurrentFileWritesConflictResolutionStrategy();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(
        metaClient, currentInstant.get(), lastSuccessfulInstant).collect(Collectors.toList());
    Assertions.assertEquals(1, candidateInstants.size());
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation));
    Assertions.assertThrows(
        HoodieWriteConflictException.class,
        () -> strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation));
  }
}
