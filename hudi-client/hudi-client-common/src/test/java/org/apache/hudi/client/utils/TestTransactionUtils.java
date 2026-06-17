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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommitMetadata;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createInflightCommit;
import static org.apache.hudi.common.model.WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL;
import static org.apache.hudi.common.model.WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestTransactionUtils extends HoodieCommonTestHarness {
  @BeforeEach
  void init() throws IOException {
    initMetaClient();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void resolveWriteConflictIfAnyThrowsExceptionIfConflict(boolean timelineRefreshedWithinTransaction) throws Exception {
    createCommit(HoodieTestTable.makeNewCommitTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieTestTable.makeNewCommitTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // writer 2 starts and finishes
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant, "file-1");

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath().toString())
        .withWriteConcurrencyMode(OPTIMISTIC_CONCURRENCY_CONTROL)
        .build();
    metaClient.reloadActiveTimeline();
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient spyMetaClient = spy(metaClient);
    when(table.getMetaClient()).thenReturn(spyMetaClient);
    assertThrows(HoodieWriteConflictException.class,
        () -> TransactionUtils.resolveWriteConflictIfAny(table, currentInstant, Option.of(currentMetadata), writeConfig,
            lastSuccessfulInstant, timelineRefreshedWithinTransaction, Collections.singleton(newInstantTime)));
    verify(spyMetaClient, times(timelineRefreshedWithinTransaction ? 0 : 1)).reloadActiveTimeline();
  }

  @Test
  void resolveWriteConflictIfAnyNoExceptionForMetadataTable() throws Exception {
    // instantiate MOR table for metadata table.
    metaClient = HoodieTestUtils.init(basePath + "/.hoodie/metadata/", HoodieTableType.MERGE_ON_READ);

    createCommit(HoodieTestTable.makeNewCommitTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();
    // writer 1 starts
    String currentWriterInstant = HoodieTestTable.makeNewCommitTime();
    createInflightCommit(currentWriterInstant, metaClient);
    // writer 2 starts and finishes
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime, metaClient);

    Option<HoodieInstant> currentInstant = Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, currentWriterInstant));
    HoodieCommitMetadata currentMetadata = createCommitMetadata(currentWriterInstant, "file-1");

    // mimic all props for metadata table.
    Properties props = new Properties();
    props.setProperty(HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key(), "true");
    props.setProperty(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath().toString())
        .withWriteConcurrencyMode(NON_BLOCKING_CONCURRENCY_CONTROL)
        .withProperties(props)
        .build();
    metaClient.reloadActiveTimeline();
    HoodieTable table = mock(HoodieTable.class);
    when(table.isMetadataTable()).thenReturn(true);
    HoodieTableMetaClient spyMetaClient = spy(metaClient);
    when(table.getMetaClient()).thenReturn(spyMetaClient);
    Option<HoodieCommitMetadata> actualResult = TransactionUtils.resolveWriteConflictIfAny(table, currentInstant, Option.of(currentMetadata), writeConfig,
            lastSuccessfulInstant, false, Collections.singleton(newInstantTime));
    // since we bypass entire conflict resolution
    verify(spyMetaClient, times(0)).reloadActiveTimeline();
  }

  @Test
  void getLastCompletedTxnInstantAndMetadataSelectsMaxRequestedTime() throws Exception {
    // Simulate interleaved completions:
    // Commit A: requested=T1, completed=T5 (slow commit)
    // Commit B: requested=T2, completed=T3 (fast commit)
    // currentInstantTime=T6
    // The method should return B (max requestedTime=T2), not A (max completionTime=T5).
    String t1 = "20240101010101000";
    String t2 = "20240101010102000";
    String t3 = "20240101010103000";
    String t5 = "20240101010105000";
    String t6 = "20240101010106000";

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    HoodieCommitMetadata metadataA = createCommitMetadata(t1, "file-1");
    HoodieCommitMetadata metadataB = createCommitMetadata(t2, "file-2");
    testTable.addCommit(t1, Option.of(t5), Option.of(metadataA));
    testTable.addCommit(t2, Option.of(t3), Option.of(metadataB));

    metaClient.reloadActiveTimeline();
    Option<Pair<HoodieInstant, Map<String, String>>> result =
        TransactionUtils.getLastCompletedTxnInstantAndMetadata(metaClient, t6);

    assertTrue(result.isPresent());
    assertEquals(t2, result.get().getLeft().requestedTime(),
        "Should select the instant with the latest requestedTime (T2), not the latest completionTime (T5)");
  }

  @Test
  void getLastCompletedTxnInstantAndMetadataExcludesInstantsCompletedAfterCurrent() throws Exception {
    // Commit A: requested=T1, completed=T3 (completed before current)
    // Commit B: requested=T2, completed=T5 (completed after current)
    // currentInstantTime=T4
    // Should return A only, since B's completionTime >= currentInstantTime.
    String t1 = "20240101010101000";
    String t2 = "20240101010102000";
    String t3 = "20240101010103000";
    String t4 = "20240101010104000";
    String t5 = "20240101010105000";

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    HoodieCommitMetadata metadataA = createCommitMetadata(t1, "file-1");
    HoodieCommitMetadata metadataB = createCommitMetadata(t2, "file-2");
    testTable.addCommit(t1, Option.of(t3), Option.of(metadataA));
    testTable.addCommit(t2, Option.of(t5), Option.of(metadataB));

    metaClient.reloadActiveTimeline();
    Option<Pair<HoodieInstant, Map<String, String>>> result =
        TransactionUtils.getLastCompletedTxnInstantAndMetadata(metaClient, t4);

    assertTrue(result.isPresent());
    assertEquals(t1, result.get().getLeft().requestedTime(),
        "Should only include instants whose completionTime < currentInstantTime");
  }

  @Test
  void getLastCompletedTxnInstantAndMetadataReturnsEmptyWhenNoInstantsQualify() throws Exception {
    // Commit A: requested=T1, completed=T3
    // currentInstantTime=T2 (before A's completion)
    // Should return empty.
    String t1 = "20240101010101000";
    String t2 = "20240101010102000";
    String t3 = "20240101010103000";

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    HoodieCommitMetadata metadataA = createCommitMetadata(t1, "file-1");
    testTable.addCommit(t1, Option.of(t3), Option.of(metadataA));

    metaClient.reloadActiveTimeline();
    Option<Pair<HoodieInstant, Map<String, String>>> result =
        TransactionUtils.getLastCompletedTxnInstantAndMetadata(metaClient, t2);

    assertFalse(result.isPresent(),
        "Should return empty when no instants have completionTime < currentInstantTime");
  }
}
