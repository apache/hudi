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

package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieTableServiceManagerConfig;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantGeneratorV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HoodieBackedTableMetadataWriterTableVersionSix}.
 */
class TestHoodieBackedTableMetadataWriterTableVersionSix {

  // Use V1 instant generator for table version 6 (V2 is for table version 8)
  private static final InstantGenerator INSTANT_GENERATOR = new InstantGeneratorV1();

  /**
   * Test shouldInitializeFromFilesystem returns true when there are no pending data instants.
   */
  @Test
  void testShouldInitializeFromFilesystem_noPendingInstants() throws Exception {
    HoodieTableMetaClient mockDataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = createMockTimeline(Collections.emptyList());
    when(mockDataMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(mockDataMetaClient);

    Set<String> pendingDataInstants = Collections.emptySet();
    Option<String> inflightInstantTimestamp = Option.empty();

    boolean result = invokeShouldInitializeFromFilesystem(writer, pendingDataInstants, inflightInstantTimestamp);
    assertTrue(result, "Should allow initialization when there are no pending data instants");
  }

  /**
   * Test shouldInitializeFromFilesystem returns true when the only pending instant is the current inflight instant.
   */
  @Test
  void testShouldInitializeFromFilesystem_onlyCurrentInflightInstant() throws Exception {
    HoodieTableMetaClient mockDataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = createMockTimeline(Collections.emptyList());
    when(mockDataMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(mockDataMetaClient);

    String currentInflightTime = "20250101120000000";
    Set<String> pendingDataInstants = new HashSet<>();
    pendingDataInstants.add(currentInflightTime);
    Option<String> inflightInstantTimestamp = Option.of(currentInflightTime);

    boolean result = invokeShouldInitializeFromFilesystem(writer, pendingDataInstants, inflightInstantTimestamp);
    assertTrue(result, "Should allow initialization when only pending instant is the current inflight instant");
  }

  /**
   * Test shouldInitializeFromFilesystem returns false when there are blocking pending instants with no rollbacks.
   */
  @Test
  void testShouldInitializeFromFilesystem_blockingInstantsWithNoRollbacks() throws Exception {
    HoodieTableMetaClient mockDataMetaClient = mock(HoodieTableMetaClient.class);
    // Empty rollback timeline
    HoodieActiveTimeline mockTimeline = createMockTimeline(Collections.emptyList());
    when(mockDataMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(mockDataMetaClient);

    Set<String> pendingDataInstants = new HashSet<>();
    pendingDataInstants.add("20250101110000000");
    pendingDataInstants.add("20250101120000000");
    Option<String> inflightInstantTimestamp = Option.empty();

    boolean result = invokeShouldInitializeFromFilesystem(writer, pendingDataInstants, inflightInstantTimestamp);
    assertFalse(result, "Should block initialization when there are pending instants without rollbacks");
  }

  /**
   * Test shouldInitializeFromFilesystem returns true when all blocking pending instants have corresponding pending rollbacks.
   */
  @Test
  void testShouldInitializeFromFilesystem_allBlockingInstantsBeingRolledBack() throws Exception {
    String pendingInstant1 = "20250101110000000";
    String pendingInstant2 = "20250101120000000";

    // Create pending rollback instants for all blocking instants
    List<HoodieInstant> rollbackInstants = new ArrayList<>();
    rollbackInstants.add(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, pendingInstant1));
    rollbackInstants.add(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, pendingInstant2));

    HoodieTableMetaClient mockDataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = createMockTimeline(rollbackInstants);
    when(mockDataMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(mockDataMetaClient);

    Set<String> pendingDataInstants = new HashSet<>();
    pendingDataInstants.add(pendingInstant1);
    pendingDataInstants.add(pendingInstant2);
    Option<String> inflightInstantTimestamp = Option.empty();

    boolean result = invokeShouldInitializeFromFilesystem(writer, pendingDataInstants, inflightInstantTimestamp);
    assertTrue(result, "Should allow initialization when all blocking instants are being rolled back");
  }

  /**
   * Test shouldInitializeFromFilesystem returns false when only some blocking pending instants have rollbacks.
   */
  @Test
  void testShouldInitializeFromFilesystem_partialRollbacks() throws Exception {
    String pendingInstant1 = "20250101110000000";
    String pendingInstant2 = "20250101120000000";

    // Create pending rollback instant for only one of the blocking instants
    List<HoodieInstant> rollbackInstants = new ArrayList<>();
    rollbackInstants.add(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, pendingInstant1));

    HoodieTableMetaClient mockDataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = createMockTimeline(rollbackInstants);
    when(mockDataMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(mockDataMetaClient);

    Set<String> pendingDataInstants = new HashSet<>();
    pendingDataInstants.add(pendingInstant1);
    pendingDataInstants.add(pendingInstant2);
    Option<String> inflightInstantTimestamp = Option.empty();

    boolean result = invokeShouldInitializeFromFilesystem(writer, pendingDataInstants, inflightInstantTimestamp);
    assertFalse(result, "Should block initialization when only some blocking instants have rollbacks");
  }

  /**
   * Test shouldInitializeFromFilesystem with a mix of current inflight instant and other pending instants being rolled back.
   */
  @Test
  void testShouldInitializeFromFilesystem_mixedInflightAndRollbacks() throws Exception {
    String currentInflightTime = "20250101130000000";
    String pendingInstant1 = "20250101110000000";
    String pendingInstant2 = "20250101120000000";

    // Create pending rollback instants for the blocking instants (not including current inflight)
    List<HoodieInstant> rollbackInstants = new ArrayList<>();
    rollbackInstants.add(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, pendingInstant1));
    rollbackInstants.add(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, pendingInstant2));

    HoodieTableMetaClient mockDataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = createMockTimeline(rollbackInstants);
    when(mockDataMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(mockDataMetaClient);

    Set<String> pendingDataInstants = new HashSet<>();
    pendingDataInstants.add(currentInflightTime);
    pendingDataInstants.add(pendingInstant1);
    pendingDataInstants.add(pendingInstant2);
    Option<String> inflightInstantTimestamp = Option.of(currentInflightTime);

    boolean result = invokeShouldInitializeFromFilesystem(writer, pendingDataInstants, inflightInstantTimestamp);
    assertTrue(result, "Should allow initialization when current inflight is excluded and other blocking instants are being rolled back");
  }

  /**
   * Test shouldInitializeFromFilesystem returns false when completed rollback instants exist
   * but no pending rollbacks for blocking instants.
   */
  @Test
  void testShouldInitializeFromFilesystem_completedRollbacksDoNotCount() throws Exception {
    String pendingInstant1 = "20250101110000000";

    // Create only completed rollback instant (should not count as pending)
    List<HoodieInstant> rollbackInstants = new ArrayList<>();
    rollbackInstants.add(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, pendingInstant1, "20250101110100000"));

    HoodieTableMetaClient mockDataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockTimeline = createMockTimeline(rollbackInstants);
    when(mockDataMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(mockDataMetaClient);

    Set<String> pendingDataInstants = new HashSet<>();
    pendingDataInstants.add(pendingInstant1);
    Option<String> inflightInstantTimestamp = Option.empty();

    boolean result = invokeShouldInitializeFromFilesystem(writer, pendingDataInstants, inflightInstantTimestamp);
    assertFalse(result, "Should block initialization when rollbacks are completed, not pending");
  }

  @Test
  void testGenerateUniqueInstantTimePreservesIndexingInstant() throws Exception {
    // An indexing instant is already globally unique and must be reused.
    String indexingInstant = "20250101120000000";
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline timeline = createMockTimeline(Collections.singletonList(
        INSTANT_GENERATOR.createNewInstant(
            HoodieInstant.State.REQUESTED, HoodieTimeline.INDEXING_ACTION, indexingInstant)));
    when(dataMetaClient.getActiveTimeline()).thenReturn(timeline);
    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(dataMetaClient);

    assertTrue(indexingInstant.equals(writer.generateUniqueInstantTime(indexingInstant)));
  }

  @Test
  void testValidateCompactionSchedulingRejectsPendingMetadataTableService() throws Exception {
    // Do not schedule compaction while another metadata table service is pending.
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline dataTimeline = createMockTimeline(Collections.emptyList());
    when(dataMetaClient.reloadActiveTimeline()).thenReturn(dataTimeline);
    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(dataMetaClient);

    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline metadataTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    HoodieInstant pendingCompaction = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "20250101120000001");
    when(metadataMetaClient.getActiveTimeline()).thenReturn(metadataTimeline);
    when(metadataTimeline.filterPendingLogCompactionTimeline().firstInstant()).thenReturn(Option.empty());
    when(metadataTimeline.filterPendingCompactionTimeline().firstInstant()).thenReturn(Option.of(pendingCompaction));
    writer.metadataMetaClient = metadataMetaClient;

    assertFalse(writer.validateCompactionScheduling(Option.empty(), "20250101120000002"));
  }

  @Test
  void testValidateCompactionSchedulingRejectsExcessiveDeltaCommits() throws Exception {
    // Protect pending data commits from unbounded metadata delta commits.
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieInstant pendingCommit = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "20250101120000000");
    HoodieActiveTimeline dataTimeline = createMockTimeline(Collections.singletonList(pendingCommit));
    when(dataMetaClient.reloadActiveTimeline()).thenReturn(dataTimeline);
    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = createMockWriter(dataMetaClient);

    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline metadataTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    when(metadataMetaClient.reloadActiveTimeline()).thenReturn(metadataTimeline);
    when(metadataTimeline.filterCompletedInstants().filter(any()).lastInstant()).thenReturn(Option.empty());
    when(metadataTimeline.getDeltaCommitTimeline().countInstants()).thenReturn(2);
    writer.metadataMetaClient = metadataMetaClient;
    writer.dataWriteConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/table")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltacommitsWhenPending(1)
            .build())
        .build();

    assertThrows(HoodieMetadataException.class,
        () -> writer.validateCompactionScheduling(Option.empty(), "20250101120000001"));
  }

  @Test
  void testCompactIfNecessaryCoversExistingDelegatedAndLogCompactionPaths() {
    // Exercise completed, delegated, and log-compaction fallback paths.
    Properties tableServiceManagerProperties = new Properties();
    tableServiceManagerProperties.put(
        HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ENABLED.key(), "true");
    tableServiceManagerProperties.put(
        HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(), ActionType.compaction.name());
    HoodieTableServiceManagerConfig tableServiceManagerConfig =
        HoodieTableServiceManagerConfig.newBuilder()
            .fromProperties(tableServiceManagerProperties)
            .build();
    HoodieWriteConfig metadataWriteConfig = mock(HoodieWriteConfig.class);
    when(metadataWriteConfig.getTableServiceManagerConfig()).thenReturn(tableServiceManagerConfig);
    when(metadataWriteConfig.isLogCompactionEnabled()).thenReturn(true);

    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    when(metadataMetaClient.getActiveTimeline()).thenReturn(timeline);
    when(timeline.filterCompletedInstants().containsInstant("100001")).thenReturn(true);
    when(timeline.filterCompletedInstants().containsInstant("200001")).thenReturn(false);
    when(timeline.filterCompletedInstants().containsInstant("300001")).thenReturn(false);
    when(timeline.filterCompletedInstants().containsInstant("300005")).thenReturn(false);
    when(timeline.filterCompletedInstants().containsInstant("400001")).thenReturn(false);
    when(timeline.filterCompletedInstants().containsInstant("400005")).thenReturn(false);

    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer =
        mock(HoodieBackedTableMetadataWriterTableVersionSix.class, CALLS_REAL_METHODS);
    writer.metadataMetaClient = metadataMetaClient;
    writer.metadataWriteConfig = metadataWriteConfig;
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    when(writeClient.scheduleCompactionAtInstant("200001", Option.empty())).thenReturn(true);
    when(writeClient.scheduleCompactionAtInstant("300001", Option.empty())).thenReturn(false);
    when(writeClient.scheduleLogCompactionAtInstant("300005", Option.empty())).thenReturn(true);

    // Version 6 derives compaction and log-compaction instants with fixed suffixes.
    writer.compactIfNecessary(writeClient, Option.of("100"));
    writer.compactIfNecessary(writeClient, Option.of("200"));
    writer.compactIfNecessary(writeClient, Option.of("300"));

    Properties allTableServicesProperties = new Properties();
    allTableServicesProperties.put(
        HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ENABLED.key(), "true");
    allTableServicesProperties.put(
        HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(), "compaction,logcompaction");
    when(metadataWriteConfig.getTableServiceManagerConfig()).thenReturn(
        HoodieTableServiceManagerConfig.newBuilder()
            .fromProperties(allTableServicesProperties)
            .build());
    when(writeClient.scheduleCompactionAtInstant("400001", Option.empty())).thenReturn(false);
    when(writeClient.scheduleLogCompactionAtInstant("400005", Option.empty())).thenReturn(true);
    writer.compactIfNecessary(writeClient, Option.of("400"));

    verify(writeClient).scheduleCompactionAtInstant("200001", Option.empty());
    verify(writeClient).scheduleLogCompactionAtInstant("300005", Option.empty());
    verify(writeClient).logCompact("300005", true);
  }

  @Test
  void testValidateRollbackRejectsCommitBeforeLatestCompaction() throws Exception {
    // Version 6 cannot roll back beyond the latest compaction boundary.
    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer =
        mock(HoodieBackedTableMetadataWriterTableVersionSix.class, CALLS_REAL_METHODS);
    HoodieInstant compactionInstant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "200", "201");
    HoodieTimeline deltaCommits = mock(HoodieTimeline.class);
    when(deltaCommits.countInstants()).thenReturn(2);
    when(deltaCommits.getInstants()).thenReturn(Collections.emptyList());
    Method validateRollback = HoodieBackedTableMetadataWriterTableVersionSix.class
        .getDeclaredMethod(
            "validateRollbackVersionSix", String.class, HoodieInstant.class, HoodieTimeline.class);
    validateRollback.setAccessible(true);

    InvocationTargetException exception = assertThrows(
        InvocationTargetException.class,
        () -> validateRollback.invoke(writer, "100", compactionInstant, deltaCommits));
    assertTrue(exception.getCause() instanceof HoodieMetadataException);
  }

  private HoodieBackedTableMetadataWriterTableVersionSix<?, ?> createMockWriter(HoodieTableMetaClient dataMetaClient) throws Exception {
    // Use CALLS_REAL_METHODS so that shouldInitializeFromFilesystem executes the real logic
    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = mock(HoodieBackedTableMetadataWriterTableVersionSix.class, CALLS_REAL_METHODS);

    // Set the dataMetaClient field
    java.lang.reflect.Field dataMetaClientField = HoodieBackedTableMetadataWriter.class.getDeclaredField("dataMetaClient");
    dataMetaClientField.setAccessible(true);
    dataMetaClientField.set(writer, dataMetaClient);

    // Set the metrics field to avoid NPE
    java.lang.reflect.Field metricsField = HoodieBackedTableMetadataWriter.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(writer, Option.empty());

    return writer;
  }

  private boolean invokeShouldInitializeFromFilesystem(
      HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer,
      Set<String> pendingDataInstants,
      Option<String> inflightInstantTimestamp) {
    // The test class is in the same package, so we can call the package-private method directly
    return writer.shouldInitializeFromFilesystem(pendingDataInstants, inflightInstantTimestamp);
  }

  @SuppressWarnings("deprecation")
  private HoodieActiveTimeline createMockTimeline(List<HoodieInstant> instants) {
    // Use V1 timeline for table version 6 (V2 is for table version 8)
    ActiveTimelineV1 timeline = new ActiveTimelineV1();
    timeline.setInstants(instants);
    return timeline;
  }
}
