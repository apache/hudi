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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantGeneratorV1;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
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

  private HoodieBackedTableMetadataWriterTableVersionSix<?, ?> createMockWriter(HoodieTableMetaClient dataMetaClient) throws Exception {
    HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer = mock(HoodieBackedTableMetadataWriterTableVersionSix.class);

    // Set the dataMetaClient field
    java.lang.reflect.Field dataMetaClientField = HoodieBackedTableMetadataWriter.class.getDeclaredField("dataMetaClient");
    dataMetaClientField.setAccessible(true);
    dataMetaClientField.set(writer, dataMetaClient);

    return writer;
  }

  private boolean invokeShouldInitializeFromFilesystem(
      HoodieBackedTableMetadataWriterTableVersionSix<?, ?> writer,
      Set<String> pendingDataInstants,
      Option<String> inflightInstantTimestamp) throws Exception {
    java.lang.reflect.Method method = HoodieBackedTableMetadataWriterTableVersionSix.class.getDeclaredMethod(
        "shouldInitializeFromFilesystem", Set.class, Option.class);
    method.setAccessible(true);
    return (boolean) method.invoke(writer, pendingDataInstants, inflightInstantTimestamp);
  }

  @SuppressWarnings("deprecation")
  private HoodieActiveTimeline createMockTimeline(List<HoodieInstant> instants) {
    // Use V1 timeline for table version 6 (V2 is for table version 8)
    ActiveTimelineV1 timeline = new ActiveTimelineV1();
    timeline.setInstants(instants);
    return timeline;
  }
}
