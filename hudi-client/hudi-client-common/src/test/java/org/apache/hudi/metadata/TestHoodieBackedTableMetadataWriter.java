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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieBackedTableMetadataWriter {
  private HoodieEngineContext engineContext;
  private HoodieTableMetaClient dataMetaClient;
  private HoodieMetadataConfig metadataConfig;
  private StorageConfiguration<?> storageConf;

  @BeforeEach
  void setUp() {
    engineContext = mock(HoodieEngineContext.class);
    dataMetaClient = mock(HoodieTableMetaClient.class);
    metadataConfig = mock(HoodieMetadataConfig.class);
    storageConf = mock(StorageConfiguration.class);

    when(metadataConfig.getMaxReaderBufferSize()).thenReturn(1024);
  }

  @ParameterizedTest
  @CsvSource(value = {
      "true,true,false,true",
      "false,true,false,true",
      "true,false,false,true",
      "false,false,false,false",
      "false,false,true,false",
  })
  void runPendingTableServicesOperations(boolean hasPendingCompaction, boolean hasPendingLogCompaction, boolean requiresRefresh, boolean ranService) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline initialTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    if (requiresRefresh) {
      when(metaClient.reloadActiveTimeline()).thenReturn(initialTimeline);
    } else {
      when(metaClient.getActiveTimeline()).thenReturn(initialTimeline);
    }
    if (hasPendingCompaction) {
      when(initialTimeline.filterPendingCompactionTimeline().countInstants()).thenReturn(1);
    }
    if (hasPendingLogCompaction) {
      when(initialTimeline.filterPendingLogCompactionTimeline().countInstants()).thenReturn(1);
    }
    HoodieActiveTimeline expectedResult;
    if (ranService) {
      HoodieActiveTimeline timelineReloadedAfterServicesRun = mock(HoodieActiveTimeline.class);
      when(metaClient.reloadActiveTimeline()).thenReturn(timelineReloadedAfterServicesRun);
      expectedResult = timelineReloadedAfterServicesRun;
    } else {
      expectedResult = initialTimeline;
    }
    assertSame(expectedResult, HoodieBackedTableMetadataWriter.runPendingTableServicesOperationsAndRefreshTimeline(
        metaClient, writeClient, requiresRefresh, Option.empty()));

    verify(writeClient, times(hasPendingCompaction ? 1 : 0)).runAnyPendingCompactions();
    verify(writeClient, times(hasPendingLogCompaction ? 1 : 0)).runAnyPendingLogCompactions();
    int expectedTimelineReloads = (requiresRefresh ? 1 : 0) + (ranService ? 1 : 0);
    verify(metaClient, times(expectedTimelineReloads)).reloadActiveTimeline();
  }

  @Test
  void rollbackFailedWrites_reloadsTimelineOnWritesRolledBack() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(true);
    try (MockedStatic<HoodieTableMetaClient> mockedStatic = mockStatic(HoodieTableMetaClient.class)) {
      HoodieTableMetaClient reloadedClient = mock(HoodieTableMetaClient.class);
      mockedStatic.when(() -> HoodieTableMetaClient.reload(mockMetaClient)).thenReturn(reloadedClient);
      assertSame(reloadedClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(writeConfig, mockWriteClient, mockMetaClient));
    }
  }

  @Test
  void rollbackFailedWrites_avoidsTimelineReload() {
    HoodieWriteConfig eagerWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(false);
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(eagerWriteConfig, mockWriteClient, mockMetaClient));

    HoodieWriteConfig lazyWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(lazyWriteConfig, mockWriteClient, mockMetaClient));
  }

  @Test
  void testConvertToColumnStatsRecordWithEmptyInputs() {
    Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
    Map<String, List<String>> partitionFilesToDelete = new HashMap<>();

    Map<String, HoodieData<HoodieRecord>> result =
        HoodieBackedTableMetadataWriter.convertToColumnStatsRecord(
            partitionFilesToAdd, partitionFilesToDelete, engineContext, dataMetaClient, metadataConfig, Option.empty(), 4);
    assertTrue(result.isEmpty());
  }

  @Test
  void testConvertToColumnStatsRecordWithEmptyColumnsToIndex() {
    // Mock HoodieTableMetadataUtil.getColumnsToIndex to return empty list
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> emptyColumnsMap = new HashMap<>();
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
              any(), any(), any(), eq(false), any()))
          .thenReturn(emptyColumnsMap);

      Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
      Map<String, Long> filesToAdd = new HashMap<>();
      filesToAdd.put("file1.parquet", 1024L);
      partitionFilesToAdd.put("partition1", filesToAdd);
      Map<String, List<String>> partitionFilesToDelete = new HashMap<>();

      Map<String, HoodieData<HoodieRecord>> result = HoodieBackedTableMetadataWriter.convertToColumnStatsRecord(
          partitionFilesToAdd, partitionFilesToDelete, engineContext, dataMetaClient, metadataConfig,
          Option.empty(), 4);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  void testConvertToColumnStatsRecordWithValidColumns() {
    // Mock HoodieTableMetadataUtil.getColumnsToIndex to return valid columns
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> columnsMap = new HashMap<>();
      columnsMap.put("col1", null);
      columnsMap.put("col2", null);
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
              any(), any(), any(), eq(false), any(), any()))
          .thenReturn(columnsMap);

      // Mock convertFilesToColumnStatsRecords to return empty HoodieData
      HoodieData<HoodieRecord> mockHoodieData = mock(HoodieData.class);
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
              any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
          .thenReturn(mockHoodieData);

      Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
      partitionFilesToAdd.put("partition1", new HashMap<>());
      Map<String, List<String>> partitionFilesToDelete = new HashMap<>();

      Map<String, HoodieData<HoodieRecord>> result = HoodieBackedTableMetadataWriter.convertToColumnStatsRecord(
          partitionFilesToAdd, partitionFilesToDelete, engineContext, dataMetaClient, metadataConfig,
          Option.empty(), 4);

      // Verify result contains COLUMN_STATS partition
      assertEquals(1, result.size());
      assertTrue(result.containsKey(MetadataPartitionType.COLUMN_STATS.getPartitionPath()));
      assertEquals(mockHoodieData, result.get(MetadataPartitionType.COLUMN_STATS.getPartitionPath()));

      // Verify the method was called with correct parameters
      mockedUtil.verify(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          eq(engineContext),
          eq(partitionFilesToDelete),
          eq(partitionFilesToAdd),
          eq(dataMetaClient),
          eq(metadataConfig),
          eq(4),
          eq(1024),
          any()
      ));
    }
  }

  @Test
  void testConvertToColumnStatsRecordWithMixedInputs() {
    // Mock HoodieTableMetadataUtil.getColumnsToIndex to return valid columns
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> columnsMap = new HashMap<>();
      columnsMap.put("col1", null);
      columnsMap.put("col2", null);
      columnsMap.put("col3", null);
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
              any(), any(), any(), eq(false), any(), any()))
          .thenReturn(columnsMap);

      // Mock convertFilesToColumnStatsRecords to return empty HoodieData
      HoodieData<HoodieRecord> mockHoodieData = mock(HoodieData.class);
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
              any(), any(), any(), any(), any(), anyInt(), anyInt(), any()))
          .thenReturn(mockHoodieData);

      Map<String, Map<String, Long>> partitionFilesToAdd = new HashMap<>();
      Map<String, Long> filesToAdd = new HashMap<>();
      filesToAdd.put("file1.parquet", 1024L);
      filesToAdd.put("file2.parquet", 2048L);
      partitionFilesToAdd.put("partition1", filesToAdd);

      Map<String, List<String>> partitionFilesToDelete = new HashMap<>();
      List<String> filesToDelete = new ArrayList<>();
      filesToDelete.add("old_file1.parquet");
      filesToDelete.add("old_file2.parquet");
      partitionFilesToDelete.put("partition1", filesToDelete);

      Map<String, HoodieData<HoodieRecord>> result = HoodieBackedTableMetadataWriter.convertToColumnStatsRecord(
          partitionFilesToAdd, partitionFilesToDelete, engineContext, dataMetaClient, metadataConfig,
          Option.empty(), 4);

      // Verify result contains COLUMN_STATS partition.
      assertEquals(1, result.size());
      assertTrue(result.containsKey(MetadataPartitionType.COLUMN_STATS.getPartitionPath()));

      // Verify the method was called with correct parameters.
      mockedUtil.verify(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          eq(engineContext),
          eq(partitionFilesToDelete),
          eq(partitionFilesToAdd),
          eq(dataMetaClient),
          eq(metadataConfig),
          eq(4),
          eq(1024),
          any()
      ));
    }
  }

  @Test
  void testValidateRollbackForMDT() throws Exception {
    List<HoodieInstant> instants = new ArrayList<>();

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012123905"));

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012447357", "20250925013432341"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012518125", "20250925012831379"));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012851950", "20250925013157886"));

    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20250925012523368", "20250925015000000"));

    // a deltacommit instant requested before the compaction request time and finished after it.
    HoodieInstant instantToRollback = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20250925012123804", "20250925014634434");
    instants.add(instantToRollback);

    HoodieActiveTimeline timeline = createMockTimeline(instants);

    HoodieBackedTableMetadataWriter<?, ?> writer = mock(HoodieBackedTableMetadataWriter.class);

    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockMetaClient.getActiveTimeline()).thenReturn(timeline);

    java.lang.reflect.Field metadataMetaClientField = HoodieBackedTableMetadataWriter.class.getDeclaredField("metadataMetaClient");
    metadataMetaClientField.setAccessible(true);
    metadataMetaClientField.set(writer, mockMetaClient);

    java.lang.reflect.Method validateRollbackMethod = HoodieBackedTableMetadataWriter.class.getDeclaredMethod("validateRollback", HoodieInstant.class);
    validateRollbackMethod.setAccessible(true);

    assertDoesNotThrow(() -> validateRollbackMethod.invoke(writer, instantToRollback));
  }

  @SuppressWarnings("deprecation")
  private HoodieActiveTimeline createMockTimeline(List<HoodieInstant> instants) {
    ActiveTimelineV2 timeline = new ActiveTimelineV2();
    timeline.setInstants(instants);
    return timeline;
  }

  static Stream<Arguments> performTableServicesFailureTestCases() {
    return Stream.of(
        Arguments.of(
            "compaction",
            true,
            false,
            new RuntimeException("Compaction failed"),
            true
        ),
        Arguments.of(
            "compaction",
            true,
            false,
            new RuntimeException("Compaction failed"),
            false
        ),
        Arguments.of(
            "log compaction",
            false,
            true,
            new HoodieException("Log compaction failed"),
            true
        ),
        Arguments.of(
            "log compaction",
            false,
            true,
            new HoodieException("Log compaction failed"),
            false
        )
    );
  }

  @ParameterizedTest
  @MethodSource("performTableServicesFailureTestCases")
  void testPerformTableServicesWithFailureHandling(
      String serviceType,
      boolean hasPendingCompaction,
      boolean hasPendingLogCompaction,
      RuntimeException exceptionToThrow,
      boolean shouldFailOnTableServiceFailures) throws Exception {
    // Create mocks for dependencies
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    HoodieMetadataMetrics metrics = mock(HoodieMetadataMetrics.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);

    // Set up config mocks
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.shouldFailOnTableServiceFailures()).thenReturn(shouldFailOnTableServiceFailures);
    when(writeConfig.getTableName()).thenReturn("test_table");

    // Set up timeline mocks
    when(metaClient.reloadActiveTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(timeline);
    when(timeline.filterPendingCompactionTimeline().countInstants()).thenReturn(hasPendingCompaction ? 1 : 0);
    when(timeline.filterPendingLogCompactionTimeline().countInstants()).thenReturn(hasPendingLogCompaction ? 1 : 0);
    when(timeline.getDeltaCommitTimeline().filterCompletedInstants().lastInstant()).thenReturn(Option.empty());

    // Set up write client mocks
    when(writeClient.getConfig()).thenReturn(writeConfig);

    // Simulate failure based on service type
    if (hasPendingCompaction) {
      doThrow(exceptionToThrow).when(writeClient).runAnyPendingCompactions();
    }
    if (hasPendingLogCompaction) {
      doThrow(exceptionToThrow).when(writeClient).runAnyPendingLogCompactions();
    }

    // Create a partial mock of HoodieBackedTableMetadataWriter
    HoodieBackedTableMetadataWriter writer = mock(HoodieBackedTableMetadataWriter.class);

    // Mock getWriteClient to return our mock write client
    when(writer.getWriteClient()).thenReturn(writeClient);

    // Set up the writer's fields using reflection
    java.lang.reflect.Field metadataMetaClientField = HoodieBackedTableMetadataWriter.class.getDeclaredField("metadataMetaClient");
    metadataMetaClientField.setAccessible(true);
    metadataMetaClientField.set(writer, metaClient);

    java.lang.reflect.Field writeClientField = HoodieBackedTableMetadataWriter.class.getDeclaredField("writeClient");
    writeClientField.setAccessible(true);
    writeClientField.set(writer, writeClient);

    java.lang.reflect.Field writeConfigField = HoodieBackedTableMetadataWriter.class.getDeclaredField("dataWriteConfig");
    writeConfigField.setAccessible(true);
    writeConfigField.set(writer, writeConfig);

    java.lang.reflect.Field metricsField = HoodieBackedTableMetadataWriter.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(writer, Option.of(metrics));

    // Call the real performTableServices method
    doCallRealMethod().when(writer).performTableServices(any(), eq(true));

    if (shouldFailOnTableServiceFailures) {
      // When shouldFailOnTableServiceFailures is true, exception should propagate
      assertThrows(exceptionToThrow.getClass(), () ->
          writer.performTableServices(Option.empty(), true),
          "Expected exception to be thrown when " + serviceType + " fails and shouldFailOnTableServiceFailures is true");
    } else {
      // When shouldFailOnTableServiceFailures is false, exception should not propagate
      assertDoesNotThrow(() ->
          writer.performTableServices(Option.empty(), true),
          "Exception should not be thrown when shouldFailOnTableServiceFailures is false");
    }

    // Verify the appropriate service method was called
    if (hasPendingCompaction) {
      verify(writeClient, times(1)).runAnyPendingCompactions();
    }
    if (hasPendingLogCompaction) {
      verify(writeClient, times(1)).runAnyPendingLogCompactions();
    }

    // Verify metrics are incremented when there's a failure
    verify(metrics, times(1)).incrementMetric(HoodieMetadataMetrics.PENDING_COMPACTIONS_FAILURES, 1);
  }

  @Test
  void testExecuteCleanWhenCleanInstantDoesNotExist() throws Exception {
    // Create mocks
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);

    // Set up timeline to indicate clean instant doesn't exist
    String instantTime = "20250101120000000";
    String expectedCleanInstant = HoodieBackedTableMetadataWriterTableVersionSix.createCleanTimestamp(instantTime);
    when(metadataMetaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getCleanerTimeline().filterCompletedInstants().containsInstant(expectedCleanInstant)).thenReturn(false);

    // Create a partial mock of HoodieBackedTableMetadataWriterTableVersionSix
    HoodieBackedTableMetadataWriterTableVersionSix writer = mock(HoodieBackedTableMetadataWriterTableVersionSix.class);
    when(writer.getMetadataMetaClient()).thenReturn(metadataMetaClient);

    // Call the real executeClean method
    doCallRealMethod().when(writer).executeClean(any(), any());

    // Execute the clean
    writer.executeClean(writeClient, instantTime);

    // Verify that writeClient.clean() was called with the correct timestamp
    verify(writeClient, times(1)).clean(expectedCleanInstant);
  }

  @Test
  void testExecuteCleanWhenCleanInstantAlreadyExists() throws Exception {
    // Create mocks
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);

    // Set up timeline to indicate clean instant already exists
    String instantTime = "20250101120000000";
    String expectedCleanInstant = HoodieBackedTableMetadataWriterTableVersionSix.createCleanTimestamp(instantTime);
    when(metadataMetaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getCleanerTimeline().filterCompletedInstants().containsInstant(expectedCleanInstant)).thenReturn(true);

    // Create a partial mock of HoodieBackedTableMetadataWriterTableVersionSix
    HoodieBackedTableMetadataWriterTableVersionSix writer = mock(HoodieBackedTableMetadataWriterTableVersionSix.class);
    when(writer.getMetadataMetaClient()).thenReturn(metadataMetaClient);

    // Call the real executeClean method
    doCallRealMethod().when(writer).executeClean(any(), any());

    // Execute the clean
    writer.executeClean(writeClient, instantTime);

    // Verify that writeClient.clean() was NOT called since the instant already exists
    verify(writeClient, times(0)).clean(any());
  }

  @Test
  void testCreateCleanTimestamp() {
    // Test that createCleanTimestamp appends the correct suffix
    String baseTimestamp = "20250101120000000";
    String cleanTimestamp = HoodieBackedTableMetadataWriterTableVersionSix.createCleanTimestamp(baseTimestamp);

    // The clean operation suffix should be "002"
    String expectedTimestamp = baseTimestamp + "002";
    assertEquals(expectedTimestamp, cleanTimestamp, "Clean timestamp should have the correct suffix appended");
  }

  @ParameterizedTest
  @CsvSource({
      "20250101120000000,20250101120000000002",
      "20240615100530456,20240615100530456002",
      "20231231235959999,20231231235959999002"
  })
  void testCreateCleanTimestampWithMultipleValues(String input, String expected) {
    String result = HoodieBackedTableMetadataWriterTableVersionSix.createCleanTimestamp(input);
    assertEquals(expected, result, "Clean timestamp should match expected format");
  }

}
