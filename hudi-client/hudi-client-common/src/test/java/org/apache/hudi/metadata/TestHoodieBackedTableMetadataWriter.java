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

import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieTableServiceManagerConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.index.Indexer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class TestHoodieBackedTableMetadataWriter {
  @Test
  void completeStreamingCommitSkipsAlreadyCompletedMetadataInstant() {
    String instantTime = "20260709120000000";
    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> metadataWriter =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);

    metadataWriter.metadataMetaClient = metadataMetaClient;
    when(metadataMetaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    when(completedTimeline.containsInstant(instantTime)).thenReturn(true);
    when(metadataWriter.initializeWriteClient()).thenReturn(writeClient);

    metadataWriter.completeStreamingCommit(instantTime, engineContext, Collections.emptyList(), mock(HoodieCommitMetadata.class));

    verify(writeClient).postCommit(instantTime);
    verifyNoMoreInteractions(writeClient);
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
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/").build();
    when(writeClient.getConfig()).thenReturn(writeConfig);
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
  void runPendingTableServicesSkipsExecutionWhenTSMEnabled() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline initialTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);

    Properties tsmProps = new Properties();
    tsmProps.put(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ENABLED.key(), "true");
    tsmProps.put(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(), "compaction,logcompaction");
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/").withProperties(tsmProps).build();
    when(writeClient.getConfig()).thenReturn(writeConfig);
    when(writeClient.shouldDelegateToTableServiceManager(any(), any())).thenCallRealMethod();

    when(metaClient.getActiveTimeline()).thenReturn(initialTimeline);
    when(initialTimeline.filterPendingCompactionTimeline().countInstants()).thenReturn(1);
    when(initialTimeline.filterPendingLogCompactionTimeline().countInstants()).thenReturn(1);

    HoodieActiveTimeline result = HoodieBackedTableMetadataWriter.runPendingTableServicesOperationsAndRefreshTimeline(
        metaClient, writeClient, false, Option.empty());

    // TSM-delegated actions should not be executed
    verify(writeClient, times(0)).runAnyPendingCompactions();
    verify(writeClient, times(0)).runAnyPendingLogCompactions();
    // No services ran, so no timeline reload needed
    assertSame(initialTimeline, result);
  }

  @Test
  void runPendingTableServicesPartialTSMDelegation() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline initialTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);

    // Only compaction is delegated to TSM, logcompaction is not
    Properties tsmProps = new Properties();
    tsmProps.put(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ENABLED.key(), "true");
    tsmProps.put(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(), "compaction");
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/").withProperties(tsmProps).build();
    when(writeClient.getConfig()).thenReturn(writeConfig);
    when(writeClient.shouldDelegateToTableServiceManager(any(), any())).thenCallRealMethod();

    when(metaClient.getActiveTimeline()).thenReturn(initialTimeline);
    when(initialTimeline.filterPendingCompactionTimeline().countInstants()).thenReturn(1);
    when(initialTimeline.filterPendingLogCompactionTimeline().countInstants()).thenReturn(1);

    HoodieActiveTimeline reloadedTimeline = mock(HoodieActiveTimeline.class);
    when(metaClient.reloadActiveTimeline()).thenReturn(reloadedTimeline);

    HoodieActiveTimeline result = HoodieBackedTableMetadataWriter.runPendingTableServicesOperationsAndRefreshTimeline(
        metaClient, writeClient, false, Option.empty());

    // Compaction delegated to TSM → skipped; logcompaction not delegated → executed
    verify(writeClient, times(0)).runAnyPendingCompactions();
    verify(writeClient, times(1)).runAnyPendingLogCompactions();
    assertSame(reloadedTimeline, result);
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

  @Test
  void exercisesNoOpAndUnsupportedBaseWriterPaths() throws Exception {
    // Cover base no-op hooks and fail fast when an indexer is disabled.
    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> writer =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);

    assertDoesNotThrow(() -> writer.buildMetadataPartitions(
        mock(HoodieEngineContext.class), Collections.emptyList(), "001"));
    assertThrows(UnsupportedOperationException.class,
        () -> writer.streamWriteToMetadataTable(null, "001"));
    assertThrows(UnsupportedOperationException.class,
        () -> writer.secondaryWriteToMetadataTablePartitions(Collections.emptyList(), "001"));

    HoodieWriteConfig metadataWriteConfig = mock(HoodieWriteConfig.class);
    when(metadataWriteConfig.getBasePath()).thenReturn("/tmp/metadata");
    writer.metadataWriteConfig = metadataWriteConfig;
    setField(writer, "enabledIndexerMap", Collections.emptyMap());
    HoodieIndexPartitionInfo disabledPartition = HoodieIndexPartitionInfo.newBuilder()
        .setVersion(1)
        .setMetadataPartitionPath(MetadataPartitionType.COLUMN_STATS.getPartitionPath())
        .setIndexUptoInstant("001")
        .build();
    assertThrows(HoodieIndexException.class,
        () -> writer.buildMetadataPartitions(
            mock(HoodieEngineContext.class), Collections.singletonList(disabledPartition), "001"));
  }

  @Test
  void fallsBackToConfiguredIndexersWhenTableConfigHasNoMetadataPartitions() throws Exception {
    // A fresh table must derive active indexers from writer configuration.
    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> writer =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(dataMetaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(new HashSet<>());
    when(tableConfig.getMetadataPartitionsInflight()).thenReturn(Collections.emptySet());
    writer.dataMetaClient = dataMetaClient;
    Map<MetadataPartitionType, Indexer> enabledIndexers = new HashMap<>();
    enabledIndexers.put(MetadataPartitionType.FILES, mock(Indexer.class));
    setField(writer, "enabledIndexerMap", enabledIndexers);

    assertDoesNotThrow(() -> writer.processAndCommit("001", Collections::emptyList));

    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> streamingWriter =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieData<HoodieRecord> emptyData = mock(HoodieData.class);
    when(engineContext.<HoodieRecord>emptyHoodieData()).thenReturn(emptyData);
    streamingWriter.dataMetaClient = dataMetaClient;
    setField(streamingWriter, "engineContext", engineContext);
    setField(streamingWriter, "enabledIndexerMap", Collections.emptyMap());
    assertSame(emptyData, streamingWriter.streamWriteToMetadataPartitions(
        mock(HoodieData.class), Collections.emptySet(), "001"));
  }

  @Test
  void wrapsMetadataReaderAndFileSliceReadFailures() throws Exception {
    // Reader setup and lazy file listing must preserve the public exception contract.
    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> writer =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    writer.dataWriteConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/missing-table").build();
    writer.dataMetaClient = mock(HoodieTableMetaClient.class);
    Method maybeReinitializeReader =
        HoodieBackedTableMetadataWriter.class.getDeclaredMethod("mayBeReinitMetadataReader");
    maybeReinitializeReader.setAccessible(true);
    InvocationTargetException readerFailure = assertThrows(
        InvocationTargetException.class, () -> maybeReinitializeReader.invoke(writer));
    assertTrue(readerFailure.getCause() instanceof HoodieException);

    HoodieBackedTableMetadata metadata = mock(HoodieBackedTableMetadata.class);
    HoodieTableFileSystemView metadataView = mock(HoodieTableFileSystemView.class);
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(dataMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant())
        .thenReturn(Option.empty());
    when(metadata.getMetadataFileSystemView()).thenReturn(metadataView);
    when(metadata.getAllPartitionPaths()).thenThrow(new IOException("listing failed"));
    writer.metadata = metadata;
    writer.dataMetaClient = dataMetaClient;
    setField(writer, "metadataView", metadataView);
    Method getLazyMergedFileSlices =
        HoodieBackedTableMetadataWriter.class.getDeclaredMethod("getLazyMergedFileSlices");
    getLazyMergedFileSlices.setAccessible(true);
    Lazy<?> lazyFileSlices = (Lazy<?>) getLazyMergedFileSlices.invoke(writer);
    assertThrows(HoodieIOException.class, lazyFileSlices::get);
  }

  @Test
  void detectsEmptyMetadataTimelineAndHandlesMissingMetadataTable(@TempDir Path tempDir) throws Exception {
    // Missing MDT state requires bootstrap without trusting stale table config.
    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> writer =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    Method isBootstrapNeeded = HoodieBackedTableMetadataWriter.class
        .getDeclaredMethod("isBootstrapNeeded", Option.class);
    isBootstrapNeeded.setAccessible(true);
    assertTrue((boolean) isBootstrapNeeded.invoke(writer, Option.empty()));

    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(dataMetaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isMetadataTableAvailable()).thenReturn(true);
    writer.storageConf = org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf();
    writer.dataWriteConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("data-table").toString())
        .build();
    writer.metadataWriteConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("missing-metadata-table").toString())
        .build();
    Method metadataTableExists = HoodieBackedTableMetadataWriter.class
        .getDeclaredMethod("metadataTableExists", HoodieTableMetaClient.class);
    metadataTableExists.setAccessible(true);
    assertFalse((boolean) metadataTableExists.invoke(writer, dataMetaClient));
  }

  @Test
  void ignoresIOExceptionWhileRemovingPendingIndexInstant() throws Exception {
    // A corrupt pending index plan must not block partition cleanup.
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    HoodieInstant pendingIndex = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.INDEXING_ACTION, "001");
    when(metaClient.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(metaClient.reloadActiveTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(timeline);
    when(timeline.filterPendingIndexTimeline()).thenReturn(timeline);
    when(timeline.getInstantsAsStream()).thenReturn(Stream.of(pendingIndex));
    when(timeline.readIndexPlan(pendingIndex)).thenThrow(new IOException("cannot read plan"));
    Method deletePendingIndexingInstant = HoodieBackedTableMetadataWriter.class
        .getDeclaredMethod("deletePendingIndexingInstant", HoodieTableMetaClient.class, String.class);
    deletePendingIndexingInstant.setAccessible(true);

    assertDoesNotThrow(() -> deletePendingIndexingInstant.invoke(null, metaClient, "column_stats"));
  }

  @Test
  void wrapsRestorePlanReadFailure() throws Exception {
    // Restore-plan I/O failures must surface as HoodieIOException.
    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> writer =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    HoodieBackedTableMetadata metadata = mock(HoodieBackedTableMetadata.class);
    HoodieTableFileSystemView metadataView = mock(HoodieTableFileSystemView.class);
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    when(metadata.getMetadataFileSystemView()).thenReturn(metadataView);
    when(dataMetaClient.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(dataMetaClient.getActiveTimeline()).thenReturn(timeline);
    when(timeline.readRestorePlan(any())).thenThrow(new IOException("cannot read restore plan"));
    writer.metadata = metadata;
    writer.metadataMetaClient = metadataMetaClient;
    writer.dataMetaClient = dataMetaClient;

    assertThrows(HoodieIOException.class,
        () -> writer.update(mock(HoodieRestoreMetadata.class), "001"));
  }

  @Test
  void rejectsPendingMetadataCompactionAndWrapsCloseFailures() {
    // Pending compaction blocks scheduling, while close errors remain visible.
    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> writer =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    HoodieWriteConfig metadataWriteConfig = mock(HoodieWriteConfig.class);
    when(metadataWriteConfig.isLogCompactionEnabled()).thenReturn(true);
    writer.metadataWriteConfig = metadataWriteConfig;
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline metadataTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    HoodieInstant pendingCompaction = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "001");
    when(metadataMetaClient.getActiveTimeline()).thenReturn(metadataTimeline);
    when(metadataTimeline.filterPendingLogCompactionTimeline().firstInstant()).thenReturn(Option.empty());
    when(metadataTimeline.filterPendingCompactionTimeline().firstInstant()).thenReturn(Option.of(pendingCompaction));
    writer.metadataMetaClient = metadataMetaClient;

    assertThrows(HoodieException.class, () -> {
      doThrow(new HoodieException("close failed")).when(writer).close();
      writer.closeInternal();
    });
    assertFalse(writer.validateCompactionScheduling(Option.empty(), "002"));
  }

  @Test
  void compactIfNecessaryHandlesSkipDelegationAndFailures() {
    // Exercise skip, delegation, and failure propagation for both compaction types.
    Properties tableServiceManagerProperties = new Properties();
    tableServiceManagerProperties.put(
        HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ENABLED.key(), "true");
    tableServiceManagerProperties.put(
        HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(), "compaction,logcompaction");
    HoodieTableServiceManagerConfig tableServiceManagerConfig =
        HoodieTableServiceManagerConfig.newBuilder().fromProperties(tableServiceManagerProperties).build();
    HoodieWriteConfig metadataWriteConfig = mock(HoodieWriteConfig.class);
    when(metadataWriteConfig.getTableServiceManagerConfig()).thenReturn(tableServiceManagerConfig);
    when(metadataWriteConfig.isLogCompactionEnabled()).thenReturn(true);

    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(dataMetaClient.reloadActiveTimeline().filterInflightsAndRequested()
        .filter(any()).firstInstant()).thenReturn(Option.empty());
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline metadataTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(metadataMetaClient.getActiveTimeline()).thenReturn(metadataTimeline);
    when(metadataTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    when(completedTimeline.containsInstant(any(String.class)))
        .thenAnswer(invocation -> "100".equals(invocation.getArgument(0)));

    HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<?>> writer =
        mock(HoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    writer.dataMetaClient = dataMetaClient;
    writer.metadataMetaClient = metadataMetaClient;
    writer.metadataWriteConfig = metadataWriteConfig;
    writer.metrics = Option.empty();

    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    when(writeClient.createNewInstantTime(false)).thenReturn("100", "200", "300", "400");
    when(writeClient.scheduleCompactionAtInstant("200", Option.empty())).thenReturn(true);
    when(writeClient.scheduleCompactionAtInstant("300", Option.empty()))
        .thenThrow(new HoodieException("compaction failed"));
    when(writeClient.scheduleCompactionAtInstant("400", Option.empty())).thenReturn(false);
    when(writeClient.scheduleLogCompaction(Option.empty()))
        .thenReturn(Option.of("201"))
        .thenThrow(new HoodieException("log compaction failed"));

    writer.compactIfNecessary(writeClient, Option.empty());
    writer.compactIfNecessary(writeClient, Option.empty());
    assertThrows(HoodieException.class, () -> writer.compactIfNecessary(writeClient, Option.empty()));
    assertThrows(HoodieException.class, () -> writer.compactIfNecessary(writeClient, Option.empty()));
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    // Exercise private failure paths without changing production visibility.
    java.lang.reflect.Field field = HoodieBackedTableMetadataWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
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
    when(writeConfig.getTableServiceManagerConfig()).thenReturn(HoodieTableServiceManagerConfig.newBuilder().build());

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
}
