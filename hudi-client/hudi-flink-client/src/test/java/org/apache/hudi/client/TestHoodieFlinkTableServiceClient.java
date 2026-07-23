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

package org.apache.hudi.client;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieFlinkTableServiceClient extends HoodieFlinkClientTestHarness {

  @BeforeEach
  void setUp() throws IOException {
    initPath();
    initFileSystem();
    initMetaClient();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testInitMetadataTableRespectsStreamingWriteFlag(boolean metadataStreamingWriteEnabled) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withStreamingWriteEnabled(metadataStreamingWriteEnabled)
            .build())
        .build();

    HoodieFlinkTable<?> table = mock(HoodieFlinkTable.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline inflightAndRequestedTimeline = mock(HoodieTimeline.class);
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(activeTimeline.filterInflightsAndRequested()).thenReturn(inflightAndRequestedTimeline);
    when(inflightAndRequestedTimeline.lastInstant()).thenReturn(Option.empty());

    FlinkHoodieBackedTableMetadataWriter metadataWriter = mock(FlinkHoodieBackedTableMetadataWriter.class);
    when(metadataWriter.isInitialized()).thenReturn(true);
    when(metadataWriter.hasPartitionsStateChanged()).thenReturn(true);

    TestableHoodieFlinkTableServiceClient tableServiceClient =
        new TestableHoodieFlinkTableServiceClient(context, writeConfig, Option.empty(), table);
    try (MockedStatic<FlinkHoodieBackedTableMetadataWriter> writerFactory =
             Mockito.mockStatic(FlinkHoodieBackedTableMetadataWriter.class)) {
      writerFactory.when(() -> FlinkHoodieBackedTableMetadataWriter.create(any(), any(), any(), any()))
          .thenReturn(metadataWriter);

      tableServiceClient.initMetadataTable();
    } finally {
      tableServiceClient.close();
    }

    verify(metadataWriter, times(metadataStreamingWriteEnabled ? 0 : 1)).performTableServices(any());
    verify(table).deleteMetadataIndexIfNecessary();
    verify(table, never()).maybeDeleteMetadataTable();
  }

  @Test
  void testInitMetadataTableWrapsMetadataWriterFailure() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
    HoodieFlinkTable<?> table = mock(HoodieFlinkTable.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline pendingTimeline = mock(HoodieTimeline.class);
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterInflightsAndRequested()).thenReturn(pendingTimeline);
    when(pendingTimeline.lastInstant()).thenReturn(Option.empty());

    TestableHoodieFlinkTableServiceClient client =
        new TestableHoodieFlinkTableServiceClient(context, writeConfig, Option.empty(), table);
    try (MockedStatic<FlinkHoodieBackedTableMetadataWriter> writerFactory =
             Mockito.mockStatic(FlinkHoodieBackedTableMetadataWriter.class)) {
      writerFactory.when(() -> FlinkHoodieBackedTableMetadataWriter.create(any(), any(), any(), any()))
          .thenThrow(new IllegalStateException("expected metadata writer failure"));
      assertThrows(HoodieException.class, client::initMetadataTable);
    } finally {
      client.close();
    }
  }

  @Test
  void testMetadataDisabledDeletesStaleMetadataTable() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieFlinkTable<?> table = mock(HoodieFlinkTable.class);
    TestableHoodieFlinkTableServiceClient client =
        new TestableHoodieFlinkTableServiceClient(context, writeConfig, Option.empty(), table);
    try {
      client.initMetadataTable();
    } finally {
      client.close();
    }

    verify(table).maybeDeleteMetadataTable();
    verify(table, never()).deleteMetadataIndexIfNecessary();
  }

  @Test
  void testOutputConversionAndNoOpHooks() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    TestableHoodieFlinkTableServiceClient client =
        new TestableHoodieFlinkTableServiceClient(context, writeConfig, Option.empty(), mock(HoodieTable.class));
    try {
      WriteStatus status = new WriteStatus(false, 0.0);
      status.setStat(new HoodieWriteStat());
      HoodieWriteMetadata<java.util.List<WriteStatus>> metadata = new HoodieWriteMetadata<>();
      metadata.setWriteStatuses(Collections.singletonList(status));

      client.callTriggerWritesAndFetchWriteStats(metadata);
      assertSame(metadata, client.callConvertToOutputMetadata(metadata));
      client.callHandleWriteErrors(Collections.singletonList(status.getStat()));
      // cluster() is intentionally unimplemented for Flink.
      assertNull(client.cluster("001", false));
      assertNotNull(client.createRealTable());
    } finally {
      client.close();
    }
  }

  @Test
  void testCompleteCompactionCommitsAndCleansMarkers() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieTable table = mock(HoodieTable.class);
    when(table.getInstantGenerator()).thenReturn(metaClient.getInstantGenerator());
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    TestableHoodieFlinkTableServiceClient client =
        new TestableHoodieFlinkTableServiceClient(context, writeConfig, Option.empty(), table);
    CompactHelpers compactHelpers = mock(CompactHelpers.class);
    WriteMarkers writeMarkers = mock(WriteMarkers.class);

    try (MockedStatic<CompactHelpers> helpersFactory = Mockito.mockStatic(CompactHelpers.class);
         MockedStatic<WriteMarkersFactory> markersFactory = Mockito.mockStatic(WriteMarkersFactory.class)) {
      helpersFactory.when(CompactHelpers::getInstance).thenReturn(compactHelpers);
      markersFactory.when(() -> WriteMarkersFactory.get(any(), any(), any())).thenReturn(writeMarkers);
      client.callCompleteCompaction(metadata, table, "20260723120000000");
    } finally {
      client.close();
    }

    verify(compactHelpers).completeInflightCompaction(table, "20260723120000000", metadata);
    verify(writeMarkers).quietDeleteMarkerDir(any(), any(Integer.class));
  }

  @Test
  void testCompleteClusteringCommitsAndCleansMarkers() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(table.getInstantGenerator()).thenReturn(metaClient.getInstantGenerator());
    HoodieInstant clusteringInstant = mock(HoodieInstant.class);
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    TestableHoodieFlinkTableServiceClient client =
        new TestableHoodieFlinkTableServiceClient(context, writeConfig, Option.empty(), table);
    WriteMarkers writeMarkers = mock(WriteMarkers.class);

    try (MockedStatic<ClusteringUtils> clusteringUtils = Mockito.mockStatic(ClusteringUtils.class);
         MockedStatic<WriteMarkersFactory> markersFactory = Mockito.mockStatic(WriteMarkersFactory.class)) {
      clusteringUtils.when(() -> ClusteringUtils.getInflightClusteringInstant(
          "20260723120000001", activeTimeline, metaClient.getInstantGenerator()))
          .thenReturn(Option.of(clusteringInstant));
      markersFactory.when(() -> WriteMarkersFactory.get(any(), any(), any())).thenReturn(writeMarkers);
      client.callCompleteClustering(metadata, table, "20260723120000001");
    } finally {
      client.close();
    }

    verify(writeMarkers).quietDeleteMarkerDir(any(), any(Integer.class));
  }

  private static class TestableHoodieFlinkTableServiceClient extends HoodieFlinkTableServiceClient<Object> {
    private final HoodieTable mockedTable;

    protected TestableHoodieFlinkTableServiceClient(HoodieFlinkEngineContext context,
                                                    HoodieWriteConfig clientConfig,
                                                    Option<EmbeddedTimelineService> timelineService,
                                                    HoodieTable mockedTable) {
      super(context, clientConfig, timelineService);
      this.mockedTable = mockedTable;
    }

    @Override
    protected HoodieTable createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
      return mockedTable;
    }

    private void callTriggerWritesAndFetchWriteStats(HoodieWriteMetadata<java.util.List<WriteStatus>> metadata) {
      triggerWritesAndFetchWriteStats(metadata);
    }

    private HoodieWriteMetadata<java.util.List<WriteStatus>> callConvertToOutputMetadata(
        HoodieWriteMetadata<java.util.List<WriteStatus>> metadata) {
      return convertToOutputMetadata(metadata);
    }

    private void callHandleWriteErrors(java.util.List<HoodieWriteStat> writeStats) {
      handleWriteErrors(writeStats, TableServiceType.COMPACT);
    }

    private HoodieTable createRealTable() {
      return super.createTable(config, storageConf, false);
    }

    private void callCompleteCompaction(
        HoodieCommitMetadata metadata, HoodieTable table, String instantTime) {
      completeCompaction(metadata, table, instantTime, Collections.emptyList());
    }

    private void callCompleteClustering(
        HoodieReplaceCommitMetadata metadata, HoodieTable table, String instantTime) {
      completeClustering(metadata, table, instantTime);
    }

    @Override
    protected void finalizeWrite(HoodieTable table, String instantTime, java.util.List<HoodieWriteStat> stats) {
      // The test covers Flink orchestration; base finalize-write behavior is covered by client-common tests.
    }

    @Override
    protected void writeTableMetadata(HoodieTable table, String instantTime, HoodieCommitMetadata metadata) {
      // The test covers Flink orchestration; metadata writer behavior is covered independently.
    }
  }
}
