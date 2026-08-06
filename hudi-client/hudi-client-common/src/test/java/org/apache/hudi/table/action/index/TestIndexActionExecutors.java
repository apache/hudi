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

package org.apache.hudi.table.action.index;

import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestIndexActionExecutors {

  private HoodieEngineContext context;
  private HoodieTable table;
  private HoodieTableMetaClient metaClient;
  private HoodieTableConfig tableConfig;
  private HoodieActiveTimeline activeTimeline;
  private HoodieStorage storage;

  @BeforeEach
  void setUp() {
    context = mock(HoodieEngineContext.class);
    table = mock(HoodieTable.class);
    metaClient = mock(HoodieTableMetaClient.class);
    tableConfig = mock(HoodieTableConfig.class);
    activeTimeline = mock(HoodieActiveTimeline.class);
    storage = mock(HoodieStorage.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(table.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(table.getStorage()).thenReturn(storage);
    doReturn(getDefaultStorageConf()).when(storage).getConf();
    doReturn(getDefaultStorageConf()).when(context).getStorageConf();
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getInstantGenerator()).thenReturn(INSTANT_GENERATOR);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(metaClient.reloadActiveTimeline()).thenReturn(activeTimeline);
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.emptySet());
    when(tableConfig.getMetadataPartitionsInflight()).thenReturn(Collections.emptySet());
  }

  @Test
  void testScheduleRejectsSingleWriterConfiguration() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/table").build();
    ScheduleIndexActionExecutor executor = new ScheduleIndexActionExecutor(
        context, config, table, "002", Collections.singletonList(MetadataPartitionType.COLUMN_STATS), Collections.emptyList());

    HoodieIndexException exception = assertThrows(HoodieIndexException.class, executor::execute);
    assertTrue(exception.getMessage().contains(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key()));
  }

  @Test
  void testScheduleCreatesPlanAndIsIdempotent() {
    HoodieWriteConfig config = multiWriterConfig();
    HoodieInstant completed = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001");
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(activeTimeline.getContiguousCompletedWriteTimeline()).thenReturn(completedTimeline);
    when(completedTimeline.lastInstant()).thenReturn(Option.of(completed));
    ScheduleIndexActionExecutor executor = new ScheduleIndexActionExecutor(
        context, config, table, "002", Collections.singletonList(MetadataPartitionType.COLUMN_STATS), Collections.emptyList());

    Option<HoodieIndexPlan> plan = executor.execute();

    assertTrue(plan.isPresent());
    assertEquals(1, plan.get().getIndexPartitionInfos().size());
    assertEquals(MetadataPartitionType.COLUMN_STATS.getPartitionPath(),
        plan.get().getIndexPartitionInfos().get(0).getMetadataPartitionPath());
    assertEquals("001", plan.get().getIndexPartitionInfos().get(0).getIndexUptoInstant());
    verify(activeTimeline).saveToPendingIndexAction(any(HoodieInstant.class), any(HoodieIndexPlan.class));

    when(tableConfig.getMetadataPartitions()).thenReturn(Set.of(MetadataPartitionType.COLUMN_STATS.getPartitionPath()));
    assertFalse(executor.execute().isPresent());
  }

  @Test
  void testScheduleAbortsWhenPendingPlanCannotBeSaved() {
    HoodieWriteConfig config = multiWriterConfig();
    HoodieInstant completed = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001");
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(activeTimeline.getContiguousCompletedWriteTimeline()).thenReturn(completedTimeline);
    when(completedTimeline.lastInstant()).thenReturn(Option.of(completed));
    doThrow(new HoodieIOException("write failed"))
        .when(activeTimeline).saveToPendingIndexAction(any(HoodieInstant.class), any(HoodieIndexPlan.class));

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.getInflightAndCompletedMetadataPartitions(tableConfig))
          .thenReturn(Collections.emptySet());
      metadataUtil.when(() -> HoodieTableMetadataUtil.metadataPartitionExists(
          metaClient.getBasePath(), context, MetadataPartitionType.COLUMN_STATS.getPartitionPath())).thenReturn(false);

      Option<HoodieIndexPlan> result = new ScheduleIndexActionExecutor(
          context, config, table, "002", Collections.singletonList(MetadataPartitionType.COLUMN_STATS), Collections.emptyList()).execute();

      assertFalse(result.isPresent());
      verify(activeTimeline).deleteInstantFileIfExists(INSTANT_GENERATOR.getIndexRequestedInstant("002"));
    }
  }

  @Test
  void testRunRejectsInvalidConfigurationAndMissingInstant() {
    RunIndexActionExecutor invalidExecutor = new RunIndexActionExecutor(
        context, HoodieWriteConfig.newBuilder().withPath("/table").build(), table, "002");
    assertThrows(HoodieIndexException.class, invalidExecutor::execute);

    HoodieWriteConfig config = multiWriterConfig();
    when(activeTimeline.filterPendingIndexTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filter(any())).thenReturn(activeTimeline);
    when(activeTimeline.lastInstant()).thenReturn(Option.empty());
    assertThrows(HoodieIndexException.class,
        () -> new RunIndexActionExecutor(context, config, table, "002").execute());
  }

  @Test
  void testRunRejectsUnreadableAndEmptyPlans() throws IOException {
    HoodieWriteConfig config = multiWriterConfig();
    HoodieInstant requested = requestedIndexInstant();
    stubRequestedIndexInstant(requested);
    doThrow(new IOException("read failed")).when(activeTimeline).readIndexPlan(requested);
    assertThrows(HoodieIndexException.class,
        () -> new RunIndexActionExecutor(context, config, table, "002").execute());

    doReturn(new HoodieIndexPlan(1, Collections.emptyList())).when(activeTimeline).readIndexPlan(requested);
    assertThrows(HoodieIndexException.class,
        () -> new RunIndexActionExecutor(context, config, table, "002").execute());
  }

  @Test
  void testRunRejectsPartitionThatAlreadyExists() throws IOException {
    HoodieWriteConfig config = multiWriterConfig();
    HoodieInstant requested = requestedIndexInstant();
    stubRequestedIndexInstant(requested);
    HoodieIndexPartitionInfo info = new HoodieIndexPartitionInfo(
        1, MetadataPartitionType.COLUMN_STATS.getPartitionPath(), "001", Collections.emptyMap());
    when(activeTimeline.readIndexPlan(requested)).thenReturn(new HoodieIndexPlan(1, Collections.singletonList(info)));
    when(tableConfig.getMetadataPartitions()).thenReturn(Set.of(MetadataPartitionType.COLUMN_STATS.getPartitionPath()));

    assertThrows(HoodieIndexException.class,
        () -> new RunIndexActionExecutor(context, config, table, "002").execute());
  }

  @Test
  void testRunInitializesFilesPartitionAndCompletesIndexInstant() throws IOException {
    HoodieWriteConfig config = multiWriterConfig();
    HoodieInstant requested = requestedIndexInstant();
    stubRequestedIndexInstant(requested);
    HoodieIndexPartitionInfo info = new HoodieIndexPartitionInfo(
        1, MetadataPartitionType.FILES.getPartitionPath(), "001", Collections.emptyMap());
    when(activeTimeline.readIndexPlan(requested)).thenReturn(new HoodieIndexPlan(1, Collections.singletonList(info)));
    HoodieTableMetadataWriter writer = mock(HoodieTableMetadataWriter.class);
    when(table.getIndexingMetadataWriter("002")).thenReturn(Option.of(writer));

    Option<HoodieIndexCommitMetadata> result;
    try (MockedConstruction<TransactionManager> ignored = mockConstruction(TransactionManager.class)) {
      result = new RunIndexActionExecutor(context, config, table, "002").execute();
    }

    assertTrue(result.isPresent());
    List<HoodieIndexPartitionInfo> completedPartitions = result.get().getIndexPartitionInfos();
    assertEquals(1, completedPartitions.size());
    assertEquals("001", completedPartitions.get(0).getIndexUptoInstant());
    verify(activeTimeline).transitionIndexRequestedToInflight(requested);
    verify(tableConfig).setMetadataPartitionState(metaClient, MetadataPartitionType.FILES.getPartitionPath(), true);
    verify(activeTimeline).saveAsComplete(any(Boolean.class), any(HoodieInstant.class), any(Option.class));
  }

  @Test
  void testRunBuildsPartitionAndCatchesUpConcurrentCommits() throws Exception {
    HoodieWriteConfig config = multiWriterConfig();
    HoodieInstant requested = requestedIndexInstant();
    stubRequestedIndexInstant(requested);
    HoodieIndexPartitionInfo info = new HoodieIndexPartitionInfo(
        1, MetadataPartitionType.COLUMN_STATS.getPartitionPath(), "001", Collections.emptyMap());
    when(activeTimeline.readIndexPlan(requested)).thenReturn(new HoodieIndexPlan(1, Collections.singletonList(info)));
    HoodieTableMetadataWriter writer = mock(HoodieTableMetadataWriter.class);
    when(table.getIndexingMetadataWriter("002")).thenReturn(Option.of(writer));
    when(table.getConfig()).thenReturn(config);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));

    HoodieTimeline emptyTimeline = mock(HoodieTimeline.class);
    when(emptyTimeline.filterInflightsAndRequested()).thenReturn(emptyTimeline);
    when(emptyTimeline.findInstantsBefore("001")).thenReturn(emptyTimeline);
    when(emptyTimeline.firstInstant()).thenReturn(Option.empty());
    when(emptyTimeline.findInstantsAfter(any())).thenReturn(emptyTimeline);
    when(emptyTimeline.filterCompletedInstants()).thenReturn(emptyTimeline);
    when(emptyTimeline.getInstantsAsStream()).thenAnswer(ignored -> java.util.stream.Stream.empty());
    when(activeTimeline.getTimelineOfActions(any())).thenReturn(emptyTimeline);
    when(activeTimeline.findInstantsAfter("001")).thenReturn(emptyTimeline);
    HoodieArchivedTimeline archivedTimeline = mock(HoodieArchivedTimeline.class);
    when(archivedTimeline.getInstantsAsStream()).thenAnswer(ignored -> java.util.stream.Stream.empty());
    when(metaClient.getArchivedTimeline()).thenReturn(archivedTimeline);

    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieArchivedTimeline metadataArchivedTimeline = mock(HoodieArchivedTimeline.class);
    when(metadataArchivedTimeline.filterCompletedInstants()).thenReturn(metadataArchivedTimeline);
    when(metadataArchivedTimeline.findInstantsAfter("001")).thenReturn(metadataArchivedTimeline);
    when(metadataArchivedTimeline.getInstantsAsStream()).thenAnswer(ignored -> java.util.stream.Stream.empty());
    when(metadataMetaClient.getArchivedTimeline()).thenReturn(metadataArchivedTimeline);
    HoodieActiveTimeline metadataActiveTimeline = mock(HoodieActiveTimeline.class);
    when(metadataActiveTimeline.filterCompletedInstants()).thenReturn(metadataActiveTimeline);
    when(metadataActiveTimeline.findInstantsAfter("001")).thenReturn(metadataActiveTimeline);
    when(metadataActiveTimeline.getInstantsAsStream()).thenAnswer(ignored -> java.util.stream.Stream.empty());
    when(metadataMetaClient.reloadActiveTimeline()).thenReturn(metadataActiveTimeline);
    HoodieTableMetaClient.Builder builder = mock(HoodieTableMetaClient.Builder.class, Mockito.RETURNS_SELF);
    when(builder.build()).thenReturn(metadataMetaClient);
    IndexingCatchupTask catchupTask = mock(IndexingCatchupTask.class);

    Option<HoodieIndexCommitMetadata> result;
    try (MockedStatic<HoodieTableMetaClient> metaClientStatic = mockStatic(HoodieTableMetaClient.class);
         MockedStatic<IndexingCatchupTaskFactory> catchupFactory = mockStatic(IndexingCatchupTaskFactory.class);
         MockedConstruction<TransactionManager> ignoredTxn = mockConstruction(TransactionManager.class);
         MockedConstruction<HoodieHeartbeatClient> ignoredHeartbeat = mockConstruction(HoodieHeartbeatClient.class)) {
      metaClientStatic.when(HoodieTableMetaClient::builder).thenReturn(builder);
      catchupFactory.when(() -> IndexingCatchupTaskFactory.createCatchupTask(
          any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(catchupTask);

      result = new RunIndexActionExecutor(context, config, table, "002").execute();
    }

    assertTrue(result.isPresent());
    verify(writer).buildMetadataPartitions(context, Collections.singletonList(info), "002");
    verify(catchupTask).run();
    verify(writer).close();
  }

  @Test
  void testRunAbortsAndCleansPartialIndexOnTimelineFailure() throws IOException {
    HoodieWriteConfig config = multiWriterConfig();
    HoodieInstant requested = requestedIndexInstant();
    stubRequestedIndexInstant(requested);
    String partition = MetadataPartitionType.FILES.getPartitionPath();
    HoodieIndexPartitionInfo info = new HoodieIndexPartitionInfo(1, partition, "001", Collections.emptyMap());
    when(activeTimeline.readIndexPlan(requested)).thenReturn(new HoodieIndexPlan(1, Collections.singletonList(info)));
    when(table.getIndexingMetadataWriter("002")).thenReturn(Option.of(mock(HoodieTableMetadataWriter.class)));
    doAnswer(ignored -> {
      throw new IOException("timeline failure");
    })
        .when(activeTimeline).saveAsComplete(any(Boolean.class), any(HoodieInstant.class), any(Option.class));
    when(tableConfig.getMetadataPartitionsInflight()).thenReturn(new HashSet<>(Collections.singleton(partition)));
    when(tableConfig.getMetadataPartitions()).thenReturn(new HashSet<>(Collections.singleton(partition)));
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));

    try (MockedStatic<HoodieTableConfig> tableConfigStatic = mockStatic(HoodieTableConfig.class);
         MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class);
         MockedConstruction<TransactionManager> ignored = mockConstruction(TransactionManager.class)) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.metadataPartitionExists(
          metaClient.getBasePath(), context, partition)).thenReturn(true);

      assertThrows(HoodieIndexException.class,
          () -> new RunIndexActionExecutor(context, config, table, "002").execute());

      metadataUtil.verify(() -> HoodieTableMetadataUtil.deleteMetadataPartition(metaClient.getBasePath(), context, partition));
      tableConfigStatic.verify(() -> HoodieTableConfig.update(storage, metaClient.getMetaPath(), tableConfig.getProps()));
    }
    verify(activeTimeline).deleteInstantFileIfExists(INSTANT_GENERATOR.getIndexInflightInstant("002"));
  }

  private HoodieWriteConfig multiWriterConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath("/table")
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .build();
  }

  private HoodieInstant requestedIndexInstant() {
    return INSTANT_GENERATOR.getIndexRequestedInstant("002");
  }

  private void stubRequestedIndexInstant(HoodieInstant requested) {
    when(activeTimeline.filterPendingIndexTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filter(any())).thenReturn(activeTimeline);
    when(activeTimeline.lastInstant()).thenReturn(Option.of(requested));
  }
}
