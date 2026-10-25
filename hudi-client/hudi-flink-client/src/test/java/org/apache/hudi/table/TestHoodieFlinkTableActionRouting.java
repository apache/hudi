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

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieInlineLogAppendHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.clean.CleanPlanActionExecutor;
import org.apache.hudi.table.action.cluster.ClusteringPlanActionExecutor;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.FlinkDeletePreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkInsertOverwriteCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkInsertOverwriteTableCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkPartitionTTLActionExecutor;
import org.apache.hudi.table.action.commit.FlinkUpsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.FlinkUpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.delta.FlinkUpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.action.commit.delta.FlinkUpsertPreppedDeltaCommitActionExecutor;
import org.apache.hudi.table.action.compact.RunCompactionActionExecutor;
import org.apache.hudi.table.action.compact.ScheduleCompactionActionExecutor;
import org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests Flink table action routing and explicitly unsupported engine APIs. */
@SuppressWarnings({"rawtypes", "unchecked"})
class TestHoodieFlinkTableActionRouting extends HoodieFlinkClientTestHarness {

  @BeforeEach
  void setUp() {
    initPath();
    initFileSystem();
  }

  @AfterEach
  void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  void testCopyOnWriteUnsupportedActionsFailFast() throws IOException {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieFlinkCopyOnWriteTable table = new HoodieFlinkCopyOnWriteTable(config(), context, metaClient);

    assertUnsupported(() -> table.upsert(context, "001", Collections.emptyList()));
    assertUnsupported(() -> table.insert(context, "001", Collections.emptyList()));
    assertUnsupported(() -> table.bulkInsert(context, "001", Collections.emptyList(), Option.empty()));
    assertUnsupported(() -> table.delete(context, "001", Collections.<HoodieKey>emptyList()));
    assertUnsupported(() -> table.deletePrepped(context, "001", Collections.<HoodieRecord>emptyList()));
    assertUnsupported(() -> table.upsertPrepped(context, "001", Collections.<HoodieRecord>emptyList()));
    assertUnsupported(() -> table.insertPrepped(context, "001", Collections.<HoodieRecord>emptyList()));
    assertUnsupported(() -> table.bulkInsertPrepped(
        context, "001", Collections.<HoodieRecord>emptyList(), Option.empty()));
    assertUnsupported(() -> table.insertOverwrite(context, "001", Collections.<HoodieRecord>emptyList()));
    assertUnsupported(() -> table.insertOverwriteTable(context, "001", Collections.<HoodieRecord>emptyList()));
    assertUnsupported(() -> table.scheduleCompaction(context, "001", Option.empty()));
    assertUnsupported(() -> table.compact(context, "001"));
    assertUnsupported(() -> table.cluster(context, "001"));
    assertUnsupported(() -> table.bootstrap(context, Option.empty()));
    assertUnsupported(() -> table.rollbackBootstrap(context, "001"));
    assertUnsupported(() -> table.scheduleIndexing(context, "001", Collections.emptyList(), Collections.emptyList()));
    assertUnsupported(() -> table.index(context, "001"));
    assertUnsupported(() -> table.savepoint(context, "001", "user", "comment"));
    assertUnsupported(() -> table.scheduleRestore(context, "002", "001"));
    assertUnsupported(() -> table.restore(context, "002", "001"));
  }

  @Test
  void testCopyOnWriteRoutesSupportedPlanningActions() throws IOException {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieFlinkCopyOnWriteTable table = new HoodieFlinkCopyOnWriteTable(config(), context, metaClient);

    try (MockedConstruction<ClusteringPlanActionExecutor> ignored = Mockito.mockConstruction(
        ClusteringPlanActionExecutor.class,
        (executor, constructionContext) -> when(executor.execute()).thenReturn(Option.empty()))) {
      assertFalse(table.scheduleClustering(context, "001", Option.empty()).isPresent());
    }
    try (MockedConstruction<CleanPlanActionExecutor> ignored = Mockito.mockConstruction(
        CleanPlanActionExecutor.class,
        (executor, constructionContext) -> when(executor.execute()).thenReturn(Option.empty()))) {
      assertFalse(table.createCleanerPlan(context, Option.empty()).isPresent());
    }
    try (MockedConstruction<FlinkPartitionTTLActionExecutor> ignored = Mockito.mockConstruction(
        FlinkPartitionTTLActionExecutor.class,
        (executor, constructionContext) -> {
          HoodieWriteMetadata<List<WriteStatus>> metadata = new HoodieWriteMetadata<>();
          metadata.setWriteStatuses(Collections.emptyList());
          when(executor.execute()).thenReturn(metadata);
        })) {
      assertEquals(Collections.emptyList(), table.managePartitionTTL(context, "002").getWriteStatuses());
    }
    Option<HoodieRollbackPlan> rollbackPlan = Option.of(mock(HoodieRollbackPlan.class));
    assertResultPropagated(BaseRollbackPlanActionExecutor.class, rollbackPlan,
        () -> table.scheduleRollback(
            context, "003", mock(HoodieInstant.class), false, false, false));
  }

  @Test
  void testCopyOnWriteRoutesWriteActionsAndCompactionInsert() throws IOException {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieFlinkCopyOnWriteTable table = new HoodieFlinkCopyOnWriteTable(config(), context, metaClient);
    HoodieWriteHandle writeHandle = mock(HoodieWriteHandle.class);
    BucketInfo bucketInfo = new BucketInfo(BucketType.INSERT, "file-1", "partition");

    assertWriteMetadataPropagated(FlinkUpsertCommitActionExecutor.class,
        () -> table.upsert(context, writeHandle, bucketInfo, "001", Collections.emptyIterator()));
    assertWriteMetadataPropagated(FlinkInsertCommitActionExecutor.class,
        () -> table.insert(context, writeHandle, bucketInfo, "001", Collections.emptyIterator()));
    assertWriteMetadataPropagated(FlinkDeletePreppedCommitActionExecutor.class,
        () -> table.deletePrepped(context, writeHandle, bucketInfo, "001", Collections.emptyList()));
    assertWriteMetadataPropagated(FlinkUpsertPreppedCommitActionExecutor.class,
        () -> table.upsertPrepped(context, writeHandle, bucketInfo, "001", Collections.emptyList()));
    assertWriteMetadataPropagated(FlinkInsertPreppedCommitActionExecutor.class,
        () -> table.insertPrepped(context, writeHandle, bucketInfo, "001", Collections.emptyList()));
    assertWriteMetadataPropagated(FlinkInsertOverwriteCommitActionExecutor.class,
        () -> table.insertOverwrite(context, writeHandle, bucketInfo, "001", Collections.emptyIterator()));
    assertWriteMetadataPropagated(FlinkInsertOverwriteTableCommitActionExecutor.class,
        () -> table.insertOverwriteTable(context, writeHandle, bucketInfo, "001", Collections.emptyIterator()));

    try (MockedConstruction<HoodieCreateHandle> ignored = Mockito.mockConstruction(HoodieCreateHandle.class)) {
      assertEquals(Collections.emptyList(),
          table.handleInsert("001", "partition", "file-1", Collections.emptyMap()).next());
    }
  }

  @Test
  void testMergeOnReadValidatesHandlesAndRoutesScheduling() throws IOException {
    initMetaClient(HoodieTableType.MERGE_ON_READ);
    HoodieFlinkMergeOnReadTable table = new HoodieFlinkMergeOnReadTable(config(), context, metaClient);
    HoodieWriteHandle writeHandle = mock(HoodieWriteHandle.class);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, "file-1", "partition");

    assertThrows(IllegalArgumentException.class,
        () -> table.upsert(context, writeHandle, bucketInfo, "001", Collections.emptyIterator()));
    assertThrows(IllegalArgumentException.class,
        () -> table.upsertPrepped(context, writeHandle, bucketInfo, "001", Collections.emptyList()));

    try (MockedConstruction<ScheduleCompactionActionExecutor> mocked = Mockito.mockConstruction(
        ScheduleCompactionActionExecutor.class,
        (executor, constructionContext) -> when(executor.execute()).thenReturn(Option.empty()))) {
      assertFalse(table.scheduleCompaction(context, "002", Option.empty()).isPresent());
      assertFalse(table.scheduleLogCompaction(context, "003", Option.empty()).isPresent());
      assertEquals(2, mocked.constructed().size());
    }
  }

  @Test
  void testMergeOnReadRoutesAppendAndCompactionActions() throws IOException {
    initMetaClient(HoodieTableType.MERGE_ON_READ);
    HoodieFlinkMergeOnReadTable table = new HoodieFlinkMergeOnReadTable(config(), context, metaClient);
    HoodieAppendHandle appendHandle = mock(HoodieAppendHandle.class);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, "file-1", "partition");

    assertWriteMetadataPropagated(FlinkUpsertDeltaCommitActionExecutor.class,
        () -> table.upsert(context, appendHandle, bucketInfo, "001", Collections.emptyIterator()));
    assertWriteMetadataPropagated(FlinkUpsertPreppedDeltaCommitActionExecutor.class,
        () -> table.upsertPrepped(context, appendHandle, bucketInfo, "001", Collections.emptyList()));
    assertWriteMetadataPropagated(FlinkUpsertDeltaCommitActionExecutor.class,
        () -> table.insert(context, appendHandle, bucketInfo, "001", Collections.emptyIterator()));

    HoodieWriteMetadata compactionMetadata = new HoodieWriteMetadata();
    compactionMetadata.setWriteStatuses(HoodieListData.eager(Collections.emptyList()));
    try (MockedConstruction<RunCompactionActionExecutor> ignored = Mockito.mockConstruction(
        RunCompactionActionExecutor.class,
        (executor, constructionContext) -> when(executor.execute()).thenReturn(compactionMetadata))) {
      assertEquals(Collections.emptyList(), table.compact(context, "002").getWriteStatuses());
      assertEquals(Collections.emptyList(), table.logCompact(context, "003").getWriteStatuses());
    }

    try (MockedConstruction<HoodieInlineLogAppendHandle> ignored = Mockito.mockConstruction(
        HoodieInlineLogAppendHandle.class,
        (handle, constructionContext) -> when(handle.close()).thenReturn(Collections.emptyList()))) {
      assertEquals(Collections.emptyList(),
          table.handleInsertsForLogCompaction(
              "004", "partition", "file-1", Collections.emptyMap(), Collections.emptyMap()).next());
    }
  }

  private <E extends BaseActionExecutor> void assertWriteMetadataPropagated(
      Class<E> executorClass, Supplier<Object> invocation) {
    HoodieWriteMetadata<List<WriteStatus>> metadata = new HoodieWriteMetadata<>();
    metadata.setWriteStatuses(Collections.emptyList());
    assertResultPropagated(executorClass, metadata, invocation);
  }

  private <E extends BaseActionExecutor> void assertResultPropagated(
      Class<E> executorClass, Object expected, Supplier<Object> invocation) {
    try (MockedConstruction<E> mocked = Mockito.mockConstruction(
        executorClass,
        (executor, constructionContext) -> when(executor.execute()).thenReturn(expected))) {
      assertSame(expected, invocation.get());
      assertEquals(1, mocked.constructed().size());
    }
  }

  private HoodieWriteConfig config() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEngineType(EngineType.FLINK)
        .withWriteTableVersion(HoodieTableVersion.NINE.versionCode())
        .build();
  }

  private void assertUnsupported(ThrowingRunnable runnable) {
    assertThrows(HoodieNotSupportedException.class, runnable::run);
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
