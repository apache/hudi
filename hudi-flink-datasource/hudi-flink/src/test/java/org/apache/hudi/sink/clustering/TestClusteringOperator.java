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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.adapter.Utils;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.core.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.io.MergeUtils;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.HoodieRowDataParquetReader;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ClusteringOperator}.
 */
class TestClusteringOperator {

  @TempDir
  File tempDir;

  @Test
  @SuppressWarnings("unchecked")
  void testProcessClusteringPlanSynchronously() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    conf.set(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT, 2048L);
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeClient.getHoodieTable()).thenReturn(table);

    ClusteringOperator operator = new ClusteringOperator(conf, TestConfigurations.ROW_TYPE);
    assertEquals(2048L, conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT));
    ClusteringPlanEvent planEvent = event("001");

    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedConstruction<BulkInsertWriterHelper> writerHelpers =
             mockConstruction(BulkInsertWriterHelper.class, (writerHelper, context) ->
                 when(writerHelper.getWriteStatuses(anyInt())).thenReturn(Collections.emptyList()));
         OneInputStreamOperatorTestHarness<ClusteringPlanEvent, ClusteringCommitEvent> harness =
             new OneInputStreamOperatorTestHarness<>(operator, 1, 1, 0)) {
      writeClients.when(() -> FlinkWriteClients.getHoodieClientConfig(
          any(Configuration.class), eq(false), eq(false))).thenReturn(writeConfig);
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          any(Configuration.class), any(RuntimeContext.class))).thenReturn(writeClient);

      harness.open();
      harness.processElement(new StreamRecord<>(planEvent));
      operator.endInput();

      StreamRecord<ClusteringCommitEvent> output =
          (StreamRecord<ClusteringCommitEvent>) harness.getOutput().poll();
      assertEquals("001", output.getValue().getInstant());
      assertEquals("", output.getValue().getFileIds());
      assertEquals(0, output.getValue().getTaskID());
      assertFalse(output.getValue().isFailed());
      assertEquals(1, writerHelpers.constructed().size());
      verify(writerHelpers.constructed().get(0)).close();
    }

    verify(writeClient).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testAsyncFailureProducesFailedCommitEvent() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
    conf.set(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true);
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeClient.getHoodieTable()).thenReturn(table);

    ClusteringOperator operator = new ClusteringOperator(conf, TestConfigurations.ROW_TYPE);
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedConstruction<NonThrownExecutor> executors =
             mockConstruction(NonThrownExecutor.class, (executor, context) ->
                 doAnswer(invocation -> {
                   NonThrownExecutor.ExceptionHook hook = invocation.getArgument(1);
                   hook.apply("expected failure", new RuntimeException("test"));
                   return null;
                 }).when(executor).execute(
                     any(ThrowingRunnable.class),
                     any(NonThrownExecutor.ExceptionHook.class),
                     anyString(),
                     anyString(),
                     anyInt()));
         OneInputStreamOperatorTestHarness<ClusteringPlanEvent, ClusteringCommitEvent> harness =
             new OneInputStreamOperatorTestHarness<>(operator, 1, 1, 0)) {
      writeClients.when(() -> FlinkWriteClients.getHoodieClientConfig(
          any(Configuration.class), eq(false), eq(false))).thenReturn(writeConfig);
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          any(Configuration.class), any(RuntimeContext.class))).thenReturn(writeClient);

      harness.open();
      harness.processElement(new StreamRecord<>(event("002")));

      StreamRecord<ClusteringCommitEvent> output =
          (StreamRecord<ClusteringCommitEvent>) harness.getOutput().poll();
      assertEquals("002", output.getValue().getInstant());
      assertEquals("", output.getValue().getFileIds());
      assertTrue(output.getValue().isFailed());
      assertEquals(1, executors.constructed().size());
    }

    verify(writeClient).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testReadAndWriteRecordsFromBaseFiles() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieIOFactory ioFactory = mock(HoodieIOFactory.class);
    HoodieFileReaderFactory readerFactory = mock(HoodieFileReaderFactory.class);
    HoodieRowDataParquetReader fileReader = mock(HoodieRowDataParquetReader.class);
    HoodieRecord<RowData> hoodieRecord = mock(HoodieRecord.class);
    RowData row = mock(RowData.class);
    when(writeClient.getHoodieTable()).thenReturn(table);
    when(table.getStorage()).thenReturn(storage);
    when(table.getConfig()).thenReturn(writeConfig);
    when(ioFactory.getReaderFactory(HoodieRecord.HoodieRecordType.FLINK)).thenReturn(readerFactory);
    when(readerFactory.getFileReader(
        same(writeConfig), any(StoragePath.class))).thenReturn(fileReader);
    when(hoodieRecord.getData()).thenReturn(row);
    when(fileReader.getRecordIterator(any(HoodieSchema.class))).thenReturn(
        ClosableIterator.wrap(Collections.singletonList(hoodieRecord).iterator()));

    ClusteringOperation operation = new ClusteringOperation(
        tempDir.toPath().resolve("base.parquet").toString(),
        Collections.emptyList(),
        "old-file",
        "partition",
        null,
        0);
    ClusteringOperator operator = new ClusteringOperator(conf, TestConfigurations.ROW_TYPE);

    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedStatic<HoodieIOFactory> ioFactories = mockStatic(HoodieIOFactory.class);
         MockedConstruction<BulkInsertWriterHelper> writerHelpers =
             mockConstruction(BulkInsertWriterHelper.class, (writerHelper, context) ->
                 when(writerHelper.getWriteStatuses(anyInt())).thenReturn(Collections.emptyList()));
         OneInputStreamOperatorTestHarness<ClusteringPlanEvent, ClusteringCommitEvent> harness =
             new OneInputStreamOperatorTestHarness<>(operator, 1, 1, 0)) {
      writeClients.when(() -> FlinkWriteClients.getHoodieClientConfig(
          any(Configuration.class), eq(false), eq(false))).thenReturn(writeConfig);
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          any(Configuration.class), any(RuntimeContext.class))).thenReturn(writeClient);
      ioFactories.when(() -> HoodieIOFactory.getIOFactory(storage)).thenReturn(ioFactory);

      harness.open();
      harness.processElement(new StreamRecord<>(event("003", Collections.singletonList(operation))));

      verify(writerHelpers.constructed().get(0)).write(row);
      StreamRecord<ClusteringCommitEvent> output =
          (StreamRecord<ClusteringCommitEvent>) harness.getOutput().poll();
      assertEquals("old-file", output.getValue().getFileIds());
      assertFalse(output.getValue().isFailed());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCloseReaderAndWriterWhenWritingFails() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieIOFactory ioFactory = mock(HoodieIOFactory.class);
    HoodieFileReaderFactory readerFactory = mock(HoodieFileReaderFactory.class);
    HoodieRowDataParquetReader fileReader = mock(HoodieRowDataParquetReader.class);
    ClosableIterator<HoodieRecord<RowData>> recordIterator = mock(ClosableIterator.class);
    HoodieRecord<RowData> hoodieRecord = mock(HoodieRecord.class);
    RowData row = mock(RowData.class);
    when(writeClient.getHoodieTable()).thenReturn(table);
    when(table.getStorage()).thenReturn(storage);
    when(table.getConfig()).thenReturn(writeConfig);
    when(ioFactory.getReaderFactory(HoodieRecord.HoodieRecordType.FLINK)).thenReturn(readerFactory);
    when(readerFactory.getFileReader(
        same(writeConfig), any(StoragePath.class))).thenReturn(fileReader);
    when(fileReader.getRecordIterator(any(HoodieSchema.class))).thenReturn(recordIterator);
    when(recordIterator.hasNext()).thenReturn(true);
    when(recordIterator.next()).thenReturn(hoodieRecord);
    when(hoodieRecord.getData()).thenReturn(row);

    ClusteringOperation operation = new ClusteringOperation(
        tempDir.toPath().resolve("base.parquet").toString(),
        Collections.emptyList(),
        "old-file",
        "partition",
        null,
        0);
    ClusteringOperator operator = new ClusteringOperator(conf, TestConfigurations.ROW_TYPE);

    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedStatic<HoodieIOFactory> ioFactories = mockStatic(HoodieIOFactory.class);
         MockedConstruction<BulkInsertWriterHelper> writerHelpers =
             mockConstruction(BulkInsertWriterHelper.class, (writerHelper, context) ->
                 doThrow(new IOException("expected failure")).when(writerHelper).write(row));
         OneInputStreamOperatorTestHarness<ClusteringPlanEvent, ClusteringCommitEvent> harness =
             new OneInputStreamOperatorTestHarness<>(operator, 1, 1, 0)) {
      writeClients.when(() -> FlinkWriteClients.getHoodieClientConfig(
          any(Configuration.class), eq(false), eq(false))).thenReturn(writeConfig);
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          any(Configuration.class), any(RuntimeContext.class))).thenReturn(writeClient);
      ioFactories.when(() -> HoodieIOFactory.getIOFactory(storage)).thenReturn(ioFactory);

      harness.open();
      assertThrows(IOException.class, () ->
          harness.processElement(new StreamRecord<>(event("004", Collections.singletonList(operation)))));

      verify(recordIterator).close();
      verify(writerHelpers.constructed().get(0)).close();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testReadAndWriteRecordsFromFileSliceWithLogs() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    org.apache.hudi.common.table.read.HoodieRecordReader<RowData> recordReader =
        mock(org.apache.hudi.common.table.read.HoodieRecordReader.class);
    RowData row = mock(RowData.class);
    when(writeClient.getHoodieTable()).thenReturn(table);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(recordReader.getClosableIterator()).thenReturn(
        ClosableIterator.wrap(Collections.singletonList(row).iterator()));

    ClusteringOperation operation = new ClusteringOperation(
        tempDir.toPath().resolve("old-file_0-0-0_001.parquet").toString(),
        Collections.singletonList(tempDir.toPath().resolve(".old-file_001.log.1_0-0-0").toString()),
        "old-file",
        "partition",
        null,
        0);
    ClusteringOperator operator = new ClusteringOperator(conf, TestConfigurations.ROW_TYPE);

    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedStatic<MergeUtils> mergeUtils = mockStatic(MergeUtils.class);
         MockedStatic<FormatUtils> formatUtils = mockStatic(FormatUtils.class);
         MockedConstruction<BulkInsertWriterHelper> writerHelpers =
             mockConstruction(BulkInsertWriterHelper.class, (writerHelper, context) ->
                 when(writerHelper.getWriteStatuses(anyInt())).thenReturn(Collections.emptyList()));
         OneInputStreamOperatorTestHarness<ClusteringPlanEvent, ClusteringCommitEvent> harness =
             new OneInputStreamOperatorTestHarness<>(operator, 1, 1, 0)) {
      writeClients.when(() -> FlinkWriteClients.getHoodieClientConfig(
          any(Configuration.class), eq(false), eq(false))).thenReturn(writeConfig);
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          any(Configuration.class), any(RuntimeContext.class))).thenReturn(writeClient);
      mergeUtils.when(() -> MergeUtils.getMaxMemoryPerCompaction(any(), same(writeConfig)))
          .thenReturn(1024L);
      formatUtils.when(() -> FormatUtils.createRecordReader(
          same(metaClient), same(writeConfig), any(), any(), any(), any(), eq("004"),
          eq(FlinkOptions.REALTIME_PAYLOAD_COMBINE), eq(false), eq(Collections.emptyList()), any()))
          .thenReturn(recordReader);

      harness.open();
      harness.processElement(new StreamRecord<>(event("004", Collections.singletonList(operation))));

      verify(recordReader).getClosableIterator();
      verify(writerHelpers.constructed().get(0)).write(row);
      StreamRecord<ClusteringCommitEvent> output =
          (StreamRecord<ClusteringCommitEvent>) harness.getOutput().poll();
      assertEquals("old-file", output.getValue().getFileIds());
      assertFalse(output.getValue().isFailed());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testSortClusteringRejectsUnsortableColumnInsideTheTask() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
    conf.set(FlinkOptions.CLUSTERING_ASYNC_ENABLED, true);
    // f_map is a MAP, which the generated comparator cannot order. The rejection has to come out of the
    // clustering task and not out of open(): this operator sits on the ingestion pipeline, so a throw from
    // open() would fail the write job on every restart, while a task failure becomes a failed commit event
    // that rolls the clustering instant back.
    conf.set(FlinkOptions.CLUSTERING_SORT_COLUMNS, "f_map");
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeClient.getHoodieTable()).thenReturn(table);
    Throwable[] failure = new Throwable[1];

    ClusteringOperator operator = new ClusteringOperator(conf, TestConfigurations.ROW_TYPE_EVOLUTION_BEFORE);
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         // Runs the task inline and hands its failure to the hook, as the real executor does on its thread
         MockedConstruction<NonThrownExecutor> executors =
             mockConstruction(NonThrownExecutor.class, (executor, context) ->
                 doAnswer(invocation -> {
                   ThrowingRunnable<Throwable> action = invocation.getArgument(0);
                   NonThrownExecutor.ExceptionHook hook = invocation.getArgument(1);
                   try {
                     action.run();
                   } catch (Throwable t) {
                     failure[0] = t;
                     hook.apply("expected failure", t);
                   }
                   return null;
                 }).when(executor).execute(
                     any(ThrowingRunnable.class),
                     any(NonThrownExecutor.ExceptionHook.class),
                     anyString(),
                     anyString(),
                     anyInt()));
         OneInputStreamOperatorTestHarness<ClusteringPlanEvent, ClusteringCommitEvent> harness =
             new OneInputStreamOperatorTestHarness<>(operator, 1, 1, 0)) {
      writeClients.when(() -> FlinkWriteClients.getHoodieClientConfig(
          any(Configuration.class), eq(false), eq(false))).thenReturn(writeConfig);
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          any(Configuration.class), any(RuntimeContext.class))).thenReturn(writeClient);

      // open() succeeds; the plan is what fails, and it fails as a commit event rather than an exception
      harness.open();
      harness.processElement(new StreamRecord<>(event("006")));

      StreamRecord<ClusteringCommitEvent> output =
          (StreamRecord<ClusteringCommitEvent>) harness.getOutput().poll();
      assertEquals("006", output.getValue().getInstant());
      assertTrue(output.getValue().isFailed());
      assertTrue(failure[0] instanceof IllegalArgumentException, "Unexpected failure: " + failure[0]);
      assertTrue(failure[0].getMessage().contains("'f_map'"),
          "The error must name the unsortable sort column, got: " + failure[0].getMessage());
      assertEquals(1, executors.constructed().size());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testSortClustering() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    // Padded on purpose: the config list is user-written ("a, b"), and the operator trims each name before lookup.
    conf.set(FlinkOptions.CLUSTERING_SORT_COLUMNS, " " + TestConfigurations.ROW_TYPE.getFieldNames().get(0) + " ");
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieIOFactory ioFactory = mock(HoodieIOFactory.class);
    HoodieFileReaderFactory readerFactory = mock(HoodieFileReaderFactory.class);
    HoodieRowDataParquetReader fileReader = mock(HoodieRowDataParquetReader.class);
    HoodieRecord<RowData> hoodieRecord = mock(HoodieRecord.class);
    GenericRowData inputRow = new GenericRowData(
        DataTypeUtils.addMetadataFields(TestConfigurations.ROW_TYPE, false).getFieldCount());
    BinaryExternalSorter sorter = mock(BinaryExternalSorter.class);
    MutableObjectIterator<BinaryRowData> iterator = mock(MutableObjectIterator.class);
    BinaryRowData sortedRow = mock(BinaryRowData.class);
    when(writeClient.getHoodieTable()).thenReturn(table);
    when(table.getStorage()).thenReturn(storage);
    when(table.getConfig()).thenReturn(writeConfig);
    when(ioFactory.getReaderFactory(HoodieRecord.HoodieRecordType.FLINK)).thenReturn(readerFactory);
    when(readerFactory.getFileReader(
        same(writeConfig), any(StoragePath.class))).thenReturn(fileReader);
    when(hoodieRecord.getData()).thenReturn(inputRow);
    when(fileReader.getRecordIterator(any(HoodieSchema.class))).thenReturn(
        ClosableIterator.wrap(Collections.singletonList(hoodieRecord).iterator()));
    when(sorter.getIterator()).thenReturn(iterator);
    when(iterator.next(any(BinaryRowData.class))).thenReturn(sortedRow).thenReturn(null);

    ClusteringOperation operation = new ClusteringOperation(
        tempDir.toPath().resolve("base.parquet").toString(),
        Collections.emptyList(),
        "old-file",
        "partition",
        null,
        0);
    ClusteringOperator operator = new ClusteringOperator(conf, TestConfigurations.ROW_TYPE);
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedStatic<Utils> utils = mockStatic(Utils.class);
         MockedStatic<HoodieIOFactory> ioFactories = mockStatic(HoodieIOFactory.class);
         MockedConstruction<BulkInsertWriterHelper> writerHelpers =
             mockConstruction(BulkInsertWriterHelper.class, (writerHelper, context) ->
                 when(writerHelper.getWriteStatuses(anyInt())).thenReturn(Collections.emptyList()));
         OneInputStreamOperatorTestHarness<ClusteringPlanEvent, ClusteringCommitEvent> harness =
             new OneInputStreamOperatorTestHarness<>(operator, 1, 1, 0)) {
      writeClients.when(() -> FlinkWriteClients.getHoodieClientConfig(
          any(Configuration.class), eq(false), eq(false))).thenReturn(writeConfig);
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          any(Configuration.class), any(RuntimeContext.class))).thenReturn(writeClient);
      utils.when(() -> Utils.getBinaryExternalSorter(
          any(), any(), anyLong(), any(), any(), any(), any(), any(), any(Configuration.class)))
          .thenReturn(sorter);
      ioFactories.when(() -> HoodieIOFactory.getIOFactory(storage)).thenReturn(ioFactory);

      harness.open();
      harness.processElement(new StreamRecord<>(event("005", Collections.singletonList(operation))));

      verify(sorter).startThreads();
      verify(sorter).write(any(BinaryRowData.class));
      verify(writerHelpers.constructed().get(0)).write(same(sortedRow));
      verify(sorter).close();
      assertEquals("005",
          ((StreamRecord<ClusteringCommitEvent>) harness.getOutput().poll()).getValue().getInstant());
    }
  }

  private ClusteringPlanEvent event(String instant) {
    return event(instant, Collections.emptyList());
  }

  private ClusteringPlanEvent event(String instant, List<ClusteringOperation> operations) {
    ClusteringGroupInfo groupInfo = new ClusteringGroupInfo();
    groupInfo.setOperations(operations);
    groupInfo.setNumOutputGroups(1);
    return new ClusteringPlanEvent(instant, groupInfo, Map.of());
  }
}
