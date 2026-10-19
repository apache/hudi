/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bulk;

import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieRowDataCreation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link BulkInsertWriterHelper}.
 */
public class TestBulkInsertWriteHelper {
  protected Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws IOException {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath(), TestConfigurations.ROW_DATA_TYPE);
    StreamerUtil.initTableIfNotExists(conf);
  }

  @ParameterizedTest
  @MethodSource("metaFieldsModeParams")
  void testMetaFieldsMode(MetaFieldsMode mode, boolean preserveMetadata) throws Exception {
    for (String operation : new String[] {"insert", "bulk_insert"}) {
      File path = new File(tempFile, operation);
      Configuration modeConf = TestConfigurations.getDefaultConf(path.getAbsolutePath(), TestConfigurations.ROW_DATA_TYPE);
      modeConf.set(FlinkOptions.TABLE_TYPE, "COPY_ON_WRITE");
      modeConf.set(FlinkOptions.OPERATION, operation);
      modeConf.set(FlinkOptions.INSERT_CLUSTER, false);
      modeConf.setString(HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(), "DEFAULT");
      modeConf.setString(HoodieTableConfig.META_FIELDS_MODE.key(), mode.name());
      StreamerUtil.initTableIfNotExists(modeConf);
      HoodieFlinkTable<?> table = FlinkTables.createTable(modeConf);
      String instant = WriteClientTestUtils.createNewInstantTime();
      String expectedCommitTime = preserveMetadata ? "old-instant" : instant;
      RowType rowType = preserveMetadata ? DataTypeUtils.addMetadataFields(TestConfigurations.ROW_TYPE, false) : TestConfigurations.ROW_TYPE;
      BulkInsertWriterHelper helper = new BulkInsertWriterHelper(modeConf, table, table.getConfig(),
          instant, 1, 1, 0, rowType, preserveMetadata);
      for (RowData row : TestData.DATA_SET_INSERT) {
        if (preserveMetadata) {
          // Clustering reads rows with metadata columns, but selective modes leave keys null.
          row = HoodieRowDataCreation.create(mode.isCommitTimePopulated() ? expectedCommitTime : null,
              mode == MetaFieldsMode.ALL ? "old-sequence" : null,
              mode.isRecordKeyPopulated() ? row.getString(0).toString() : null,
              mode == MetaFieldsMode.ALL ? row.getString(4).toString() : null,
              mode.isFileNamePopulated() ? "old.parquet" : null, row, false, false);
        }
        helper.write(row);
      }
      List<WriteStatus> statuses = helper.getWriteStatuses(1);
      assertWriteStatus(statuses);
      assertEquals(TestData.DATA_SET_INSERT.size(), statuses.stream().mapToLong(status -> status.getStat().getNumWrites()).sum());
      for (WriteStatus status : statuses) {
        assertFalse(status.hasErrors());
        StoragePath file = new StoragePath(path.getAbsolutePath(), status.getStat().getPath());
        for (GenericRecord row : new ParquetUtils().readAvroRecords(table.getStorage(), file)) {
          if (mode == MetaFieldsMode.NONE && operation.equals("insert") && !preserveMetadata) {
            assertEquals(TestConfigurations.ROW_TYPE.getFieldCount(), row.getSchema().getFields().size());
            for (String field : HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION) {
              assertEquals(null, row.getSchema().getField(field), field);
            }
            continue;
          }
          assertEquals(row.get("partition").toString(), status.getStat().getPartitionPath());
          assertEquals(mode.isCommitTimePopulated() ? expectedCommitTime : null,
              Objects.toString(row.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD), null));
          assertEquals(mode != MetaFieldsMode.ALL, row.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD) == null);
          assertEquals(mode.isRecordKeyPopulated() ? row.get("uuid").toString() : null,
              Objects.toString(row.get(HoodieRecord.RECORD_KEY_METADATA_FIELD), null));
          assertEquals(mode == MetaFieldsMode.ALL ? status.getStat().getPartitionPath() : null,
              Objects.toString(row.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD), null));
          assertEquals(mode.isFileNamePopulated() ? file.getName() : null,
              Objects.toString(row.get(HoodieRecord.FILENAME_METADATA_FIELD), null));
        }
      }
    }
  }

  private static Stream<Arguments> metaFieldsModeParams() {
    return Arrays.stream(MetaFieldsMode.values()).flatMap(mode -> Stream.of(
        Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  @Test
  void testWrite() throws Exception {
    HoodieFlinkTable<?> table = FlinkTables.createTable(conf);
    String instant = WriteClientTestUtils.createNewInstantTime();
    RowType rowType = TestConfigurations.ROW_TYPE;
    BulkInsertWriterHelper writerHelper = new BulkInsertWriterHelper(conf, table, table.getConfig(), instant,
        1, 1, 0, rowType, false);
    for (RowData row: TestData.DATA_SET_INSERT) {
      writerHelper.write(row);
    }
    List<WriteStatus> writeStatusList = writerHelper.getWriteStatuses(1);
    assertWriteStatus(writeStatusList);

    Map<String, String> expected = new HashMap<>();
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1, id2,par1,id2,Stephen,33,2,par1]");
    expected.put("par2", "[id3,par2,id3,Julian,53,3,par2, id4,par2,id4,Fabian,31,4,par2]");
    expected.put("par3", "[id5,par3,id5,Sophia,18,5,par3, id6,par3,id6,Emma,20,6,par3]");
    expected.put("par4", "[id7,par4,id7,Bob,44,7,par4, id8,par4,id8,Han,56,8,par4]");

    TestData.checkWrittenData(tempFile, expected);

    // set up preserveHoodieMetadata as true and check again
    RowType rowType2 = DataTypeUtils.addMetadataFields(rowType, false);
    BulkInsertWriterHelper writerHelper2 = new BulkInsertWriterHelper(conf, table, table.getConfig(), instant,
        1, 1, 0, rowType2, true);
    for (RowData row: rowsWithMetadata(instant, TestData.DATA_SET_INSERT)) {
      writerHelper.write(row);
    }
    List<WriteStatus> writeStatusList2 = writerHelper.getWriteStatuses(1);
    assertWriteStatus(writeStatusList2);

    String expectRows = "[" + instant + ", " + instant + "]";
    Map<String, String> expected2 = new HashMap<>();
    expected2.put("par1", expectRows);
    expected2.put("par2", expectRows);
    expected2.put("par3", expectRows);
    expected2.put("par4", expectRows);

    TestData.checkWrittenData(tempFile, expected2, 4, TestBulkInsertWriteHelper::filterCommitTime);
  }

  @Test
  void testInvalidRecordIsWrappedAsIOException() {
    HoodieFlinkTable<?> table = FlinkTables.createTable(conf);
    BulkInsertWriterHelper writerHelper = new BulkInsertWriterHelper(
        conf,
        table,
        table.getConfig(),
        WriteClientTestUtils.createNewInstantTime(),
        1,
        1,
        0,
        TestConfigurations.ROW_TYPE);

    assertThrows(IOException.class, () -> writerHelper.write(new GenericRowData(0)));
  }

  @ParameterizedTest
  @CsvSource({"false, false", "true, false", "false, true", "true, true"})
  void testCloseFailureDoesNotSkipQueuedHandles(boolean runtimeFailure, boolean multipleFailures) throws Exception {
    BulkInsertWriterHelper helper = newWriterHelper();
    List<HoodieRowDataCreateHandle> handles = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      HoodieRowDataCreateHandle handle = mock(HoodieRowDataCreateHandle.class);
      when(handle.close()).thenReturn(new WriteStatus());
      helper.handles.put("partition-" + i, handle);
      handles.add(handle);
    }
    HoodieRowDataCreateHandle failedHandle = helper.handles.values().iterator().next();
    Exception failure = runtimeFailure ? new IllegalStateException("close failed") : new IOException("close failed");
    when(failedHandle.close()).thenThrow(failure);
    List<Exception> failures = new ArrayList<>();
    failures.add(failure);
    if (multipleFailures) {
      HoodieRowDataCreateHandle secondFailedHandle = helper.handles.values().stream().skip(1).findFirst().get();
      Exception secondFailure = runtimeFailure ? new IOException("second close failed") : new IllegalStateException("second close failed");
      when(secondFailedHandle.close()).thenThrow(secondFailure);
      failures.add(secondFailure);
    }

    // Hold all close tasks in a queue until the helper is waiting for their results.
    // This makes cancellation of not-yet-started closes deterministic.
    ExecutorService executor = mock(ExecutorService.class);
    BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
    doAnswer(invocation -> {
      tasks.add(invocation.getArgument(0));
      return null;
    }).when(executor).execute(any(Runnable.class));
    FutureTask<CompletionException> closeTask = new FutureTask<>(() -> {
      try (MockedStatic<Executors> executors = mockStatic(Executors.class)) {
        executors.when(() -> Executors.newFixedThreadPool(10)).thenReturn(executor);
        return assertThrows(CompletionException.class, helper::close);
      }
    });
    Thread closingThread = new Thread(closeTask);
    closingThread.setDaemon(true);
    closingThread.start();
    List<Runnable> submitted = new ArrayList<>();
    try {
      for (int i = 0; i < 11; i++) {
        Runnable task = tasks.poll(10, TimeUnit.SECONDS);
        assertTrue(task != null, "Every handle must be submitted for closing");
        submitted.add(task);
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (closingThread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertEquals(Thread.State.WAITING, closingThread.getState());
      submitted.get(0).run();
      assertFalse(closeTask.isDone(), "A failed close must not complete the helper before the remaining closes");
      submitted.subList(1, submitted.size()).forEach(Runnable::run);
      CompletionException thrown = closeTask.get(10, TimeUnit.SECONDS);
      Throwable cause = thrown;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      // allOf preserves a close failure, but does not guarantee which one when several handles fail.
      assertTrue(failures.contains(cause));
      assertEquals(0, thrown.getSuppressed().length);
      verify(executor).shutdown();
      for (HoodieRowDataCreateHandle handle : handles) {
        verify(handle).close();
      }
    } finally {
      // join() is uninterruptible: finish queued tasks even if an assertion failed before they ran.
      submitted.forEach(Runnable::run);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (closingThread.isAlive() && System.nanoTime() < deadline) {
        Runnable task = tasks.poll(100, TimeUnit.MILLISECONDS);
        if (task != null) {
          task.run();
        }
      }
      closingThread.join(1000);
      assertFalse(closingThread.isAlive(), "The closing thread must terminate after cleanup");
    }
  }

  @Test
  void testFailedCloseRetainsHandlesForCleanupRetry() throws Exception {
    BulkInsertWriterHelper helper = newWriterHelper();
    HoodieRowDataCreateHandle successfulHandle = mock(HoodieRowDataCreateHandle.class);
    HoodieRowDataCreateHandle failedHandle = mock(HoodieRowDataCreateHandle.class);
    when(successfulHandle.close()).thenReturn(new WriteStatus());
    when(failedHandle.close()).thenThrow(new IOException("close failed"));
    helper.handles.put("par1", successfulHandle);
    helper.handles.put("par2", failedHandle);

    assertThrows(CompletionException.class, helper::close);
    assertEquals(2, helper.handles.size());
    // A repeated status request must still fail, rather than returning an incomplete set of statuses.
    assertThrows(CompletionException.class, () -> helper.getWriteStatuses(1));
    verify(successfulHandle, times(2)).close();
    verify(failedHandle, times(2)).close();
  }

  @Test
  void testSuccessfulCloseIsIdempotent() throws Exception {
    BulkInsertWriterHelper helper = newWriterHelper();
    HoodieRowDataCreateHandle handle = mock(HoodieRowDataCreateHandle.class);
    WriteStatus status = new WriteStatus();
    when(handle.close()).thenReturn(status);
    helper.handles.put("par1", handle);

    helper.close();
    helper.close();

    verify(handle).close();
    assertTrue(helper.handles.isEmpty());
    assertEquals(Arrays.asList(status), helper.getWriteStatuses(1));
  }

  private BulkInsertWriterHelper newWriterHelper() {
    HoodieFlinkTable<?> table = FlinkTables.createTable(conf);
    return new BulkInsertWriterHelper(conf, table, table.getConfig(), WriteClientTestUtils.createNewInstantTime(),
        1, 1, 0, TestConfigurations.ROW_TYPE);
  }

  private void assertWriteStatus(List<WriteStatus> writeStatusList) {
    String partitions = writeStatusList.stream()
        .map(writeStatus -> StringUtils.nullToEmpty(writeStatus.getStat().getPartitionPath()))
        .sorted()
        .collect(Collectors.joining(","));
    assertThat(partitions, is("par1,par2,par3,par4"));
    List<String> files = writeStatusList.stream()
        .map(writeStatus -> writeStatus.getStat().getPath())
        .collect(Collectors.toList());
    assertThat(files.size(), is(4));
  }

  private static List<RowData> rowsWithMetadata(String instantTime, List<RowData> rows) {
    List<RowData> rowsWithMetadata = new ArrayList<>();
    int seqNum = 0;
    for (RowData row : rows) {
      GenericRowData rebuilt = new GenericRowData(row.getArity() + 5);
      rebuilt.setField(0, StringData.fromString(instantTime));
      rebuilt.setField(1, seqNum++);
      rebuilt.setField(2, row.getString(0));
      rebuilt.setField(3, row.getString(4));
      rebuilt.setField(4, StringData.fromString("f" + seqNum));
    }
    return rowsWithMetadata;
  }

  private static String filterCommitTime(GenericRecord genericRecord) {
    return genericRecord.get("_hoodie_commit_time").toString();
  }
}
