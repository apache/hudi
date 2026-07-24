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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieAppendException;
import org.apache.hudi.io.cdc.HoodieNativeLogFormatWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieNativeLogAppendHandle {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"}]}";

  @Test
  public void testCreatesWriterAndRoutesDataAndDeleteRecords() throws Exception {
    HoodieWriteConfig config = config();
    HoodieTable table = table(config);
    HoodieRecord inputRecord = mock(HoodieRecord.class);
    HoodieRecord populatedRecord = mock(HoodieRecord.class);
    when(inputRecord.prependMetaFields(any(HoodieSchema.class), any(HoodieSchema.class), any(), any()))
        .thenReturn(populatedRecord);

    try (MockedConstruction<HoodieNativeLogFormatWriter> writers = mockConstruction(
        HoodieNativeLogFormatWriter.class, (writer, context) -> {
          when(writer.canWriteDataFile()).thenReturn(true);
          when(writer.canWriteDeleteFile()).thenReturn(true);
          when(writer.hasPendingWrites()).thenReturn(true);
          when(writer.getLastAppendResults()).thenReturn(Collections.emptyList());
          when(writer.getLogFile()).thenReturn(new HoodieLogFile(new StoragePath("/tmp/native.log.parquet")));
        })) {
      TestableNativeLogAppendHandle handle = new TestableNativeLogAppendHandle(config, table);
      handle.createWriter();
      HoodieNativeLogFormatWriter writer = writers.constructed().get(0);

      handle.writeData(inputRecord, true);
      verify(writer).appendRecord(eq(populatedRecord), any(HoodieSchema.class),
          eq(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      handle.writeDeleteRecord(inputRecord);
      verify(inputRecord).clearNewLocation();
      verify(writer).appendDeleteRecord(eq(inputRecord), any(HoodieSchema.class),
          eq(HoodieRecord.RECORD_KEY_METADATA_FIELD));

      handle.flushWriter();
      verify(writer).flushAppend(any());
      assertEquals("/tmp/native.log.parquet", handle.logFilePath().toString());
      assertTrue(handle.canWrite(inputRecord));
      handle.closeWriter();
      verify(writer).close();
    }
  }

  @Test
  public void testFlushesBeforeWriterRolloverAndTracksPerFileCounts() throws Exception {
    HoodieWriteConfig config = config();
    HoodieTable table = table(config);
    HoodieRecord inputRecord = mock(HoodieRecord.class);
    HoodieRecord populatedRecord = mock(HoodieRecord.class);
    when(inputRecord.prependMetaFields(any(HoodieSchema.class), any(HoodieSchema.class), any(), any()))
        .thenReturn(populatedRecord);

    try (MockedConstruction<HoodieNativeLogFormatWriter> writers = mockConstruction(
        HoodieNativeLogFormatWriter.class, (writer, context) -> {
          when(writer.canWriteDataFile()).thenReturn(false);
          when(writer.hasPendingWrites()).thenReturn(true);
          when(writer.getLastAppendResults()).thenReturn(
              Arrays.asList(appendResult(1, "log", 13L), appendResult(1, "deletes", 7L)),
              Collections.singletonList(appendResult(2, "log", 11L)));
          when(writer.getLastDataFileFormatMetadata()).thenReturn(Option.empty());
        })) {
      TestableNativeLogAppendHandle handle = new TestableNativeLogAppendHandle(config, table);
      handle.createWriter();
      HoodieNativeLogFormatWriter writer = writers.constructed().get(0);

      handle.setCounts(5, 2, 3, 4);
      handle.writeData(inputRecord, false);
      InOrder rolloverOrder = inOrder(writer);
      rolloverOrder.verify(writer).flushAppend(any());
      rolloverOrder.verify(writer).appendRecord(eq(populatedRecord), any(HoodieSchema.class), any());

      handle.flushWriter();
      verify(writer, times(2)).flushAppend(any());

      List<WriteStatus> statuses = handle.getWriteStatuses();
      assertEquals(3, statuses.size());
      HoodieDeltaWriteStat firstDataStat = (HoodieDeltaWriteStat) statuses.get(0).getStat();
      assertEquals(5, firstDataStat.getNumWrites());
      assertEquals(2, firstDataStat.getNumUpdateWrites());
      assertEquals(3, firstDataStat.getNumInserts());
      assertEquals(13L, firstDataStat.getTotalWriteBytes());

      HoodieDeltaWriteStat deleteStat = (HoodieDeltaWriteStat) statuses.get(1).getStat();
      assertEquals(4, deleteStat.getNumDeletes());
      assertEquals(7L, deleteStat.getTotalWriteBytes());

      HoodieDeltaWriteStat secondDataStat = (HoodieDeltaWriteStat) statuses.get(2).getStat();
      assertEquals(1, secondDataStat.getNumWrites());
      assertEquals(0, secondDataStat.getNumUpdateWrites());
      assertEquals(1, secondDataStat.getNumInserts());
      assertEquals(11L, secondDataStat.getTotalWriteBytes());

      assertThrows(HoodieAppendException.class,
          () -> handle.accumulateWriteCounts(firstDataStat, appendResult(1, "log", 13L)));
    }
  }

  @Test
  public void testUsesConfiguredKeyWithoutMetadataFieldsAndSkipsIgnoredRecords() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withSchema(SCHEMA)
        .withPopulateMetaFields(false)
        .withWriteTableVersion(HoodieTableVersion.TEN.versionCode())
        .withWriteRecordPositionsEnabled(false)
        .build();
    HoodieTable table = table(config);
    when(table.getMetaClient().getTableConfig().getRecordKeyFieldProp()).thenReturn("id");

    try (MockedConstruction<HoodieNativeLogFormatWriter> writers = mockConstruction(
        HoodieNativeLogFormatWriter.class, (writer, context) -> {
          when(writer.canWriteDataFile()).thenReturn(false);
          when(writer.canWriteDeleteFile()).thenReturn(false);
          when(writer.hasPendingWrites()).thenReturn(false);
        })) {
      TestableNativeLogAppendHandle handle = new TestableNativeLogAppendHandle(config, table, new HashMap<>());
      handle.createWriter();
      HoodieNativeLogFormatWriter writer = writers.constructed().get(0);

      HoodieRecord ignoredRecord = mock(HoodieRecord.class);
      when(ignoredRecord.shouldIgnore(any(HoodieSchema.class), any())).thenReturn(true);
      handle.writeData(ignoredRecord, false);
      verify(writer, never()).appendRecord(eq(ignoredRecord), any(), any());

      HoodieRecord inputRecord = mock(HoodieRecord.class);
      HoodieRecord populatedRecord = mock(HoodieRecord.class);
      when(inputRecord.prependMetaFields(any(HoodieSchema.class), any(HoodieSchema.class), any(), any()))
          .thenReturn(populatedRecord);
      handle.writeData(inputRecord, false);
      verify(writer).appendRecord(eq(populatedRecord), any(HoodieSchema.class), eq("id"));
      handle.writeDeleteWithoutMetadata(inputRecord);
      verify(writer).appendDeleteRecord(eq(inputRecord), any(HoodieSchema.class), eq("id"));
    }
  }

  @Test
  public void testWrapsFlushFailure() throws Exception {
    HoodieWriteConfig config = config();
    HoodieTable table = table(config);

    try (MockedConstruction<HoodieNativeLogFormatWriter> writers = mockConstruction(
        HoodieNativeLogFormatWriter.class, (writer, context) -> {
          when(writer.hasPendingWrites()).thenReturn(true);
          doThrow(new IOException("flush failed")).when(writer).flushAppend(any());
        })) {
      TestableNativeLogAppendHandle handle = new TestableNativeLogAppendHandle(config, table);
      handle.createWriter();

      HoodieAppendException exception = assertThrows(HoodieAppendException.class, handle::flushWriter);
      assertTrue(exception.getMessage().contains("file-1"));
    }
  }

  private static HoodieWriteConfig config() {
    return HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withSchema(SCHEMA)
        .withWriteTableVersion(HoodieTableVersion.TEN.versionCode())
        .withWriteRecordPositionsEnabled(false)
        .build();
  }

  private static HoodieTable table(HoodieWriteConfig config) {
    HoodieTable table = mock(HoodieTable.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(table.getStorage()).thenReturn(storage);
    when(table.getConfig()).thenReturn(config);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(table.getRecordContextForWrite()).thenReturn(mock(RecordContext.class));
    when(table.version()).thenReturn(HoodieTableVersion.TEN);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp"));
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.TEN);
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    return table;
  }

  private static AppendResult appendResult(int version, String extension, long size) throws IOException {
    StoragePath path = new StoragePath("/tmp", FSUtils.makeNativeLogFileName(
        "file-1", "1-0-1", "100", version, extension, HoodieFileFormat.PARQUET));
    return new AppendResult(new HoodieLogFile(path), 0L, size);
  }

  private static class TestableNativeLogAppendHandle extends HoodieNativeLogAppendHandle {

    private TestableNativeLogAppendHandle(HoodieWriteConfig config, HoodieTable table) {
      super(config, "100", table, "partition", "file-1", Collections.emptyIterator(),
          new LocalTaskContextSupplier());
    }

    private TestableNativeLogAppendHandle(
        HoodieWriteConfig config, HoodieTable table, HashMap header) {
      super(config, "100", table, "partition", "file-1", Collections.emptyIterator(),
          new LocalTaskContextSupplier(), header);
    }

    private void createWriter() {
      HoodieDeltaWriteStat stat = new HoodieDeltaWriteStat();
      stat.setPartitionPath(partitionPath);
      stat.setFileId(fileId);
      writeStatus.setStat(stat);
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      createLogWriterForAppend("100", Option.empty());
    }

    private void writeData(HoodieRecord record, boolean isUpdate) throws IOException {
      writeInsertAndUpdate(writeSchema, record, isUpdate);
    }

    private void writeDeleteRecord(HoodieRecord record) throws IOException {
      writeDelete(writeSchemaWithMetaFields, record);
    }

    private void writeDeleteWithoutMetadata(HoodieRecord record) throws IOException {
      writeDelete(writeSchema, record);
    }

    private void flushWriter() {
      flushAppend();
    }

    private void closeWriter() {
      closeLogWriter();
    }

    private StoragePath logFilePath() {
      return getLogFilePath();
    }

    private void setCounts(long writes, long updates, long inserts, long deletes) {
      recordsWritten = writes;
      updatedRecordsWritten = updates;
      insertRecordsWritten = inserts;
      recordsDeleted = deletes;
    }
  }
}
