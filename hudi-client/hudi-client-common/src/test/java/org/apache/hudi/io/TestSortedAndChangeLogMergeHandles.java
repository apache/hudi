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
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.cdc.HoodieCDCLogWriter;
import org.apache.hudi.io.cdc.HoodieCDCLogWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSortedAndChangeLogMergeHandles {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"}]}";

  @Test
  public void testSortedMergeWritesInsertsAroundExistingRecordsAndOnClose() throws Exception {
    HoodieWriteConfig config = config();
    TestContext context = new TestContext(config);
    HoodieRecord recordB = record("b");
    HoodieRecord recordD = record("d");
    Map<String, HoodieRecord> records = new HashMap<>();
    records.put("d", recordD);
    records.put("b", recordB);

    try (MockedStatic<WriteMarkersFactory> markers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> writers = mockStatic(HoodieFileWriterFactory.class)) {
      context.stubWriters(markers, writers);
      TestableSortedMergeHandle handle = new TestableSortedMergeHandle(config, context.table, records);
      handle.write(record("c"));
      assertEquals(Collections.singletonList("b"), handle.writtenInserts);

      List<WriteStatus> statuses = handle.close();
      assertEquals(Arrays.asList("b", "d"), handle.writtenInserts);
      assertEquals(1, statuses.size());
      verify(context.fileWriter).close();
    }
  }

  @Test
  public void testSortedMergeRejectsAlreadyWrittenInsert() throws Exception {
    HoodieWriteConfig config = config();
    TestContext context = new TestContext(config);
    Map<String, HoodieRecord> records = Collections.singletonMap("b", record("b"));

    try (MockedStatic<WriteMarkersFactory> markers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> writers = mockStatic(HoodieFileWriterFactory.class)) {
      context.stubWriters(markers, writers);
      TestableSortedMergeHandle handle = new TestableSortedMergeHandle(config, context.table, records);
      handle.markWritten("b");
      assertThrows(HoodieUpsertException.class, () -> handle.write(record("c")));
    }
  }

  @Test
  public void testChangeLogHandleWritesUpdateAndInsertCDCAndPublishesStats() throws Exception {
    HoodieWriteConfig config = config();
    TestContext context = new TestContext(config);
    HoodieCDCLogWriter<IndexedRecord> cdcWriter = mock(HoodieCDCLogWriter.class);
    Map<String, Long> cdcStats = Collections.singletonMap("partition/file.cdc.parquet", 19L);
    when(cdcWriter.getCDCWriteStats()).thenReturn(cdcStats);

    try (MockedStatic<WriteMarkersFactory> markers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> writers = mockStatic(HoodieFileWriterFactory.class);
         MockedStatic<HoodieCDCLogWriterFactory> cdcWriters = mockStatic(HoodieCDCLogWriterFactory.class)) {
      context.stubWriters(markers, writers);
      cdcWriters.when(() -> HoodieCDCLogWriterFactory.createAvroCDCLogWriter(
          anyString(), any(), any(), anyString(), any(), any(), anyString(), anyString(), any(), any(), any()))
          .thenReturn(cdcWriter);

      TestableChangeLogMergeHandle handle = new TestableChangeLogMergeHandle(
          config, context.table, new HashMap<>());
      HoodieRecord insert = record("i");
      HoodieRecord savedInsert = record("i");
      when(insert.newInstance()).thenReturn(savedInsert);
      when(savedInsert.toIndexedRecord(any(HoodieSchema.class), any()))
          .thenReturn(Option.of(mock(HoodieAvroIndexedRecord.class)));
      handle.writeInsert(insert);

      HoodieRecord newRecord = record("u");
      HoodieRecord oldRecord = record("u");
      HoodieRecord combinedRecord = record("u");
      GenericRecord oldData = mock(GenericRecord.class);
      GenericRecord combinedData = mock(GenericRecord.class);
      when(oldRecord.getData()).thenReturn(oldData);
      when(combinedRecord.getData()).thenReturn(combinedData);
      when(combinedRecord.newInstance()).thenReturn(combinedRecord);
      when(combinedRecord.toIndexedRecord(any(HoodieSchema.class), any())).thenReturn(Option.empty());
      handle.writeUpdate(newRecord, oldRecord, combinedRecord);

      List<WriteStatus> statuses = handle.close();
      assertEquals(cdcStats, statuses.get(0).getStat().getCdcStats());
      verify(cdcWriter, times(2)).put(any(HoodieRecord.class), any(), any());
      verify(cdcWriter).close();
    }
  }

  @Test
  public void testWriteUpdateRecordDropsIncomingRecordWhenMergeKeepsOldValue() throws Exception {
    HoodieWriteConfig config = config();
    TestContext context = new TestContext(config);

    try (MockedStatic<WriteMarkersFactory> markers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> writers = mockStatic(HoodieFileWriterFactory.class)) {
      context.stubWriters(markers, writers);
      TestableWriteMergeHandle handle = new TestableWriteMergeHandle(config, context.table, new HashMap<>());

      // The merge kept the old value, so the combined record carries the old payload instance.
      GenericRecord oldData = mock(GenericRecord.class);
      HoodieRecord oldRecord = record("u");
      HoodieRecord combinedRecord = record("u");
      when(oldRecord.getData()).thenReturn(oldData);
      when(combinedRecord.getData()).thenReturn(oldData);

      assertFalse(handle.writeUpdate(record("u"), oldRecord, combinedRecord));
      assertEquals(Collections.emptyList(), handle.writtenKeys);
    }
  }

  @Test
  public void testWriteInsertRecordSkipsIgnoredRecord() throws Exception {
    HoodieWriteConfig config = config();
    TestContext context = new TestContext(config);

    try (MockedStatic<WriteMarkersFactory> markers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> writers = mockStatic(HoodieFileWriterFactory.class)) {
      context.stubWriters(markers, writers);
      TestableWriteMergeHandle handle = new TestableWriteMergeHandle(config, context.table, new HashMap<>());

      HoodieRecord ignored = record("i");
      when(ignored.shouldIgnore(any(HoodieSchema.class), any())).thenReturn(true);
      handle.writeInsert(ignored);

      assertEquals(Collections.emptyList(), handle.writtenKeys);
    }
  }

  @Test
  public void testWriteRecordMarksFailureWhenPartitionPathDoesNotMatch() throws Exception {
    HoodieWriteConfig config = config();
    TestContext context = new TestContext(config);

    try (MockedStatic<WriteMarkersFactory> markers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> writers = mockStatic(HoodieFileWriterFactory.class)) {
      context.stubWriters(markers, writers);
      TestableWriteMergeHandle handle = new TestableWriteMergeHandle(config, context.table, new HashMap<>());

      HoodieRecord foreignRecord = record("f");
      when(foreignRecord.getPartitionPath()).thenReturn("another-partition");
      handle.writeInsert(foreignRecord);

      assertEquals(Collections.emptyList(), handle.writtenKeys);
      assertTrue(handle.status().hasErrors());
      assertEquals(1, handle.status().getTotalErrorRecords());
    }
  }

  @Test
  public void testCloseIsIdempotent() throws Exception {
    HoodieWriteConfig config = config();
    TestContext context = new TestContext(config);

    try (MockedStatic<WriteMarkersFactory> markers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> writers = mockStatic(HoodieFileWriterFactory.class)) {
      context.stubWriters(markers, writers);
      TestableWriteMergeHandle handle = new TestableWriteMergeHandle(config, context.table, new HashMap<>());

      List<WriteStatus> first = handle.close();
      List<WriteStatus> second = handle.close();

      assertEquals(1, second.size());
      assertSame(first.get(0), second.get(0));
      verify(context.fileWriter, times(1)).close();
    }
  }

  private static HoodieWriteConfig config() {
    return HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withSchema(SCHEMA)
        .withWriteTableVersion(HoodieTableVersion.TEN.versionCode())
        .build();
  }

  private static HoodieRecord record(String key) {
    HoodieRecord record = mock(HoodieRecord.class);
    when(record.getRecordKey()).thenReturn(key);
    when(record.getRecordKey(any(HoodieSchema.class), any(Option.class))).thenReturn(key);
    when(record.getPartitionPath()).thenReturn("partition");
    when(record.getKey()).thenReturn(new HoodieKey(key, "partition"));
    when(record.getOperation()).thenReturn(HoodieOperation.INSERT);
    when(record.newInstance()).thenReturn(record);
    when(record.getMetadata()).thenReturn(Option.empty());
    return record;
  }

  private static class TestContext {
    private final HoodieTable table = mock(HoodieTable.class);
    private final HoodieStorage storage = mock(HoodieStorage.class);
    private final HoodieFileWriter fileWriter = mock(HoodieFileWriter.class);
    private final WriteMarkers writeMarkers = mock(WriteMarkers.class);

    private TestContext(HoodieWriteConfig config) throws IOException {
      HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
      HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
      ReaderContextFactory contextFactory = mock(ReaderContextFactory.class);
      when(table.getStorage()).thenReturn(storage);
      when(table.getConfig()).thenReturn(config);
      when(table.getMetaClient()).thenReturn(metaClient);
      when(table.getBaseFileExtension()).thenReturn(HoodieFileFormat.PARQUET.getFileExtension());
      when(table.getPartitionMetafileFormat()).thenReturn(Option.empty());
      when(table.getReaderContextFactoryForWrite()).thenReturn(contextFactory);
      when(contextFactory.getContext()).thenReturn(mock(HoodieReaderContext.class));
      when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp"));
      when(metaClient.getTableConfig()).thenReturn(tableConfig);
      when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
      when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.TEN);
      when(tableConfig.getMetaFieldsMode()).thenReturn(MetaFieldsMode.ALL);
      when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
      when(tableConfig.getPayloadClassIfPresent()).thenReturn(Option.empty());
      when(tableConfig.getPartitionMetafileFormat()).thenReturn(Option.empty());
      when(storage.getPathInfo(any(StoragePath.class))).thenAnswer(invocation ->
          new StoragePathInfo(invocation.getArgument(0), 23L, false, (short) 1, 1L, 1L));
    }

    private void stubWriters(
        MockedStatic<WriteMarkersFactory> markers, MockedStatic<HoodieFileWriterFactory> writers) {
      markers.when(() -> WriteMarkersFactory.get(any(), any(HoodieTable.class), anyString()))
          .thenReturn(writeMarkers);
      writers.when(() -> HoodieFileWriterFactory.getFileWriter(
          anyString(), any(StoragePath.class), any(HoodieStorage.class), any(HoodieWriteConfig.class),
          any(HoodieSchema.class), any(), any(HoodieRecord.HoodieRecordType.class)))
          .thenReturn(fileWriter);
    }
  }

  private static class TestableSortedMergeHandle extends HoodieSortedMergeHandle {
    private final List<String> writtenInserts = new ArrayList<>();

    private TestableSortedMergeHandle(
        HoodieWriteConfig config, HoodieTable table, Map<String, HoodieRecord> records) {
      super(config, "100", table, records, "partition", "file-1", null,
          new LocalTaskContextSupplier(), Option.empty());
    }

    @Override
    protected boolean writeRecord(
        HoodieRecord newRecord, HoodieRecord insertRecord, HoodieSchema schema, Properties props) {
      writtenInserts.add(newRecord.getRecordKey());
      return true;
    }

    @Override
    protected void writeToFile(
        HoodieKey key, HoodieRecord record, HoodieSchema schema, Properties props,
        boolean shouldPreserveRecordMetadata) {
      // The test targets merge ordering; the physical writer is covered separately.
    }

    private void markWritten(String key) {
      writtenRecordKeys.add(key);
    }
  }

  private static class TestableWriteMergeHandle extends HoodieWriteMergeHandle {
    private final List<String> writtenKeys = new ArrayList<>();

    private TestableWriteMergeHandle(
        HoodieWriteConfig config, HoodieTable table, Map<String, HoodieRecord> records) {
      super(config, "100", table, records, "partition", "file-1", null,
          new LocalTaskContextSupplier(), Option.empty());
    }

    @Override
    protected void writeToFile(
        HoodieKey key, HoodieRecord record, HoodieSchema schema, Properties props,
        boolean shouldPreserveRecordMetadata) {
      // These tests target the decisions taken before the record reaches the writer.
      writtenKeys.add(key.getRecordKey());
    }

    private void writeInsert(HoodieRecord record) throws IOException {
      writeInsertRecord(record);
    }

    private boolean writeUpdate(
        HoodieRecord newRecord, HoodieRecord oldRecord, HoodieRecord combinedRecord) throws IOException {
      return writeUpdateRecord(newRecord, oldRecord, combinedRecord, writeSchema);
    }

    private WriteStatus status() {
      return writeStatus;
    }
  }

  private static class TestableChangeLogMergeHandle extends HoodieMergeHandleWithChangeLog {

    private TestableChangeLogMergeHandle(
        HoodieWriteConfig config, HoodieTable table, Map<String, HoodieRecord> records) {
      super(config, "100", table, records, "partition", "file-1", null,
          new LocalTaskContextSupplier(), Option.empty());
    }

    private void writeInsert(HoodieRecord record) throws IOException {
      writeInsertRecord(record);
    }

    private boolean writeUpdate(
        HoodieRecord newRecord, HoodieRecord oldRecord, HoodieRecord combinedRecord) throws IOException {
      return writeUpdateRecord(newRecord, oldRecord, combinedRecord, writeSchema);
    }
  }
}
