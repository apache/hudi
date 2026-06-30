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

package org.apache.hudi.io.cdc;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.table.log.LogReaderUtils;
import org.apache.hudi.common.table.log.NativeLogFooterMetadata;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieNativeLogFormatWriter {

  @Test
  public void testAddsRecordPositionsToDataLogFooter() throws Exception {
    Map<HeaderMetadataType, String> parsedHeader = writeDataLogFooterWithPositions(2L, 7L);

    assertEquals("001", parsedHeader.get(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS));
    assertEquals(Arrays.asList(2L, 7L),
        toList(LogReaderUtils.decodeRecordPositionsHeader(parsedHeader.get(HeaderMetadataType.RECORD_POSITIONS))));
  }

  @Test
  public void testSkipsOutOfOrderRecordPositions() throws Exception {
    Map<HeaderMetadataType, String> parsedHeader = writeDataLogFooterWithPositions(7L, 2L);

    assertFalse(parsedHeader.containsKey(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS));
    assertFalse(parsedHeader.containsKey(HeaderMetadataType.RECORD_POSITIONS));
  }

  @Test
  public void testRemovesBaseFileInstantForInvalidRecordPosition() throws Exception {
    Map<HeaderMetadataType, String> parsedHeader = writeDataLogFooterWithPositions(7L, -1L);

    assertFalse(parsedHeader.containsKey(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS));
    assertFalse(parsedHeader.containsKey(HeaderMetadataType.RECORD_POSITIONS));
  }

  @Test
  public void testSkipsDuplicateRecordPositions() throws Exception {
    Map<HeaderMetadataType, String> parsedHeader = writeDataLogFooterWithPositions(7L, 7L);

    assertNull(parsedHeader.get(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS));
    assertFalse(parsedHeader.containsKey(HeaderMetadataType.RECORD_POSITIONS));
  }

  @Test
  public void testAddsRecordPositionsToDeleteLogFooter() throws Exception {
    Map<HeaderMetadataType, String> parsedHeader = writeDeleteLogFooterWithPositions(Option.of("001"));

    assertEquals("001", parsedHeader.get(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS));
    assertEquals(Arrays.asList(7L),
        toList(LogReaderUtils.decodeRecordPositionsHeader(parsedHeader.get(HeaderMetadataType.RECORD_POSITIONS))));
  }

  @Test
  public void testSkipsDeleteRecordPositionsWithoutBaseFileInstant() throws Exception {
    Map<HeaderMetadataType, String> parsedHeader = writeDeleteLogFooterWithPositions(Option.empty());

    assertFalse(parsedHeader.containsKey(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS));
    assertFalse(parsedHeader.containsKey(HeaderMetadataType.RECORD_POSITIONS));
  }

  @Test
  public void testUsesDefaultOrderingValueForCommitTimeDeleteLog() throws Exception {
    HoodieSchema schema = mock(HoodieSchema.class);
    HoodieRecord record = mock(HoodieRecord.class);
    when(record.getRecordKey(schema, HoodieRecord.RECORD_KEY_METADATA_FIELD)).thenReturn("key-1");
    when(record.getCurrentPosition()).thenReturn(7L);
    doReturn(HoodieRecord.DEFAULT_ORDERING_VALUE + 1).when(record).getOrderingValue(eq(schema), any(), any());

    writeDeleteLogFooterWithPositions(Option.empty(), record, schema, new ArrayList<>());

    verify(record, never()).getOrderingValue(eq(schema), any(), any());
  }

  private static Map<HeaderMetadataType, String> writeDataLogFooterWithPositions(long... positions) throws Exception {
    String instantTime = "100";
    String schemaString = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[]}";
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieSchema schema = mock(HoodieSchema.class);
    HoodieRecordMerger merger = mock(HoodieRecordMerger.class);
    HoodieFileWriter fileWriter = mock(HoodieFileWriter.class);
    StoragePath parentPath = new StoragePath("/tmp/partition");

    when(config.getProps()).thenReturn(new TypedProperties());
    when(config.getRecordMerger()).thenReturn(merger);
    when(merger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(storage.exists(any(StoragePath.class))).thenReturn(false);
    when(storage.getPathInfo(any(StoragePath.class))).thenAnswer(invocation ->
        new StoragePathInfo(invocation.getArgument(0), 1L, false, (short) 1, 1L, 1L));

    try (MockedStatic<HoodieFileWriterFactory> writerFactory = mockStatic(HoodieFileWriterFactory.class)) {
      writerFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
              eq(instantTime), any(StoragePath.class), eq(storage), eq(config), eq(schema),
              any(TaskContextSupplier.class), eq(HoodieRecord.HoodieRecordType.AVRO)))
          .thenReturn(fileWriter);

      HoodieNativeLogFormatWriter writer = new HoodieNativeLogFormatWriter(
          4096,
          storage,
          parentPath,
          "file-1",
          instantTime,
          1,
          "1-0-1",
          1024L,
          new LogFileCreationCallback() {
          },
          HoodieTableVersion.current(),
          config,
          HoodieFileFormat.PARQUET,
          schema,
          mock(TaskContextSupplier.class),
          mock(RecordContext.class),
          new ArrayList<>(),
          Option.of("001"));

      for (int i = 0; i < positions.length; i++) {
        writer.appendRecord(recordWithPosition("key-" + i, positions[i], schema),
            schema, HoodieRecord.RECORD_KEY_METADATA_FIELD);
      }

      Map<HeaderMetadataType, String> header = new HashMap<>();
      header.put(HeaderMetadataType.SCHEMA, schemaString);
      header.put(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS, "001");
      writer.flushAppend(header);
    }

    ArgumentCaptor<Map<String, String>> footerCaptor = ArgumentCaptor.forClass(Map.class);
    verify(fileWriter).addFooterMetadata(footerCaptor.capture());
    return NativeLogFooterMetadata.fromFooterMetadata(footerCaptor.getValue());
  }

  private static Map<HeaderMetadataType, String> writeDeleteLogFooterWithPositions(
      Option<String> baseFileInstantTimeOfPositions) throws Exception {
    HoodieSchema schema = mock(HoodieSchema.class);
    return writeDeleteLogFooterWithPositions(
        baseFileInstantTimeOfPositions, recordWithPosition("key-1", 7L, schema), schema, new ArrayList<>());
  }

  private static Map<HeaderMetadataType, String> writeDeleteLogFooterWithPositions(
      Option<String> baseFileInstantTimeOfPositions, HoodieRecord record, HoodieSchema schema,
      List<String> orderingFieldNames) throws Exception {
    String instantTime = "100";
    String schemaString = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[]}";
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieRecordMerger merger = mock(HoodieRecordMerger.class);
    HoodieFileWriter fileWriter = mock(HoodieFileWriter.class);
    RecordContext recordContext = mock(RecordContext.class);
    StoragePath parentPath = new StoragePath("/tmp/partition");

    when(config.getProps()).thenReturn(new TypedProperties());
    when(config.getRecordMerger()).thenReturn(merger);
    when(merger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(recordContext.getEngineRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(recordContext.convertValueToEngineType(any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(recordContext.constructEngineRecord(any(HoodieSchema.class), any(Object[].class)))
        .thenReturn(new Object());
    when(storage.exists(any(StoragePath.class))).thenReturn(false);
    when(storage.getPathInfo(any(StoragePath.class))).thenAnswer(invocation ->
        new StoragePathInfo(invocation.getArgument(0), 1L, false, (short) 1, 1L, 1L));

    try (MockedStatic<HoodieFileWriterFactory> writerFactory = mockStatic(HoodieFileWriterFactory.class)) {
      writerFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
              eq(instantTime), any(StoragePath.class), eq(storage), eq(config), any(HoodieSchema.class),
              any(TaskContextSupplier.class), eq(HoodieRecord.HoodieRecordType.AVRO)))
          .thenReturn(fileWriter);

      HoodieNativeLogFormatWriter writer = new HoodieNativeLogFormatWriter(
          4096,
          storage,
          parentPath,
          "file-1",
          instantTime,
          1,
          "1-0-1",
          1024L,
          new LogFileCreationCallback() {
          },
          HoodieTableVersion.current(),
          config,
          HoodieFileFormat.PARQUET,
          schema,
          mock(TaskContextSupplier.class),
          recordContext,
          orderingFieldNames,
          baseFileInstantTimeOfPositions);

      writer.appendDeleteRecord(record, schema, HoodieRecord.RECORD_KEY_METADATA_FIELD);

      Map<HeaderMetadataType, String> header = new HashMap<>();
      header.put(HeaderMetadataType.SCHEMA, schemaString);
      baseFileInstantTimeOfPositions.ifPresent(baseInstantTime ->
          header.put(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS, baseInstantTime));
      writer.flushAppend(header);
    }

    ArgumentCaptor<Map<String, String>> footerCaptor = ArgumentCaptor.forClass(Map.class);
    verify(fileWriter).addFooterMetadata(footerCaptor.capture());
    return NativeLogFooterMetadata.fromFooterMetadata(footerCaptor.getValue());
  }

  private static HoodieRecord recordWithPosition(String key, long position, HoodieSchema schema) throws Exception {
    HoodieRecord record = mock(HoodieRecord.class);
    when(record.getRecordKey(schema, HoodieRecord.RECORD_KEY_METADATA_FIELD)).thenReturn(key);
    when(record.getCurrentPosition()).thenReturn(position);
    when(record.getOrderingValue(eq(schema), any(), any())).thenReturn(OrderingValues.getDefault());
    return record;
  }

  private static List<Long> toList(Roaring64NavigableMap positions) {
    List<Long> values = new ArrayList<>();
    positions.iterator().forEachRemaining(values::add);
    return values;
  }
}
