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
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.table.log.NativeLogFooterMetadata;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieNativeCDCFileWriter {

  @Test
  public void testAddsLogBlockHeaderToFooterMetadata() throws Exception {
    String instantTime = "100";
    String schemaString = "{\"type\":\"record\",\"name\":\"cdc\",\"fields\":[]}";
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieSchema cdcSchema = mock(HoodieSchema.class);
    HoodieFileWriter fileWriter = mock(HoodieFileWriter.class);
    StoragePath parentPath = new StoragePath("/tmp/partition");

    when(config.getProps()).thenReturn(new TypedProperties());
    when(cdcSchema.toString()).thenReturn(schemaString);
    when(storage.exists(any(StoragePath.class))).thenReturn(false);

    try (MockedStatic<HoodieFileWriterFactory> writerFactory = mockStatic(HoodieFileWriterFactory.class)) {
      writerFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
              eq(instantTime), any(StoragePath.class), eq(storage), eq(config), eq(cdcSchema),
              any(TaskContextSupplier.class), eq(HoodieRecord.HoodieRecordType.AVRO)))
          .thenReturn(fileWriter);

      HoodieNativeCDCFileWriter<IndexedRecord> writer = new HoodieNativeCDCFileWriter<>(
          instantTime,
          "partition",
          storage,
          config,
          cdcSchema,
          HoodieFileFormat.PARQUET,
          parentPath,
          "file-1",
          "1-0-1",
          new LogFileCreationCallback() {
          },
          mock(TaskContextSupplier.class),
          HoodieRecord.HoodieRecordType.AVRO);

      writer.write("key1", mock(IndexedRecord.class));
    }

    ArgumentCaptor<Map<String, String>> footerCaptor = ArgumentCaptor.forClass(Map.class);
    verify(fileWriter).addFooterMetadata(footerCaptor.capture());
    Map<HeaderMetadataType, String> header =
        NativeLogFooterMetadata.fromFooterMetadata(footerCaptor.getValue());
    assertEquals(instantTime, header.get(HeaderMetadataType.INSTANT_TIME));
    assertEquals(schemaString, header.get(HeaderMetadataType.SCHEMA));
  }

  @Test
  public void testRollsFilesSkipsExistingVersionsAndReportsStats() throws Exception {
    String instantTime = "100";
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieSchema cdcSchema = mock(HoodieSchema.class);
    HoodieFileWriter firstFileWriter = mock(HoodieFileWriter.class);
    HoodieFileWriter secondFileWriter = mock(HoodieFileWriter.class);
    LogFileCreationCallback creationCallback = mock(LogFileCreationCallback.class);
    StoragePath parentPath = new StoragePath("/tmp/partition");

    when(config.getProps()).thenReturn(new TypedProperties());
    when(cdcSchema.toString()).thenReturn("cdc-schema");
    // Version 1 already exists; the first and second writes therefore use versions 2 and 3.
    when(storage.exists(any(StoragePath.class))).thenReturn(true, false, false);
    when(firstFileWriter.canWrite()).thenReturn(false);
    when(storage.getPathInfo(any(StoragePath.class))).thenAnswer(invocation ->
        new StoragePathInfo(invocation.getArgument(0), 17L, false, (short) 1, 1L, 1L));

    try (MockedStatic<HoodieFileWriterFactory> writerFactory = mockStatic(HoodieFileWriterFactory.class)) {
      writerFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
              eq(instantTime), any(StoragePath.class), eq(storage), eq(config), eq(cdcSchema),
              any(TaskContextSupplier.class), eq(HoodieRecord.HoodieRecordType.AVRO)))
          .thenReturn(firstFileWriter, secondFileWriter);

      HoodieNativeCDCFileWriter<IndexedRecord> writer = new HoodieNativeCDCFileWriter<>(
          instantTime, "partition", storage, config, cdcSchema, HoodieFileFormat.PARQUET,
          parentPath, "file-1", "1-0-1", creationCallback, mock(TaskContextSupplier.class),
          HoodieRecord.HoodieRecordType.AVRO);

      IndexedRecord firstRecord = mock(IndexedRecord.class);
      IndexedRecord secondRecord = mock(IndexedRecord.class);
      writer.write("key-1", firstRecord);
      writer.write("key-2", secondRecord);

      verify(firstFileWriter).writeRow("key-1", firstRecord);
      verify(firstFileWriter).close();
      verify(secondFileWriter).writeRow("key-2", secondRecord);
      verify(creationCallback, times(2)).preFileCreation(any());

      ArgumentCaptor<StoragePath> pathCaptor = ArgumentCaptor.forClass(StoragePath.class);
      writerFactory.verify(() -> HoodieFileWriterFactory.getFileWriter(
          eq(instantTime), pathCaptor.capture(), eq(storage), eq(config), eq(cdcSchema),
          any(TaskContextSupplier.class), eq(HoodieRecord.HoodieRecordType.AVRO)), times(2));
      assertNotEquals(pathCaptor.getAllValues().get(0), pathCaptor.getAllValues().get(1));
      assertEquals(2, new HoodieLogFile(pathCaptor.getAllValues().get(0)).getLogVersion());
      assertEquals(3, new HoodieLogFile(pathCaptor.getAllValues().get(1)).getLogVersion());

      Map<String, Long> stats = writer.getCDCWriteStats();
      assertEquals(2, stats.size());
      assertTrue(stats.keySet().stream().allMatch(path -> path.startsWith("partition/")));
      assertTrue(stats.values().stream().allMatch(size -> size == 17L));

      writer.close();
      verify(secondFileWriter).close();
    }
  }

  @Test
  public void testUsesFileNameForNonPartitionedTableAndWrapsStatsFailure() throws Exception {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieSchema cdcSchema = mock(HoodieSchema.class);
    HoodieFileWriter fileWriter = mock(HoodieFileWriter.class);
    when(config.getProps()).thenReturn(new TypedProperties());
    when(cdcSchema.toString()).thenReturn("cdc-schema");
    when(storage.exists(any(StoragePath.class))).thenReturn(false);

    try (MockedStatic<HoodieFileWriterFactory> writerFactory = mockStatic(HoodieFileWriterFactory.class)) {
      writerFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
              eq("100"), any(StoragePath.class), eq(storage), eq(config), eq(cdcSchema),
              any(TaskContextSupplier.class), eq(HoodieRecord.HoodieRecordType.AVRO)))
          .thenReturn(fileWriter);

      HoodieNativeCDCFileWriter<IndexedRecord> writer = new HoodieNativeCDCFileWriter<>(
          "100", "", storage, config, cdcSchema, HoodieFileFormat.PARQUET,
          new StoragePath("/tmp"), "file-1", "1-0-1", new LogFileCreationCallback() {
          }, mock(TaskContextSupplier.class), HoodieRecord.HoodieRecordType.AVRO);
      writer.write("key-1", mock(IndexedRecord.class));

      when(storage.getPathInfo(any(StoragePath.class))).thenAnswer(invocation ->
          new StoragePathInfo(invocation.getArgument(0), 11L, false, (short) 1, 1L, 1L));
      Map<String, Long> stats = writer.getCDCWriteStats();
      assertEquals(1, stats.size());
      assertTrue(stats.keySet().stream().noneMatch(path -> path.contains("/")));

      when(storage.getPathInfo(any(StoragePath.class))).thenThrow(new java.io.IOException("failure"));
      assertThrows(HoodieUpsertException.class, writer::getCDCWriteStats);
    }
  }
}
