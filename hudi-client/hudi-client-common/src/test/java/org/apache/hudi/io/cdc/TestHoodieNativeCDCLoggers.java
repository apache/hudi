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

import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.cdc.HoodieCDCOperation;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_AFTER_IMAGE;
import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_BEFORE_IMAGE;
import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_COMMIT_TIMESTAMP;
import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_OPERATION_TYPE;
import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.CDC_RECORD_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieNativeCDCLoggers {

  private static final String COMMIT_TIME = "100";
  private static final HoodieSchema DATA_SCHEMA = HoodieSchema.fromAvroSchema(new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"trip\",\"namespace\":\"org.apache.hudi\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"));
  private static final HoodieSchema SCHEMA_WITH_METADATA = HoodieSchemaUtils.addMetadataFields(DATA_SCHEMA);

  @ParameterizedTest
  @EnumSource(HoodieCDCSupplementalLoggingMode.class)
  public void testAvroLoggerWritesAllOperationsAndSupportsRetraction(
      HoodieCDCSupplementalLoggingMode mode) throws Exception {
    try (MockedConstruction<HoodieNativeCDCFileWriter> mockedWriters = mockConstruction(HoodieNativeCDCFileWriter.class)) {
      HoodieAvroNativeCDCLogger logger = new HoodieAvroNativeCDCLogger(
          COMMIT_TIME, mock(HoodieWriteConfig.class), tableConfig(mode), "partition",
          mock(HoodieStorage.class), SCHEMA_WITH_METADATA, new StoragePath("/tmp/partition"),
          "file-1", "1-0-1", new LogFileCreationCallback() {
          }, mock(TaskContextSupplier.class));
      HoodieNativeCDCFileWriter<IndexedRecord> writer = mockedWriters.constructed().get(0);
      Map<String, Long> expectedStats = Collections.singletonMap("partition/file.cdc.parquet", 10L);
      when(writer.getCDCWriteStats()).thenReturn(expectedStats);

      GenericRecord oldRecord = record("id-1", "old");
      GenericRecord newRecord = record("id-1", "new");
      GenericRecord insertRecord = record("id-2", "insert");
      logger.put("id-1", oldRecord, Option.of(newRecord));
      verify(writer, never()).write(anyString(), any());
      logger.put("id-2", null, Option.of(insertRecord));
      logger.put("id-3", oldRecord, Option.empty());
      logger.put("id-4", null, Option.of(record("id-4", "retracted")));
      logger.remove("different-key");
      logger.remove("id-4");
      logger.close();

      assertSame(expectedStats, logger.getCDCWriteStats());
      verify(writer, times(3)).write(anyString(), any());
      verify(writer).close();

      ArgumentCaptor<IndexedRecord> recordCaptor = ArgumentCaptor.forClass(IndexedRecord.class);
      verify(writer, times(3)).write(anyString(), recordCaptor.capture());
      List<IndexedRecord> cdcRecords = recordCaptor.getAllValues();
      assertCDCRecord((GenericRecord) cdcRecords.get(0), mode, HoodieCDCOperation.UPDATE, "id-1",
          oldRecord, newRecord);
      assertCDCRecord((GenericRecord) cdcRecords.get(1), mode, HoodieCDCOperation.INSERT, "id-2",
          null, insertRecord);
      assertCDCRecord((GenericRecord) cdcRecords.get(2), mode, HoodieCDCOperation.DELETE, "id-3",
          oldRecord, null);
    }
  }

  @ParameterizedTest
  @EnumSource(HoodieCDCSupplementalLoggingMode.class)
  public void testEngineNativeLoggerWritesAllOperationsAndProjectsMetadata(
      HoodieCDCSupplementalLoggingMode mode) throws Exception {
    AvroRecordContext recordContext = new AvroRecordContext();
    try (MockedConstruction<HoodieNativeCDCFileWriter> mockedWriters = mockConstruction(HoodieNativeCDCFileWriter.class)) {
      HoodieNativeCDCLogger<IndexedRecord> logger = new HoodieNativeCDCLogger<>(
          COMMIT_TIME, mock(HoodieWriteConfig.class), tableConfig(mode), "partition",
          mock(HoodieStorage.class), SCHEMA_WITH_METADATA, new StoragePath("/tmp/partition"),
          "file-1", "1-0-1", new LogFileCreationCallback() {
          }, mock(TaskContextSupplier.class), recordContext, HoodieRecord.HoodieRecordType.AVRO);
      HoodieNativeCDCFileWriter<IndexedRecord> writer = mockedWriters.constructed().get(0);

      GenericRecord oldRecord = record("id-1", "old");
      GenericRecord newRecord = record("id-1", "new");
      GenericRecord insertRecord = record("id-2", "insert");
      BufferedRecord<IndexedRecord> oldBufferedRecord = bufferedRecord(oldRecord);
      BufferedRecord<IndexedRecord> newBufferedRecord = bufferedRecord(newRecord);
      BufferedRecord<IndexedRecord> insertBufferedRecord = bufferedRecord(insertRecord);
      logger.put("id-1", oldBufferedRecord, Option.of(newBufferedRecord));
      logger.put("id-2", null, Option.of(insertBufferedRecord));
      logger.put("id-3", oldBufferedRecord, Option.empty());
      logger.put("id-4", null, Option.of(bufferedRecord(record("id-4", "retracted"))));
      logger.remove("id-4");
      logger.close();

      ArgumentCaptor<IndexedRecord> recordCaptor = ArgumentCaptor.forClass(IndexedRecord.class);
      verify(writer, times(3)).write(anyString(), recordCaptor.capture());
      List<IndexedRecord> cdcRecords = recordCaptor.getAllValues();
      assertCDCRecord((GenericRecord) cdcRecords.get(0), mode, HoodieCDCOperation.UPDATE, "id-1",
          oldRecord, newRecord);
      assertCDCRecord((GenericRecord) cdcRecords.get(1), mode, HoodieCDCOperation.INSERT, "id-2",
          null, insertRecord);
      assertCDCRecord((GenericRecord) cdcRecords.get(2), mode, HoodieCDCOperation.DELETE, "id-3",
          oldRecord, null);
      verify(writer).close();
    }
  }

  private static HoodieTableConfig tableConfig(HoodieCDCSupplementalLoggingMode mode) {
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.cdcSupplementalLoggingMode()).thenReturn(mode);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    return tableConfig;
  }

  private static GenericRecord record(String id, String name) {
    GenericRecord record = new GenericData.Record(SCHEMA_WITH_METADATA.toAvroSchema());
    record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "001");
    record.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, "001_0_1");
    record.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, id);
    record.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "partition");
    record.put(HoodieRecord.FILENAME_METADATA_FIELD, "file.parquet");
    record.put("id", id);
    record.put("name", name);
    return record;
  }

  private static BufferedRecord<IndexedRecord> bufferedRecord(GenericRecord record) {
    return new BufferedRecord<>(
        record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(), null, record, 0, null);
  }

  private static void assertCDCRecord(
      GenericRecord record,
      HoodieCDCSupplementalLoggingMode mode,
      HoodieCDCOperation operation,
      String recordKey,
      GenericRecord expectedBefore,
      GenericRecord expectedAfter) {
    assertEquals(operation.getValue(), record.get(CDC_OPERATION_TYPE).toString());
    if (mode == HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER) {
      assertEquals(COMMIT_TIME, record.get(CDC_COMMIT_TIMESTAMP).toString());
      assertImage(record, CDC_BEFORE_IMAGE, expectedBefore);
      assertImage(record, CDC_AFTER_IMAGE, expectedAfter);
    } else {
      assertEquals(recordKey, record.get(CDC_RECORD_KEY).toString());
      if (mode == HoodieCDCSupplementalLoggingMode.DATA_BEFORE) {
        assertImage(record, CDC_BEFORE_IMAGE, expectedBefore);
        assertNull(record.getSchema().getField(CDC_AFTER_IMAGE));
      } else {
        assertNull(record.getSchema().getField(CDC_BEFORE_IMAGE));
        assertNull(record.getSchema().getField(CDC_AFTER_IMAGE));
      }
    }
  }

  private static void assertImage(GenericRecord cdcRecord, String imageField, GenericRecord expectedImage) {
    assertNotNull(cdcRecord.getSchema().getField(imageField));
    GenericRecord actualImage = (GenericRecord) cdcRecord.get(imageField);
    if (expectedImage == null) {
      assertNull(actualImage);
    } else {
      assertNotNull(actualImage);
      assertEquals(expectedImage.get("id").toString(), actualImage.get("id").toString());
      assertEquals(expectedImage.get("name").toString(), actualImage.get("name").toString());
      assertFalse(actualImage.getSchema().getFields().stream()
          .anyMatch(field -> HoodieRecord.HOODIE_META_COLUMNS.contains(field.name())));
    }
  }
}
