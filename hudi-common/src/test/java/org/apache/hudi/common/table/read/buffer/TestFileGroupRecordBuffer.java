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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.apache.hudi.common.table.read.buffer.FileGroupRecordBuffer.getOrderingValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link FileGroupRecordBuffer}
 */
class TestFileGroupRecordBuffer {
  private String schemaString = "{"
      + "\"type\": \"record\","
      + "\"name\": \"EventRecord\","
      + "\"namespace\": \"com.example.avro\","
      + "\"fields\": ["
      + "{\"name\": \"id\", \"type\": \"string\"},"
      + "{\"name\": \"ts\", \"type\": \"long\"},"
      + "{\"name\": \"op\", \"type\": \"string\"},"
      + "{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\"}"
      + "]"
      + "}";
  private Schema schema = new Schema.Parser().parse(schemaString);
  private final HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
  private final FileGroupReaderSchemaHandler schemaHandler =
      mock(FileGroupReaderSchemaHandler.class);
  private final UpdateProcessor updateProcessor = mock(UpdateProcessor.class);
  private HoodieTableMetaClient hoodieTableMetaClient = mock(HoodieTableMetaClient.class);
  private TypedProperties props = new TypedProperties();
  private HoodieReadStats readStats = mock(HoodieReadStats.class);

  @BeforeEach
  void setUp() {
    when(readerContext.getSchemaHandler()).thenReturn(schemaHandler);
    when(schemaHandler.getRequiredSchema()).thenReturn(schema);
    when(readerContext.getRecordMerger()).thenReturn(Option.empty());
    when(readerContext.getRecordSerializer()).thenReturn(new DefaultSerializer<>());
    when(readerContext.getRecordSizeEstimator()).thenReturn(new DefaultSizeEstimator<>());
  }

  @Test
  void testGetOrderingValueFromDeleteRecord() {
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    DeleteRecord deleteRecord = mock(DeleteRecord.class);
    mockDeleteRecord(deleteRecord, null);
    assertEquals(DEFAULT_ORDERING_VALUE, getOrderingValue(readerContext, deleteRecord));
    mockDeleteRecord(deleteRecord, DEFAULT_ORDERING_VALUE);
    assertEquals(DEFAULT_ORDERING_VALUE, getOrderingValue(readerContext, deleteRecord));
    String orderingValue = "xyz";
    String convertedValue = "_xyz";
    mockDeleteRecord(deleteRecord, orderingValue);
    when(readerContext.convertValueToEngineType(orderingValue)).thenReturn(convertedValue);
    assertEquals(convertedValue, getOrderingValue(readerContext, deleteRecord));
  }

  @ParameterizedTest
  @CsvSource({"true,false", "false,true"})
  void testInvalidCustomDeleteConfigs(boolean configureCustomDeleteKey,
                                      boolean configureCustomDeleteMarker) {
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getHasBootstrapBaseFile()).thenReturn(false);
    when(readerContext.getHasLogFiles()).thenReturn(true);
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    when(readerContext.getRecordMerger()).thenReturn(Option.of(recordMerger));
    when(recordMerger.isProjectionCompatible()).thenReturn(false);

    String customDeleteKey = "colC";
    String customDeleteValue = "D";
    List<String> dataSchemaFields = new ArrayList<>(Arrays.asList(
        HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD,
        "colA", "colB", "colC", "colD"));

    Schema dataSchema = SchemaTestUtil.getSchemaFromFields(dataSchemaFields);
    Schema requestedSchema = SchemaTestUtil.getSchemaFromFields(Arrays.asList(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD));

    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    TypedProperties props = new TypedProperties();
    if (configureCustomDeleteKey) {
      props.setProperty(DELETE_KEY, customDeleteKey);
    }
    if (configureCustomDeleteMarker) {
      props.setProperty(DELETE_MARKER, customDeleteValue);
    }
    Throwable exception = assertThrows(IllegalArgumentException.class,
        () -> new FileGroupReaderSchemaHandler(readerContext,
            dataSchema, requestedSchema, Option.empty(), tableConfig, props));
    assertEquals("Either custom delete key or marker is not specified",
        exception.getMessage());
  }

  private void mockDeleteRecord(DeleteRecord deleteRecord,
                                Comparable orderingValue) {
    when(deleteRecord.getOrderingValue()).thenReturn(orderingValue);
  }

  @Test
  void testIsCustomDeleteRecord() {
    String customDeleteKey = "op";
    String customDeleteValue = "d";
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "12345");
    record.put("ts", System.currentTimeMillis());
    record.put(customDeleteKey, "d");

    when(schemaHandler.getCustomDeleteMarkerKeyValue())
        .thenReturn(Option.of(Pair.of(customDeleteKey, customDeleteValue)));
    KeyBasedFileGroupRecordBuffer keyBasedBuffer =
        new KeyBasedFileGroupRecordBuffer(
            readerContext,
            hoodieTableMetaClient,
            RecordMergeMode.COMMIT_TIME_ORDERING,
            PartialUpdateMode.NONE,
            props,
            Option.empty(),
            updateProcessor
        );
    when(readerContext.getValue(any(), any(), any())).thenReturn(null);
    assertFalse(keyBasedBuffer.isCustomDeleteRecord(record));

    props.setProperty(DELETE_KEY, customDeleteKey);
    props.setProperty(DELETE_MARKER, customDeleteValue);
    keyBasedBuffer = new KeyBasedFileGroupRecordBuffer(
            readerContext,
            hoodieTableMetaClient,
            RecordMergeMode.COMMIT_TIME_ORDERING,
            PartialUpdateMode.NONE,
            props,
            Option.empty(),
            updateProcessor
    );
    when(readerContext.getValue(any(), any(), any())).thenReturn("i");
    assertFalse(keyBasedBuffer.isCustomDeleteRecord(record));
    when(readerContext.getValue(any(), any(), any())).thenReturn("d");
    assertTrue(keyBasedBuffer.isCustomDeleteRecord(record));
  }

  @Test
  void testProcessCustomDeleteRecord() throws IOException {
    String customDeleteKey = "op";
    String customDeleteValue = "d";
    when(schemaHandler.getCustomDeleteMarkerKeyValue())
        .thenReturn(Option.of(Pair.of(customDeleteKey, customDeleteValue)));
    when(schemaHandler.hasBuiltInDelete()).thenReturn(true);
    KeyBasedFileGroupRecordBuffer keyBasedBuffer =
        new KeyBasedFileGroupRecordBuffer(
            readerContext,
            hoodieTableMetaClient,
            RecordMergeMode.COMMIT_TIME_ORDERING,
            PartialUpdateMode.NONE,
            props,
            Option.empty(),
            updateProcessor
        );

    // CASE 1: With custom delete marker.
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "12345");
    record.put("ts", System.currentTimeMillis());
    record.put("op", "d");
    record.put("_hoodie_is_deleted", false);
    when(readerContext.getOrderingValue(any(), any(), any())).thenReturn(1);
    when(readerContext.convertValueToEngineType(any())).thenReturn(1);
    BufferedRecord<GenericRecord> bufferedRecord = BufferedRecord.forRecordWithContext(record, schema, readerContext, Option.of("ts"), true);

    keyBasedBuffer.processNextDataRecord(bufferedRecord, "12345");
    Map<Serializable, BufferedRecord<GenericRecord>> records = keyBasedBuffer.getLogRecords();
    assertEquals(1, records.size());
    BufferedRecord<GenericRecord> deleteRecord = records.get("12345");
    assertNull(deleteRecord.getRecordKey(), "The record key metadata field is missing");
    assertEquals(1, deleteRecord.getOrderingValue());

    // CASE 2: With _hoodie_is_deleted is true.
    GenericRecord anotherRecord = new GenericData.Record(schema);
    anotherRecord.put("id", "54321");
    anotherRecord.put("ts", System.currentTimeMillis());
    anotherRecord.put("op", "i");
    anotherRecord.put("_hoodie_is_deleted", true);
    bufferedRecord = BufferedRecord.forRecordWithContext(anotherRecord, schema, readerContext, Option.of("ts"), true);

    keyBasedBuffer.processNextDataRecord(bufferedRecord, "54321");
    records = keyBasedBuffer.getLogRecords();
    assertEquals(2, records.size());
    deleteRecord = records.get("54321");
    assertNull(deleteRecord.getRecordKey(), "The record key metadata field is missing");
    assertEquals(1, deleteRecord.getOrderingValue());
  }
}
