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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
import static org.apache.hudi.common.table.read.FileGroupRecordBuffer.getOrderingValue;
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
  @CsvSource({
      "true, true, true, EVENT_TIME_ORDERING, false, EIGHT, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5",
      "true, false, false, EVENT_TIME_ORDERING, false, EIGHT, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5",
      "false, true, false, EVENT_TIME_ORDERING, false, EIGHT, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5",
      "false, false, true, EVENT_TIME_ORDERING, false, EIGHT, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5",
      "true, true, true, COMMIT_TIME_ORDERING, false, EIGHT, ce9acb64-bde0-424c-9b91-f6ebba25356d",
      "true, false, false, COMMIT_TIME_ORDERING, false, EIGHT, ce9acb64-bde0-424c-9b91-f6ebba25356d",
      "false, true, false, COMMIT_TIME_ORDERING, false, EIGHT, ce9acb64-bde0-424c-9b91-f6ebba25356d",
      "false, false, true, COMMIT_TIME_ORDERING, false, EIGHT, ce9acb64-bde0-424c-9b91-f6ebba25356d",
      "true, true, true, CUSTOM, false, EIGHT, 00000000-0000-0000-0000-000000000000",
      "true, false, false, CUSTOM, false, EIGHT, 00000000-0000-0000-0000-000000000000",
      "false, true, false, CUSTOM, false, EIGHT, 00000000-0000-0000-0000-000000000000",
      "false, false, true, CUSTOM, false, EIGHT, 00000000-0000-0000-0000-000000000000",
      "true, true, true, , false, EIGHT, 00000000-0000-0000-0000-000000000000",
      "true, false, false, , false, EIGHT, 00000000-0000-0000-0000-000000000000",
      "false, true, false, , false, EIGHT, 00000000-0000-0000-0000-000000000000",
      "false, false, true, , false, EIGHT, 00000000-0000-0000-0000-000000000000",
      "true, true, true, EVENT_TIME_ORDERING, false, SIX, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5",
      "true, false, false, EVENT_TIME_ORDERING, false, SIX, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5",
      "false, true, false, EVENT_TIME_ORDERING, false, SIX, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5",
      "false, false, true, EVENT_TIME_ORDERING, false, SIX, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5",
      "true, true, true, COMMIT_TIME_ORDERING, false, SIX, ce9acb64-bde0-424c-9b91-f6ebba25356d",
      "true, false, false, COMMIT_TIME_ORDERING, false, SIX, ce9acb64-bde0-424c-9b91-f6ebba25356d",
      "false, true, false, COMMIT_TIME_ORDERING, false, SIX, ce9acb64-bde0-424c-9b91-f6ebba25356d",
      "false, false, true, COMMIT_TIME_ORDERING, false, SIX, ce9acb64-bde0-424c-9b91-f6ebba25356d",
      "true, true, true, CUSTOM, false, SIX, 00000000-0000-0000-0000-000000000000",
      "true, false, false, CUSTOM, false, SIX, 00000000-0000-0000-0000-000000000000",
      "false, true, false, CUSTOM, false, SIX, 00000000-0000-0000-0000-000000000000",
      "false, false, true, CUSTOM, false, SIX, 00000000-0000-0000-0000-000000000000",
      "true, true, true, , false, SIX, 00000000-0000-0000-0000-000000000000",
      "true, false, false, , false, SIX, 00000000-0000-0000-0000-000000000000",
      "false, true, false, , false, SIX, 00000000-0000-0000-0000-000000000000",
      "false, false, true, , false, SIX, 00000000-0000-0000-0000-000000000000",
      "true, true, true, COMMIT_TIME_ORDERING, true, SIX, eeb8d96f-b1e4-49fd-bbf8-28ac514178e5", /// with table version 6, commit time based merge mode can have event time based merge strategy id.
  })
  public void testSchemaForMandatoryFields(boolean setPrecombine,
                                           boolean addHoodieIsDeleted,
                                           boolean addCustomDeleteMarker,
                                           RecordMergeMode mergeMode,
                                           boolean isProjectionCompatible,
                                           HoodieTableVersion tableVersion,
                                           String mergeStrategyId) {
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getInstantRange()).thenReturn(Option.empty());
    when(readerContext.getHasBootstrapBaseFile()).thenReturn(false);
    when(readerContext.getHasLogFiles()).thenReturn(true);
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    when(readerContext.getRecordMerger()).thenReturn(Option.of(recordMerger));
    when(recordMerger.isProjectionCompatible()).thenReturn(isProjectionCompatible);

    String preCombineField = "ts";
    String customDeleteKey = "colC";
    String customDeleteValue = "D";
    List<String> dataSchemaFields = new ArrayList<>();
    dataSchemaFields.addAll(Arrays.asList(
        HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, preCombineField,
        "colA", "colB", "colC", "colD"));
    if (addHoodieIsDeleted) {
      dataSchemaFields.add(HoodieRecord.HOODIE_IS_DELETED_FIELD);
    }

    Schema dataSchema = getSchema(dataSchemaFields);
    Schema requestedSchema = getSchema(Arrays.asList(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD));

    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordMergeMode()).thenReturn(mergeMode);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    when(tableConfig.getPreCombineField()).thenReturn(setPrecombine ? preCombineField : StringUtils.EMPTY_STRING);
    when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    if (tableConfig.getTableVersion() == HoodieTableVersion.SIX) {
      if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
        when(tableConfig.getPayloadClass()).thenReturn(DefaultHoodieRecordPayload.class.getName());
      } else if (mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING) {
        when(tableConfig.getPayloadClass()).thenReturn(OverwriteWithLatestAvroPayload.class.getName());
      } else {
        when(tableConfig.getPayloadClass()).thenReturn(OverwriteNonDefaultsWithLatestAvroPayload.class.getName());
      }
    }
    if (mergeMode != null) {
      when(tableConfig.getRecordMergeStrategyId()).thenReturn(mergeStrategyId);
    }

    TypedProperties props = new TypedProperties();
    if (addCustomDeleteMarker) {
      props.setProperty(DELETE_KEY, customDeleteKey);
      props.setProperty(DELETE_MARKER, customDeleteValue);
    }

    List<String> expectedFields = new ArrayList();
    expectedFields.add(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    expectedFields.add(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    if (addCustomDeleteMarker) {
      expectedFields.add(customDeleteKey);
    }
    if (setPrecombine && mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING) { // commit time ordering does not project ordering field.
      expectedFields.add(preCombineField);
    }
    if (addHoodieIsDeleted) {
      expectedFields.add(HoodieRecord.HOODIE_IS_DELETED_FIELD);
    }
    Schema expectedSchema = ((mergeMode == RecordMergeMode.CUSTOM) && !isProjectionCompatible) ? dataSchema : getSchema(expectedFields);
    when(recordMerger.getMandatoryFieldsForMerging(dataSchema, tableConfig, props)).thenReturn(expectedFields.toArray(new String[0]));

    FileGroupReaderSchemaHandler fileGroupReaderSchemaHandler = new FileGroupReaderSchemaHandler(readerContext,
        dataSchema, requestedSchema, Option.empty(), tableConfig, props);
    Schema actualSchema = fileGroupReaderSchemaHandler.generateRequiredSchema();
    assertEquals(expectedSchema, actualSchema);
    assertEquals(addHoodieIsDeleted, fileGroupReaderSchemaHandler.hasBuiltInDelete());
    assertEquals(addCustomDeleteMarker
            ? Option.of(Pair.of(customDeleteKey, customDeleteValue)) : Option.empty(),
        fileGroupReaderSchemaHandler.getCustomDeleteMarkerKeyValue());
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

    Schema dataSchema = getSchema(dataSchemaFields);
    Schema requestedSchema = getSchema(Arrays.asList(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD));

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

  private Schema getSchema(List<String> fields) {
    SchemaBuilder.FieldAssembler<Schema> schemaFieldAssembler = SchemaBuilder.builder().record("test_schema")
        .namespace("test_namespace").fields();
    for (String field : fields) {
      schemaFieldAssembler = schemaFieldAssembler.name(field).type().stringType().noDefault();
    }
    return schemaFieldAssembler.endRecord();
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
            readStats,
            Option.empty(),
            false
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
            readStats,
            Option.empty(),
            false
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
            readStats,
            Option.empty(),
            false
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
