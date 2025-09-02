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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFileGroupReaderSchemaHandler extends SchemaHandlerTestBase {

  @Test
  public void testCow() {
    when(hoodieTableConfig.populateMetaFields()).thenReturn(true);
    HoodieReaderContext<String> readerContext = createReaderContext(hoodieTableConfig, false, false, false, false, null);
    Schema requestedSchema = DATA_SCHEMA;
    FileGroupReaderSchemaHandler schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, requestedSchema, false);
    assertEquals(requestedSchema, schemaHandler.getRequiredSchema());

    //read subset of columns
    requestedSchema = generateProjectionSchema("begin_lat", "tip_history", "rider");
    schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, requestedSchema, false);
    assertEquals(requestedSchema, schemaHandler.getRequiredSchema());
    assertFalse(readerContext.getNeedsBootstrapMerge());
  }

  @Test
  public void testCowBootstrap() {
    when(hoodieTableConfig.populateMetaFields()).thenReturn(true);
    HoodieReaderContext<String> readerContext = createReaderContext(hoodieTableConfig, false, false, true, false, null);
    Schema requestedSchema = generateProjectionSchema("begin_lat", "tip_history", "_hoodie_record_key", "rider");

    //meta cols must go first in the required schema
    FileGroupReaderSchemaHandler schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, requestedSchema, false);
    assertTrue(readerContext.getNeedsBootstrapMerge());
    Schema expectedRequiredSchema = generateProjectionSchema("_hoodie_record_key", "begin_lat", "tip_history", "rider");
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    Pair<List<Schema.Field>, List<Schema.Field>> bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertEquals(Collections.singletonList(getField("_hoodie_record_key")), bootstrapFields.getLeft());
    assertEquals(Arrays.asList(getField("begin_lat"), getField("tip_history"), getField("rider")), bootstrapFields.getRight());
  }

  @Test
  void testGetRequiredSchemaForFileAndRenameColumns() {
    when(hoodieTableConfig.populateMetaFields()).thenReturn(true);
    HoodieReaderContext<String> readerContext = createReaderContext(hoodieTableConfig, false, false, true, false, null);
    Schema requestedSchema = generateProjectionSchema("_hoodie_record_key", "timestamp", "rider");

    InternalSchema internalSchema = AvroInternalSchemaConverter.convert(DATA_SCHEMA);
    InternalSchema originalSchema = new InternalSchema(Types.RecordType.get(internalSchema.columns().stream().map(field -> {
      if (field.name().equals("timestamp")) {
        // rename timestamp to ts in file schema and change type to int, output schema names and types must match the requested schema
        return Types.Field.get(field.fieldId(), "ts", Types.IntType.get());
      }
      return field;
    }).collect(Collectors.toList())));
    FileGroupReaderSchemaHandler<String> schemaHandler = new FileGroupReaderSchemaHandler<>(readerContext, DATA_SCHEMA, requestedSchema,
        Option.of(internalSchema), new TypedProperties(), metaClient);

    try (MockedStatic<InternalSchemaCache> mockedStatic = Mockito.mockStatic(InternalSchemaCache.class)) {
      String instantTime = "20231010101010";
      mockedStatic.when(() -> InternalSchemaCache.searchSchemaAndCache(Long.parseLong(instantTime), metaClient))
          .thenReturn(originalSchema);
      StoragePath filePath = new StoragePath("/2023-01-01/" + FSUtils.makeBaseFileName(instantTime, "1-0-1", UUID.randomUUID().toString(), HoodieFileFormat.PARQUET.getFileExtension()));
      Pair<Schema, Map<String, String>> requiredSchemaAndRenamedFields = schemaHandler.getRequiredSchemaForFileAndRenamedColumns(filePath);
      assertEquals(Collections.singletonMap("timestamp", "ts"), requiredSchemaAndRenamedFields.getRight());
      assertEquals(requestedSchema, requiredSchemaAndRenamedFields.getLeft());
    }
  }

  private static Stream<Arguments> testMorParams() {
    return testMorParams(false);
  }

  @ParameterizedTest
  @MethodSource("testMorParams")
  public void testMor(RecordMergeMode mergeMode,
                      boolean hasPrecombine,
                      boolean isProjectionCompatible,
                      boolean mergeUseRecordPosition,
                      boolean supportsParquetRowIndex,
                      boolean hasBuiltInDelete) throws IOException {
    super.testMor(mergeMode, hasPrecombine, isProjectionCompatible, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete);
  }

  @ParameterizedTest
  @MethodSource("testMorParams")
  public void testMorBootstrap(RecordMergeMode mergeMode,
                               boolean hasPrecombine,
                               boolean isProjectionCompatible,
                               boolean mergeUseRecordPosition,
                               boolean supportsParquetRowIndex,
                               boolean hasBuiltInDelete) throws IOException {
    super.testMorBootstrap(mergeMode, hasPrecombine, isProjectionCompatible, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete);
  }

  @Override
  FileGroupReaderSchemaHandler createSchemaHandler(HoodieReaderContext<String> readerContext, Schema dataSchema, Schema requestedSchema,
                                                   boolean supportsParquetRowIndex) {
    return new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), new TypedProperties(), metaClient);
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

    Schema dataSchema = SchemaTestUtil.getSchemaFromFields(dataSchemaFields);
    Schema requestedSchema = SchemaTestUtil.getSchemaFromFields(Arrays.asList(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD));

    when(hoodieTableConfig.getRecordMergeMode()).thenReturn(mergeMode);
    when(hoodieTableConfig.populateMetaFields()).thenReturn(true);
    when(hoodieTableConfig.getOrderingFieldsStr()).thenReturn(Option.of(setPrecombine ? preCombineField : StringUtils.EMPTY_STRING));
    when(hoodieTableConfig.getOrderingFields()).thenReturn(setPrecombine ? Collections.singletonList(preCombineField) : Collections.emptyList());
    when(hoodieTableConfig.getTableVersion()).thenReturn(tableVersion);
    if (hoodieTableConfig.getTableVersion() == HoodieTableVersion.SIX) {
      if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
        when(hoodieTableConfig.getPayloadClass()).thenReturn(DefaultHoodieRecordPayload.class.getName());
      } else if (mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING) {
        when(hoodieTableConfig.getPayloadClass()).thenReturn(OverwriteWithLatestAvroPayload.class.getName());
      } else {
        when(hoodieTableConfig.getPayloadClass()).thenReturn(OverwriteNonDefaultsWithLatestAvroPayload.class.getName());
      }
    }
    if (mergeMode != null) {
      when(hoodieTableConfig.getRecordMergeStrategyId()).thenReturn(mergeStrategyId);
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
    Schema expectedSchema = ((mergeMode == RecordMergeMode.CUSTOM) && !isProjectionCompatible) ? dataSchema : SchemaTestUtil.getSchemaFromFields(expectedFields);
    when(recordMerger.getMandatoryFieldsForMerging(dataSchema, hoodieTableConfig, props)).thenReturn(expectedFields.toArray(new String[0]));

    DeleteContext deleteContext = new DeleteContext(props, dataSchema);
    assertEquals(addHoodieIsDeleted, deleteContext.hasBuiltInDeleteField());
    assertEquals(addCustomDeleteMarker
            ? Option.of(Pair.of(customDeleteKey, customDeleteValue)) : Option.empty(),
        deleteContext.getCustomDeleteMarkerKeyValue());
    FileGroupReaderSchemaHandler fileGroupReaderSchemaHandler = new FileGroupReaderSchemaHandler(readerContext,
        dataSchema, requestedSchema, Option.empty(), props, metaClient);
    Schema actualSchema = fileGroupReaderSchemaHandler.generateRequiredSchema(deleteContext);
    assertEquals(expectedSchema, actualSchema);
  }
}
