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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.RecordMergeMode.COMMIT_TIME_ORDERING;
import static org.apache.hudi.common.config.RecordMergeMode.CUSTOM;
import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.common.table.read.ParquetRowIndexBasedSchemaHandler.addPositionalMergeCol;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class SchemaHandlerTestBase {

  protected static final Schema DATA_SCHEMA = HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.AVRO_SCHEMA);
  protected static final Schema DATA_SCHEMA_NO_DELETE = generateProjectionSchema(DATA_SCHEMA.getFields().stream()
      .map(Schema.Field::name).filter(f -> !f.equals("_hoodie_is_deleted")).toArray(String[]::new));
  protected static final Schema DATA_COLS_ONLY_SCHEMA = generateProjectionSchema("begin_lat", "tip_history", "rider");
  protected static final Schema META_COLS_ONLY_SCHEMA = generateProjectionSchema("_hoodie_commit_seqno", "_hoodie_record_key");

  static Stream<Arguments> testMorParams(boolean supportsParquetRowIndex) {
    Stream.Builder<Arguments> b = Stream.builder();
    for (boolean mergeUseRecordPosition : new boolean[] {true, false}) {
      for (boolean hasBuiltInDelete : new boolean[] {true, false}) {
        b.add(Arguments.of(EVENT_TIME_ORDERING, true, false, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
        b.add(Arguments.of(EVENT_TIME_ORDERING, false, false, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
        b.add(Arguments.of(COMMIT_TIME_ORDERING, false, false, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
        b.add(Arguments.of(CUSTOM, false, true, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
        b.add(Arguments.of(CUSTOM, false, false, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
      }
    }
    return b.build();
  }

  public void testMor(RecordMergeMode mergeMode,
                      boolean hasPrecombine,
                      boolean isProjectionCompatible,
                      boolean mergeUseRecordPosition,
                      boolean supportsParquetRowIndex,
                      boolean hasBuiltInDelete) throws IOException {
    Schema dataSchema = hasBuiltInDelete ? DATA_SCHEMA : DATA_SCHEMA_NO_DELETE;
    HoodieReaderContext<String> readerContext = mockReaderContext(supportsParquetRowIndex, true, false, mergeUseRecordPosition, null);
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    setupMORTable(mergeMode, hasPrecombine, isProjectionCompatible, hoodieTableConfig, readerContext, false);
    Schema requestedSchema = dataSchema;
    FileGroupReaderSchemaHandler schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    Schema expectedRequiredFullSchema = supportsParquetRowIndex && mergeUseRecordPosition
        ? ParquetRowIndexBasedSchemaHandler.addPositionalMergeCol(requestedSchema)
        : requestedSchema;
    assertEquals(expectedRequiredFullSchema, schemaHandler.getRequiredSchema());
    when(readerContext.getNeedsBootstrapMerge()).thenReturn(false);

    //read subset of columns
    requestedSchema = DATA_COLS_ONLY_SCHEMA;
    schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    Schema expectedRequiredSchema;
    if (mergeMode == EVENT_TIME_ORDERING && hasPrecombine) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "begin_lat", "tip_history", "rider", "_hoodie_record_key", "timestamp");
    } else if (mergeMode == EVENT_TIME_ORDERING || mergeMode == COMMIT_TIME_ORDERING) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "begin_lat", "tip_history", "rider", "_hoodie_record_key");
    } else if (mergeMode == CUSTOM && isProjectionCompatible) {
      expectedRequiredSchema = generateProjectionSchema("begin_lat", "tip_history", "rider", "begin_lon", "_hoodie_record_key", "timestamp");
    } else {
      expectedRequiredSchema = dataSchema;
    }
    if (supportsParquetRowIndex && mergeUseRecordPosition) {
      expectedRequiredSchema = addPositionalMergeCol(expectedRequiredSchema);
    }
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    when(readerContext.getNeedsBootstrapMerge()).thenReturn(false);
  }

  public void testMorBootstrap(RecordMergeMode mergeMode,
                               boolean hasPrecombine,
                               boolean isProjectionCompatible,
                               boolean mergeUseRecordPosition,
                               boolean supportsParquetRowIndex,
                               boolean hasBuiltInDelete) throws IOException {
    Schema dataSchema = hasBuiltInDelete ? DATA_SCHEMA : DATA_SCHEMA_NO_DELETE;
    HoodieReaderContext<String> readerContext = mockReaderContext(supportsParquetRowIndex, true, true, mergeUseRecordPosition, null);
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    setupMORTable(mergeMode, hasPrecombine, isProjectionCompatible, hoodieTableConfig, readerContext, true);
    Schema requestedSchema = dataSchema;
    FileGroupReaderSchemaHandler schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    Schema expectedRequiredFullSchema = supportsParquetRowIndex && mergeUseRecordPosition
        ? ParquetRowIndexBasedSchemaHandler.addPositionalMergeCol(requestedSchema)
        : requestedSchema;
    assertEquals(expectedRequiredFullSchema, schemaHandler.getRequiredSchema());
    when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
    Pair<List<Schema.Field>, List<Schema.Field>> bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    Pair<List<Schema.Field>, List<Schema.Field>> expectedBootstrapFields = FileGroupReaderSchemaHandler.getDataAndMetaCols(expectedRequiredFullSchema);
    if (supportsParquetRowIndex) {
      expectedBootstrapFields.getLeft().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
      expectedBootstrapFields.getRight().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
    }
    assertEquals(expectedBootstrapFields.getLeft(), bootstrapFields.getLeft());
    assertEquals(expectedBootstrapFields.getRight(), bootstrapFields.getRight());

    //read subset of columns
    requestedSchema = generateProjectionSchema("begin_lat", "tip_history", "_hoodie_record_key", "rider");
    schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    Schema expectedRequiredSchema;
    if (mergeMode == EVENT_TIME_ORDERING && hasPrecombine) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_record_key", "begin_lat", "tip_history", "rider", "timestamp");
    } else if (mergeMode == EVENT_TIME_ORDERING || mergeMode == COMMIT_TIME_ORDERING) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_record_key", "begin_lat", "tip_history", "rider");
    } else if (mergeMode == CUSTOM && isProjectionCompatible) {
      expectedRequiredSchema = generateProjectionSchema("_hoodie_record_key", "begin_lat", "tip_history", "rider", "begin_lon", "timestamp");
    } else {
      expectedRequiredSchema = dataSchema;
    }
    if (supportsParquetRowIndex && mergeUseRecordPosition) {
      expectedRequiredSchema = addPositionalMergeCol(expectedRequiredSchema);
    }
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    expectedBootstrapFields = FileGroupReaderSchemaHandler.getDataAndMetaCols(expectedRequiredSchema);
    if (supportsParquetRowIndex) {
      expectedBootstrapFields.getLeft().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
      expectedBootstrapFields.getRight().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
    }
    assertEquals(expectedBootstrapFields.getLeft(), bootstrapFields.getLeft());
    assertEquals(expectedBootstrapFields.getRight(), bootstrapFields.getRight());

    // request only data cols
    requestedSchema = DATA_COLS_ONLY_SCHEMA;
    if (mergeMode == EVENT_TIME_ORDERING && hasPrecombine) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_record_key", "begin_lat", "tip_history", "rider", "timestamp");
      when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
    } else if (mergeMode == EVENT_TIME_ORDERING || mergeMode == COMMIT_TIME_ORDERING) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_record_key", "begin_lat", "tip_history", "rider");
      when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
    } else if (mergeMode == CUSTOM && isProjectionCompatible) {
      expectedRequiredSchema = generateProjectionSchema("begin_lat", "tip_history", "rider", "begin_lon", "timestamp");
      when(readerContext.getNeedsBootstrapMerge()).thenReturn(false);
    } else {
      when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
      expectedRequiredSchema = dataSchema;
    }
    if (supportsParquetRowIndex && mergeUseRecordPosition) {
      expectedRequiredSchema = addPositionalMergeCol(expectedRequiredSchema);
    }
    schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    expectedBootstrapFields = FileGroupReaderSchemaHandler.getDataAndMetaCols(expectedRequiredSchema);
    if (supportsParquetRowIndex) {
      if (mergeMode == CUSTOM && isProjectionCompatible) {
        if (mergeUseRecordPosition) {
          expectedBootstrapFields.getRight().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
        }
      } else {
        expectedBootstrapFields.getLeft().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
        expectedBootstrapFields.getRight().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
      }
    }
    assertEquals(expectedBootstrapFields.getLeft(), bootstrapFields.getLeft());
    assertEquals(expectedBootstrapFields.getRight(), bootstrapFields.getRight());

    // request only meta cols
    requestedSchema = META_COLS_ONLY_SCHEMA;
    if (mergeMode == EVENT_TIME_ORDERING && hasPrecombine) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_commit_seqno", "_hoodie_record_key", "timestamp");
      when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
    } else if (mergeMode == EVENT_TIME_ORDERING || mergeMode == COMMIT_TIME_ORDERING) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_commit_seqno", "_hoodie_record_key");
      when(readerContext.getNeedsBootstrapMerge()).thenReturn(hasBuiltInDelete);
    } else if (mergeMode == CUSTOM && isProjectionCompatible) {
      expectedRequiredSchema = generateProjectionSchema("_hoodie_commit_seqno", "_hoodie_record_key", "begin_lat", "begin_lon", "timestamp");
      when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
    } else {
      expectedRequiredSchema = dataSchema;
      when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
    }
    if (supportsParquetRowIndex && mergeUseRecordPosition) {
      expectedRequiredSchema = addPositionalMergeCol(expectedRequiredSchema);
    }
    schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    expectedBootstrapFields = FileGroupReaderSchemaHandler.getDataAndMetaCols(expectedRequiredSchema);
    if (supportsParquetRowIndex) {
      if (!expectedBootstrapFields.getRight().isEmpty()) {
        expectedBootstrapFields.getLeft().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
        expectedBootstrapFields.getRight().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
      } else if (mergeUseRecordPosition) {
        expectedBootstrapFields.getLeft().add(ParquetRowIndexBasedSchemaHandler.getPositionalMergeField());
      }
    }
    assertEquals(expectedBootstrapFields.getLeft(), bootstrapFields.getLeft());
    assertEquals(expectedBootstrapFields.getRight(), bootstrapFields.getRight());
  }

  private static void setupMORTable(RecordMergeMode mergeMode, boolean hasPrecombine, boolean isProjectionCompatible, HoodieTableConfig hoodieTableConfig, HoodieReaderContext<String> readerContext,
                                    boolean isBootstrap)
      throws IOException {
    when(hoodieTableConfig.populateMetaFields()).thenReturn(Boolean.TRUE);
    when(hoodieTableConfig.getRecordMergeMode()).thenReturn(mergeMode);
    if (hasPrecombine) {
      when(hoodieTableConfig.getPreCombineField()).thenReturn("timestamp");
    } else {
      when(hoodieTableConfig.getPreCombineField()).thenReturn(null);
    }
    if (mergeMode == CUSTOM) {
      when(hoodieTableConfig.getRecordMergeStrategyId()).thenReturn("asdf");
      // NOTE: in this test custom doesn't have any meta cols because it is more interesting of a test case
      when(hoodieTableConfig.getTableVersion()).thenReturn(HoodieTableVersion.EIGHT);
      HoodieRecordMerger merger = mockRecordMerger(isProjectionCompatible,
          isBootstrap ? new String[] {"begin_lat", "begin_lon", "timestamp"} : new String[] {"begin_lat", "begin_lon", "_hoodie_record_key", "timestamp"});
      when(readerContext.getRecordMerger()).thenReturn((Option.of(merger)));
    }
  }

  private static HoodieRecordMerger mockRecordMerger(boolean isProjectionCompatible, String[] mandatoryFields) throws IOException {
    HoodieRecordMerger merger = mock(HoodieRecordMerger.class);
    when(merger.isProjectionCompatible()).thenReturn(isProjectionCompatible);
    when(merger.merge(any(), any(), any(), any(), any())).thenReturn(null);
    when(merger.getMandatoryFieldsForMerging(any(), any(), any())).thenReturn(mandatoryFields);
    return merger;
  }

  static HoodieReaderContext<String> mockReaderContext(boolean supportsParquetRowIndex, boolean hasLogFiles,
                                                       boolean hasBootstrapBaseFile, boolean mergeUseRecordPosition, HoodieRecordMerger merger) {
    HoodieReaderContext<String> readerContext = mock(HoodieReaderContext.class);
    when(readerContext.supportsParquetRowIndex()).thenReturn(supportsParquetRowIndex);
    when(readerContext.getHasLogFiles()).thenReturn(hasLogFiles);
    when(readerContext.getHasBootstrapBaseFile()).thenReturn(hasBootstrapBaseFile);
    when(readerContext.getShouldMergeUseRecordPosition()).thenReturn(mergeUseRecordPosition);
    when(readerContext.getRecordMerger()).thenReturn(Option.ofNullable(merger));
    when(readerContext.getInstantRange()).thenReturn(Option.empty());
    return readerContext;
  }

  static FileGroupReaderSchemaHandler createSchemaHandler(HoodieReaderContext<String> readerContext, Schema dataSchema,
                                                          Schema requestedSchema, HoodieTableConfig hoodieTableConfig,
                                                          boolean supportsParquetRowIndex) {
    return supportsParquetRowIndex
        ? new ParquetRowIndexBasedSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties())
        : new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties());
  }

  static Schema generateProjectionSchema(String... fields) {
    return HoodieAvroUtils.generateProjectionSchema(DATA_SCHEMA, Arrays.asList(fields));
  }

  private static Schema generateProjectionSchema(boolean hasBuiltInDelete, String... fields) {
    List<String> fieldList = Arrays.asList(fields);
    if (hasBuiltInDelete) {
      fieldList = new ArrayList<>(fieldList);
      fieldList.add("_hoodie_is_deleted");
    }
    return HoodieAvroUtils.generateProjectionSchema(DATA_SCHEMA, fieldList);
  }

  Schema.Field getField(String fieldName) {
    return DATA_SCHEMA.getField(fieldName);
  }
}
