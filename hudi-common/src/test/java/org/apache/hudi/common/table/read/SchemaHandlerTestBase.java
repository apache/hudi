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
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.RecordMergeMode.COMMIT_TIME_ORDERING;
import static org.apache.hudi.common.config.RecordMergeMode.CUSTOM;
import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.common.table.read.ParquetRowIndexBasedSchemaHandler.addPositionalMergeCol;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    setupMORTable(mergeMode, hasPrecombine, hoodieTableConfig);
    HoodieRecordMerger merger = mockRecordMerger(isProjectionCompatible,
        isProjectionCompatible ? new String[] {"begin_lat", "begin_lon", "_hoodie_record_key", "timestamp"} : new String[] {"begin_lat", "begin_lon", "timestamp"});
    HoodieReaderContext<String> readerContext = createReaderContext(hoodieTableConfig, supportsParquetRowIndex, true, false, mergeUseRecordPosition, merger);
    readerContext.setRecordMerger(Option.of(merger));
    Schema requestedSchema = dataSchema;
    FileGroupReaderSchemaHandler schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    Schema expectedRequiredFullSchema = supportsParquetRowIndex && mergeUseRecordPosition
        ? ParquetRowIndexBasedSchemaHandler.addPositionalMergeCol(requestedSchema)
        : requestedSchema;
    assertEquals(expectedRequiredFullSchema, schemaHandler.getRequiredSchema());
    assertFalse(readerContext.getNeedsBootstrapMerge());

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
    assertFalse(readerContext.getNeedsBootstrapMerge());
  }

  public void testMorBootstrap(RecordMergeMode mergeMode,
                               boolean hasPrecombine,
                               boolean isProjectionCompatible,
                               boolean mergeUseRecordPosition,
                               boolean supportsParquetRowIndex,
                               boolean hasBuiltInDelete) throws IOException {
    Schema dataSchema = hasBuiltInDelete ? DATA_SCHEMA : DATA_SCHEMA_NO_DELETE;
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    setupMORTable(mergeMode, hasPrecombine, hoodieTableConfig);
    HoodieRecordMerger merger = mockRecordMerger(isProjectionCompatible, new String[] {"begin_lat", "begin_lon", "timestamp"});
    HoodieReaderContext<String> readerContext = createReaderContext(hoodieTableConfig, supportsParquetRowIndex, true, true, mergeUseRecordPosition, merger);
    readerContext.setRecordMerger(Option.of(merger));
    Schema requestedSchema = dataSchema;
    FileGroupReaderSchemaHandler schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    Schema expectedRequiredFullSchema = supportsParquetRowIndex && mergeUseRecordPosition
        ? ParquetRowIndexBasedSchemaHandler.addPositionalMergeCol(requestedSchema)
        : requestedSchema;
    assertEquals(expectedRequiredFullSchema, schemaHandler.getRequiredSchema());
    assertTrue(readerContext.getNeedsBootstrapMerge());
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
    assertTrue(readerContext.getNeedsBootstrapMerge());
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
    schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    if (mergeMode == EVENT_TIME_ORDERING && hasPrecombine) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_record_key", "begin_lat", "tip_history", "rider", "timestamp");
      assertTrue(readerContext.getNeedsBootstrapMerge());
    } else if (mergeMode == EVENT_TIME_ORDERING || mergeMode == COMMIT_TIME_ORDERING) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_record_key", "begin_lat", "tip_history", "rider");
      assertTrue(readerContext.getNeedsBootstrapMerge());
    } else if (mergeMode == CUSTOM && isProjectionCompatible) {
      expectedRequiredSchema = generateProjectionSchema("begin_lat", "tip_history", "rider", "begin_lon", "timestamp");
      assertFalse(readerContext.getNeedsBootstrapMerge());
    } else {
      assertTrue(readerContext.getNeedsBootstrapMerge());
      expectedRequiredSchema = dataSchema;
    }
    if (supportsParquetRowIndex && mergeUseRecordPosition) {
      expectedRequiredSchema = addPositionalMergeCol(expectedRequiredSchema);
    }
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
    schemaHandler = createSchemaHandler(readerContext, dataSchema, requestedSchema, hoodieTableConfig, supportsParquetRowIndex);
    if (mergeMode == EVENT_TIME_ORDERING && hasPrecombine) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_commit_seqno", "_hoodie_record_key", "timestamp");
      assertTrue(readerContext.getNeedsBootstrapMerge());
    } else if (mergeMode == EVENT_TIME_ORDERING || mergeMode == COMMIT_TIME_ORDERING) {
      expectedRequiredSchema = generateProjectionSchema(hasBuiltInDelete, "_hoodie_commit_seqno", "_hoodie_record_key");
      assertEquals(hasBuiltInDelete, readerContext.getNeedsBootstrapMerge());
    } else if (mergeMode == CUSTOM && isProjectionCompatible) {
      expectedRequiredSchema = generateProjectionSchema("_hoodie_commit_seqno", "_hoodie_record_key", "begin_lat", "begin_lon", "timestamp");
      assertTrue(readerContext.getNeedsBootstrapMerge());
    } else {
      expectedRequiredSchema = dataSchema;
      assertTrue(readerContext.getNeedsBootstrapMerge());
    }
    if (supportsParquetRowIndex && mergeUseRecordPosition) {
      expectedRequiredSchema = addPositionalMergeCol(expectedRequiredSchema);
    }
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

  private static void setupMORTable(RecordMergeMode mergeMode, boolean hasPrecombine, HoodieTableConfig hoodieTableConfig) {
    when(hoodieTableConfig.populateMetaFields()).thenReturn(true);
    when(hoodieTableConfig.getRecordMergeMode()).thenReturn(mergeMode);
    if (hasPrecombine) {
      when(hoodieTableConfig.getPreCombineFieldsStr()).thenReturn(Option.of("timestamp"));
      when(hoodieTableConfig.getPreCombineFields()).thenReturn(Collections.singletonList("timestamp"));
    } else {
      when(hoodieTableConfig.getPreCombineFieldsStr()).thenReturn(Option.empty());
      when(hoodieTableConfig.getPreCombineFields()).thenReturn(Collections.emptyList());
    }
    if (mergeMode == CUSTOM) {
      when(hoodieTableConfig.getRecordMergeStrategyId()).thenReturn("asdf");
      // NOTE: in this test custom doesn't have any meta cols because it is more interesting of a test case
      when(hoodieTableConfig.getTableVersion()).thenReturn(HoodieTableVersion.EIGHT);
    }
  }

  private static HoodieRecordMerger mockRecordMerger(boolean isProjectionCompatible, String[] mandatoryFields) throws IOException {
    HoodieRecordMerger merger = mock(HoodieRecordMerger.class);
    when(merger.isProjectionCompatible()).thenReturn(isProjectionCompatible);
    when(merger.merge(any(), any(), any(), any(), any())).thenReturn(null);
    when(merger.getMandatoryFieldsForMerging(any(), any(), any())).thenReturn(mandatoryFields);
    return merger;
  }

  static HoodieReaderContext<String> createReaderContext(HoodieTableConfig hoodieTableConfig, boolean supportsParquetRowIndex, boolean hasLogFiles,
                                                         boolean hasBootstrapBaseFile, boolean mergeUseRecordPosition, HoodieRecordMerger merger) {
    HoodieReaderContext<String> readerContext = new StubbedReaderContext(hoodieTableConfig, supportsParquetRowIndex);
    readerContext.setHasLogFiles(hasLogFiles);
    readerContext.setHasBootstrapBaseFile(hasBootstrapBaseFile);
    readerContext.setShouldMergeUseRecordPosition(mergeUseRecordPosition);
    readerContext.setRecordMerger(Option.ofNullable(merger));
    return readerContext;
  }

  abstract FileGroupReaderSchemaHandler createSchemaHandler(HoodieReaderContext<String> readerContext, Schema dataSchema,
                                                          Schema requestedSchema, HoodieTableConfig hoodieTableConfig,
                                                          boolean supportsParquetRowIndex);

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

  static class StubbedReaderContext extends HoodieReaderContext<String> {
    private final boolean supportsParquetRowIndex;

    protected StubbedReaderContext(HoodieTableConfig hoodieTableConfig, boolean supportsParquetRowIndex) {
      super(null, hoodieTableConfig, Option.empty(), Option.empty());
      this.supportsParquetRowIndex = supportsParquetRowIndex;
    }

    @Override
    public boolean supportsParquetRowIndex() {
      return this.supportsParquetRowIndex;
    }

    @Override
    public ClosableIterator<String> getFileRecordIterator(StoragePath filePath, long start, long length, Schema dataSchema, Schema requiredSchema, HoodieStorage storage) throws IOException {
      return null;
    }

    @Override
    public String convertAvroRecord(IndexedRecord avroRecord) {
      return "";
    }

    @Override
    public GenericRecord convertToAvroRecord(String record, Schema schema) {
      return null;
    }

    @Override
    public String getDeleteRow(String record, String recordKey) {
      return "";
    }

    @Override
    public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
      return null;
    }

    @Override
    public Object getValue(String record, Schema schema, String fieldName) {
      return null;
    }

    @Override
    public String getMetaFieldValue(String record, int pos) {
      return "";
    }

    @Override
    public HoodieRecord<String> constructHoodieRecord(BufferedRecord<String> bufferedRecord) {
      return null;
    }

    @Override
    public String constructEngineRecord(Schema schema, Map<Integer, Object> updateValues, BufferedRecord<String> baseRecord) {
      return "";
    }

    @Override
    public String seal(String record) {
      return "";
    }

    @Override
    public String toBinaryRow(Schema avroSchema, String record) {
      return "";
    }

    @Override
    public ClosableIterator<String> mergeBootstrapReaders(ClosableIterator<String> skeletonFileIterator, Schema skeletonRequiredSchema, ClosableIterator<String> dataFileIterator,
                                                          Schema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues) {
      return null;
    }

    @Override
    public UnaryOperator<String> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
      return null;
    }
  }
}
