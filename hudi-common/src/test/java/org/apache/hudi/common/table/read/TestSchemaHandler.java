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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
import static org.apache.hudi.common.table.read.ParquetRowIndexBasedSchemaHandler.getPositionalMergeField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSchemaHandler {

  protected static final Schema DATA_SCHEMA = HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.AVRO_SCHEMA);
  protected static final Schema DATA_SCHEMA_NO_DELETE = generateProjectionSchema(DATA_SCHEMA.getFields().stream()
      .map(Schema.Field::name).filter(f -> !f.equals("_hoodie_is_deleted")).toArray(String[]::new));
  protected static final Schema DATA_COLS_ONLY_SCHEMA = generateProjectionSchema("begin_lat", "tip_history", "rider");
  protected static final Schema META_COLS_ONLY_SCHEMA = generateProjectionSchema("_hoodie_commit_seqno", "_hoodie_record_key");

  @Test
  public void testCow() {
    HoodieReaderContext<String> readerContext = new MockReaderContext(false);
    readerContext.setHasLogFiles(false);
    readerContext.setHasBootstrapBaseFile(false);
    readerContext.setShouldMergeUseRecordPosition(false);
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    Schema requestedSchema = DATA_SCHEMA;
    FileGroupReaderSchemaHandler schemaHandler = new FileGroupReaderSchemaHandler(readerContext, DATA_SCHEMA,
        requestedSchema, Option.empty(), hoodieTableConfig, new TypedProperties());
    assertEquals(requestedSchema, schemaHandler.getRequiredSchema());

    //read subset of columns
    requestedSchema = generateProjectionSchema("begin_lat", "tip_history", "rider");
    schemaHandler =
        new FileGroupReaderSchemaHandler(readerContext, DATA_SCHEMA, requestedSchema,
            Option.empty(), hoodieTableConfig, new TypedProperties());
    assertEquals(requestedSchema, schemaHandler.getRequiredSchema());
    assertFalse(readerContext.getNeedsBootstrapMerge());
  }

  @Test
  public void testCowBootstrap() {
    HoodieReaderContext<String> readerContext = new MockReaderContext(false);
    readerContext.setHasLogFiles(false);
    readerContext.setHasBootstrapBaseFile(true);
    readerContext.setShouldMergeUseRecordPosition(false);
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    Schema requestedSchema = generateProjectionSchema("begin_lat", "tip_history", "_hoodie_record_key", "rider");
    FileGroupReaderSchemaHandler schemaHandler =
        new FileGroupReaderSchemaHandler(readerContext, DATA_SCHEMA, requestedSchema,
            Option.empty(), hoodieTableConfig, new TypedProperties());
    //meta cols must go first in the required schema
    Schema expectedRequiredSchema = generateProjectionSchema("_hoodie_record_key", "begin_lat", "tip_history", "rider");
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    Pair<List<Schema.Field>, List<Schema.Field>> bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertEquals(Collections.singletonList(getField("_hoodie_record_key")), bootstrapFields.getLeft());
    assertEquals(Arrays.asList(getField("begin_lat"), getField("tip_history"), getField("rider")), bootstrapFields.getRight());
    assertTrue(readerContext.getNeedsBootstrapMerge());
  }

  @Test
  public void testCowBootstrapWithPositionMerge() {
    HoodieReaderContext<String> readerContext = new MockReaderContext(true);
    readerContext.setHasLogFiles(false);
    readerContext.setHasBootstrapBaseFile(true);
    readerContext.setShouldMergeUseRecordPosition(false);
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    Schema requestedSchema = generateProjectionSchema("begin_lat", "tip_history", "_hoodie_record_key", "rider");
    FileGroupReaderSchemaHandler schemaHandler =
        new ParquetRowIndexBasedSchemaHandler(readerContext, DATA_SCHEMA, requestedSchema,
            Option.empty(), hoodieTableConfig, new TypedProperties());
    //meta cols must go first in the required schema
    Schema expectedRequiredSchema = generateProjectionSchema("_hoodie_record_key", "begin_lat", "tip_history", "rider");
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    Pair<List<Schema.Field>, List<Schema.Field>> bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertEquals(Arrays.asList(getField("_hoodie_record_key"), getPositionalMergeField()), bootstrapFields.getLeft());
    assertEquals(Arrays.asList(getField("begin_lat"), getField("tip_history"), getField("rider"), getPositionalMergeField()), bootstrapFields.getRight());
    assertTrue(readerContext.getNeedsBootstrapMerge());

    schemaHandler = new ParquetRowIndexBasedSchemaHandler(readerContext, DATA_SCHEMA, DATA_COLS_ONLY_SCHEMA,
        Option.empty(), hoodieTableConfig, new TypedProperties());
    assertEquals(DATA_COLS_ONLY_SCHEMA, schemaHandler.getRequiredSchema());
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertTrue(bootstrapFields.getLeft().isEmpty());
    assertEquals(Arrays.asList(getField("begin_lat"), getField("tip_history"), getField("rider")), bootstrapFields.getRight());
    assertFalse(readerContext.getNeedsBootstrapMerge());

    schemaHandler = new ParquetRowIndexBasedSchemaHandler(readerContext, DATA_SCHEMA, META_COLS_ONLY_SCHEMA,
        Option.empty(), hoodieTableConfig, new TypedProperties());
    assertEquals(META_COLS_ONLY_SCHEMA, schemaHandler.getRequiredSchema());
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertEquals(Arrays.asList(getField("_hoodie_commit_seqno"), getField("_hoodie_record_key")), bootstrapFields.getLeft());
    assertTrue(bootstrapFields.getRight().isEmpty());
    assertFalse(readerContext.getNeedsBootstrapMerge());
  }

  private static Stream<Arguments> testMorParams() {
    Stream.Builder<Arguments> b = Stream.builder();
    for (boolean mergeUseRecordPosition : new boolean[] {true, false}) {
      for (boolean supportsParquetRowIndex : new boolean[] {true, false}) {
        for (boolean hasBuiltInDelete : new boolean[] {true, false}) {
          b.add(Arguments.of(EVENT_TIME_ORDERING, true, false, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
          b.add(Arguments.of(EVENT_TIME_ORDERING, false, false, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
          b.add(Arguments.of(COMMIT_TIME_ORDERING, false, false, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
          b.add(Arguments.of(CUSTOM, false, true, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
          b.add(Arguments.of(CUSTOM, false, false, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete));
        }
      }
    }
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testMorParams")
  public void testMor(RecordMergeMode mergeMode,
                      boolean hasPrecombine,
                      boolean isProjectionCompatible,
                      boolean mergeUseRecordPosition,
                      boolean supportsParquetRowIndex,
                      boolean hasBuiltInDelete) {
    Schema dataSchema = hasBuiltInDelete ? DATA_SCHEMA : DATA_SCHEMA_NO_DELETE;
    HoodieReaderContext<String> readerContext = new MockReaderContext(supportsParquetRowIndex);
    readerContext.setHasLogFiles(true);
    readerContext.setHasBootstrapBaseFile(false);
    //has no effect on schema unless we support position based merging
    readerContext.setShouldMergeUseRecordPosition(mergeUseRecordPosition);
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    when(hoodieTableConfig.populateMetaFields()).thenReturn(Boolean.TRUE);
    when(hoodieTableConfig.getRecordMergeMode()).thenReturn(mergeMode);
    if (hasPrecombine) {
      when(hoodieTableConfig.getPreCombineField()).thenReturn("timestamp");
    } else {
      when(hoodieTableConfig.getPreCombineField()).thenReturn(null);
    }
    if (mergeMode == CUSTOM) {
      when(hoodieTableConfig.getRecordMergeStrategyId()).thenReturn("asdf");
      Option<HoodieRecordMerger> merger = Option.of(new MockMerger(isProjectionCompatible, new String[]{"begin_lat", "begin_lon", "_hoodie_record_key", "timestamp"}));
      readerContext.setRecordMerger(merger);
    }
    Schema requestedSchema = dataSchema;
    FileGroupReaderSchemaHandler schemaHandler = supportsParquetRowIndex
        ? new ParquetRowIndexBasedSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties())
        : new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties());
    Schema expectedRequiredFullSchema = supportsParquetRowIndex && mergeUseRecordPosition
        ? ParquetRowIndexBasedSchemaHandler.addPositionalMergeCol(requestedSchema)
        : requestedSchema;
    assertEquals(expectedRequiredFullSchema, schemaHandler.getRequiredSchema());
    assertFalse(readerContext.getNeedsBootstrapMerge());

    //read subset of columns
    requestedSchema = DATA_COLS_ONLY_SCHEMA;
    schemaHandler = supportsParquetRowIndex
        ? new ParquetRowIndexBasedSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties())
        : new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties());
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

  @ParameterizedTest
  @MethodSource("testMorParams")
  public void testMorBootstrap(RecordMergeMode mergeMode,
                               boolean hasPrecombine,
                               boolean isProjectionCompatible,
                               boolean mergeUseRecordPosition,
                               boolean supportsParquetRowIndex,
                               boolean hasBuiltInDelete) {
    Schema dataSchema = hasBuiltInDelete ? DATA_SCHEMA : DATA_SCHEMA_NO_DELETE;
    HoodieReaderContext<String> readerContext = new MockReaderContext(supportsParquetRowIndex);
    readerContext.setHasLogFiles(true);
    readerContext.setHasBootstrapBaseFile(true);
    readerContext.setShouldMergeUseRecordPosition(mergeUseRecordPosition);
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
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
      Option<HoodieRecordMerger> merger = Option.of(new MockMerger(isProjectionCompatible, new String[]{"begin_lat", "begin_lon", "timestamp"}));
      readerContext.setRecordMerger(merger);
    }
    Schema requestedSchema = dataSchema;
    FileGroupReaderSchemaHandler schemaHandler = supportsParquetRowIndex
        ? new ParquetRowIndexBasedSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties())
        : new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties());
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
    schemaHandler = supportsParquetRowIndex
        ? new ParquetRowIndexBasedSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties())
        : new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties());
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
    schemaHandler = supportsParquetRowIndex
        ? new ParquetRowIndexBasedSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties())
        : new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties());
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
    schemaHandler = supportsParquetRowIndex
        ? new ParquetRowIndexBasedSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties())
        : new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema,
        Option.empty(), hoodieTableConfig, new TypedProperties());
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

  private static Schema generateProjectionSchema(String... fields) {
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

  private Schema.Field getField(String fieldName) {
    return DATA_SCHEMA.getField(fieldName);
  }

  static class MockMerger implements HoodieRecordMerger {

    private final boolean isProjectionCompatible;
    private final String[] mandatoryMergeFields;

    MockMerger(boolean isProjectionCompatible, String[] mandatoryMergeFields) {
      this.isProjectionCompatible = isProjectionCompatible;
      this.mandatoryMergeFields = mandatoryMergeFields;
    }

    @Override
    public boolean isProjectionCompatible() {
      return this.isProjectionCompatible;
    }

    @Override
    public String[] getMandatoryFieldsForMerging(Schema dataSchema, HoodieTableConfig cfg, TypedProperties properties) {
      return this.mandatoryMergeFields;
    }

    @Override
    public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer,
                                                    Schema newSchema, TypedProperties props) throws IOException {
      return null;
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType() {
      return null;
    }

    @Override
    public String getMergingStrategy() {
      return "";
    }
  }

  static class MockReaderContext extends HoodieReaderContext<String> {
    private final boolean supportsParquetRowIndex;

    MockReaderContext(boolean supportsParquetRowIndex) {
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
    public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
      return null;
    }

    @Override
    public Object getValue(String record, Schema schema, String fieldName) {
      return null;
    }

    @Override
    public HoodieRecord<String> constructHoodieRecord(Option<String> recordOption, Map<String, Object> metadataMap) {
      return null;
    }

    @Override
    public String seal(String record) {
      return "";
    }

    @Override
    public ClosableIterator<String> mergeBootstrapReaders(ClosableIterator<String> skeletonFileIterator, Schema skeletonRequiredSchema, ClosableIterator<String> dataFileIterator,
                                                          Schema dataRequiredSchema) {
      return null;
    }

    @Override
    public UnaryOperator<String> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
      return null;
    }
  }
}
