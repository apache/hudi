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

package org.apache.hudi.functional;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DefaultSparkRecordMerger;
import org.apache.hudi.OverwriteWithLatestSparkRecordMerger;
import org.apache.hudi.avro.AvroReaderContextTypeConverter;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import scala.collection.immutable.Seq;

import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBufferedRecordMerger extends SparkClientFunctionalTestHarness {
  private static final String RECORD_KEY = "test_key";
  private static final String PARTITION_PATH = "test_partition";
  private static final long ORDERING_VALUE = 100L;
  private static final String IGNORE_MARKERS_VALUE = "__HUDI_DEFAULT_MARKER__";
  private static final Schema fullSchema = getFullSchema();
  private static final Schema partialSchema = getPartialSchema();
  private HoodieTableConfig tableConfig;
  private StorageConfiguration<?> storageConfig;
  private TypedProperties props;
  private HoodieReaderContext<InternalRow> readerContext;

  @BeforeEach
  void setUp() throws IOException {
    // Setup mocks
    tableConfig = mock(HoodieTableConfig.class);
    storageConfig = mock(StorageConfiguration.class);
    when(tableConfig.getPayloadClass()).thenReturn(
        "org.apache.hudi.common.model.DefaultHoodieRecordPayload");
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"id"}));

    // Create reader context.
    props = new TypedProperties();
    readerContext = new DummyInternalRowReaderContext(
        storageConfig, tableConfig, Option.empty(), Option.empty());
  }

  // ============================================================================
  // Test Set 1: enablePartialMerging = false
  // ============================================================================
  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testNoPartialMerging(RecordMergeMode mergeMode) throws IOException {
    // Test 1: NONE partial update mode (regular path)
    BufferedRecordMerger<InternalRow> noneMerger = createMerger(readerContext, mergeMode, PartialUpdateMode.NONE);
    // Create records with all columns
    InternalRow oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    InternalRow newRecord = createFullRecord("new_id", "New Name", 30, "New City", 2000L);
    BufferedRecord<InternalRow> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<InternalRow> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE - 1, newRecord, 1, false);

    Option<BufferedRecord<InternalRow>> deltaResult = noneMerger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    if (mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING) {
      assertTrue(deltaResult.isPresent());
      assertEquals(newRecord, deltaResult.get().getRecord());
      BufferedRecord<InternalRow> mergedRecord = deltaResult.get();
      assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
      assertFalse(mergedRecord.isDelete());
      assertNotNull(mergedRecord.getRecord());
    } else {
      assertTrue(deltaResult.isEmpty());
    }

    // Test final merge
    InternalRow olderRecord = createFullRecord(
        "older_id", "Older Name", 20, "Older City", 500L);
    InternalRow newerRecord = createFullRecord(
        "newer_id", "Newer Name", 35, "Newer City", 3000L);
    BufferedRecord<InternalRow> olderBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecord, 1, false);
    BufferedRecord<InternalRow> newerBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecord, 1, false);
    Pair<Boolean, InternalRow> finalResult = noneMerger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    assertFalse(finalResult.getLeft());
    assertNotNull(finalResult.getRight());

    // Test 2: IGNORE_DEFAULTS partial update mode
    BufferedRecordMerger<InternalRow> ignoreDefaultsMerger =
        createMerger(readerContext, mergeMode, PartialUpdateMode.IGNORE_DEFAULTS);
    
    // Old record has all columns, new record has some columns with default/null values
    InternalRow oldRecordWithDefaults = createFullRecord(
        "old_id", "Old Name", 25, "Old City", 1000L);
    InternalRow newRecordWithDefaults = createRecordWithDefaults(
        "new_id", "New Name", 0, null, 0L);
    BufferedRecord<InternalRow> oldBufferedRecordWithDefaults =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecordWithDefaults, 1, false);
    BufferedRecord<InternalRow> newBufferedRecordWithDefaults =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecordWithDefaults, 1, false);
    Option<BufferedRecord<InternalRow>> deltaResultDefaults =
        ignoreDefaultsMerger.deltaMerge(newBufferedRecordWithDefaults, oldBufferedRecordWithDefaults);
    assertTrue(deltaResultDefaults.isPresent());
    BufferedRecord<InternalRow> mergedRecordDefaults = deltaResultDefaults.get();
    assertEquals("Old City", mergedRecordDefaults.getRecord().getString(3));
    assertEquals(RECORD_KEY, mergedRecordDefaults.getRecordKey());
    assertFalse(mergedRecordDefaults.isDelete());
    assertNotNull(mergedRecordDefaults.getRecord());

    // Test final merge with defaults
    InternalRow olderRecordWithDefaults =
        createFullRecord("older_id", "Older Name", 20, "Older City", 500L);
    InternalRow newerRecordWithDefaults =
        createRecordWithDefaults("newer_id", "Newer Name", 0, null, 0L);
    BufferedRecord<InternalRow> olderBufferedRecordWithDefaults =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecordWithDefaults, 1, false);
    BufferedRecord<InternalRow> newerBufferedRecordWithDefaults =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecordWithDefaults, 1, false);

    Pair<Boolean, InternalRow> finalResultDefaults =
        ignoreDefaultsMerger.finalMerge(olderBufferedRecordWithDefaults, newerBufferedRecordWithDefaults);
    assertFalse(finalResultDefaults.getLeft());
    assertNotNull(finalResultDefaults.getRight());

    // Test 3: IGNORE_MARKERS partial update mode
    props.put(HoodieTableConfig.MERGE_PROPERTIES.key(), PARTIAL_UPDATE_CUSTOM_MARKER + "=" + IGNORE_MARKERS_VALUE);
    BufferedRecordMerger<InternalRow> ignoreMarkersMerger =
        createMerger(readerContext, mergeMode, PartialUpdateMode.IGNORE_MARKERS);

    // Old record has all columns, new record has some columns with marker values
    InternalRow oldRecordWithMarkers =
        createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    InternalRow newRecordWithMarkers =
        createRecordWithMarkers("new_id", "New Name", 0, IGNORE_MARKERS_VALUE, 0L);
    
    BufferedRecord<InternalRow> oldBufferedRecordWithMarkers =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecordWithMarkers, 1, false);
    BufferedRecord<InternalRow> newBufferedRecordWithMarkers =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecordWithMarkers, 1, false);

    Option<BufferedRecord<InternalRow>> deltaResultMarkers =
        ignoreMarkersMerger.deltaMerge(newBufferedRecordWithMarkers, oldBufferedRecordWithMarkers);
    assertTrue(deltaResultMarkers.isPresent());
    BufferedRecord<InternalRow> mergedRecordMarkers = deltaResultMarkers.get();
    assertEquals(RECORD_KEY, mergedRecordMarkers.getRecordKey());
    assertFalse(mergedRecordMarkers.isDelete());
    assertNotNull(mergedRecordMarkers.getRecord());

    // Test final merge with markers
    InternalRow olderRecordWithMarkers =
        createFullRecord("older_id", "Older Name", 20, "Older City", 500L);
    InternalRow newerRecordWithMarkers =
        createRecordWithMarkers("newer_id", "Newer Name", 0, IGNORE_MARKERS_VALUE, 0L);
    
    BufferedRecord<InternalRow> olderBufferedRecordWithMarkers =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecordWithMarkers, 1, false);
    BufferedRecord<InternalRow> newerBufferedRecordWithMarkers =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecordWithMarkers, 1, false);

    Pair<Boolean, InternalRow> finalResultMarkers =
        ignoreMarkersMerger.finalMerge(olderBufferedRecordWithMarkers, newerBufferedRecordWithMarkers);
    assertEquals("Older City", finalResultMarkers.getRight().getString(3));
    assertFalse(finalResultMarkers.getLeft());
    assertNotNull(finalResultMarkers.getRight());
  }

  // ============================================================================
  // Test Set 2: enablePartialMerging = true
  // ============================================================================
  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testPartialMerging(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<InternalRow> merger = createPartialMerger(mergeMode, readerContext);
    
    // Test 1: Old record has all columns, new record has partial columns
    InternalRow oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    InternalRow newRecord = createPartialRecord("new_id", "New Name");
    
    BufferedRecord<InternalRow> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<InternalRow> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 2, false);

    Option<BufferedRecord<InternalRow>> deltaResult = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    assertTrue(deltaResult.isPresent());
    BufferedRecord<InternalRow> mergedRecord = deltaResult.get();
    assertEquals("new_id", mergedRecord.getRecordKey());
    assertFalse(mergedRecord.isDelete());
    assertNotNull(mergedRecord.getRecord());

    // Test 2: Old record has fewer columns than new columns.
    deltaResult = merger.deltaMerge(oldBufferedRecord, newBufferedRecord);
    assertTrue(deltaResult.isPresent());
    assertEquals("old_id", deltaResult.get().getRecordKey());
    assertFalse(deltaResult.get().isDelete());
    assertNotNull(deltaResult.get().getRecord());

    // Test 3: Test final merge with partial records
    InternalRow olderRecord = createFullRecord("older_id", "Older Name", 20, "Older City", 500L);
    InternalRow newerRecord = createPartialRecord("newer_id", "Newer Name");
    BufferedRecord<InternalRow> olderBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecord, 1, false);
    BufferedRecord<InternalRow> newerBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecord, 2, false);

    Pair<Boolean, InternalRow> finalResult = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    assertFalse(finalResult.getLeft());
    assertNotNull(finalResult.getRight());

    // Test 2: Different column sets - Old record has id, name, age, city; New record has id, name, timestamp
    InternalRow oldRecordDifferent = createRecordWithIdNameAgeCity(
        "old_id", "Old Name", 25, "Old City");
    InternalRow newRecordDifferent = createRecordWithIdNameTimestamp(
        "new_id", "New Name", 2000L);
    
    BufferedRecord<InternalRow> oldBufferedRecordDifferent =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecordDifferent, 1, false);
    BufferedRecord<InternalRow> newBufferedRecordDifferent =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecordDifferent, 1, false);

    Option<BufferedRecord<InternalRow>> deltaResultDifferent =
        merger.deltaMerge(newBufferedRecordDifferent, oldBufferedRecordDifferent);
    assertTrue(deltaResultDifferent.isPresent());
    BufferedRecord<InternalRow> mergedRecordDifferent = deltaResultDifferent.get();
    assertEquals("new_id", mergedRecordDifferent.getRecordKey());
    assertFalse(mergedRecordDifferent.isDelete());
    assertNotNull(mergedRecordDifferent.getRecord());

    // Test final merge with different column sets
    InternalRow olderRecordDifferent =
        createRecordWithIdNameAgeCity("older_id", "Older Name", 20, "Older City");
    InternalRow newerRecordDifferent =
        createRecordWithIdNameTimestamp("newer_id", "Newer Name", 3000L);
    
    BufferedRecord<InternalRow> olderBufferedRecordDifferent =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecordDifferent, 1, false);
    BufferedRecord<InternalRow> newerBufferedRecordDifferent =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecordDifferent, 1, false);

    Pair<Boolean, InternalRow> finalResultDifferent =
        merger.finalMerge(olderBufferedRecordDifferent, newerBufferedRecordDifferent);
    assertFalse(finalResultDifferent.getLeft()); // Should not be a delete
    assertNotNull(finalResultDifferent.getRight()); // Should have a record
  }

  // ============================================================================
  // Edge Cases and Additional Tests
  // ============================================================================

  @Test
  void testDeltaMergeWithNullExistingRecord() throws IOException {
    BufferedRecordMerger<InternalRow> merger = createMerger(
        readerContext, RecordMergeMode.COMMIT_TIME_ORDERING, PartialUpdateMode.NONE);
    
    InternalRow newRecord = createFullRecord(
        "new_id", "New Name", 30, "New City", 2000L);
    BufferedRecord<InternalRow> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, newRecord, 1, false);
    Option<BufferedRecord<InternalRow>> result =
        merger.deltaMerge(newBufferedRecord, null);
    
    assertTrue(result.isPresent());
    BufferedRecord<InternalRow> mergedRecord = result.get();
    assertEquals(newRecord, mergedRecord.getRecord());
  }

  @Test
  void testDeltaMergeWithDeleteRecord() {
    BufferedRecordMerger<InternalRow> merger = createMerger(
        readerContext, RecordMergeMode.COMMIT_TIME_ORDERING, PartialUpdateMode.NONE);
    
    InternalRow oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    BufferedRecord<InternalRow> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    DeleteRecord deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH);
    Option<DeleteRecord> result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    
    assertTrue(result.isPresent());
    DeleteRecord mergedDeleteRecord = result.get();
    assertEquals(RECORD_KEY, mergedDeleteRecord.getRecordKey());
  }

  @Test
  void testDeltaMergeWithDeleteRecords() throws IOException {
    BufferedRecordMerger<InternalRow> merger = createMerger(
        readerContext, RecordMergeMode.COMMIT_TIME_ORDERING, PartialUpdateMode.NONE);
    
    BufferedRecord<InternalRow> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, null, 1, true);
    BufferedRecord<InternalRow> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, null, 1, true);

    Option<BufferedRecord<InternalRow>> result = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    assertTrue(result.isPresent());
    BufferedRecord<InternalRow> mergedRecord = result.get();
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertTrue(mergedRecord.isDelete());
  }

  @Test
  void testFinalMergeWithDeleteRecords() throws IOException {
    BufferedRecordMerger<InternalRow> merger = createMerger(
        readerContext, RecordMergeMode.COMMIT_TIME_ORDERING, PartialUpdateMode.NONE);
    
    BufferedRecord<InternalRow> olderBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, null, 1, true);
    BufferedRecord<InternalRow> newerBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, null, 1, true);

    Pair<Boolean, InternalRow> result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    assertTrue(result.getLeft());
    assertTrue(result.getRight() == null);
  }
  
  // Helper method to create records for the parameterized test
  private static InternalRow createFullRecord(String id, String name, int age, String city, long timestamp) {
    return new GenericInternalRow(new Object[]{
        UTF8String.fromString(id),
        UTF8String.fromString(name),
        age,
        UTF8String.fromString(city),
        timestamp
    });
  }
  
  private static InternalRow createPartialRecord(String id, String name) {
    return new GenericInternalRow(new Object[]{
        UTF8String.fromString(id),
        UTF8String.fromString(name)
    });
  }
  
  private static InternalRow createRecordWithDefaults(String id, String name, int age, String city, long timestamp) {
    return new GenericInternalRow(new Object[]{
        UTF8String.fromString(id),
        UTF8String.fromString(name),
        age,
        city != null ? UTF8String.fromString(city) : null,
        timestamp
    });
  }
  
  private static InternalRow createRecordWithMarkers(String id, String name, int age, String city, long timestamp) {
    return new GenericInternalRow(new Object[]{
        UTF8String.fromString(id),
        UTF8String.fromString(name),
        age,
        UTF8String.fromString(city),
        timestamp
    });
  }
  
  private static InternalRow createRecordWithIdNameAgeCity(String id, String name, int age, String city) {
    return new GenericInternalRow(new Object[]{
        UTF8String.fromString(id),
        UTF8String.fromString(name),
        age,
        UTF8String.fromString(city),
        null // timestamp is null
    });
  }
  
  private static InternalRow createRecordWithIdNameTimestamp(String id, String name, long timestamp) {
    return new GenericInternalRow(new Object[]{
        UTF8String.fromString(id),
        UTF8String.fromString(name),
        null, // age is null
        null, // city is null
        timestamp
    });
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private BufferedRecordMerger<InternalRow> createMerger(HoodieReaderContext<InternalRow> readerContext,
                                                         RecordMergeMode mergeMode,
                                                         PartialUpdateMode partialUpdateMode) {
    return BufferedRecordMergerFactory.create(
        readerContext,
        mergeMode,
        false,
        mergeMode == RecordMergeMode.EVENT_TIME_ORDERING
            ? Option.of(new DefaultSparkRecordMerger())
            : Option.of(new OverwriteWithLatestSparkRecordMerger()),
        Collections.emptyList(), // orderingFieldNames
        Option.empty(), // payloadClass
        fullSchema, // readerSchema
        props, // props
        partialUpdateMode
    );
  }

  private BufferedRecordMerger<InternalRow> createPartialMerger(RecordMergeMode mergeMode,
                                                                HoodieReaderContext<InternalRow> readerContext) {
    return BufferedRecordMergerFactory.create(
        readerContext,
        mergeMode,
        true,
        mergeMode == RecordMergeMode.EVENT_TIME_ORDERING
            ? Option.of(new DefaultSparkRecordMerger())
            : Option.of(new OverwriteWithLatestSparkRecordMerger()),
        Collections.emptyList(), // orderingFieldNames
        Option.empty(), // payloadClass
        fullSchema, // readerSchema
        props, // props
        PartialUpdateMode.KEEP_VALUES // partialUpdateMode
    );
  }

  private static Schema getFullSchema() {
    Schema fullSchema = Schema.createRecord("TestRecord", null, null, false);

    List<Schema.Field> fields = Arrays.asList(
        new Schema.Field("id", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("name", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("age", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))), null, 0),
        new Schema.Field("city", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("timestamp", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL))), null, 0L)
    );

    fullSchema.setFields(fields);
    return fullSchema;
  }

  private static Schema getPartialSchema() {
    // Create a partial schema with only some fields
    Schema partialSchema = Schema.createRecord("PartialRecord", null, null, false);
    partialSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("name", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE)
    ));
    return partialSchema;
  }

  static class DummyInternalRowReaderContext extends HoodieReaderContext<InternalRow> {

    protected DummyInternalRowReaderContext(StorageConfiguration<?> storageConfiguration,
                                            HoodieTableConfig tableConfig,
                                            Option<InstantRange> instantRangeOpt,
                                            Option<Predicate> keyFilterOpt) {
      super(storageConfiguration, tableConfig, instantRangeOpt, keyFilterOpt);
    }

    @Override
    public ClosableIterator<InternalRow> getFileRecordIterator(StoragePath filePath,
                                                               long start,
                                                               long length,
                                                               Schema dataSchema,
                                                               Schema requiredSchema,
                                                               HoodieStorage storage) throws IOException {
      return null;
    }

    @Override
    public InternalRow convertAvroRecord(IndexedRecord avroRecord) {
      return null;
    }

    @Override
    public GenericRecord convertToAvroRecord(InternalRow record, Schema schema) {
      return null;
    }

    @Nullable
    @Override
    public InternalRow getDeleteRow(InternalRow record, String recordKey) {
      return null;
    }

    @Override
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode,
                                                         String mergeStrategyId,
                                                         String mergeImplClasses) {
      return null;
    }

    @Override
    public Object getValue(InternalRow record, Schema schema, String fieldName) {
      return null;
    }

    @Override
    public String getMetaFieldValue(InternalRow record, int pos) {
      return null;
    }

    @Override
    public HoodieRecord<InternalRow> constructHoodieRecord(BufferedRecord<InternalRow> bufferedRecord) {
      HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
      if (bufferedRecord.isDelete()) {
        return new HoodieEmptyRecord<>(
            hoodieKey,
            HoodieRecord.HoodieRecordType.SPARK);
      }

      Schema schema = getSchemaFromBufferRecord(bufferedRecord);
      InternalRow row = bufferedRecord.getRecord();
      StructType sparkSchema = AvroConversionUtils.convertAvroSchemaToStructType(schema);
      return new HoodieSparkRecord(hoodieKey, row, sparkSchema, false);
    }

    @Override
    public InternalRow constructEngineRecord(Schema schema, Map<Integer, Object> updateValues, BufferedRecord<InternalRow> baseRecord) {
      List<Schema.Field> fields = schema.getFields();
      Object[] values = new Object[fields.size()];
      for (Schema.Field field : fields) {
        int pos = field.pos();
        if (updateValues.containsKey(pos)) {
          values[pos] = updateValues.get(pos);
        } else {
          values[pos] = getValue(baseRecord.getRecord(), schema, field.name());
        }
      }
      return new GenericInternalRow(values);
    }

    @Override
    public InternalRow seal(InternalRow record) {
      return null;
    }

    @Override
    public InternalRow toBinaryRow(Schema avroSchema, InternalRow record) {
      return null;
    }

    @Override
    public ClosableIterator<InternalRow> mergeBootstrapReaders(ClosableIterator<InternalRow> skeletonFileIterator,
                                                               Schema skeletonRequiredSchema,
                                                               ClosableIterator<InternalRow> dataFileIterator,
                                                               Schema dataRequiredSchema,
                                                               List<Pair<String, Object>> requiredPartitionFieldAndValues) {
      return null;
    }

    @Override
    public UnaryOperator<InternalRow> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
      return null;
    }

    @Override
    public Schema getSchemaFromBufferRecord(BufferedRecord<InternalRow> record) {
      int id = record.getSchemaId();
      if (id == 1) {
        return fullSchema;
      } else if (id == 2) {
        return partialSchema;
      }
      return fullSchema;
    }
  }
}
