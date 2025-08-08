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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.util.DefaultJavaTypeConverter;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
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

import static org.apache.hudi.BaseSparkInternalRecordContext.getFieldValueFromInternalRow;
import static org.apache.hudi.common.config.RecordMergeMode.COMMIT_TIME_ORDERING;
import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.PRECOMBINE_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBufferedRecordMerger extends SparkClientFunctionalTestHarness {
  private static final String RECORD_KEY = "initial_id";
  private static final String PARTITION_PATH = "test_partition";
  private static final long ORDERING_VALUE = 100L;
  private static final String IGNORE_MARKERS_VALUE = "__HUDI_DEFAULT_MARKER__";
  private static final List<Schema> SCHEMAS = Arrays.asList(
      getSchema1(),
      getSchema2(),
      getSchema3(),
      getSchema4(),
      getSchema5(),
      getSchema6());
  private static final Schema READER_SCHEMA = getSchema6();
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
    props.put(PRECOMBINE_FIELDS.key(), "precombine");
    readerContext = new DummyInternalRowReaderContext(
        storageConfig, tableConfig, Option.empty(), Option.empty(), new DummyRecordContext(tableConfig));
  }

  // ============================================================================
  // Test Set 1: enablePartialMerging = false
  // ============================================================================
  @ParameterizedTest
  @MethodSource("mergeModeAndStageProvider")
  void testRegularMerging(RecordMergeMode mergeMode, PartialUpdateMode updateMode, MergeStage stage) throws IOException {
    if (updateMode == PartialUpdateMode.IGNORE_MARKERS) {
      props.put(
          HoodieTableConfig.MERGE_PROPERTIES_PREFIX + PARTIAL_UPDATE_CUSTOM_MARKER,
          IGNORE_MARKERS_VALUE);
    }

    if (stage == MergeStage.DELTA_MERGE) {
      runDeltaMerge(mergeMode, updateMode);
    } else if (stage == MergeStage.FINAL_MERGE) {
      runFinalMerge(mergeMode, updateMode);
    } else {
      runDeltaDeleteMerge(mergeMode, updateMode);
    }
  }

  private void runDeltaMerge(RecordMergeMode mergeMode, PartialUpdateMode updateMode) throws IOException {
    BufferedRecordMerger<InternalRow> merger = createMerger(readerContext, mergeMode, updateMode);
    // Create records with all columns.
    InternalRow oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    InternalRow newRecord = createFullRecord("new_id", "New Name", 0, IGNORE_MARKERS_VALUE, 0L);

    // CASE 1: New record has lower ordering value.
    BufferedRecord<InternalRow> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    BufferedRecord<InternalRow> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE - 1, newRecord, 1, null);
    Option<BufferedRecord<InternalRow>> deltaResult = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertTrue(deltaResult.isPresent());
      if (updateMode == PartialUpdateMode.NONE) {
        assertEquals(newRecord, deltaResult.get().getRecord());
      } else if (updateMode == PartialUpdateMode.IGNORE_DEFAULTS) {
        assertEquals(25, deltaResult.get().getRecord().getInt(2));
        assertEquals(1000L, deltaResult.get().getRecord().getLong(4));
      } else if (updateMode == PartialUpdateMode.IGNORE_MARKERS) {
        assertEquals("Old City", deltaResult.get().getRecord().getString(3));
      }
    } else {
      if (updateMode == PartialUpdateMode.NONE) {
        assertTrue(deltaResult.isEmpty());
      } else if (updateMode == PartialUpdateMode.IGNORE_DEFAULTS) {
        assertTrue(deltaResult.isPresent());
        assertEquals(oldRecord, deltaResult.get().getRecord());
      }
    }

    // CASE 2: New record has higher ordering value.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, null);
    deltaResult = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    assertTrue(deltaResult.isPresent());
    if (updateMode == PartialUpdateMode.NONE) {
      assertEquals(newRecord, deltaResult.get().getRecord());
    } else if (updateMode == PartialUpdateMode.IGNORE_DEFAULTS) {
      assertEquals(25, deltaResult.get().getRecord().getInt(2));
      assertEquals(1000L, deltaResult.get().getRecord().getLong(4));
    } else if (updateMode == PartialUpdateMode.IGNORE_MARKERS) {
      assertEquals("Old City", deltaResult.get().getRecord().getString(3));
    }

    // CASE 3: New record and old record have the same ordering value.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, newRecord, 1, null);
    deltaResult = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    assertTrue(deltaResult.isPresent());
    if (updateMode == PartialUpdateMode.NONE) {
      assertEquals(newRecord, deltaResult.get().getRecord());
    } else if (updateMode == PartialUpdateMode.IGNORE_DEFAULTS) {
      assertEquals(25, deltaResult.get().getRecord().getInt(2));
      assertEquals(1000L, deltaResult.get().getRecord().getLong(4));
    } else if (updateMode == PartialUpdateMode.IGNORE_MARKERS) {
      assertEquals("Old City", deltaResult.get().getRecord().getString(3));
    }
  }

  private void runDeltaDeleteMerge(RecordMergeMode mergeMode, PartialUpdateMode updateMode) throws IOException {
    BufferedRecordMerger<InternalRow> merger = createMerger(readerContext, mergeMode, updateMode);
    // Create records with all columns.
    InternalRow oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    InternalRow newRecord = createFullRecord("new_id", "New Name", 0, IGNORE_MARKERS_VALUE, 0L);

    // CASE 1: New record has lower ordering value.
    BufferedRecord<InternalRow> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    DeleteRecord deleteRecord = DeleteRecord.create(RECORD_KEY, "anyPath", ORDERING_VALUE - 1);
    Option<DeleteRecord> deltaResult = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertTrue(deltaResult.isPresent());
      assertEquals(deleteRecord, deltaResult.get());
    } else {
      assertTrue(deltaResult.isEmpty());
    }

    // CASE 2: New record has higher ordering value.
    deleteRecord = DeleteRecord.create(RECORD_KEY, "anyPath", ORDERING_VALUE + 1);
    deltaResult = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(deltaResult.isPresent());
    assertEquals(deleteRecord, deltaResult.get());

    // CASE 3: New record and old record have the same ordering value.
    deleteRecord = DeleteRecord.create(RECORD_KEY, "anyPath", ORDERING_VALUE);
    deltaResult = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(deltaResult.isPresent());
    assertEquals(deleteRecord, deltaResult.get());
  }

  private void runFinalMerge(RecordMergeMode mergeMode, PartialUpdateMode updateMode) throws IOException {
    BufferedRecordMerger<InternalRow> merger = createMerger(readerContext, mergeMode, updateMode);
    InternalRow oldRecord = createFullRecord(
        "older_id", "Older Name", 20, "Older City", 500L);
    InternalRow newRecord = createFullRecord(
        "new_id", "New Name", 0, IGNORE_MARKERS_VALUE, 0L);

    // New record has lower ordering value.
    BufferedRecord<InternalRow> olderBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    BufferedRecord<InternalRow> newerBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE - 1, newRecord, 1, null);
    BufferedRecord<InternalRow> finalResult = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    assertFalse(finalResult.isDelete());
    if (mergeMode == COMMIT_TIME_ORDERING) {
      if (updateMode == PartialUpdateMode.NONE) {
        assertEquals(newRecord, finalResult.getRecord());
      } else if (updateMode == PartialUpdateMode.IGNORE_DEFAULTS) {
        assertEquals(20, finalResult.getRecord().getInt(2));
        assertEquals(500L, finalResult.getRecord().getLong(4));
      } else if (updateMode == PartialUpdateMode.IGNORE_MARKERS) {
        assertEquals("Older City", finalResult.getRecord().getString(3));
      }
    } else {
      assertEquals(oldRecord, finalResult.getRecord());
    }

    // New record has higher ordering value.
    olderBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, null);
    finalResult = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    assertFalse(finalResult.isDelete());
    if (updateMode == PartialUpdateMode.NONE) {
      assertEquals(newRecord, finalResult.getRecord());
    } else if (updateMode == PartialUpdateMode.IGNORE_DEFAULTS) {
      assertEquals(20, finalResult.getRecord().getInt(2));
      assertEquals(500, finalResult.getRecord().getLong(4));
    } else if (updateMode == PartialUpdateMode.IGNORE_MARKERS) {
      assertEquals("Older City", finalResult.getRecord().getString(3));
    }

    // New record has equal ordering value.
    olderBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, newRecord, 1, null);
    finalResult = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    assertFalse(finalResult.isDelete());
    if (updateMode == PartialUpdateMode.NONE) {
      assertEquals(newRecord, finalResult.getRecord());
    } else if (updateMode == PartialUpdateMode.IGNORE_DEFAULTS) {
      assertEquals(20, finalResult.getRecord().getInt(2));
      assertEquals(500, finalResult.getRecord().getLong(4));
    } else if (updateMode == PartialUpdateMode.IGNORE_MARKERS) {
      assertEquals("Older City", finalResult.getRecord().getString(3));
    }
  }

  // ============================================================================
  // Test Set 2: enablePartialMerging = true
  // ============================================================================
  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testPartialMerging(RecordMergeMode mergeMode) throws IOException {
    runPartialDeltaMerge(mergeMode);
    runPartialFinalMerge(mergeMode);
  }

  private void runPartialFinalMerge(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<InternalRow> merger = createPartialMerger(mergeMode, readerContext);

    // Case 1: new record has lower ordering value.
    InternalRow olderRecord = createFullRecordForPartial(
        (int) ORDERING_VALUE, "older_id", "Older Name", 20, "Older City", 500L);
    InternalRow newerRecord = createPartialRecord(
        (int) ORDERING_VALUE - 1, "newer_id", "Newer Name");
    BufferedRecord<InternalRow> olderBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecord, 6, null);
    BufferedRecord<InternalRow> newerBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE - 1, newerRecord, 2, null);

    BufferedRecord<InternalRow> finalResult = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    InternalRow expected = createFullRecordForPartial(
        (int) ORDERING_VALUE - 1, "newer_id", "Newer Name", 20, "Older City", 500L);
    assertFalse(finalResult.isDelete());
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertRowEqual(expected, finalResult.getRecord(), READER_SCHEMA);
    } else {
      assertRowEqual(olderRecord, finalResult.getRecord(), READER_SCHEMA);
    }
    // Case 2: new record has higher ordering value.
    newerRecord = createPartialRecord((int) ORDERING_VALUE + 1, "newer_id", "Newer Name");
    newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecord, 2, null);
    finalResult = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    expected = createFullRecordForPartial(
        (int) ORDERING_VALUE + 1, "newer_id", "Newer Name", 20, "Older City", 500L);
    assertRowEqual(expected, finalResult.getRecord(), READER_SCHEMA);
    // Case 3: new record has equal ordering value.
    newerRecord = createPartialRecord((int) ORDERING_VALUE, "newer_id", "Newer Name");
    newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, newerRecord, 2, null);
    finalResult = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    expected = createFullRecordForPartial(
        (int) ORDERING_VALUE, "newer_id", "Newer Name", 20, "Older City", 500L);
    assertRowEqual(expected, finalResult.getRecord(), READER_SCHEMA);
  }

  private void runPartialDeltaMerge(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<InternalRow> merger = createPartialMerger(mergeMode, readerContext);

    // Test 1: Old record more columns than new record.
    // Case 1: New record has lower ordering value.
    InternalRow oldRecord = createFullRecordForPartial((int) ORDERING_VALUE, "old_id", "Old Name", 25, "Old City", 1000L);
    InternalRow newRecord = createPartialRecord((int) ORDERING_VALUE - 1, "new_id", "New Name");
    BufferedRecord<InternalRow> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 6, null);
    BufferedRecord<InternalRow> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE - 1, newRecord, 2, null);
    Option<BufferedRecord<InternalRow>> deltaResult = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    assertTrue(deltaResult.isPresent());
    BufferedRecord<InternalRow> mergedRecord = deltaResult.get();
    InternalRow expected = createFullRecordForPartial(
        (int) ORDERING_VALUE - 1, "new_id", "New Name", 25, "Old City", 1000L);
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertRowEqual(expected, mergedRecord.getRecord(), READER_SCHEMA);
    } else {
      assertRowEqual(oldRecord, mergedRecord.getRecord(), READER_SCHEMA);
    }
    // Test 2: New record has higher columns ordering value.
    newRecord = createPartialRecord((int) ORDERING_VALUE + 1, "new_id", "New Name");
    newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 2, null);
    deltaResult = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    expected = createFullRecordForPartial(
        (int) ORDERING_VALUE + 1, "new_id", "New Name", 25, "Old City", 1000L);
    assertTrue(deltaResult.isPresent());
    assertEquals(expected, deltaResult.get().getRecord());
    // Test 3: New record has equal ordering value.
    newRecord = createPartialRecord((int) ORDERING_VALUE, "new_id", "New Name");
    newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, newRecord, 2, null);
    deltaResult = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    assertTrue(deltaResult.isPresent());
    expected = createFullRecordForPartial(
        (int) ORDERING_VALUE, "new_id", "New Name", 25, "Old City", 1000L);
    assertEquals(expected, deltaResult.get().getRecord());
  }

  // ============================================================================
  // Edge Cases and Additional Tests
  // ============================================================================

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testRegularMergingWithIgnoreDefaultsNested(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<InternalRow> ignoreDefaultsMerger =
        createMerger(readerContext, mergeMode, PartialUpdateMode.IGNORE_DEFAULTS);

    // Old record has all columns, new record has some columns with default/null values
    InternalRow oldRecordWithDefaults = createFullRecordWithCompany(
        "old_id", "Old Name", 25, "Old City", 1000L, "def", "ACity");
    InternalRow newRecordWithDefaults = createFullRecordWithCompany(
        "new_id", "New Name", 0, null, 0L, "abc", "treecity");
    BufferedRecord<InternalRow> oldBufferedRecordWithDefaults =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecordWithDefaults, 5, null);
    BufferedRecord<InternalRow> newBufferedRecordWithDefaults =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecordWithDefaults, 5, null);

    Option<BufferedRecord<InternalRow>> deltaResultDefaults =
        ignoreDefaultsMerger.deltaMerge(newBufferedRecordWithDefaults, oldBufferedRecordWithDefaults);

    assertTrue(deltaResultDefaults.isPresent());
    BufferedRecord<InternalRow> mergedRecordDefaults = deltaResultDefaults.get();
    InternalRow row = mergedRecordDefaults.getRecord();
    assertEquals("Old City", row.getString(3));
    // NOTE: Ignore default does not work for nested value due to design tradeoff.
    assertEquals("abc", row.getStruct(5, 2).getString(0));
    assertEquals("treecity", row.getStruct(5, 2).getString(1));
    assertEquals(RECORD_KEY, mergedRecordDefaults.getRecordKey());
    assertFalse(mergedRecordDefaults.isDelete());
    assertNotNull(mergedRecordDefaults.getRecord());
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testDeltaMergeWithNullExistingRecord(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<InternalRow> merger = createMerger(
        readerContext, mergeMode, PartialUpdateMode.NONE);

    // New record is not delete.
    InternalRow newRecord = createFullRecord(
        "new_id", "New Name", 30, "New City", 2000L);
    BufferedRecord<InternalRow> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, newRecord, 1, null);
    Option<BufferedRecord<InternalRow>> result =
        merger.deltaMerge(newBufferedRecord, null);
    assertTrue(result.isPresent());
    BufferedRecord<InternalRow> mergedRecord = result.get();
    assertEquals(newRecord, mergedRecord.getRecord());

    // New record is delete with 100 ordering value.
    newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, newRecord, 1, HoodieOperation.DELETE);
    result = merger.deltaMerge(newBufferedRecord, null);
    assertTrue(result.isPresent());
    assertEquals(newRecord, result.get().getRecord());

    // New record is delete with default ordering value.
    newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, OrderingValues.getDefault(), newRecord, 1, HoodieOperation.DELETE);
    result = merger.deltaMerge(newBufferedRecord, null);
    assertTrue(result.isPresent());
    assertEquals(newRecord, result.get().getRecord());

    // NOTE: no need to test null case at final stage since
    //       record buffer ensures both new and old records are not null.
  }

  /**
   * Test delete related logic in delta stage.
   */
  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testDeltaMergeWithDeleteRecord(RecordMergeMode mergeMode) {
    BufferedRecordMerger<InternalRow> merger = createMerger(readerContext, mergeMode, PartialUpdateMode.NONE);

    // Delete record has null ordering value.
    InternalRow oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    BufferedRecord<InternalRow> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    DeleteRecord deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH);
    Option<DeleteRecord> result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(result.isPresent());
    assertEquals(deleteRecord, result.get());

    // Delete record has lower ordering value.
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH, ORDERING_VALUE - 1);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertTrue(result.isPresent());
      assertEquals(deleteRecord, result.get());
    } else {
      assertTrue(result.isEmpty());
    }

    // Delete record has ordering value > default value.
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH, ORDERING_VALUE + 1);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(result.isPresent());
    assertEquals(deleteRecord, result.get());

    // Existing record is delete.
    // Delete record has null ordering value.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, HoodieOperation.DELETE);
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(result.isPresent());

    // Delete record has lower ordering value.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, HoodieOperation.DELETE);
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH, ORDERING_VALUE - 1);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertTrue(result.isPresent());
    } else {
      assertTrue(result.isEmpty());
    }

    // Delete record has higher ordering value.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, HoodieOperation.DELETE);
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH, ORDERING_VALUE + 1);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(result.isPresent());

    // Old record is delete record.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, HoodieOperation.DELETE);
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(result.isPresent());
    assertEquals(deleteRecord, result.get());

    // Delete record has lower ordering value.
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH, ORDERING_VALUE - 1);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertTrue(result.isPresent());
      assertEquals(deleteRecord, result.get());
    } else {
      assertTrue(result.isEmpty());
    }

    // Delete record has ordering value > default value.
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH, ORDERING_VALUE + 1);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(result.isPresent());
    assertEquals(deleteRecord, result.get());

    // Existing record is delete.
    // Delete record has null ordering value.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, HoodieOperation.DELETE);
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(result.isPresent());

    // Delete record has lower ordering value.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, HoodieOperation.DELETE);
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH, ORDERING_VALUE - 1);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertTrue(result.isPresent());
    } else {
      assertTrue(result.isEmpty());
    }

    // Delete record has higher ordering value.
    oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, HoodieOperation.DELETE);
    deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH, ORDERING_VALUE + 1);
    result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    assertTrue(result.isPresent());
  }

  /**
   * Test delete related logic in final stage.
   */
  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testFinalMergeWithDeleteRecords(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<InternalRow> merger = createMerger(
        readerContext, mergeMode, PartialUpdateMode.NONE);

    InternalRow oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    InternalRow newRecord = createFullRecord("new_id", "New Name", 29, "New City", 2000L);

    // new record has lower ordering value.
    BufferedRecord<InternalRow> olderBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, null);
    BufferedRecord<InternalRow> newerBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE - 1, newRecord, 1, HoodieOperation.DELETE);

    BufferedRecord<InternalRow> result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    if (mergeMode == COMMIT_TIME_ORDERING) {
      assertEquals(newRecord, result.getRecord());
    } else {
      assertEquals(oldRecord, result.getRecord());
    }

    // new record has higher ordering value.
    newerBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, HoodieOperation.DELETE);
    result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    assertTrue(result.isDelete());
    assertEquals(newRecord, result.getRecord());

    // new record has default ordering value.
    newerBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, OrderingValues.getDefault(), newRecord, 1, HoodieOperation.DELETE);
    result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    assertTrue(result.isDelete());
    assertEquals(newRecord, result.getRecord());
  }

  // ============================================================================
  // Helper methods or class to create records for the parameterized test
  // ============================================================================
  static enum MergeStage {
    DELTA_MERGE,
    DELTA_DELETE_MERGE,
    FINAL_MERGE;
  }

  private static Stream<Arguments> mergeModeAndStageProvider() {
    List<MergeStage> stages = Arrays.asList(MergeStage.values());
    return Arrays.stream(RecordMergeMode.values())
        .filter(mode -> mode == RecordMergeMode.COMMIT_TIME_ORDERING || mode == RecordMergeMode.EVENT_TIME_ORDERING)
        .flatMap(mode ->
            Arrays.stream(PartialUpdateMode.values())
                .flatMap(updateMode ->
                    stages.stream()
                        .map(stage -> Arguments.of(mode, updateMode, stage))
                )
        )
        .filter(args -> {
          PartialUpdateMode updateMode = (PartialUpdateMode) args.get()[1];
          return updateMode != PartialUpdateMode.FILL_DEFAULTS && updateMode != PartialUpdateMode.KEEP_VALUES;
        });
  }

  private static InternalRow createFullRecord(
      String id, String name, int age, String city, long timestamp) {
    return new GenericInternalRow(new Object[]{
        UTF8String.fromString(id),
        UTF8String.fromString(name),
        age,
        UTF8String.fromString(city),
        timestamp
    });
  }

  private static InternalRow createFullRecordForPartial(
      int precombine, String id, String name, int age, String city, long timestamp) {
    return new GenericInternalRow(new Object[]{
        precombine,
        UTF8String.fromString(id),
        UTF8String.fromString(name),
        age,
        UTF8String.fromString(city),
        timestamp
    });
  }
  
  private static InternalRow createPartialRecord(int precombine, String id, String name) {
    return new GenericInternalRow(new Object[]{
        precombine,
        UTF8String.fromString(id),
        UTF8String.fromString(name)
    });
  }

  private static InternalRow createFullRecordWithCompany(
      String id, String name, int age, String city, long timestamp, String companyName, String companyCity) {
    InternalRow companyRow = new GenericInternalRow(new Object[]{
        UTF8String.fromString(companyName),
        UTF8String.fromString(companyCity)
    });
    return new GenericInternalRow(new Object[]{
        UTF8String.fromString(id),
        UTF8String.fromString(name),
        age,
        UTF8String.fromString(city),
        timestamp,
        companyRow
    });
  }

  private BufferedRecordMerger<InternalRow> createMerger(HoodieReaderContext<InternalRow> readerContext,
                                                         RecordMergeMode mergeMode,
                                                         PartialUpdateMode partialUpdateMode) {
    return BufferedRecordMergerFactory.create(
        readerContext,
        mergeMode,
        false,
        mergeMode == EVENT_TIME_ORDERING
            ? Option.of(new DefaultSparkRecordMerger())
            : Option.of(new OverwriteWithLatestSparkRecordMerger()),
        Collections.emptyList(), // orderingFieldNames
        Option.empty(), // payloadClass
        READER_SCHEMA, // readerSchema
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
        mergeMode == EVENT_TIME_ORDERING
            ? Option.of(new DefaultSparkRecordMerger())
            : Option.of(new OverwriteWithLatestSparkRecordMerger()),
        Collections.emptyList(), // orderingFieldNames
        Option.empty(), // payloadClass
        READER_SCHEMA, // readerSchema
        props, // props
        PartialUpdateMode.KEEP_VALUES // partialUpdateMode
    );
  }

  private static Schema getSchema1() {
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

  private static Schema getSchema2() {
    // Create a partial schema with only some fields
    Schema partialSchema = Schema.createRecord("PartialRecord", null, null, false);
    partialSchema.setFields(Arrays.asList(
        new Schema.Field("precombine", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL))), null, 0),
        new Schema.Field("id", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("name", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE)
    ));
    return partialSchema;
  }

  private static Schema getSchema3() {
    Schema fullSchema = Schema.createRecord("TestRecord", null, null, false);
    List<Schema.Field> fields = Arrays.asList(
        new Schema.Field("id", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("name", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("age", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))), null, 0),
        new Schema.Field("city", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE)
    );
    fullSchema.setFields(fields);
    return fullSchema;
  }

  private static Schema getSchema4() {
    Schema fullSchema = Schema.createRecord("TestRecord", null, null, false);
    List<Schema.Field> fields = Arrays.asList(
        new Schema.Field("id", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("name", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("timestamp", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL))), null, 0L)
    );
    fullSchema.setFields(fields);
    return fullSchema;
  }

  private static Schema getSchema5() {
    String schemaJson =
        "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"TestRecord\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
          + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
          + "    {\"name\": \"age\", \"type\": [\"int\", \"null\"], \"default\": 0},\n"
          + "    {\"name\": \"city\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
          + "    {\"name\": \"timestamp\", \"type\": [\"long\", \"null\"], \"default\": 0},\n"
          + "    {\n"
          + "      \"name\": \"company\",\n"
          + "      \"type\": {\n"
          + "        \"type\": \"record\",\n"
          + "        \"name\": \"Company\",\n"
          + "        \"fields\": [\n"
          + "          {\"name\": \"name\", \"type\": \"string\", \"default\": \"abc\"},\n"
          + "          {\"name\": \"city\", \"type\": \"string\", \"default\": \"treecity\"}\n"
          + "        ]\n"
          + "      },\n"
          + "      \"default\": {\"name\": \"abc\", \"city\": \"treecity\"}\n"
          + "    }\n"
          + "  ]\n"
          + "}";
    return new Schema.Parser().parse(schemaJson);
  }

  private static Schema getSchema6() {
    Schema fullSchema = Schema.createRecord("TestRecord", null, null, false);
    List<Schema.Field> fields = Arrays.asList(
        new Schema.Field("precombine", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))), null, 0),
        new Schema.Field("id", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("name", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("age", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))), null, 0),
        new Schema.Field("city", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))), null, Schema.NULL_VALUE),
        new Schema.Field("timestamp", Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL))), null, 0L)
    );
    fullSchema.setFields(fields);
    return fullSchema;
  }

  static class DummyRecordContext extends RecordContext<InternalRow> {

    public DummyRecordContext(HoodieTableConfig tableConfig) {
      super(tableConfig, new DefaultJavaTypeConverter());
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
    public InternalRow getDeleteRow(String recordKey) {
      return null;
    }

    @Override
    public Object getValue(InternalRow record, Schema schema, String fieldName) {
      return getFieldValueFromInternalRow(record, schema, fieldName);
    }

    @Override
    public String getMetaFieldValue(InternalRow record, int pos) {
      return null;
    }

    @Override
    public HoodieRecord<InternalRow> constructHoodieRecord(BufferedRecord<InternalRow> bufferedRecord, String partitionPath) {
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
    public InternalRow mergeWithEngineRecord(Schema schema,
                                             Map<Integer, Object> updateValues,
                                             BufferedRecord<InternalRow> baseRecord) {
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
    public Schema getSchemaFromBufferRecord(BufferedRecord<InternalRow> record) {
      int id = record.getSchemaId();
      if (id >= 1 && id <= SCHEMAS.size()) {
        return SCHEMAS.get(id - 1);
      } else {
        throw new RuntimeException("Schema id is illegal: " + id);
      }
    }
  }

  static class DummyInternalRowReaderContext extends HoodieReaderContext<InternalRow> {
    protected DummyInternalRowReaderContext(StorageConfiguration<?> storageConfiguration,
                                            HoodieTableConfig tableConfig,
                                            Option<InstantRange> instantRangeOpt,
                                            Option<Predicate> keyFilterOpt,
                                            RecordContext<InternalRow> recordContext) {
      super(storageConfiguration, tableConfig, instantRangeOpt, keyFilterOpt, recordContext);
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
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode,
                                                         String mergeStrategyId,
                                                         String mergeImplClasses) {
      return null;
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
    public ClosableIterator<InternalRow> mergeBootstrapReaders(
        ClosableIterator<InternalRow> skeletonFileIterator,
        Schema skeletonRequiredSchema,
        ClosableIterator<InternalRow> dataFileIterator,
        Schema dataRequiredSchema,
        List<Pair<String, Object>> requiredPartitionFieldAndValues) {
      return null;
    }

    @Override
    public UnaryOperator<InternalRow> projectRecord(
        Schema from, Schema to, Map<String, String> renamedColumns) {
      return null;
    }
  }

  public static void assertRowEqual(InternalRow expected, InternalRow actual, Schema schema) {
    assertRowEqualsRecursive(expected, actual, schema.getFields(), "");
  }

  private static void assertRowEqualsRecursive(InternalRow expected, InternalRow actual,
                                               List<Schema.Field> fields, String pathPrefix) {
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);
      Schema fieldSchema = getNonNullSchema(field.schema());
      String path = pathPrefix + field.name();

      if (expected.isNullAt(i) || actual.isNullAt(i)) {
        assertEquals(expected.isNullAt(i), actual.isNullAt(i), "Null mismatch at: " + path);
        continue;
      }

      switch (fieldSchema.getType()) {
        case STRING:
          assertEquals(expected.getUTF8String(i).toString(),
              actual.getUTF8String(i).toString(),
              "Mismatch at: " + path);
          break;
        case INT:
          assertEquals(expected.getInt(i), actual.getInt(i), "Mismatch at: " + path);
          break;
        case LONG:
          assertEquals(expected.getLong(i), actual.getLong(i), "Mismatch at: " + path);
          break;
        case FLOAT:
          assertEquals(expected.getFloat(i), actual.getFloat(i), "Mismatch at: " + path);
          break;
        case DOUBLE:
          assertEquals(expected.getDouble(i), actual.getDouble(i), "Mismatch at: " + path);
          break;
        case BOOLEAN:
          assertEquals(expected.getBoolean(i), actual.getBoolean(i), "Mismatch at: " + path);
          break;
        case RECORD:
          InternalRow expectedStruct = expected.getStruct(i, fieldSchema.getFields().size());
          InternalRow actualStruct = actual.getStruct(i, fieldSchema.getFields().size());
          assertRowEqualsRecursive(expectedStruct, actualStruct, fieldSchema.getFields(), path + ".");
          break;
        default:
          throw new UnsupportedOperationException("Unsupported type: " + fieldSchema.getType() + " at " + path);
      }
    }
  }

  private static Schema getNonNullSchema(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema s : schema.getTypes()) {
        if (s.getType() != Schema.Type.NULL) {
          return s;
        }
      }
    }
    return schema;
  }
}
