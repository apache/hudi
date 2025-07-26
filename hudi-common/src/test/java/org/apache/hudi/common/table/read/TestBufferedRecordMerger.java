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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBufferedRecordMerger {
  private static final String RECORD_KEY = "test_key";
  private static final String PARTITION_PATH = "test_partition";
  private static final long ORDERING_VALUE = 100L;
  
  private Schema fullSchema;
  private Schema partialSchema;
  private HoodieAvroReaderContext readerContext;
  private HoodieTableConfig tableConfig;
  private StorageConfiguration<?> storageConfig;
  private TypedProperties props;

  @BeforeEach
  void setUp() throws IOException {
    // Create a schema with multiple fields
    fullSchema = Schema.createRecord("TestRecord", null, null, false);
    fullSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("timestamp", Schema.create(Schema.Type.LONG), null, null)
    ));

    // Create a partial schema with only some fields
    partialSchema = Schema.createRecord("PartialRecord", null, null, false);
    partialSchema.setFields(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)
    ));

    // Setup mocks
    tableConfig = mock(HoodieTableConfig.class);
    storageConfig = mock(StorageConfiguration.class);
    when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.DefaultHoodieRecordPayload");
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"id"}));
    readerContext = new HoodieAvroReaderContext(storageConfig, tableConfig, Option.empty(), Option.empty());
    readerContext.setPartitionPath(PARTITION_PATH);
    props = new TypedProperties();
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testDeltaMergeWithFullRecords(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(mergeMode);
    
    // Create records with all columns
    IndexedRecord oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    IndexedRecord newRecord = createFullRecord("new_id", "New Name", 30, "New City", 2000L);
    
    BufferedRecord<IndexedRecord> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<IndexedRecord> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, false);
    Option<BufferedRecord<IndexedRecord>> result = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    assertTrue(result.isPresent());
    BufferedRecord<IndexedRecord> mergedRecord = result.get();
    // For both commit time and event time, mergedRecord is newRecord.
    assertEquals(newRecord, mergedRecord.getRecord());
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertFalse(mergedRecord.isDelete());
    assertNotNull(mergedRecord.getRecord());
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testDeltaMergeWithPartialRecords(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(mergeMode);
    
    // Create records with partial columns
    IndexedRecord oldRecord = createPartialRecord("old_id", "Old Name");
    IndexedRecord newRecord = createPartialRecord("new_id", "New Name");
    
    BufferedRecord<IndexedRecord> oldBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<IndexedRecord> newBufferedRecord =
        new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, false);
    Option<BufferedRecord<IndexedRecord>> result = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);

    assertTrue(result.isPresent());
    BufferedRecord<IndexedRecord> mergedRecord = result.get();
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertFalse(mergedRecord.isDelete());
    assertNotNull(mergedRecord.getRecord());
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testDeltaMergeWithMixedRecords(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(mergeMode);
    
    // Old record has all columns, new record has partial columns
    IndexedRecord oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    IndexedRecord newRecord = createPartialRecord("new_id", "New Name");
    
    BufferedRecord<IndexedRecord> oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<IndexedRecord> newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, false);

    Option<BufferedRecord<IndexedRecord>> result = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    assertTrue(result.isPresent());
    BufferedRecord<IndexedRecord> mergedRecord = result.get();
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertFalse(mergedRecord.isDelete());
    assertNotNull(mergedRecord.getRecord());
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testDeltaMergeWithDeleteRecord(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(mergeMode);
    
    IndexedRecord oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    BufferedRecord<IndexedRecord> oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    
    DeleteRecord deleteRecord = DeleteRecord.create(RECORD_KEY, PARTITION_PATH);
    Option<DeleteRecord> result = merger.deltaMerge(deleteRecord, oldBufferedRecord);
    
    assertTrue(result.isPresent());
    DeleteRecord mergedDeleteRecord = result.get();
    assertEquals(RECORD_KEY, mergedDeleteRecord.getRecordKey());
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testFinalMergeWithFullRecords(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(mergeMode);
    
    // Create records with all columns
    IndexedRecord olderRecord = createFullRecord("older_id", "Older Name", 20, "Older City", 500L);
    IndexedRecord newerRecord = createFullRecord("newer_id", "Newer Name", 35, "Newer City", 3000L);
    
    BufferedRecord<IndexedRecord> olderBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecord, 1, false);

    Pair<Boolean, IndexedRecord> result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    
    assertFalse(result.getLeft()); // Should not be a delete
    assertNotNull(result.getRight()); // Should have a record
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testFinalMergeWithPartialRecords(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(mergeMode);
    
    // Create records with partial columns
    IndexedRecord olderRecord = createPartialRecord("older_id", "Older Name");
    IndexedRecord newerRecord = createPartialRecord("newer_id", "Newer Name");
    
    BufferedRecord<IndexedRecord> olderBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecord, 1, false);

    Pair<Boolean, IndexedRecord> result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    
    assertFalse(result.getLeft()); // Should not be a delete
    assertNotNull(result.getRight()); // Should have a record
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testFinalMergeWithMixedRecords(RecordMergeMode mergeMode) throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(mergeMode);
    
    // Older record has all columns, newer record has partial columns
    IndexedRecord olderRecord = createFullRecord("older_id", "Older Name", 20, "Older City", 500L);
    IndexedRecord newerRecord = createPartialRecord("newer_id", "Newer Name");
    
    BufferedRecord<IndexedRecord> olderBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecord, 1, false);

    Pair<Boolean, IndexedRecord> result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    
    assertFalse(result.getLeft()); // Should not be a delete
    assertNotNull(result.getRight()); // Should have a record
  }

  @Test
  void testDeltaMergeWithNullExistingRecord() throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(RecordMergeMode.COMMIT_TIME_ORDERING);
    
    IndexedRecord newRecord = createFullRecord("new_id", "New Name", 30, "New City", 2000L);
    BufferedRecord<IndexedRecord> newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, newRecord, 1, false);

    Option<BufferedRecord<IndexedRecord>> result = merger.deltaMerge(newBufferedRecord, null);
    
    assertTrue(result.isPresent());
    BufferedRecord<IndexedRecord> mergedRecord = result.get();
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertEquals(newRecord, mergedRecord.getRecord());
  }

  @Test
  void testDeltaMergeWithDeleteRecords() throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(RecordMergeMode.COMMIT_TIME_ORDERING);
    
    BufferedRecord<IndexedRecord> oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, null, 1, true);
    BufferedRecord<IndexedRecord> newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, null, 1, true);

    Option<BufferedRecord<IndexedRecord>> result = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    assertTrue(result.isPresent());
    BufferedRecord<IndexedRecord> mergedRecord = result.get();
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertTrue(mergedRecord.isDelete());
  }

  @Test
  void testFinalMergeWithDeleteRecords() throws IOException {
    BufferedRecordMerger<IndexedRecord> merger = createMerger(RecordMergeMode.COMMIT_TIME_ORDERING);
    
    BufferedRecord<IndexedRecord> olderBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, null, 1, true);
    BufferedRecord<IndexedRecord> newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, null, 1, true);

    Pair<Boolean, IndexedRecord> result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    
    assertTrue(result.getLeft()); // Should be a delete
    assertTrue(result.getRight() == null); // Should have no record
  }



  @Test
  void testPartialUpdateMerger() throws IOException {
    // Test with partial update mode enabled
    BufferedRecordMerger<IndexedRecord> merger = createPartialUpdateMerger();
    
    IndexedRecord oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    IndexedRecord newRecord = createPartialRecord("new_id", "New Name");
    
    BufferedRecord<IndexedRecord> oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<IndexedRecord> newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, false);

    Option<BufferedRecord<IndexedRecord>> result = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    // Partial update merger should handle the merge differently
    assertTrue(result.isPresent());
    BufferedRecord<IndexedRecord> mergedRecord = result.get();
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertFalse(mergedRecord.isDelete());
  }

  @Test
  void testIgnoreDefaultsPartialUpdateMerger() throws IOException {
    // Test with IGNORE_DEFAULTS partial update mode
    BufferedRecordMerger<IndexedRecord> merger = createIgnoreDefaultsMerger();
    
    IndexedRecord oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    IndexedRecord newRecord = createPartialRecord("new_id", "New Name");
    
    BufferedRecord<IndexedRecord> oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<IndexedRecord> newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, false);

    Option<BufferedRecord<IndexedRecord>> result = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    // IGNORE_DEFAULTS should handle partial updates by ignoring default values
    assertTrue(result.isPresent());
    BufferedRecord<IndexedRecord> mergedRecord = result.get();
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertFalse(mergedRecord.isDelete());
  }

  @Test
  void testIgnoreMarkersPartialUpdateMerger() throws IOException {
    // Test with IGNORE_MARKERS partial update mode
    BufferedRecordMerger<IndexedRecord> merger = createIgnoreMarkersMerger();
    
    IndexedRecord oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    IndexedRecord newRecord = createPartialRecord("new_id", "New Name");
    
    BufferedRecord<IndexedRecord> oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<IndexedRecord> newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, false);

    Option<BufferedRecord<IndexedRecord>> result = merger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    // IGNORE_MARKERS should handle partial updates by ignoring marker fields
    assertTrue(result.isPresent());
    BufferedRecord<IndexedRecord> mergedRecord = result.get();
    assertEquals(RECORD_KEY, mergedRecord.getRecordKey());
    assertFalse(mergedRecord.isDelete());
  }

  @Test
  void testIgnoreDefaultsFinalMerge() throws IOException {
    // Test final merge with IGNORE_DEFAULTS partial update mode
    BufferedRecordMerger<IndexedRecord> merger = createIgnoreDefaultsMerger();
    
    IndexedRecord olderRecord = createFullRecord("older_id", "Older Name", 20, "Older City", 500L);
    IndexedRecord newerRecord = createPartialRecord("newer_id", "Newer Name");
    
    BufferedRecord<IndexedRecord> olderBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecord, 1, false);

    Pair<Boolean, IndexedRecord> result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    
    assertFalse(result.getLeft()); // Should not be a delete
    assertNotNull(result.getRight()); // Should have a record
  }

  @Test
  void testIgnoreMarkersFinalMerge() throws IOException {
    // Test final merge with IGNORE_MARKERS partial update mode
    BufferedRecordMerger<IndexedRecord> merger = createIgnoreMarkersMerger();
    
    IndexedRecord olderRecord = createFullRecord("older_id", "Older Name", 20, "Older City", 500L);
    IndexedRecord newerRecord = createPartialRecord("newer_id", "Newer Name");
    
    BufferedRecord<IndexedRecord> olderBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, olderRecord, 1, false);
    BufferedRecord<IndexedRecord> newerBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newerRecord, 1, false);

    Pair<Boolean, IndexedRecord> result = merger.finalMerge(olderBufferedRecord, newerBufferedRecord);
    
    assertFalse(result.getLeft()); // Should not be a delete
    assertNotNull(result.getRight()); // Should have a record
  }

  @Test
  void testPartialUpdateModesWithFullRecords() throws IOException {
    // Test all partial update modes with full records
    BufferedRecordMerger<IndexedRecord> keepValuesMerger = createPartialUpdateMerger();
    BufferedRecordMerger<IndexedRecord> ignoreDefaultsMerger = createIgnoreDefaultsMerger();
    BufferedRecordMerger<IndexedRecord> ignoreMarkersMerger = createIgnoreMarkersMerger();
    
    IndexedRecord oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    IndexedRecord newRecord = createFullRecord("new_id", "New Name", 30, "New City", 2000L);
    
    BufferedRecord<IndexedRecord> oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<IndexedRecord> newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, false);

    // Test delta merge for all modes
    Option<BufferedRecord<IndexedRecord>> keepValuesResult = keepValuesMerger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    Option<BufferedRecord<IndexedRecord>> ignoreDefaultsResult = ignoreDefaultsMerger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    Option<BufferedRecord<IndexedRecord>> ignoreMarkersResult = ignoreMarkersMerger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    assertTrue(keepValuesResult.isPresent());
    assertTrue(ignoreDefaultsResult.isPresent());
    assertTrue(ignoreMarkersResult.isPresent());
    
    // Test final merge for all modes
    Pair<Boolean, IndexedRecord> keepValuesFinalResult = keepValuesMerger.finalMerge(oldBufferedRecord, newBufferedRecord);
    Pair<Boolean, IndexedRecord> ignoreDefaultsFinalResult = ignoreDefaultsMerger.finalMerge(oldBufferedRecord, newBufferedRecord);
    Pair<Boolean, IndexedRecord> ignoreMarkersFinalResult = ignoreMarkersMerger.finalMerge(oldBufferedRecord, newBufferedRecord);
    
    assertFalse(keepValuesFinalResult.getLeft());
    assertFalse(ignoreDefaultsFinalResult.getLeft());
    assertFalse(ignoreMarkersFinalResult.getLeft());
    assertNotNull(keepValuesFinalResult.getRight());
    assertNotNull(ignoreDefaultsFinalResult.getRight());
    assertNotNull(ignoreMarkersFinalResult.getRight());
  }

  @Test
  void testPartialUpdateModesWithMixedRecords() throws IOException {
    // Test all partial update modes with mixed records (old has full, new has partial)
    BufferedRecordMerger<IndexedRecord> keepValuesMerger = createPartialUpdateMerger();
    BufferedRecordMerger<IndexedRecord> ignoreDefaultsMerger = createIgnoreDefaultsMerger();
    BufferedRecordMerger<IndexedRecord> ignoreMarkersMerger = createIgnoreMarkersMerger();
    
    IndexedRecord oldRecord = createFullRecord("old_id", "Old Name", 25, "Old City", 1000L);
    IndexedRecord newRecord = createPartialRecord("new_id", "New Name");
    
    BufferedRecord<IndexedRecord> oldBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE, oldRecord, 1, false);
    BufferedRecord<IndexedRecord> newBufferedRecord = new BufferedRecord<>(RECORD_KEY, ORDERING_VALUE + 1, newRecord, 1, false);

    // Test delta merge for all modes
    Option<BufferedRecord<IndexedRecord>> keepValuesResult = keepValuesMerger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    Option<BufferedRecord<IndexedRecord>> ignoreDefaultsResult = ignoreDefaultsMerger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    Option<BufferedRecord<IndexedRecord>> ignoreMarkersResult = ignoreMarkersMerger.deltaMerge(newBufferedRecord, oldBufferedRecord);
    
    assertTrue(keepValuesResult.isPresent());
    assertTrue(ignoreDefaultsResult.isPresent());
    assertTrue(ignoreMarkersResult.isPresent());
    
    // Test final merge for all modes
    Pair<Boolean, IndexedRecord> keepValuesFinalResult = keepValuesMerger.finalMerge(oldBufferedRecord, newBufferedRecord);
    Pair<Boolean, IndexedRecord> ignoreDefaultsFinalResult = ignoreDefaultsMerger.finalMerge(oldBufferedRecord, newBufferedRecord);
    Pair<Boolean, IndexedRecord> ignoreMarkersFinalResult = ignoreMarkersMerger.finalMerge(oldBufferedRecord, newBufferedRecord);
    
    assertFalse(keepValuesFinalResult.getLeft());
    assertFalse(ignoreDefaultsFinalResult.getLeft());
    assertFalse(ignoreMarkersFinalResult.getLeft());
    assertNotNull(keepValuesFinalResult.getRight());
    assertNotNull(ignoreDefaultsFinalResult.getRight());
    assertNotNull(ignoreMarkersFinalResult.getRight());
  }

  // Helper methods
  private BufferedRecordMerger<IndexedRecord> createMerger(RecordMergeMode mergeMode) {
    return BufferedRecordMergerFactory.create(
        readerContext,
        mergeMode,
        false, // enablePartialMerging
        Option.empty(), // recordMerger
        Collections.emptyList(), // orderingFieldNames
        Option.empty(), // payloadClass
        fullSchema, // readerSchema
        props, // props
        PartialUpdateMode.NONE // partialUpdateMode
    );
  }



  private BufferedRecordMerger<IndexedRecord> createPartialUpdateMerger() {
    return BufferedRecordMergerFactory.create(
        readerContext,
        RecordMergeMode.EVENT_TIME_ORDERING,
        true, // enablePartialMerging
        Option.empty(), // recordMerger
        Collections.emptyList(), // orderingFieldNames
        Option.empty(), // payloadClass
        fullSchema, // readerSchema
        props, // props
        PartialUpdateMode.KEEP_VALUES // partialUpdateMode
    );
  }

  private BufferedRecordMerger<IndexedRecord> createIgnoreDefaultsMerger() {
    return BufferedRecordMergerFactory.create(
        readerContext,
        RecordMergeMode.EVENT_TIME_ORDERING,
        true, // enablePartialMerging
        Option.empty(), // recordMerger
        Collections.emptyList(), // orderingFieldNames
        Option.empty(), // payloadClass
        fullSchema, // readerSchema
        props, // props
        PartialUpdateMode.IGNORE_DEFAULTS // partialUpdateMode
    );
  }

  private BufferedRecordMerger<IndexedRecord> createIgnoreMarkersMerger() {
    return BufferedRecordMergerFactory.create(
        readerContext,
        RecordMergeMode.EVENT_TIME_ORDERING,
        true, // enablePartialMerging
        Option.empty(), // recordMerger
        Collections.emptyList(), // orderingFieldNames
        Option.empty(), // payloadClass
        fullSchema, // readerSchema
        props, // props
        PartialUpdateMode.IGNORE_MARKERS // partialUpdateMode
    );
  }

  private IndexedRecord createFullRecord(String id, String name, int age, String city, long timestamp) {
    GenericRecord record = new GenericData.Record(fullSchema);
    record.put("id", id);
    record.put("name", name);
    record.put("age", age);
    record.put("city", city);
    record.put("timestamp", timestamp);
    return record;
  }

  private IndexedRecord createPartialRecord(String id, String name) {
    GenericRecord record = new GenericData.Record(partialSchema);
    record.put("id", id);
    record.put("name", name);
    return record;
  }
}
