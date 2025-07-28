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

package org.apache.hudi.avro;

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieAvroReaderContext {
  private static final Schema SKELETON_SCHEMA = getSkeletonSchema();
  private static final Schema LIMITED_SKELETON_SCHEMA = getLimitedSkeletonSchema();
  private static final Schema BASE_SCHEMA = getBaseSchema();
  private static final Schema LIMITED_BASE_SCHEMA = getLimitedBaseSchema();
  private static final Schema FULL_MERGED_SCHEMA = AvroSchemaUtils.mergeSchemas(SKELETON_SCHEMA, BASE_SCHEMA);
  private static final Schema LIMTIED_MERGED_SCHEMA = AvroSchemaUtils.mergeSchemas(LIMITED_SKELETON_SCHEMA, LIMITED_BASE_SCHEMA);

  private final StorageConfiguration<?> storageConfig = mock(StorageConfiguration.class);
  private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

  @BeforeEach
  void setup() {
    when(tableConfig.getPayloadClass()).thenReturn(DefaultHoodieRecordPayload.class.getName());
  }

  @Test
  void bootstrapIteratorWithLimitedFields() {
    HoodieAvroReaderContext avroReaderContext = getReaderContextWithMetaFields();

    IndexedRecord skeletonRecord1 = createSkeletonRecord("s-100", "s-101", 1);
    IndexedRecord baseRecord1 = createBaseRecord("b-100", "b-101", 1.3);
    IndexedRecord expectedRecord1 = createLimitedMergedRecord("s-101", "b-100", 1.3);

    IndexedRecord skeletonRecord2 = createSkeletonRecord("s-200", "s-201", 2);
    IndexedRecord baseRecord2 = createBaseRecord("b-200", "b-201", 2.9);
    IndexedRecord expectedRecord2 = createLimitedMergedRecord("s-201", "b-200", 2.9);

    IndexedRecord skeletonRecord3 = createSkeletonRecord("s-300", "s-301", 3);
    IndexedRecord baseRecord3 = createBaseRecord("b-300", "b-301", 3.2);
    IndexedRecord expectedRecord3 = createLimitedMergedRecord("s-301", "b-300", 3.2);

    ClosableIterator<IndexedRecord> skeletonIterator = ClosableIterator.wrap(Arrays.asList(skeletonRecord1, skeletonRecord2, skeletonRecord3).iterator());
    ClosableIterator<IndexedRecord> baseIterator = ClosableIterator.wrap(Arrays.asList(baseRecord1, baseRecord2, baseRecord3).iterator());

    List<IndexedRecord> actual = new ArrayList<>();
    try (ClosableIterator<IndexedRecord> iter = avroReaderContext.mergeBootstrapReaders(skeletonIterator, LIMITED_SKELETON_SCHEMA, baseIterator, LIMITED_BASE_SCHEMA, Collections.emptyList())) {
      iter.forEachRemaining(actual::add);
    }
    assertEquals(Arrays.asList(expectedRecord1, expectedRecord2, expectedRecord3), actual);
  }

  @Test
  void bootstrapIteratorWithAllFields() {
    HoodieAvroReaderContext avroReaderContext = getReaderContextWithMetaFields();

    IndexedRecord skeletonRecord1 = createSkeletonRecord("s-100", "s-101", 1);
    IndexedRecord baseRecord1 = createBaseRecord("b-100", "b-101", 1.3);
    IndexedRecord expectedRecord1 = createFullMergedRecord("s-100", "s-101", 1, "b-100", "b-101", 1.3);

    IndexedRecord skeletonRecord2 = createSkeletonRecord("s-200", "s-201", 2);
    IndexedRecord baseRecord2 = createBaseRecord("b-200", "b-201", 2.9);
    IndexedRecord expectedRecord2 = createFullMergedRecord("s-200", "s-201", 2, "b-200", "b-201", 2.9);

    IndexedRecord skeletonRecord3 = createSkeletonRecord("s-300", "s-301", 3);
    IndexedRecord baseRecord3 = createBaseRecord("b-300", "b-301", 3.2);
    IndexedRecord expectedRecord3 = createFullMergedRecord("s-300", "s-301", 3, "b-300", "b-301", 3.2);

    ClosableIterator<IndexedRecord> skeletonIterator = ClosableIterator.wrap(Arrays.asList(skeletonRecord1, skeletonRecord2, skeletonRecord3).iterator());
    ClosableIterator<IndexedRecord> baseIterator = ClosableIterator.wrap(Arrays.asList(baseRecord1, baseRecord2, baseRecord3).iterator());

    List<IndexedRecord> actual = new ArrayList<>();
    try (ClosableIterator<IndexedRecord> iter = avroReaderContext.mergeBootstrapReaders(skeletonIterator, SKELETON_SCHEMA, baseIterator, BASE_SCHEMA, Collections.emptyList())) {
      iter.forEachRemaining(actual::add);
    }
    assertEquals(Arrays.asList(expectedRecord1, expectedRecord2, expectedRecord3), actual);
  }

  @Test
  void bootstrapIteratorWithEmptyIterators() {
    HoodieAvroReaderContext avroReaderContext = getReaderContextWithMetaFields();
    List<IndexedRecord> actual = new ArrayList<>();
    try (ClosableIterator<IndexedRecord> iter = avroReaderContext.mergeBootstrapReaders(ClosableIterator.wrap(Collections.emptyIterator()), SKELETON_SCHEMA,
        ClosableIterator.wrap(Collections.emptyIterator()), BASE_SCHEMA, Collections.emptyList())) {
      iter.forEachRemaining(actual::add);
    }
    assertEquals(Collections.emptyList(), actual);
  }

  @Test
  void bootstrapIteratorFailsDueToMismatchedIterators() {
    HoodieAvroReaderContext avroReaderContext = getReaderContextWithMetaFields();

    IndexedRecord skeletonRecord1 = createSkeletonRecord("s-100", "s-101", 1);
    IndexedRecord baseRecord1 = createBaseRecord("b-100", "b-101", 1.3);

    IndexedRecord skeletonRecord2 = createSkeletonRecord("s-200", "s-201", 2);

    ClosableIterator<IndexedRecord> skeletonIterator = ClosableIterator.wrap(Arrays.asList(skeletonRecord1, skeletonRecord2).iterator());
    ClosableIterator<IndexedRecord> baseIterator = ClosableIterator.wrap(Collections.singletonList(baseRecord1).iterator());

    List<IndexedRecord> actual = new ArrayList<>();
    assertThrows(IllegalStateException.class, () -> {
      try (ClosableIterator<IndexedRecord> iter = avroReaderContext.mergeBootstrapReaders(skeletonIterator, SKELETON_SCHEMA, baseIterator, BASE_SCHEMA, Collections.emptyList())) {
        iter.forEachRemaining(actual::add);
      }
    });
  }

  @Test
  void getField() {
    HoodieAvroReaderContext avroReaderContext = getReaderContextWithMetaFields();
    IndexedRecord indexedRecord = createSkeletonRecord("field1", "field2", 3);
    assertEquals("field2", avroReaderContext.getValue(indexedRecord, SKELETON_SCHEMA, "skeleton_field_2"));
  }

  @Test
  void getFieldNotInSchema() {
    HoodieAvroReaderContext avroReaderContext = getReaderContextWithMetaFields();
    IndexedRecord indexedRecord = createSkeletonRecord("field1", "field2", 3);
    assertNull(avroReaderContext.getValue(indexedRecord, SKELETON_SCHEMA, "missing_field"));
  }

  @Test
  void getNestedField() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HoodieAvroReaderContext avroReaderContext =
        new HoodieAvroReaderContext(storageConfig, tableConfig, Option.empty(), Option.empty());
    IndexedRecord indexedRecord = createBaseRecord("compound", "field2", 3.2);
    assertEquals(3.2, avroReaderContext.getValue(indexedRecord, BASE_SCHEMA, "base_field_3.nested_field"));
  }

  @Test
  void getRecordKeyWithSingleKey() {
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"skeleton_field_1"}));
    HoodieAvroReaderContext avroReaderContext =
        new HoodieAvroReaderContext(storageConfig, tableConfig, Option.empty(), Option.empty());
    String recordKey = "record_key";
    IndexedRecord indexedRecord = createSkeletonRecord(recordKey, "field2", 3);
    assertEquals(recordKey, avroReaderContext.getRecordKey(indexedRecord, SKELETON_SCHEMA));
  }

  @Test
  void getRecordKeyWithMultipleKeys() {
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"base_field_1", "base_field_3.nested_field"}));
    HoodieAvroReaderContext avroReaderContext =
        new HoodieAvroReaderContext(storageConfig, tableConfig, Option.empty(), Option.empty());
    String recordKey = "base_field_1:compound,base_field_3.nested_field:3.2";
    IndexedRecord indexedRecord = createBaseRecord("compound", "field2", 3.2);
    assertEquals(recordKey, avroReaderContext.getRecordKey(indexedRecord, BASE_SCHEMA));
  }

  @Test
  void getRecordKeyFromMetadataFields() {
    HoodieAvroReaderContext avroReaderContext = getReaderContextWithMetaFields();
    Schema schemaWithMetaFields = HoodieAvroUtils.addMetadataFields(SKELETON_SCHEMA);
    String recordKey = "record_key";
    IndexedRecord indexedRecord = new GenericData.Record(schemaWithMetaFields);
    indexedRecord.put(0, "commit_time");
    indexedRecord.put(1, "commit_seqno");
    indexedRecord.put(2, recordKey);
    indexedRecord.put(5, "field1");
    indexedRecord.put(6, "field2");
    indexedRecord.put(7, 3);
    assertEquals(recordKey, avroReaderContext.getRecordKey(indexedRecord, schemaWithMetaFields));
  }

  @Test
  void testConstructEngineRecordWithNoUpdate() {
    HoodieAvroReaderContext readerContext = getReaderContextWithMetaFields();
    Schema schema = getSkeletonSchema();
    IndexedRecord engineRecord = createSkeletonRecord("String1", "String2", 1);
    BufferedRecord<IndexedRecord> baseRecord =
        new BufferedRecord<>("key1", 1, engineRecord, 0, false);
    Map<Integer, Object> updates = new HashMap<>();
    IndexedRecord output = readerContext.mergeWithEngineRecord(schema, updates, baseRecord);
    assertEquals("String1", output.get(0));
    assertEquals("String2", output.get(1));
    assertEquals(1, output.get(2));
  }

  @Test
  void testConstructEngineRecordWithUpdates() {
    HoodieAvroReaderContext readerContext = getReaderContextWithMetaFields();
    Schema schema = getSkeletonSchema();
    IndexedRecord engineRecord = createSkeletonRecord("String1", "String2", 1);
    BufferedRecord<IndexedRecord> baseRecord =
        new BufferedRecord<>("key1", 1, engineRecord, 0, false);
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(0, "String1_0");
    updates.put(2, 2);
    IndexedRecord output = readerContext.mergeWithEngineRecord(schema, updates, baseRecord);
    assertEquals("String1_0", output.get(0));
    assertEquals("String2", output.get(1));
    assertEquals(2, output.get(2));
  }

  private HoodieAvroReaderContext getReaderContextWithMetaFields() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    return new HoodieAvroReaderContext(storageConfig, tableConfig, Option.empty(), Option.empty());
  }

  private static Schema getSkeletonSchema() {
    Schema skeletonDataSchema = Schema.createRecord("skeleton_full_schema", null, null, false);
    Schema.Field skeletonField1 = new Schema.Field("skeleton_field_1", Schema.create(Schema.Type.STRING));
    Schema.Field skeletonField2 = new Schema.Field("skeleton_field_2", Schema.create(Schema.Type.STRING));
    Schema.Field skeletonField3 = new Schema.Field("skeleton_field_3", Schema.create(Schema.Type.INT));
    skeletonDataSchema.setFields(Arrays.asList(skeletonField1, skeletonField2, skeletonField3));
    return skeletonDataSchema;
  }

  private static Schema getLimitedSkeletonSchema() {
    Schema skeletonDataSchema = Schema.createRecord("skeleton_limited_schema", null, null, false);
    Schema.Field skeletonField2 = new Schema.Field("skeleton_field_2", Schema.create(Schema.Type.STRING));
    skeletonDataSchema.setFields(Collections.singletonList(skeletonField2));
    return skeletonDataSchema;
  }

  private static Schema getBaseSchema() {
    Schema baseDataSchema = Schema.createRecord("base_full_schema", null, null, false);
    Schema.Field baseField1 = new Schema.Field("base_field_1", Schema.create(Schema.Type.STRING));
    Schema.Field baseField2 = new Schema.Field("base_field_2", Schema.create(Schema.Type.STRING));
    Schema.Field baseField3 = new Schema.Field("base_field_3", Schema.createRecord("nested", null, null, false, Collections.singletonList(new Schema.Field("nested_field", Schema.create(
        Schema.Type.DOUBLE)))));
    baseDataSchema.setFields(Arrays.asList(baseField1, baseField2, baseField3));
    return baseDataSchema;
  }

  private static Schema getLimitedBaseSchema() {
    Schema baseDataSchema = Schema.createRecord("base_limited_schema", null, null, false);
    Schema.Field baseField1 = new Schema.Field("base_field_1", Schema.create(Schema.Type.STRING));
    Schema.Field baseField3 = new Schema.Field("base_field_3", Schema.createRecord("nested", null, null, false,
        Collections.singletonList(new Schema.Field("nested_field", Schema.create(Schema.Type.DOUBLE)))));
    baseDataSchema.setFields(Arrays.asList(baseField1, baseField3));
    return baseDataSchema;
  }

  private IndexedRecord createSkeletonRecord(String field1, String field2, int field3) {
    GenericRecord record = new GenericData.Record(SKELETON_SCHEMA);
    record.put(0, field1);
    record.put(1, field2);
    record.put(2, field3);
    return record;
  }

  private IndexedRecord createBaseRecord(String field1, String field2, double field3) {
    GenericRecord nested = new GenericData.Record(BASE_SCHEMA.getFields().get(2).schema());
    nested.put(0, field3);
    GenericRecord record = new GenericData.Record(BASE_SCHEMA);
    record.put(0, field1);
    record.put(1, field2);
    record.put(2, nested);
    return record;
  }

  private IndexedRecord createFullMergedRecord(String skeletonField1, String skeletonField2, int skeletonField3,
                                            String baseField1, String baseField2, double baseField3) {
    GenericRecord nested = new GenericData.Record(FULL_MERGED_SCHEMA.getFields().get(5).schema());
    nested.put(0, baseField3);
    GenericRecord record = new GenericData.Record(FULL_MERGED_SCHEMA);
    record.put(0, skeletonField1);
    record.put(1, skeletonField2);
    record.put(2, skeletonField3);
    record.put(3, baseField1);
    record.put(4, baseField2);
    record.put(5, nested);
    return record;
  }

  private IndexedRecord createLimitedMergedRecord(String skeletonField2, String baseField1, double baseField3) {
    GenericRecord nested = new GenericData.Record(LIMTIED_MERGED_SCHEMA.getFields().get(2).schema());
    nested.put(0, baseField3);
    GenericRecord record = new GenericData.Record(LIMTIED_MERGED_SCHEMA);
    record.put(0, skeletonField2);
    record.put(1, baseField1);
    record.put(2, nested);
    return record;
  }
}
