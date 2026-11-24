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

import org.apache.hudi.common.BaseFileColumnResolver;
import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link FormatMetaColumnHandler} with two columns that keep a stored value and otherwise resolve one from the
 * position of a record in the base file, or a value of the base file.
 */
class TestFormatMetaColumnHandler {
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
      HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
      nullableLongField("row_id"),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING)),
      nullableLongField("seq")));
  private static final HoodieSchema SCHEMA_WITHOUT_FORMAT_META_COLUMNS = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
      HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING))));
  private static final List<FormatMetaColumn> COLUMNS = Arrays.asList(column("row_id", null), column("seq", null));
  private static final List<BaseFileColumnResolver> RESOLVERS = Arrays.asList(
      (rowPosition, storedValue) -> storedValue != null ? storedValue : (Object) (100L + rowPosition),
      (rowPosition, storedValue) -> storedValue != null ? storedValue : (Object) 5L);

  private final AvroRecordContext recordContext = new AvroRecordContext();
  private final AvroRecordContext keyedRecordContext = keyedRecordContext();
  private final FormatMetaColumnHandler<IndexedRecord> handler = new FormatMetaColumnHandler<>(recordContext, COLUMNS, RESOLVERS, "001");

  private static HoodieSchemaField nullableLongField(String name) {
    return HoodieSchemaField.of(name, HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE);
  }

  private static FormatMetaColumn column(String name, Long valueOnWrite) {
    return column(name, valueOnWrite, false);
  }

  private static FormatMetaColumn column(String name, Long valueOnWrite, boolean keptOnUpdate) {
    return new FormatMetaColumn() {
      @Override
      public HoodieSchemaField field() {
        return nullableLongField(name);
      }

      @Override
      public Object valueOnWrite(String instantTime) {
        return valueOnWrite;
      }

      @Override
      public Object valueOnUpdate(String instantTime, Object replacedValue) {
        return keptOnUpdate ? replacedValue : valueOnWrite;
      }

      @Override
      public BaseFileColumnResolver resolverFor(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile) {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * A record context that extracts the record key from the "key" field.
   */
  private static AvroRecordContext keyedRecordContext() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.META_FIELDS_MODE, MetaFieldsMode.NONE.name());
    tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "key");
    return new AvroRecordContext(tableConfig, null, new TypedProperties());
  }

  /**
   * A handler whose row id column keeps the value of the replaced record on update and whose sequence number column
   * gets 42 on write.
   */
  private FormatMetaColumnHandler<IndexedRecord> handlerWithValuesOnUpdate() {
    return new FormatMetaColumnHandler<>(keyedRecordContext, Arrays.asList(column("row_id", null, true), column("seq", 42L)), RESOLVERS, "001");
  }

  private static IndexedRecord record(String key, Long rowId, Long sequenceNumber) {
    GenericData.Record record = new GenericData.Record(SCHEMA.toAvroSchema());
    record.put("key", key);
    record.put("row_id", rowId);
    record.put("value", "value_" + key);
    record.put("seq", sequenceNumber);
    return record;
  }

  @Test
  void testResolveBaseFileRecordsKeepsStoredValuesAndResolvesTheOthersFromThePosition() {
    List<IndexedRecord> baseFileRecords = Arrays.asList(
        record("key0", 7L, 3L), record("key1", null, null), record("key2", 9L, null), record("key3", null, 4L), record("key4", null, null));
    List<IndexedRecord> resolved = new ArrayList<>();
    try (ClosableIterator<IndexedRecord> iterator = handler.resolveBaseFileRecords(ClosableIterator.wrap(baseFileRecords.iterator()), SCHEMA)) {
      iterator.forEachRemaining(resolved::add);
    }
    assertEquals(Arrays.asList(
        record("key0", 7L, 3L), record("key1", 101L, 5L), record("key2", 9L, 5L), record("key3", 103L, 4L), record("key4", 104L, 5L)), resolved);
  }

  @Test
  void testResolveBaseFileRecordsRequiresTheColumnsAndTheResolvers() {
    List<IndexedRecord> withoutColumns = Collections.singletonList(new GenericData.Record(SCHEMA_WITHOUT_FORMAT_META_COLUMNS.toAvroSchema()));
    assertThrows(IllegalArgumentException.class, () -> handler.resolveBaseFileRecords(
        ClosableIterator.wrap(withoutColumns.iterator()), SCHEMA_WITHOUT_FORMAT_META_COLUMNS).next());
    FormatMetaColumnHandler<IndexedRecord> withoutBaseFile = new FormatMetaColumnHandler<>(recordContext, COLUMNS, Collections.emptyList(), "001");
    assertThrows(IllegalArgumentException.class, () -> withoutBaseFile.resolveNextBaseFileRecord(record("key0", null, null), SCHEMA));
  }

  @Test
  void testSetValuesOnWriteOfChangedRecords() {
    Integer schemaId = recordContext.encodeSchema(SCHEMA);
    // A record returned the way it is stored in the base file has no operation and keeps its values
    BufferedRecord<IndexedRecord> unchanged = new BufferedRecord<>("key0", 0, record("key0", 7L, 3L), schemaId, null);
    handler.setValuesOnWriteIfChanged(unchanged);
    assertEquals(record("key0", 7L, 3L), unchanged.getRecord());

    for (HoodieOperation operation : Arrays.asList(HoodieOperation.UPDATE_AFTER, HoodieOperation.INSERT)) {
      BufferedRecord<IndexedRecord> changed = new BufferedRecord<>("key1", 0, record("key1", 7L, 3L), schemaId, operation);
      handler.setValuesOnWriteIfChanged(changed);
      assertEquals(record("key1", null, null), changed.getRecord());
    }

    // A changed record in a schema without the format meta columns has no values to set
    GenericData.Record withoutColumns = new GenericData.Record(SCHEMA_WITHOUT_FORMAT_META_COLUMNS.toAvroSchema());
    withoutColumns.put("key", "key2");
    withoutColumns.put("value", "value_key2");
    BufferedRecord<IndexedRecord> changedWithoutColumns = new BufferedRecord<>(
        "key2", 0, withoutColumns, recordContext.encodeSchema(SCHEMA_WITHOUT_FORMAT_META_COLUMNS), HoodieOperation.UPDATE_AFTER);
    handler.setValuesOnWriteIfChanged(changedWithoutColumns);
    assertSame(withoutColumns, changedWithoutColumns.getRecord());
    assertEquals("value_key2", withoutColumns.get("value"));

    // The schema of the previous changed record does not apply to the next one
    BufferedRecord<IndexedRecord> changedAgain = new BufferedRecord<>("key3", 0, record("key3", 8L, 4L), schemaId, HoodieOperation.UPDATE_AFTER);
    handler.setValuesOnWriteIfChanged(changedAgain);
    assertEquals(record("key3", null, null), changedAgain.getRecord());

    BufferedRecord<IndexedRecord> deleted = new BufferedRecord<>("key4", 0, record("key4", 7L, 3L), schemaId, HoodieOperation.DELETE);
    handler.setValuesOnWriteIfChanged(deleted);
    assertEquals(record("key4", 7L, 3L), deleted.getRecord());
  }

  @Test
  void testNonNullValuesOnWrite() {
    FormatMetaColumnHandler<IndexedRecord> withValues = new FormatMetaColumnHandler<>(
        recordContext, Arrays.asList(column("row_id", null), column("seq", 42L)), RESOLVERS, "001");
    BufferedRecord<IndexedRecord> changed = new BufferedRecord<>("key1", 0, record("key1", 7L, 3L), recordContext.encodeSchema(SCHEMA), HoodieOperation.UPDATE_AFTER);
    withValues.setValuesOnWriteIfChanged(changed);
    assertEquals(record("key1", null, 42L), changed.getRecord());

    // A record written as new gets the non-null values on write in place
    IndexedRecord inserted = record("key5", null, null);
    assertSame(inserted, withValues.setNonNullValuesOnWrite("key5", inserted, SCHEMA));
    assertEquals(record("key5", null, 42L), inserted);
    assertThrows(IllegalArgumentException.class, () -> withValues.setNonNullValuesOnWrite("key6",
        new GenericData.Record(SCHEMA_WITHOUT_FORMAT_META_COLUMNS.toAvroSchema()), SCHEMA_WITHOUT_FORMAT_META_COLUMNS));
  }

  @Test
  void testChangedVersionOfTheLastBaseFileRecordGetsTheValuesOnUpdate() {
    FormatMetaColumnHandler<IndexedRecord> withValuesOnUpdate = handlerWithValuesOnUpdate();
    Integer schemaId = keyedRecordContext.encodeSchema(SCHEMA);
    withValuesOnUpdate.resolveNextBaseFileRecord(record("key0", null, null), SCHEMA);
    withValuesOnUpdate.resolveNextBaseFileRecord(record("key1", 7L, 3L), SCHEMA);

    BufferedRecord<IndexedRecord> updated = new BufferedRecord<>("key1", 0, record("key1", null, null), schemaId, HoodieOperation.UPDATE_AFTER);
    withValuesOnUpdate.setValuesOnWriteIfChanged(updated);
    assertEquals(record("key1", 7L, 42L), updated.getRecord());
    // A batch that updates the record twice is combined into one record, which gets the values on update once more
    BufferedRecord<IndexedRecord> updatedAgain = new BufferedRecord<>("key1", 0, record("key1", 8L, 4L), schemaId, HoodieOperation.UPDATE_AFTER);
    withValuesOnUpdate.setValuesOnWriteIfChanged(updatedAgain);
    assertEquals(record("key1", 7L, 42L), updatedAgain.getRecord());

    BufferedRecord<IndexedRecord> inserted = new BufferedRecord<>("key9", 0, record("key9", 8L, 4L), schemaId, HoodieOperation.INSERT);
    withValuesOnUpdate.setValuesOnWriteIfChanged(inserted);
    assertEquals(record("key9", null, 42L), inserted.getRecord());
    BufferedRecord<IndexedRecord> earlierBaseFileRecord = new BufferedRecord<>("key0", 0, record("key0", 8L, 4L), schemaId, HoodieOperation.UPDATE_AFTER);
    withValuesOnUpdate.setValuesOnWriteIfChanged(earlierBaseFileRecord);
    assertEquals(record("key0", null, 42L), earlierBaseFileRecord.getRecord());
  }

  @Test
  void testRecordWrittenAsChangedVersionOfTheLastBaseFileRecordGetsTheNonNullValuesOnUpdate() {
    FormatMetaColumnHandler<IndexedRecord> withValuesOnUpdate = handlerWithValuesOnUpdate();
    IndexedRecord beforeAnyBaseFileRecord = record("key0", null, null);
    withValuesOnUpdate.setNonNullValuesOnWrite("key0", beforeAnyBaseFileRecord, SCHEMA);
    assertEquals(record("key0", null, 42L), beforeAnyBaseFileRecord);

    withValuesOnUpdate.resolveNextBaseFileRecord(record("key0", null, null), SCHEMA);
    IndexedRecord updated = record("key0", null, null);
    assertSame(updated, withValuesOnUpdate.setNonNullValuesOnWrite("key0", updated, SCHEMA));
    assertEquals(record("key0", 100L, 42L), updated);
    IndexedRecord inserted = record("key5", null, null);
    withValuesOnUpdate.setNonNullValuesOnWrite("key5", inserted, SCHEMA);
    assertEquals(record("key5", null, 42L), inserted);
  }

  @Test
  void testNullValueOfTheReplacedRecordIsKeptOnUpdate() {
    List<BaseFileColumnResolver> storedValues = Arrays.asList((rowPosition, storedValue) -> storedValue, (rowPosition, storedValue) -> storedValue);
    FormatMetaColumnHandler<IndexedRecord> keepingNull = new FormatMetaColumnHandler<>(keyedRecordContext,
        Arrays.asList(column("row_id", 9L, true), column("seq", 42L)), storedValues, "001");
    keepingNull.resolveNextBaseFileRecord(record("key0", null, 3L), SCHEMA);
    IndexedRecord updated = record("key0", null, null);
    keepingNull.setNonNullValuesOnWrite("key0", updated, SCHEMA);
    assertEquals(record("key0", null, 42L), updated);
  }

  @Test
  void testDefaultValueOnUpdateIsTheValueOnWrite() {
    FormatMetaColumn withoutValueOnUpdate = new FormatMetaColumn() {
      @Override
      public HoodieSchemaField field() {
        return nullableLongField("row_id");
      }

      @Override
      public Object valueOnWrite(String instantTime) {
        return Long.parseLong(instantTime);
      }

      @Override
      public BaseFileColumnResolver resolverFor(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile) {
        throw new UnsupportedOperationException();
      }
    };
    assertEquals(1L, withoutValueOnUpdate.valueOnUpdate("001", 7L));
  }
}
