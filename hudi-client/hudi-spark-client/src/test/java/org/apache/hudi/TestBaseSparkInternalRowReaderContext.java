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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.OrderingValueEngineTypeConverter;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBaseSparkInternalRowReaderContext {
  // Dummy schema: {"id": int, "name": string, "active": boolean}
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("TestRecord", null, null,
      Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
          HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
      ));
  private static final List<String> FIELD_NAMES = Arrays.asList("id", "name", "active");

  private BaseSparkInternalRowReaderContext readerContext;
  private StorageConfiguration<?> storageconfig;
  private HoodieTableConfig tableConfig;

  @BeforeEach
  void setUp() {
    storageconfig = mock(StorageConfiguration.class);
    tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"id"}));

    readerContext = new DummySparkReaderContext(storageconfig, tableConfig);
  }

  @Test
  void testConstructEngineRecordWithFieldValues() {
    Object[] fieldVals = new Object[]{1, UTF8String.fromString("Alice"), true};
    InternalRow row = readerContext.getRecordContext().constructEngineRecord(SCHEMA, fieldVals);
    assertEquals(fieldVals[0], row.getInt(0));
    assertEquals(fieldVals[1].toString(), row.getString(1));
    assertEquals(fieldVals[2], row.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithNoUpdate() {
    InternalRow base = new GenericInternalRow(new Object[]{1, UTF8String.fromString("Alice"), true});
    Map<Integer, Object> updates = new HashMap<>();
    BufferedRecord<InternalRow> record = new BufferedRecord<>(
        "record_key", 1, base, 1, null);
    InternalRow engineRecord = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);
    assertEquals(1, engineRecord.getInt(0));
    assertEquals("Alice", engineRecord.getString(1));
    assertTrue(engineRecord.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithOneUpdateField() {
    InternalRow base = new GenericInternalRow(new Object[]{1, UTF8String.fromString("Alice"), true});
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(1, UTF8String.fromString("Bob"));
    BufferedRecord<InternalRow> record = new BufferedRecord<>(
        "record_key", 1, base, 1, null);
    InternalRow result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);
    assertEquals(1, result.getInt(0)); // from base
    assertEquals("Bob", result.getUTF8String(1).toString()); // updated
    assertTrue(result.getBoolean(2)); // from base
  }

  @Test
  void testConstructEngineRecordWithAllFields() {
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(0, 42);
    updates.put(1, UTF8String.fromString("Carol"));
    updates.put(2, false);

    InternalRow base = new GenericInternalRow(new Object[]{1, UTF8String.fromString("Alice"), true});
    BufferedRecord<InternalRow> record = new BufferedRecord<>(
        "record_key", 1, base, 1, null);

    InternalRow result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);
    assertEquals(42, result.getInt(0));
    assertEquals("Carol", result.getUTF8String(1).toString());
    assertFalse(result.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithNullValueFromBase() {
    InternalRow base = new GenericInternalRow(new Object[]{null, UTF8String.fromString("Dan"), true});
    Map<Integer, Object> updates = new HashMap<>();
    BufferedRecord<InternalRow> record = new BufferedRecord<>(
        "record_key", 1, base, 1, null);

    InternalRow result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);
    assertTrue(result.isNullAt(0));
    assertEquals("Dan", result.getUTF8String(1).toString());
    assertTrue(result.getBoolean(2));
  }

  static class DummySparkReaderContext extends BaseSparkInternalRowReaderContext {
    public DummySparkReaderContext(StorageConfiguration<?> config,
                                   HoodieTableConfig tableConfig) {
      super(config, tableConfig, new BaseSparkInternalRecordContext(tableConfig) {
        @Override
        public InternalRow convertAvroRecord(IndexedRecord avroRecord) {
          return null;
        }

        @Override
        public GenericRecord convertToAvroRecord(InternalRow record, HoodieSchema schema) {
          return null;
        }

        @Override
        public Object getValue(InternalRow row, HoodieSchema schema, String fieldName) {
          if (fieldName.equals("id")) {
            if (row.isNullAt(0)) {
              return null;
            }
            return row.getInt(0);
          } else if (fieldName.equals("name")) {
            if (row.isNullAt(1)) {
              return null;
            }
            return UTF8String.fromString(row.getString(1));
          } else {
            if (row.isNullAt(2)) {
              return null;
            }
            return row.getBoolean(2);
          }
        }

        @Override
        public InternalRow toBinaryRow(HoodieSchema schema, InternalRow internalRow) {
          return internalRow;
        }
      });
    }

    @Override
    public ClosableIterator<InternalRow> getFileRecordIterator(StoragePath filePath,
                                                               long start,
                                                               long length,
                                                               HoodieSchema dataSchema,
                                                               HoodieSchema requiredSchema,
                                                               HoodieStorage storage) throws IOException {
      return null;
    }

    @Override
    public ClosableIterator<InternalRow> mergeBootstrapReaders(ClosableIterator<InternalRow> skeletonFileIterator,
                                                               HoodieSchema skeletonRequiredSchema,
                                                               ClosableIterator<InternalRow> dataFileIterator,
                                                               HoodieSchema dataRequiredSchema,
                                                               List<Pair<String, Object>> requiredPartitionFieldAndValues) {
      return null;
    }
  }

  @Test
  void testNullOrderingValueIsPreserved() {
    HoodieSchema schema = HoodieSchema.createRecord("nullable_record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.INT)),
        HoodieSchemaField.of("name", HoodieSchema.createNullable(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))));
    InternalRow row = new GenericInternalRow(new Object[] {null, null, true});
    BaseSparkInternalRecordContext context = (BaseSparkInternalRecordContext) readerContext.getRecordContext();
    assertEquals(OrderingValues.getDefault(), context.getOrderingValue(row, schema, Collections.emptyList()));
    assertEquals(OrderingValues.getDefault(), context.getOrderingValue(row, schema, new String[0]));
    assertNull(context.getOrderingValue(row, schema, Collections.singletonList("id")));
    assertNull(context.getOrderingValue(row, schema, new String[] {"id"}));
    assertEquals(Arrays.asList(null, null), ((ArrayComparable) context.getOrderingValue(
        row, schema, Arrays.asList("id", "name"))).getValues());

    HoodieSparkRecord record = new HoodieSparkRecord(row, HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema));
    assertEquals(OrderingValues.getDefault(), new HoodieSparkRecord(row, HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema))
        .getOrderingValue(schema, new Properties(), null));
    assertEquals(OrderingValues.getDefault(), new HoodieSparkRecord(row, HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema))
        .getOrderingValue(schema, new Properties(), new String[0]));
    assertNull(record.getOrderingValue(schema, new Properties(), new String[] {"id"}));
    assertEquals(OrderingValues.getDefault(), record.getOrderingValueAsJava(schema, new Properties(), null));
    assertEquals(OrderingValues.getDefault(), record.getOrderingValueAsJava(schema, new Properties(), new String[0]));
    assertNull(record.getOrderingValueAsJava(schema, new Properties(), new String[] {"id"}));
  }

  @Test
  void testNullTimestampOrderingConversion() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("ts", HoodieSchema.createTimestampMillis()),
        HoodieSchemaField.of("seq", HoodieSchema.create(HoodieSchemaType.LONG))));
    OrderingValueEngineTypeConverter converter = OrderingValueEngineTypeConverter.create(schema, Collections.singletonList("ts"));
    assertNull(converter.convert(null));
    assertNull(OrderingValueEngineTypeConverter.create(schema, Collections.emptyList()).convert(null));
    converter = OrderingValueEngineTypeConverter.create(schema, Arrays.asList("ts", "seq"));
    assertEquals(Arrays.asList(null, 1L), ((ArrayComparable) converter.convert(
        new ArrayComparable(new Comparable[] {null, 1L}))).getValues());
  }

}
