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

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
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
    when(tableConfig.isRecordKeyPopulated()).thenReturn(true);
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

  @Test
  void testConvertOrderingValueToJava() {
    RecordContext<InternalRow> recordContext = readerContext.getRecordContext();
    HoodieUTF8String comparableString = comparableString("abc");
    assertEquals("abc", recordContext.convertOrderingValueToJava(comparableString));
    assertEquals("raw", recordContext.convertOrderingValueToJava(UTF8String.fromString("raw")));
    assertEquals(new BigDecimal("1.50"), recordContext.convertOrderingValueToJava(Decimal.apply(new BigDecimal("1.50"))));
    assertEquals(5L, recordContext.convertOrderingValueToJava(5L));
    assertEquals(
        new ArrayComparable(new Comparable[]{"abc", new BigDecimal("1.50"), 5L}),
        recordContext.convertOrderingValueToJava(new ArrayComparable(new Comparable[]{comparableString, Decimal.apply(new BigDecimal("1.50")), 5L})));
  }

  @Test
  void testConstructHoodieRecordKeepsBothOrderingValuesOfDelete() {
    HoodieUTF8String comparableString = comparableString("abc");
    Decimal decimal = Decimal.apply(new BigDecimal("1.50"));
    assertDeleteOrderingValues(comparableString, "abc");
    assertDeleteOrderingValues(decimal, new BigDecimal("1.50"));
    assertDeleteOrderingValues(
        new ArrayComparable(new Comparable[]{comparableString, decimal, 5L}),
        new ArrayComparable(new Comparable[]{"abc", new BigDecimal("1.50"), 5L}));
    assertDeleteOrderingValues(5L, 5L);
  }

  @Test
  void testDeleteWithRowKeepsBothOrderingValues() {
    RecordContext<InternalRow> recordContext = readerContext.getRecordContext();
    HoodieUTF8String comparableName = comparableString("abc");
    InternalRow row = recordContext.constructEngineRecord(SCHEMA, new Object[]{1, UTF8String.fromString("abc"), true});
    HoodieRecord<InternalRow> record = recordContext.constructHoodieRecord(
        new BufferedRecord<>("key", comparableName, row, recordContext.encodeSchema(SCHEMA), HoodieOperation.DELETE), "partition");
    Properties props = new Properties();
    String[] orderingFields = {"name"};
    // Whether a delete keeps its row or becomes an empty record, each getter returns its own form of the value.
    assertSame(comparableName, record.getOrderingValue(SCHEMA, props, orderingFields));
    assertEquals("abc", record.getOrderingValueAsJava(SCHEMA, props, orderingFields));
  }

  private void assertDeleteOrderingValues(Comparable engineValue, Comparable javaValue) {
    Properties props = new Properties();
    String[] orderingFields = {"ts"};
    HoodieRecord<InternalRow> record = readerContext.getRecordContext().constructHoodieRecord(
        new BufferedRecord<>("key", engineValue, null, null, HoodieOperation.DELETE), "partition");
    assertInstanceOf(HoodieEmptyRecord.class, record);
    // Comparisons with other records, e.g., in precombine, need the engine value, delete blocks the Java one.
    assertSame(engineValue, record.getOrderingValue(null, props, orderingFields));
    assertEquals(javaValue, record.getOrderingValueAsJava(null, props, orderingFields));
  }

  private static HoodieUTF8String comparableString(String value) {
    return new HoodieUTF8String(UTF8String.fromString(value)) {
      @Override
      public int compareTo(HoodieUTF8String other) {
        return toString().compareTo(other.toString());
      }
    };
  }

  static class DummySparkReaderContext extends BaseSparkInternalRowReaderContext {
    public DummySparkReaderContext(StorageConfiguration<?> config,
                                   HoodieTableConfig tableConfig) {
      super(config, tableConfig, Option.empty(), new BaseSparkInternalRecordContext(tableConfig) {
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
}
