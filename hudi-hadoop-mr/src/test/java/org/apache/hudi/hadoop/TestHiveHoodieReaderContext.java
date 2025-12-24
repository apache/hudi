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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHiveHoodieReaderContext {
  private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
  private final HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator = mock(HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator.class);
  private final StorageConfiguration<?> storageConfiguration = new HadoopStorageConfiguration(false);
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("TestRecord", null, null,
      Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
          HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
      ));

  @Test
  void getRecordKeyWithSingleKey() {
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"field_1"}));
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("value1", avroReaderContext.getRecordContext().getRecordKey(row, getBaseSchema()));
  }

  @Test
  void getRecordKeyWithMultipleKeys() {
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"field_1", "field_3.nested_field"}));
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("field_1:value1,field_3.nested_field:value3", avroReaderContext.getRecordContext().getRecordKey(row, getBaseSchema()));
  }

  @Test
  void getNestedField() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("value3", avroReaderContext.getRecordContext().getValue(row, getBaseSchema(), "field_3.nested_field").toString());
  }

  @Test
  void testConstructEngineRecordWithFieldValues() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    Object[] fieldVals = new Writable[]{
        new IntWritable(1),
        new Text("Alice"),
        new BooleanWritable(true)};
    ArrayWritable row = avroReaderContext.getRecordContext().constructEngineRecord(SCHEMA, fieldVals);
    Writable[] values = row.get();
    assertEquals(fieldVals[0], values[0]);
    assertEquals(fieldVals[1], values[1]);
    assertEquals(fieldVals[2], values[2]);
  }

  @Test
  void testConstructEngineRecordWithNoUpdates() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    ArrayWritable base = createBaseRecord(new Writable[]{
        new IntWritable(1),
        new Text("Alice"),
        new BooleanWritable(true)});
    BufferedRecord<ArrayWritable> buffered = new BufferedRecord<>("anyKey", 1, base, 1, null);

    Map<Integer, Object> updates = new HashMap<>();
    ArrayWritable result = avroReaderContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, buffered);
    Writable[] values = result.get();

    assertEquals(1, ((IntWritable) values[0]).get());
    assertEquals("Alice", values[1].toString());
    assertTrue(((BooleanWritable) values[2]).get());
  }

  @Test
  void testConstructEngineRecordWithUpdates() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    ArrayWritable base = createBaseRecord(new Writable[]{
        new IntWritable(1),
        new Text("Alice"),
        new BooleanWritable(true)});
    BufferedRecord<ArrayWritable> buffered = new BufferedRecord<>("anyKey", 1, base, 1, null);

    Map<Integer, Object> updates = new HashMap<>();
    updates.put(0, new IntWritable(2));
    updates.put(1, new Text("Bob"));
    ArrayWritable result = avroReaderContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, buffered);
    Writable[] values = result.get();

    assertEquals(2, ((IntWritable) values[0]).get());
    assertEquals("Bob", values[1].toString());
    assertTrue(((BooleanWritable) values[2]).get());
  }

  @Test
  void testConstructEngineRecordWithListValues() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema simpleSchema = HoodieSchema.createRecord("SimpleRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
        ));

    Object[] fieldVals = new Object[]{1, "test", true};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(simpleSchema, fieldVals);
    Writable[] values = result.get();

    assertEquals(1, ((IntWritable) values[0]).get());
    assertEquals("test", values[1].toString());
    assertTrue(((BooleanWritable) values[2]).get());
  }

  @Test
  void testConstructEngineRecordWithNullValues() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema nullableSchema = HoodieSchema.createRecord("NullableRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
        ));

    Object[] fieldVals = new Object[]{null, null, null};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(nullableSchema, fieldVals);
    Writable[] values = result.get();

    assertEquals(NullWritable.get(), values[0]);
    assertEquals(NullWritable.get(), values[1]);
    assertEquals(NullWritable.get(), values[2]);
  }

  @Test
  void testConstructEngineRecordWithMixedNullAndNonNullValues() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema mixedSchema = HoodieSchema.createRecord("MixedRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
        ));

    Object[] fieldVals = new Object[]{42, null, true};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(mixedSchema, fieldVals);
    Writable[] values = result.get();

    assertEquals(42, ((IntWritable) values[0]).get());
    assertEquals(NullWritable.get(), values[1]);
    assertTrue(((BooleanWritable) values[2]).get());
  }

  @Test
  void testConstructEngineRecordWithNumericTypes() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema numericSchema = HoodieSchema.createRecord("NumericRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("int_val", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("long_val", HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("float_val", HoodieSchema.create(HoodieSchemaType.FLOAT)),
            HoodieSchemaField.of("double_val", HoodieSchema.create(HoodieSchemaType.DOUBLE))
        ));

    Object[] fieldVals = new Object[]{42, 123456789L, 3.14f, 2.71828};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(numericSchema, fieldVals);
    Writable[] values = result.get();

    assertEquals(42, ((IntWritable) values[0]).get());
    assertEquals(123456789L, ((LongWritable) values[1]).get());
    assertEquals(3.14f, ((FloatWritable) values[2]).get(), 0.001f);
    assertEquals(2.71828, ((DoubleWritable) values[3]).get(), 0.000001);
  }

  @Test
  void testConstructEngineRecordWithBytesType() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema bytesSchema = HoodieSchema.createRecord("BytesRecord", null, null,
        Collections.singletonList(
            HoodieSchemaField.of("bytes_val", HoodieSchema.create(HoodieSchemaType.BYTES))
        ));

    byte[] testBytes = "Hello World".getBytes();
    Object[] fieldVals = new Object[]{testBytes};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(bytesSchema, fieldVals);
    Writable[] values = result.get();

    BytesWritable bytesWritable = (BytesWritable) values[0];
    byte[] resultBytes = new byte[bytesWritable.getLength()];
    System.arraycopy(bytesWritable.getBytes(), 0, resultBytes, 0, bytesWritable.getLength());
    assertEquals("Hello World", new String(resultBytes));
  }

  @Test
  void testConstructEngineRecordWithByteBuffer() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema bytesSchema = HoodieSchema.createRecord("BytesRecord", null, null,
        Collections.singletonList(
            HoodieSchemaField.of("bytes_val", HoodieSchema.create(HoodieSchemaType.BYTES))
        ));

    ByteBuffer testBuffer = ByteBuffer.wrap("Test Buffer".getBytes());
    Object[] fieldVals = new Object[]{testBuffer};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(bytesSchema, fieldVals);
    Writable[] values = result.get();

    BytesWritable bytesWritable = (BytesWritable) values[0];
    byte[] resultBytes = new byte[bytesWritable.getLength()];
    System.arraycopy(bytesWritable.getBytes(), 0, resultBytes, 0, bytesWritable.getLength());
    assertEquals("Test Buffer", new String(resultBytes));
  }

  @Test
  void testConstructEngineRecordWithArrayType() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema arraySchema = HoodieSchema.createRecord("ArrayRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("string_array", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING))),
            HoodieSchemaField.of("int_array", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT)))
        ));

    Object[] fieldVals = new Object[]{Arrays.asList("a", "b", "c"), Arrays.asList(1, 2, 3)};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(arraySchema, fieldVals);
    Writable[] values = result.get();

    ArrayWritable stringArray = (ArrayWritable) values[0];
    assertEquals("a", stringArray.get()[0].toString());
    assertEquals("b", stringArray.get()[1].toString());
    assertEquals("c", stringArray.get()[2].toString());

    ArrayWritable intArray = (ArrayWritable) values[1];
    assertEquals(1, ((IntWritable) intArray.get()[0]).get());
    assertEquals(2, ((IntWritable) intArray.get()[1]).get());
    assertEquals(3, ((IntWritable) intArray.get()[2]).get());
  }

  @Test
  void testConstructEngineRecordWithMapType() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema mapSchema = HoodieSchema.createRecord("MapRecord", null, null,
        Collections.singletonList(
            HoodieSchemaField.of("string_map", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING)))
        ));

    Map<String, String> testMap = new HashMap<>();
    testMap.put("key1", "value1");
    testMap.put("key2", "value2");

    Object[] fieldVals = new Object[]{testMap};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(mapSchema, fieldVals);
    Writable[] values = result.get();

    MapWritable mapWritable = (MapWritable) values[0];
    assertEquals("value1", mapWritable.get(new Text("key1")).toString());
    assertEquals("value2", mapWritable.get(new Text("key2")).toString());
  }

  @Test
  void testConstructEngineRecordWithRecordType() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema nestedSchema = HoodieSchema.createRecord("NestedRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT))
        ));

    HoodieSchema recordSchema = HoodieSchema.createRecord("RecordRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("nested", nestedSchema),
            HoodieSchemaField.of("description", HoodieSchema.create(HoodieSchemaType.STRING))
        ));

    org.apache.avro.generic.GenericRecord nestedRecord = new org.apache.avro.generic.GenericData.Record(nestedSchema.toAvroSchema());
    nestedRecord.put("name", "John");
    nestedRecord.put("age", 30);

    Object[] fieldVals = new Object[]{nestedRecord, "Test description"};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(recordSchema, fieldVals);
    Writable[] values = result.get();

    ArrayWritable nestedWritable = (ArrayWritable) values[0];
    assertEquals("John", nestedWritable.get()[0].toString());
    assertEquals(30, ((IntWritable) nestedWritable.get()[1]).get());
    assertEquals("Test description", values[1].toString());
  }

  @Test
  void testConstructEngineRecordWithUnionType() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    // Create a union schema (nullable string)
    HoodieSchema unionSchema = HoodieSchema.createRecord("UnionRecord", null, null,
        Collections.singletonList(
            HoodieSchemaField.of("union_field", HoodieSchema.create(HoodieSchemaType.STRING))
        ));

    // Test with non-null value
    Object[] fieldVals1 = new Object[]{"test_value"};
    ArrayWritable result1 = avroReaderContext.getRecordContext().constructEngineRecord(unionSchema, fieldVals1);
    Writable[] values1 = result1.get();
    assertEquals("test_value", values1[0].toString());

    // Test with null value
    Object[] fieldVals2 = new Object[]{null};
    ArrayWritable result2 = avroReaderContext.getRecordContext().constructEngineRecord(unionSchema, fieldVals2);
    Writable[] values2 = result2.get();
    assertEquals(NullWritable.get(), values2[0]);
  }

  @Test
  void testConstructEngineRecordWithMismatchedSchemaAndValues() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    HoodieSchema simpleSchema = HoodieSchema.createRecord("SimpleRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
        ));

    // Test with fewer values than schema fields
    Object[] fieldVals1 = new Object[]{1};
    IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class,
        () -> avroReaderContext.getRecordContext().constructEngineRecord(simpleSchema, fieldVals1));
    assertEquals("Mismatch between schema fields and values", e1.getMessage());

    // Test with more values than schema fields
    Object[] fieldVals2 = new Object[]{1, "test", "extra"};
    IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class,
        () -> avroReaderContext.getRecordContext().constructEngineRecord(simpleSchema, fieldVals2));
    assertEquals("Mismatch between schema fields and values", e2.getMessage());
  }

  @Test
  void testConstructEngineRecordWithEnumType() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    // Create a schema with ENUM type (converted from Avro enum to string in HoodieSchema)
    HoodieSchema enumSchema = HoodieSchema.createRecord("EnumRecord", null, null,
        Collections.singletonList(
            HoodieSchemaField.of("enum_field", HoodieSchema.create(HoodieSchemaType.STRING))
        ));

    Object[] fieldVals = new Object[]{"A"};
    ArrayWritable result = avroReaderContext.getRecordContext().constructEngineRecord(enumSchema, fieldVals);
    Writable[] values = result.get();
    // ENUM is converted to Text (string)
    assertEquals("A", values[0].toString());
  }

  private static HoodieSchema getBaseSchema() {
    HoodieSchema nestedSchema = HoodieSchema.createRecord("nested", null, null,
        Collections.singletonList(HoodieSchemaField.of("nested_field", HoodieSchema.create(HoodieSchemaType.STRING))));

    return HoodieSchema.createRecord("test", null, null,
        Arrays.asList(
            HoodieSchemaField.of("field_1", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("field_2", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("field_3", nestedSchema)
        ));
  }

  private ArrayWritable createBaseRecord(Writable[] values) {
    return new ArrayWritable(Writable.class, values);
  }
}
