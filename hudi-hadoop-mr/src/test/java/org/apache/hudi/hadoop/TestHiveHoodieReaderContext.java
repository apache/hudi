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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    // Test with primitive types
    Schema simpleSchema = SchemaBuilder.record("SimpleRecord").fields()
        .requiredInt("id")
        .requiredString("name")
        .requiredBoolean("active")
        .endRecord();

    ArrayWritable result = avroReaderContext.createEngineRecord(simpleSchema,
        Arrays.asList(1, "test", true));
    Writable[] values = result.get();

    assertEquals(1, ((IntWritable) values[0]).get());
    assertEquals("test", values[1].toString());
    assertTrue(((BooleanWritable) values[2]).get());
  }

  @Test
  void testConstructEngineRecordWithNullValues() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema nullableSchema = SchemaBuilder.record("NullableRecord").fields()
        .optionalInt("id")
        .optionalString("name")
        .optionalBoolean("active")
        .endRecord();

    ArrayWritable result = avroReaderContext.createEngineRecord(nullableSchema,
        Arrays.asList(null, null, null));
    Writable[] values = result.get();

    assertEquals(org.apache.hadoop.io.NullWritable.get(), values[0]);
    assertEquals(org.apache.hadoop.io.NullWritable.get(), values[1]);
    assertEquals(org.apache.hadoop.io.NullWritable.get(), values[2]);
  }

  @Test
  void testConstructEngineRecordWithMixedNullAndNonNullValues() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema mixedSchema = SchemaBuilder.record("MixedRecord").fields()
        .optionalInt("id")
        .optionalString("name")
        .optionalBoolean("active")
        .endRecord();

    ArrayWritable result = avroReaderContext.createEngineRecord(mixedSchema,
        Arrays.asList(42, null, true));
    Writable[] values = result.get();

    assertEquals(42, ((IntWritable) values[0]).get());
    assertEquals(org.apache.hadoop.io.NullWritable.get(), values[1]);
    assertTrue(((BooleanWritable) values[2]).get());
  }

  @Test
  void testConstructEngineRecordWithNumericTypes() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema numericSchema = SchemaBuilder.record("NumericRecord").fields()
        .requiredInt("int_val")
        .requiredLong("long_val")
        .requiredFloat("float_val")
        .requiredDouble("double_val")
        .endRecord();

    ArrayWritable result = avroReaderContext.createEngineRecord(numericSchema,
        Arrays.asList(42, 123456789L, 3.14f, 2.71828));
    Writable[] values = result.get();

    assertEquals(42, ((IntWritable) values[0]).get());
    assertEquals(123456789L, ((org.apache.hadoop.io.LongWritable) values[1]).get());
    assertEquals(3.14f, ((org.apache.hadoop.io.FloatWritable) values[2]).get(), 0.001f);
    assertEquals(2.71828, ((org.apache.hadoop.io.DoubleWritable) values[3]).get(), 0.000001);
  }

  @Test
  void testConstructEngineRecordWithBytesType() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema bytesSchema = SchemaBuilder.record("BytesRecord").fields()
        .requiredBytes("bytes_val")
        .endRecord();

    byte[] testBytes = "Hello World".getBytes();
    ArrayWritable result = avroReaderContext.createEngineRecord(bytesSchema,
        Arrays.asList(testBytes));
    Writable[] values = result.get();

    org.apache.hadoop.io.BytesWritable bytesWritable = (org.apache.hadoop.io.BytesWritable) values[0];
    byte[] resultBytes = new byte[bytesWritable.getLength()];
    System.arraycopy(bytesWritable.getBytes(), 0, resultBytes, 0, bytesWritable.getLength());
    assertEquals("Hello World", new String(resultBytes));
  }

  @Test
  void testConstructEngineRecordWithByteBuffer() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema bytesSchema = SchemaBuilder.record("BytesRecord").fields()
        .requiredBytes("bytes_val")
        .endRecord();

    java.nio.ByteBuffer testBuffer = java.nio.ByteBuffer.wrap("Test Buffer".getBytes());
    ArrayWritable result = avroReaderContext.createEngineRecord(bytesSchema,
        Arrays.asList(testBuffer));
    Writable[] values = result.get();

    org.apache.hadoop.io.BytesWritable bytesWritable = (org.apache.hadoop.io.BytesWritable) values[0];
    byte[] resultBytes = new byte[bytesWritable.getLength()];
    System.arraycopy(bytesWritable.getBytes(), 0, resultBytes, 0, bytesWritable.getLength());
    assertEquals("Test Buffer", new String(resultBytes));
  }

  @Test
  void testConstructEngineRecordWithArrayType() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema arraySchema = SchemaBuilder.record("ArrayRecord").fields()
        .name("string_array").type().array().items().stringType().noDefault()
        .name("int_array").type().array().items().intType().noDefault()
        .endRecord();

    ArrayWritable result = avroReaderContext.createEngineRecord(arraySchema,
        Arrays.asList(Arrays.asList("a", "b", "c"), Arrays.asList(1, 2, 3)));
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
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema mapSchema = SchemaBuilder.record("MapRecord").fields()
        .name("string_map").type().map().values().stringType().noDefault()
        .endRecord();

    java.util.Map<String, String> testMap = new java.util.HashMap<>();
    testMap.put("key1", "value1");
    testMap.put("key2", "value2");

    ArrayWritable result = avroReaderContext.createEngineRecord(mapSchema,
        Arrays.asList(testMap));
    Writable[] values = result.get();

    org.apache.hadoop.io.MapWritable mapWritable = (org.apache.hadoop.io.MapWritable) values[0];
    assertEquals("value1", mapWritable.get(new Text("key1")).toString());
    assertEquals("value2", mapWritable.get(new Text("key2")).toString());
  }

  @Test
  void testConstructEngineRecordWithRecordType() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema nestedSchema = SchemaBuilder.record("NestedRecord").fields()
        .requiredString("name")
        .requiredInt("age")
        .endRecord();

    Schema recordSchema = SchemaBuilder.record("RecordRecord").fields()
        .name("nested").type(nestedSchema).noDefault()
        .requiredString("description")
        .endRecord();

    org.apache.avro.generic.GenericRecord nestedRecord = new org.apache.avro.generic.GenericData.Record(nestedSchema);
    nestedRecord.put("name", "John");
    nestedRecord.put("age", 30);

    ArrayWritable result = avroReaderContext.createEngineRecord(recordSchema,
        Arrays.asList(nestedRecord, "Test description"));
    Writable[] values = result.get();

    ArrayWritable nestedWritable = (ArrayWritable) values[0];
    assertEquals("John", nestedWritable.get()[0].toString());
    assertEquals(30, ((IntWritable) nestedWritable.get()[1]).get());
    assertEquals("Test description", values[1].toString());
  }

  @Test
  void testConstructEngineRecordWithUnionType() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema unionSchema = SchemaBuilder.record("UnionRecord").fields()
        .name("union_field").type().unionOf().nullType().and().stringType().endUnion().noDefault()
        .endRecord();

    // Test with non-null value
    ArrayWritable result1 = avroReaderContext.createEngineRecord(unionSchema,
        Arrays.asList("test_value"));
    Writable[] values1 = result1.get();
    assertEquals("test_value", values1[0].toString());

    // Test with null value
    ArrayWritable result2 = avroReaderContext.createEngineRecord(unionSchema,
        Arrays.asList((Object) null));
    Writable[] values2 = result2.get();
    assertEquals(org.apache.hadoop.io.NullWritable.get(), values2[0]);
  }

  @Test
  void testConstructEngineRecordWithMismatchedSchemaAndValues() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    Schema simpleSchema = SchemaBuilder.record("SimpleRecord").fields()
        .requiredInt("id")
        .requiredString("name")
        .endRecord();

    // Test with fewer values than schema fields
    try {
      avroReaderContext.createEngineRecord(simpleSchema, Arrays.asList(1));
      org.junit.jupiter.api.Assertions.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Mismatch between schema fields and values", e.getMessage());
    }

    // Test with more values than schema fields
    try {
      avroReaderContext.createEngineRecord(simpleSchema, Arrays.asList(1, "test", "extra"));
      org.junit.jupiter.api.Assertions.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Mismatch between schema fields and values", e.getMessage());
    }
  }

  @Test
  void testConstructEngineRecordWithUnsupportedType() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    // Create a schema with an unsupported type (like ENUM without proper setup)
    Schema unsupportedSchema = SchemaBuilder.record("UnsupportedRecord").fields()
        .name("enum_field").type().enumeration("TestEnum").symbols("A", "B", "C").noDefault()
        .endRecord();

    try {
      avroReaderContext.createEngineRecord(unsupportedSchema, Arrays.asList("A"));
      // This should work as ENUM is supported and converted to Text
    } catch (Exception e) {
      org.junit.jupiter.api.Assertions.fail("ENUM type should be supported and converted to Text");
    }
  }

  private JobConf getJobConf() {
    JobConf jobConf = new JobConf(storageConfiguration.unwrapAs(Configuration.class));
    jobConf.set("columns", "field_1,field_2,field_3,datestr");
    jobConf.set("columns.types", "string,string,struct<nested_field:string>,string");
    return jobConf;
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
