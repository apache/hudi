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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.ObjectInspectorCache;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
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
  private static final Schema SCHEMA = SchemaBuilder.record("TestRecord").fields()
      .requiredInt("id")
      .requiredString("name")
      .requiredBoolean("active")
      .endRecord();

  @Test
  void getRecordKeyWithSingleKey() {
    JobConf jobConf = getJobConf();

    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);

    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"field_1"}));
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("value1", avroReaderContext.getRecordKey(row, getBaseSchema()));
  }

  @Test
  void getRecordKeyWithMultipleKeys() {
    JobConf jobConf = getJobConf();

    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);

    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"field_1", "field_3.nested_field"}));
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("field_1:value1,field_3.nested_field:value3", avroReaderContext.getRecordKey(row, getBaseSchema()));
  }

  @Test
  void getNestedField() {
    JobConf jobConf = getJobConf();

    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);

    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("value3", avroReaderContext.getValue(row, getBaseSchema(), "field_3.nested_field").toString());
  }

  @Test
  void testConstructEngineRecordWithNoUpdates() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    ArrayWritable base = createBaseRecord(new Writable[]{
        new IntWritable(1),
        new Text("Alice"),
        new BooleanWritable(true)});
    BufferedRecord<ArrayWritable> buffered = new BufferedRecord<>("anyKey", 1, base, 1, false);

    Map<Integer, Object> updates = new HashMap<>();
    ArrayWritable result = avroReaderContext.constructEngineRecord(SCHEMA, updates, buffered);
    Writable[] values = result.get();

    assertEquals(1, ((IntWritable) values[0]).get());
    assertEquals("Alice", values[1].toString());
    assertTrue(((BooleanWritable) values[2]).get());
  }

  @Test
  void testConstructEngineRecordWithUpdates() {
    JobConf jobConf = getJobConf();
    Schema schema = getBaseSchema();
    ObjectInspectorCache objectInspectorCache = new ObjectInspectorCache(schema, jobConf);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), objectInspectorCache, storageConfiguration, tableConfig);

    ArrayWritable base = createBaseRecord(new Writable[]{
        new IntWritable(1),
        new Text("Alice"),
        new BooleanWritable(true)});
    BufferedRecord<ArrayWritable> buffered = new BufferedRecord<>("anyKey", 1, base, 1, false);

    Map<Integer, Object> updates = new HashMap<>();
    updates.put(0, new IntWritable(2));
    updates.put(1, new Text("Bob"));
    ArrayWritable result = avroReaderContext.constructEngineRecord(SCHEMA, updates, buffered);
    Writable[] values = result.get();

    assertEquals(2, ((IntWritable) values[0]).get());
    assertEquals("Bob", values[1].toString());
    assertTrue(((BooleanWritable) values[2]).get());
  }

  private JobConf getJobConf() {
    JobConf jobConf = new JobConf(storageConfiguration.unwrapAs(Configuration.class));
    jobConf.set("columns", "field_1,field_2,field_3,datestr");
    jobConf.set("columns.types", "string,string,struct<nested_field:string>,string");
    return jobConf;
  }

  private static Schema getBaseSchema() {
    Schema baseDataSchema = Schema.createRecord("test", null, null, false);
    Schema.Field baseField1 = new Schema.Field("field_1", Schema.create(Schema.Type.STRING));
    Schema.Field baseField2 = new Schema.Field("field_2", Schema.create(Schema.Type.STRING));
    Schema.Field baseField3 = new Schema.Field("field_3", Schema.createRecord("nested", null, null, false, Collections.singletonList(new Schema.Field("nested_field", Schema.create(
        Schema.Type.STRING)))));
    baseDataSchema.setFields(Arrays.asList(baseField1, baseField2, baseField3));
    return baseDataSchema;
  }

  private ArrayWritable createBaseRecord(Writable[] values) {
    return new ArrayWritable(Writable.class, values);
  }
}
