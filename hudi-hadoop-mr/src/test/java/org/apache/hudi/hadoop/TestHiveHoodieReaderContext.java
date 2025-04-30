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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.ObjectInspectorCache;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHiveHoodieReaderContext {
  private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
  private final HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator = mock(HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator.class);
  private final StorageConfiguration<?> storageConfiguration = new HadoopStorageConfiguration(false);

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
}
