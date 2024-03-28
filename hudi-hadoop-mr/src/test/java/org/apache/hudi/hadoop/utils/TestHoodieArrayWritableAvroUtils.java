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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieArrayWritableAvroUtils {

  HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
  Schema tableSchema = HoodieTestDataGenerator.AVRO_SCHEMA;
  ObjectInspectorCache objectInspectorCache;

  @BeforeEach
  public void setup() {
    List<Schema.Field> fields = tableSchema.getFields();
    Configuration conf = HoodieTestUtils.getDefaultHadoopConf();
    JobConf jobConf = new JobConf(conf);
    jobConf.set(serdeConstants.LIST_COLUMNS, fields.stream().map(Schema.Field::name).collect(Collectors.joining(",")));
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES);
    objectInspectorCache = new ObjectInspectorCache(HoodieTestDataGenerator.AVRO_SCHEMA, jobConf);
  }

  @Test
  public void testProjection() {
    Schema from =  tableSchema;
    Schema to = HoodieAvroUtils.generateProjectionSchema(from, Arrays.asList("trip_type", "current_ts", "weight"));
    UnaryOperator<ArrayWritable> projection = HoodieArrayWritableAvroUtils.projectRecord(from, to);
    UnaryOperator<ArrayWritable> reverseProjection = HoodieArrayWritableAvroUtils.reverseProject(to, from);

    //We reuse the ArrayWritable, so we need to get the values before projecting
    ArrayWritable record = convertArrayWritable(dataGen.generateGenericRecord());
    Object tripType = objectInspectorCache.getValue(record, from, "trip_type");
    Object currentTs = objectInspectorCache.getValue(record, from, "current_ts");
    Object weight = objectInspectorCache.getValue(record, from, "weight");

    //Make sure the projected fields can be read
    ArrayWritable projectedRecord = projection.apply(record);
    assertEquals(tripType, objectInspectorCache.getValue(projectedRecord, to, "trip_type"));
    assertEquals(currentTs, objectInspectorCache.getValue(projectedRecord, to, "current_ts"));
    assertEquals(weight, objectInspectorCache.getValue(projectedRecord, to, "weight"));

    //Reverse projection, the fields are in the original spots, but only the fields we set can be read.
    //Therefore, we can only check the 3 fields that were in the projection
    ArrayWritable reverseProjected = reverseProjection.apply(projectedRecord);
    assertEquals(tripType, objectInspectorCache.getValue(reverseProjected, from, "trip_type"));
    assertEquals(currentTs, objectInspectorCache.getValue(reverseProjected, from, "current_ts"));
    assertEquals(weight, objectInspectorCache.getValue(reverseProjected, from, "weight"));
  }

  private static ArrayWritable convertArrayWritable(GenericRecord record) {
    return  (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(record, record.getSchema(), false);
  }
}
