/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hadoop;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Writable;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link HoodieRealtimeRecordReaderUtils}.
 */
public class TestHoodieRealtimeRecorderReaderUtils {

  @Test
  public void testArrayWritableToAvro() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    // union schema
    Schema nullableStringSchema = Schema.createUnion(Arrays.asList(stringSchema, nullSchema));
    // enum schema
    ArrayList<String> enumList = new ArrayList<>();
    enumList.add("enum1");
    enumList.add("enum2");
    Schema enumSchema = Schema.createEnum("enum_col", "", null, enumList);
    // fixed schema
    Schema fixedColSchema = Schema.createFixed("fixed_col", "", null, 10);
    LogicalTypes.Decimal decimalType = LogicalTypes.decimal(10, 6);
    decimalType.addToSchema(fixedColSchema);

    // build a record schema contains all Schema Type
    Schema recordSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("string_col", stringSchema, "", null),
        new Schema.Field("bytes_col", bytesSchema, "", null),
        new Schema.Field("int_col", intSchema, "", null),
        new Schema.Field("long_col", longSchema, "", null),
        new Schema.Field("float_col", floatSchema, "", null),
        new Schema.Field("double_col", doubleSchema, "", null),
        new Schema.Field("boolean_col", booleanSchema, "", null),
        new Schema.Field("null_col", nullSchema, "", null),
        new Schema.Field("array_col", Schema.createArray(stringSchema), "", null),
        new Schema.Field("map_col", Schema.createMap(stringSchema), "", null),
        new Schema.Field("union_col", nullableStringSchema, "", null),
        new Schema.Field("enum_col", enumSchema, "", null),
        new Schema.Field("fixed_col", fixedColSchema, "", null)
    ));

    GenericRecord record1 = new GenericData.Record(recordSchema);
    record1.put("string_col", "string_value");
    record1.put("bytes_col", ByteBuffer.wrap("bytes_value".getBytes()));
    record1.put("int_col", 100);
    record1.put("long_col", 1000L);
    record1.put("float_col", 3.14f);
    record1.put("double_col", -3.14159265357);
    record1.put("boolean_col", true);
    record1.put("null_col", null);
    List<String> list = new ArrayList<>();
    list.add("array_element1");
    list.add("array_element2");
    GenericData.Array genericArray = new GenericData.Array(Schema.createArray(Schema.create(Schema.Type.STRING)), list);
    record1.put("array_col", genericArray);
    Map<String, String> mapValues = new HashMap<>();
    mapValues.put("key1", "value1");
    record1.put("map_col", mapValues);
    record1.put("union_col", "hello");
    record1.put("enum_col", "enum1");
    BigDecimal bigDecimal = new BigDecimal("1.234567");
    Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
    GenericFixed genericFixed = decimalConversions.toFixed(bigDecimal, fixedColSchema, LogicalTypes.decimal(10, 6));
    record1.put("fixed_col", genericFixed);

    Writable writable = HoodieRealtimeRecordReaderUtils.avroToArrayWritable(record1, recordSchema);
    GenericRecord record2 = (GenericRecord) HoodieRealtimeRecordReaderUtils.arrayWritableToAvro(writable, recordSchema);
    assertEquals(record1, record2);
  }
}

