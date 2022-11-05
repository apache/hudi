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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.avro.HoodieAvroUtils;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveAvroSerializer {

  private static final String SIMPLE_SCHEMA = "{\"type\":\"record\",\"name\":\"h0_record\",\"namespace\":\"hoodie.h0\",\"fields\""
      + ":[{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"col1\",\"type\":[\"null\",\"long\"],\"default\":null},"
      + "{\"name\":\"col2\",\"type\":[\"null\",\"float\"],\"default\":null},"
      + "{\"name\":\"col3\",\"type\":[\"null\",\"double\"],\"default\":null},"
      + "{\"name\":\"col4\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"hoodie.h0.h0_record.col4\","
      + "\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":4}],\"default\":null},"
      + "{\"name\":\"col5\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"col6\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
      + "{\"name\":\"col7\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}],\"default\":null},"
      + "{\"name\":\"col8\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
      + "{\"name\":\"col9\",\"type\":[\"null\",\"bytes\"],\"default\":null},"
      + "{\"name\":\"par\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null}]}";
  private static final String NESTED_CHEMA = "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":\"string\"},"
      + "{\"name\":\"lastname\",\"type\":\"string\"},"
      + "{\"name\":\"student\",\"type\":{\"name\":\"student\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":[\"null\" ,\"string\"],\"default\": null},{\"name\":\"lastname\",\"type\":[\"null\" ,\"string\"],\"default\": null}]}}]}";

  @Test
  public void testSerialize() {
    Schema avroSchema = new Schema.Parser().parse(SIMPLE_SCHEMA);
    // create a test record with avroSchema
    GenericData.Record avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("id", 1);
    avroRecord.put("col1", 1000L);
    avroRecord.put("col2", -5.001f);
    avroRecord.put("col3", 12.999d);
    Schema currentDecimalType = avroSchema.getField("col4").schema().getTypes().get(1);
    BigDecimal bd = new BigDecimal("123.456").setScale(((LogicalTypes.Decimal) currentDecimalType.getLogicalType()).getScale());
    avroRecord.put("col4", HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd, currentDecimalType, currentDecimalType.getLogicalType()));
    avroRecord.put("col5", "2011-01-01");
    avroRecord.put("col6", 18987);
    avroRecord.put("col7", 1640491505000000L);
    avroRecord.put("col8", false);
    ByteBuffer bb = ByteBuffer.wrap(new byte[]{97, 48, 53});
    avroRecord.put("col9", bb);
    assertTrue(GenericData.get().validate(avroSchema, avroRecord));
    ArrayWritable writable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(avroRecord, avroSchema);

    List<Writable> writableList = Arrays.stream(writable.get()).collect(Collectors.toList());
    writableList.remove(writableList.size() - 1);
    ArrayWritable clipWritable = new ArrayWritable(writable.getValueClass(), writableList.toArray(new Writable[0]));

    List<TypeInfo> columnTypeList = createHiveTypeInfoFrom("int,bigint,float,double,decimal(10,4),string,date,timestamp,boolean,binary,date");
    List<String> columnNameList = createHiveColumnsFrom("id,col1,col2,col3,col4,col5,col6,col7,col8,col9,par");
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);
    GenericRecord testRecord = new HiveAvroSerializer(new ArrayWritableObjectInspector(rowTypeInfo), columnNameList, columnTypeList).serialize(writable, avroSchema);
    assertTrue(GenericData.get().validate(avroSchema, testRecord));
    // test
    List<TypeInfo> columnTypeListClip = createHiveTypeInfoFrom("int,bigint,float,double,decimal(10,4),string,date,timestamp,boolean,binary");
    List<String> columnNameListClip = createHiveColumnsFrom("id,col1,col2,col3,col4,col5,col6,col7,col8,col9");
    StructTypeInfo rowTypeInfoClip = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameListClip, columnTypeListClip);
    GenericRecord testRecordClip = new HiveAvroSerializer(new ArrayWritableObjectInspector(rowTypeInfoClip), columnNameListClip, columnTypeListClip).serialize(clipWritable, avroSchema);
    assertTrue(GenericData.get().validate(avroSchema, testRecordClip));

  }

  @Test
  public void testNestedValueSerialize() {
    Schema nestedSchema = new Schema.Parser().parse(NESTED_CHEMA);
    GenericRecord avroRecord = new GenericData.Record(nestedSchema);
    avroRecord.put("firstname", "person1");
    avroRecord.put("lastname", "person2");
    GenericRecord studentRecord = new GenericData.Record(avroRecord.getSchema().getField("student").schema());
    studentRecord.put("firstname", "person1");
    studentRecord.put("lastname", "person2");
    avroRecord.put("student", studentRecord);

    assertTrue(GenericData.get().validate(nestedSchema, avroRecord));
    ArrayWritable writable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(avroRecord, nestedSchema);

    List<TypeInfo> columnTypeList = createHiveTypeInfoFrom("string,string,struct<firstname:string,lastname:string>");
    List<String> columnNameList = createHiveColumnsFrom("firstname,lastname,student");
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);
    GenericRecord testRecord = new HiveAvroSerializer(new ArrayWritableObjectInspector(rowTypeInfo), columnNameList, columnTypeList).serialize(writable, nestedSchema);
    assertTrue(GenericData.get().validate(nestedSchema, testRecord));
  }

  private List<String> createHiveColumnsFrom(final String columnNamesStr) {
    List<String> columnNames;
    if (columnNamesStr.length() == 0) {
      columnNames = new ArrayList<>();
    } else {
      columnNames = Arrays.asList(columnNamesStr.split(","));
    }

    return columnNames;
  }

  private List<TypeInfo> createHiveTypeInfoFrom(final String columnsTypeStr) {
    List<TypeInfo> columnTypes;

    if (columnsTypeStr.length() == 0) {
      columnTypes = new ArrayList<>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnsTypeStr);
    }

    return columnTypes;
  }
}
