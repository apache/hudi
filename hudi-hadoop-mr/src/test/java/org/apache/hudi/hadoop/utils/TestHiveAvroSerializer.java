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
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  private static final String NESTED_SCHEMA = "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":\"string\"},"
      + "{\"name\":\"lastname\",\"type\":\"string\"},"
      + "{\"name\":\"scores\",\"type\": {\"type\": \"array\", \"items\": [\"null\", \"int\"]}},"
      + "{\"name\":\"student\",\"type\":{\"name\":\"student\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":[\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\":\"lastname\",\"type\":[\"null\" ,\"string\"],\"default\": null}]}},"
      + "{\"name\":\"teachers\",\"type\":{\"type\":\"array\",\"items\":{\"name\":\"teachers\",\"type\":\"record\","
      + "\"fields\":[{\"name\":\"firstname\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"lastname\",\"type\":[\"null\",\"string\"],\"default\":null}]}}}"
      + "]}";

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
    avroRecord.put("col7", 1640491505111222L);
    avroRecord.put("col8", false);
    ByteBuffer bb = ByteBuffer.wrap(new byte[]{97, 48, 53});
    avroRecord.put("col9", bb);
    assertTrue(GenericData.get().validate(avroSchema, avroRecord));
    ArrayWritable writable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(avroRecord, avroSchema, true);

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
    Schema nestedSchema = new Schema.Parser().parse(NESTED_SCHEMA);
    GenericRecord avroRecord = new GenericData.Record(nestedSchema);
    avroRecord.put("firstname", "person1");
    avroRecord.put("lastname", "person2");
    GenericArray scores = new GenericData.Array<>(avroRecord.getSchema().getField("scores").schema(), Arrays.asList(1,2));
    avroRecord.put("scores", scores);
    GenericRecord studentRecord = new GenericData.Record(avroRecord.getSchema().getField("student").schema());
    studentRecord.put("firstname", "person1");
    studentRecord.put("lastname", "person2");
    avroRecord.put("student", studentRecord);

    GenericArray teachers = new GenericData.Array<>(avroRecord.getSchema().getField("teachers").schema(), Arrays.asList(studentRecord));
    avroRecord.put("teachers", teachers);

    assertTrue(GenericData.get().validate(nestedSchema, avroRecord));
    ArrayWritable writable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(avroRecord, nestedSchema, true);

    List<TypeInfo> columnTypeList = createHiveTypeInfoFrom("string,string,array<int>,struct<firstname:string,lastname:string>,array<struct<firstname:string,lastname:string>>");
    List<String> columnNameList = createHiveColumnsFrom("firstname,lastname,arrayRecord,student,teachers");
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

  private static final String SCHEMA_WITH_NESTED_RECORD = "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"TestRecord\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"id\", \"type\": \"int\"},\n"
          + "    {\"name\": \"name\", \"type\": \"string\"},\n"
          + "    {\"name\": \"address\", \"type\": {\n"
          + "      \"type\": \"record\",\n"
          + "      \"name\": \"Address\",\n"
          + "      \"fields\": [\n"
          + "        {\"name\": \"city\", \"type\": \"string\"},\n"
          + "        {\"name\": \"zip\", \"type\": \"int\"}\n"
          + "      ]\n"
          + "    }}\n"
          + "  ]\n"
          + "}";

  private static final String SCHEMA_WITH_ARRAY_AND_MAP = "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"ComplexRecord\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"id\", \"type\": \"int\"},\n"
          + "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
          + "    {\"name\": \"properties\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n"
          + "  ]\n"
          + "}";

  @Test
  public void testGetTopLevelFields() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_RECORD);
    HiveAvroSerializer serializer = new HiveAvroSerializer(schema);

    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new IntWritable(101),
        new Text("John Doe"),
        new ArrayWritable(Writable.class, new Writable[]{
            new Text("New York"),
            new IntWritable(10001)
        })
    });

    assertEquals(new IntWritable(101), serializer.getValue(record, "id"));
    assertEquals(new Text("John Doe"), serializer.getValue(record, "name"));
  }

  @Test
  public void testGetNestedFields() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_RECORD);
    HiveAvroSerializer serializer = new HiveAvroSerializer(schema);

    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new IntWritable(202),
        new Text("Alice"),
        new ArrayWritable(Writable.class, new Writable[]{
            new Text("San Francisco"),
            new IntWritable(94107)
        })
    });

    assertEquals(new Text("San Francisco"), serializer.getValue(record, "address.city"));
    assertEquals(new IntWritable(94107), serializer.getValue(record, "address.zip"));
  }

  @Test
  public void testInvalidFieldNameThrows() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_RECORD);
    HiveAvroSerializer serializer = new HiveAvroSerializer(schema);

    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new IntWritable(303),
        new Text("Bob"),
        new ArrayWritable(Writable.class, new Writable[]{
            new Text("Los Angeles"),
            new IntWritable(90001)
        })
    });

    assertThrows(HoodieException.class, () -> {
      serializer.getValue(record, "nonexistent");
    });

    assertThrows(HoodieException.class, () -> {
      serializer.getValue(record, "address.nonexistent");
    });
  }

  @Test
  public void testGetValueFromArrayOrMap() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_ARRAY_AND_MAP);
    HiveAvroSerializer serializer = new HiveAvroSerializer(schema);

    ArrayWritable tagsArray = new ArrayWritable(Text.class, new Text[]{
        new Text("a"), new Text("b")
    });

    ArrayWritable propertiesMap = new ArrayWritable(Writable.class, new Writable[]{
        new ArrayWritable(Writable.class, new Writable[]{new Text("key1"), new Text("val1")}),
        new ArrayWritable(Writable.class, new Writable[]{new Text("key2"), new Text("val2")})
    });

    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new IntWritable(1), tagsArray, propertiesMap
    });

    // Access the entire field is ok
    Object tagsResult = serializer.getValue(record, "tags");
    assertInstanceOf(ArrayWritable.class, tagsResult);
    assertEquals(tagsArray, tagsResult);

    Object propertiesResult = serializer.getValue(record, "properties");
    assertInstanceOf(ArrayWritable.class, propertiesResult);
    assertEquals(propertiesMap, propertiesResult);

    // access element or key/value is not ok
    assertThrows(HoodieException.class, () -> {
      serializer.getValue(record, "tags.element");
    });

    assertThrows(HoodieException.class, () -> {
      serializer.getValue(record, "properties.key");
    });

    assertThrows(HoodieException.class, () -> {
      serializer.getValue(record, "properties.value");
    });
  }

  @Test
  public void testGetJavaTopLevelFields() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_RECORD);
    HiveAvroSerializer serializer = new HiveAvroSerializer(schema);

    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new IntWritable(101),
        new Text("John Doe"),
        new ArrayWritable(Writable.class, new Writable[]{
            new Text("New York"),
            new IntWritable(10001)
        })
    });

    assertEquals(101, serializer.getValueAsJava(record, "id"));
    assertEquals(new Utf8("John Doe"), serializer.getValueAsJava(record, "name"));
  }

  @Test
  public void testGetJavaNestedFields() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_RECORD);
    HiveAvroSerializer serializer = new HiveAvroSerializer(schema);

    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new IntWritable(202),
        new Text("Alice"),
        new ArrayWritable(Writable.class, new Writable[]{
            new Text("San Francisco"),
            new IntWritable(94107)
        })
    });

    assertEquals(new Utf8("San Francisco"), serializer.getValueAsJava(record, "address.city"));
    assertEquals(94107, serializer.getValueAsJava(record, "address.zip"));
  }

  @Test
  public void testGetJavaArrayAndMap() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_ARRAY_AND_MAP);
    HiveAvroSerializer serializer = new HiveAvroSerializer(schema);

    ArrayWritable tagsArray = new ArrayWritable(Text.class, new Text[]{
        new Text("a"), new Text("b")
    });

    ArrayWritable propertiesMap = new ArrayWritable(Writable.class, new Writable[]{
        new ArrayWritable(Writable.class, new Writable[]{new Text("key1"), new Text("val1")}),
        new ArrayWritable(Writable.class, new Writable[]{new Text("key2"), new Text("val2")})
    });

    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new IntWritable(1), tagsArray, propertiesMap
    });

    Object tags = serializer.getValueAsJava(record, "tags");
    assertInstanceOf(Collection.class, tags);

    Collection<?> tagList = (Collection<?>) tags;
    List<Utf8> expectedValues = Arrays.asList(new Utf8("a"), new Utf8("b"));

    Iterator<?> actualIter = tagList.iterator();
    Iterator<Utf8> expectedIter = expectedValues.iterator();

    while (expectedIter.hasNext() && actualIter.hasNext()) {
      Object actual = actualIter.next();
      Utf8 expected = expectedIter.next();
      assertEquals(expected,  actual);
    }

    assertFalse(actualIter.hasNext(), "Actual has more elements than expected");
    assertFalse(expectedIter.hasNext(), "Expected has more elements than actual");


    Object props = serializer.getValueAsJava(record, "properties");
    assertInstanceOf(Map.class, props);

    Map<Utf8, Utf8> resultMap = new HashMap<>();
    resultMap.put(new Utf8("key1"), new Utf8("val1"));
    resultMap.put(new Utf8("key2"), new Utf8("val2"));

    assertEquals(resultMap, props);
  }

  @Test
  public void testGetJavaInvalidFieldAccess() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_ARRAY_AND_MAP);
    HiveAvroSerializer serializer = new HiveAvroSerializer(schema);

    ArrayWritable tagsArray = new ArrayWritable(Text.class, new Text[]{
        new Text("a"), new Text("b")
    });

    ArrayWritable propertiesMap = new ArrayWritable(Writable.class, new Writable[]{
        new ArrayWritable(Writable.class, new Writable[]{new Text("key1"), new Text("val1")}),
        new ArrayWritable(Writable.class, new Writable[]{new Text("key2"), new Text("val2")})
    });

    ArrayWritable record = new ArrayWritable(Writable.class, new Writable[]{
        new IntWritable(1), tagsArray, propertiesMap
    });

    assertThrows(HoodieException.class, () -> {
      serializer.getValueAsJava(record, "tags.element");
    });

    assertThrows(HoodieException.class, () -> {
      serializer.getValueAsJava(record, "properties.key");
    });

    assertThrows(HoodieException.class, () -> {
      serializer.getValueAsJava(record, "properties.value");
    });
  }
}
