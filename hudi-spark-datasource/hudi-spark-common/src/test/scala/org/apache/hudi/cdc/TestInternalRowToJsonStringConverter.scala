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

package org.apache.hudi.cdc

import org.apache.hudi.HoodieTableSchema
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType, Metadata, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestInternalRowToJsonStringConverter {
  private val converter = new InternalRowToJsonStringConverter(hoodieTableSchema.structTypeSchema)

  @Test
  def emptyRow(): Unit = {
    val converter = new InternalRowToJsonStringConverter(emptyHoodieTableSchema.structTypeSchema)
    val row = InternalRow.empty
    val converted = converter.convert(row)
    assertEquals("{}", converted.toString)
  }

  @Test
  def nonEmptyRow(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, UTF8String.fromString("foo")))
    val converted = converter.convert(row)
    assertEquals("""{"uuid":1,"name":"foo"}""", converted.toString)
  }

  @Test
  def emptyString(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, UTF8String.EMPTY_UTF8))
    val converted = converter.convert(row)
    assertEquals("""{"uuid":1,"name":""}""", converted.toString)
  }

  @Test
  def nullString(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, null))
    val converted = converter.convert(row)
    assertTrue(converted.toString.equals("""{"uuid":1}""") || converted.toString.equals("""{"name":null,"uuid":1}"""))
  }

  @Test
  def arrayDataTypes(): Unit = {
    val row = InternalRow.fromSeq(Seq(
      1,
      UTF8String.fromString("test_array"),
      ArrayData.toArrayData(Array(1, 2, 3))
    ))
    val converted = arrayConverter.convert(row)
    assertEquals("""{"id":1,"name":"test_array","numbers":[1,2,3]}""", converted.toString)
  }

  @Test
  def mapDataTypes(): Unit = {
    val keys = Array(UTF8String.fromString("key1"), UTF8String.fromString("key2"))
    val values = Array(UTF8String.fromString("value1"), UTF8String.fromString("value2"))
    val mapData = ArrayBasedMapData.apply(keys, values)

    val row = InternalRow.fromSeq(Seq(
      2,
      UTF8String.fromString("test_map"),
      mapData
    ))
    val converted = mapConverter.convert(row)
    assertEquals("""{"id":2,"name":"test_map","properties":{"key1":"value1","key2":"value2"}}""", converted.toString)
  }

  @Test
  def structDataTypes(): Unit = {
    val nestedRow = InternalRow.fromSeq(Seq(UTF8String.fromString("nested_name"), 25))
    val row = InternalRow.fromSeq(Seq(
      3,
      UTF8String.fromString("test_struct"),
      nestedRow
    ))
    val converted = structConverter.convert(row)
    assertEquals("""{"id":3,"name":"test_struct","person":{"name":"nested_name","age":25}}""", converted.toString)
  }

  @Test
  def nestedComplexTypes(): Unit = {
    // Array of structs
    val struct1 = InternalRow.fromSeq(Seq(UTF8String.fromString("Alice"), 30))
    val struct2 = InternalRow.fromSeq(Seq(UTF8String.fromString("Bob"), 25))
    val arrayOfStructs = ArrayData.toArrayData(Array(struct1, struct2))

    val row = InternalRow.fromSeq(Seq(
      4,
      UTF8String.fromString("test_nested"),
      arrayOfStructs
    ))
    val converted = nestedConverter.convert(row)
    assertEquals("""{"id":4,"name":"test_nested","people":[{"name":"Alice","age":30},{"name":"Bob","age":25}]}""", converted.toString)
  }

  @Test
  def arrayWithNulls(): Unit = {
    val row = InternalRow.fromSeq(Seq(
      5,
      UTF8String.fromString("test_nulls"),
      ArrayData.toArrayData(Array[Any](1, null, 3))
    ))
    val converted = arrayWithNullsConverter.convert(row)
    assertEquals("""{"id":5,"name":"test_nulls","numbers":[1,null,3]}""", converted.toString)
  }

  @Test
  def mapWithNulls(): Unit = {
    val keys = Array(UTF8String.fromString("key1"), UTF8String.fromString("key2"))
    val values = Array(UTF8String.fromString("value1"), null)
    val mapData = ArrayBasedMapData.apply(keys, values)

    val row = InternalRow.fromSeq(Seq(
      6,
      UTF8String.fromString("test_map_nulls"),
      mapData
    ))
    val converted = mapWithNullsConverter.convert(row)
    assertEquals("""{"id":6,"name":"test_map_nulls","properties":{"key1":"value1"}}""", converted.toString)
  }

  @Test
  def emptyArray(): Unit = {
    val row = InternalRow.fromSeq(Seq(
      7,
      UTF8String.fromString("empty_array_test"),
      ArrayData.toArrayData(Array[Int]())
    ))
    val converted = emptyArrayConverter.convert(row)
    assertEquals("""{"id":7,"name":"empty_array_test","numbers":[]}""", converted.toString)
  }

  @Test
  def emptyMap(): Unit = {
    val keys = Array[Int]()
    val values = Array[Int]()
    val mapData = ArrayBasedMapData.apply(keys, values)

    val row = InternalRow.fromSeq(Seq(
      8,
      UTF8String.fromString("empty_map_test"),
      mapData
    ))
    val converted = emptyMapConverter.convert(row)
    assertEquals("""{"id":8,"name":"empty_map_test","properties":{}}""", converted.toString)
  }

  private def hoodieTableSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("uuid", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "uuid", "type": "int"},
        |{"name": "name", "type": "string"}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def emptyHoodieTableSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType()
    val avroSchemaStr = """{"type": "record", "name": "test", "fields": []}"""
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def arraySchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("numbers", ArrayType(DataTypes.IntegerType), nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "id", "type": "int"},
        |{"name": "name", "type": "string"},
        |{"name": "numbers", "type": {"type": "array", "items": "int"}}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def mapSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("properties", MapType(DataTypes.StringType, DataTypes.StringType), nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "id", "type": "int"},
        |{"name": "name", "type": "string"},
        |{"name": "properties", "type": {"type": "map", "values": "string"}}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def structSchema: HoodieTableSchema = {
    val nestedStruct = new StructType(Array[StructField](
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("age", DataTypes.IntegerType, nullable = true, Metadata.empty)))
    val structTypeSchema = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("person", nestedStruct, nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "id", "type": "int"},
        |{"name": "name", "type": "string"},
        |{"name": "person", "type": {
        |  "type": "record", "name": "person", "fields": [
        |    {"name": "name", "type": "string"},
        |    {"name": "age", "type": "int"}
        |  ]
        |}}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def nestedSchema: HoodieTableSchema = {
    val nestedStruct = new StructType(Array[StructField](
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("age", DataTypes.IntegerType, nullable = true, Metadata.empty)))
    val structTypeSchema = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("people", ArrayType(nestedStruct), nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "id", "type": "int"},
        |{"name": "name", "type": "string"},
        |{"name": "people", "type": {
        |  "type": "array",
        |  "items": {
        |    "type": "record", "name": "person", "fields": [
        |      {"name": "name", "type": "string"},
        |      {"name": "age", "type": "int"}
        |    ]
        |  }
        |}}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def arrayWithNullsSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("numbers", ArrayType(DataTypes.IntegerType, containsNull = true), nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "id", "type": "int"},
        |{"name": "name", "type": "string"},
        |{"name": "numbers", "type": {"type": "array", "items": "int"}}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def mapWithNullsSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("properties", MapType(DataTypes.StringType, DataTypes.StringType, valueContainsNull = true), nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "id", "type": "int"},
        |{"name": "name", "type": "string"},
        |{"name": "properties", "type": {"type": "map", "values": "string"}}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def emptyArraySchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("numbers", ArrayType(DataTypes.IntegerType), nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "id", "type": "int"},
        |{"name": "name", "type": "string"},
        |{"name": "numbers", "type": {"type": "array", "items": "int"}}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def emptyMapSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("id", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("properties", MapType(DataTypes.StringType, DataTypes.StringType), nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "id", "type": "int"},
        |{"name": "name", "type": "string"},
        |{"name": "properties", "type": {"type": "map", "values": "string"}}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def arrayConverter: InternalRowToJsonStringConverter = new InternalRowToJsonStringConverter(arraySchema.structTypeSchema)

  private def mapConverter: InternalRowToJsonStringConverter = new InternalRowToJsonStringConverter(mapSchema.structTypeSchema)

  private def structConverter: InternalRowToJsonStringConverter = new InternalRowToJsonStringConverter(structSchema.structTypeSchema)

  private def nestedConverter: InternalRowToJsonStringConverter = new InternalRowToJsonStringConverter(nestedSchema.structTypeSchema)

  private def arrayWithNullsConverter: InternalRowToJsonStringConverter = new InternalRowToJsonStringConverter(arrayWithNullsSchema.structTypeSchema)

  private def mapWithNullsConverter: InternalRowToJsonStringConverter = new InternalRowToJsonStringConverter(mapWithNullsSchema.structTypeSchema)

  private def emptyArrayConverter: InternalRowToJsonStringConverter = new InternalRowToJsonStringConverter(emptyArraySchema.structTypeSchema)

  private def emptyMapConverter: InternalRowToJsonStringConverter = new InternalRowToJsonStringConverter(emptyMapSchema.structTypeSchema)
}
