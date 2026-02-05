/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestSparkSchemaTransformUtils {
  
  @Test
  def testGenerateNullPaddingProjection_topLevelMissingFields(): Unit = {
    // Input schema: (id: int, name: string)
    val inputSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    // Target schema: (id: int, name: string, age: int, city: string)
    val targetSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = true),
      StructField("city", StringType, nullable = true)
    ))

    val projection = SparkSchemaTransformUtils.generateNullPaddingProjection(inputSchema, targetSchema)

    // Test with sample data: (1, "Alice")
    val inputRow = new GenericInternalRow(Array[Any](
      1,
      UTF8String.fromString("Alice")
    ))

    val outputRow = projection.apply(inputRow)
    
    // Verify results: (1, "Alice", NULL, NULL)
    assertEquals(4, outputRow.numFields)
    assertEquals(1, outputRow.getInt(0))
    assertEquals(UTF8String.fromString("Alice"), outputRow.getUTF8String(1))
    assertTrue(outputRow.isNullAt(2), "age field should be NULL")
    assertTrue(outputRow.isNullAt(3), "city field should be NULL")
  }

  @Test
  def testGenerateNullPaddingProjection_noPaddingNeeded(): Unit = {
    // Schema: (id: int, name: string, age: int)
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)
    ))

    val projection = SparkSchemaTransformUtils.generateNullPaddingProjection(schema, schema)

    // Test with sample data: (1, "Bob", 30)
    val inputRow = new GenericInternalRow(Array[Any](
      1,
      UTF8String.fromString("Bob"),
      30
    ))

    val outputRow = projection.apply(inputRow)

    // Verify all values pass through unchanged
    assertEquals(3, outputRow.numFields)
    assertEquals(1, outputRow.getInt(0))
    assertEquals(UTF8String.fromString("Bob"), outputRow.getUTF8String(1))
    assertEquals(30, outputRow.getInt(2))
  }

  @Test
  def testGenerateNullPaddingProjection_nestedStructMissingFields(): Unit = {
    // Input schema: person: struct<name: string, age: int>
    val inputSchema = StructType(Seq(
      StructField("person", StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      )), nullable = false)
    ))

    // Target schema: person: struct<name: string, age: int, city: string, country: string>
    val targetSchema = StructType(Seq(
      StructField("person", StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("city", StringType, nullable = true),
        StructField("country", StringType, nullable = true)
      )), nullable = false)
    ))

    val projection = SparkSchemaTransformUtils.generateNullPaddingProjection(inputSchema, targetSchema)

    // Test with sample data: struct("Charlie", 25)
    val structData = new GenericInternalRow(Array[Any](
      UTF8String.fromString("Charlie"),
      25
    ))
    val inputRow = new GenericInternalRow(Array[Any](structData))

    val outputRow = projection.apply(inputRow)

    // Verify nested struct has padded NULL fields
    assertEquals(1, outputRow.numFields)
    val outputStruct = outputRow.getStruct(0, 4)
    assertEquals(UTF8String.fromString("Charlie"), outputStruct.getUTF8String(0))
    assertEquals(25, outputStruct.getInt(1))
    assertTrue(outputStruct.isNullAt(2), "city field should be NULL")
    assertTrue(outputStruct.isNullAt(3), "country field should be NULL")
  }

  @Test
  def testGenerateNullPaddingProjection_arrayOfStructs(): Unit = {
    // Input schema: items: array<struct<id: int, name: string>>
    val inputSchema = StructType(Seq(
      StructField("items", ArrayType(StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))), nullable = false)
    ))

    // Target schema: items: array<struct<id: int, name: string, price: double>>
    val targetSchema = StructType(Seq(
      StructField("items", ArrayType(StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("price", DoubleType, nullable = true)
      ))), nullable = false)
    ))

    val projection = SparkSchemaTransformUtils.generateNullPaddingProjection(inputSchema, targetSchema)

    // Test with sample data: Array[struct(1, "item1"), struct(2, "item2")]
    val struct1 = new GenericInternalRow(Array[Any](1, UTF8String.fromString("item1")))
    val struct2 = new GenericInternalRow(Array[Any](2, UTF8String.fromString("item2")))
    val arrayData = ArrayData.toArrayData(Array(struct1, struct2))
    val inputRow = new GenericInternalRow(Array[Any](arrayData))

    val outputRow = projection.apply(inputRow)

    // Verify array elements have padded NULL fields
    assertEquals(1, outputRow.numFields)
    val outputArray = outputRow.getArray(0)
    assertEquals(2, outputArray.numElements())

    val outputStruct1 = outputArray.getStruct(0, 3)
    assertEquals(1, outputStruct1.getInt(0))
    assertEquals(UTF8String.fromString("item1"), outputStruct1.getUTF8String(1))
    assertTrue(outputStruct1.isNullAt(2), "price field should be NULL")

    val outputStruct2 = outputArray.getStruct(1, 3)
    assertEquals(2, outputStruct2.getInt(0))
    assertEquals(UTF8String.fromString("item2"), outputStruct2.getUTF8String(1))
    assertTrue(outputStruct2.isNullAt(2), "price field should be NULL")
  }
  
  @Test
  def testGenerateNullPaddingProjection_mapWithStructValues(): Unit = {
    // Input schema: config: map<string, struct<id: int, name: string>>
    val inputSchema = StructType(Seq(
      StructField("config", MapType(
        StringType,
        StructType(Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false)
        ))
      ), nullable = false)
    ))

    // Target schema: config: map<string, struct<id: int, name: string, enabled: boolean>>
    val targetSchema = StructType(Seq(
      StructField("config", MapType(
        StringType,
        StructType(Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("enabled", BooleanType, nullable = true)
        ))
      ), nullable = false)
    ))

    val projection = SparkSchemaTransformUtils.generateNullPaddingProjection(inputSchema, targetSchema)

    // Test with sample data: Map("key1" -> struct(1, "config1"), "key2" -> struct(2, "config2"))
    val struct1 = new GenericInternalRow(Array[Any](1, UTF8String.fromString("config1")))
    val struct2 = new GenericInternalRow(Array[Any](2, UTF8String.fromString("config2")))
    val keys = ArrayData.toArrayData(Array(UTF8String.fromString("key1"), UTF8String.fromString("key2")))
    val values = ArrayData.toArrayData(Array(struct1, struct2))
    val mapData = new ArrayBasedMapData(keys, values)
    val inputRow = new GenericInternalRow(Array[Any](mapData))

    val outputRow = projection.apply(inputRow)

    // Verify map values have padded NULL fields
    assertEquals(1, outputRow.numFields)
    val outputMap = outputRow.getMap(0)
    assertEquals(2, outputMap.numElements())

    val outputKeys = outputMap.keyArray()
    val outputValues = outputMap.valueArray()

    // Verify first map entry: "key1" -> struct(1, "config1", NULL)
    assertEquals(UTF8String.fromString("key1"), outputKeys.getUTF8String(0))
    val outputStruct1 = outputValues.getStruct(0, 3)
    assertEquals(1, outputStruct1.getInt(0))
    assertEquals(UTF8String.fromString("config1"), outputStruct1.getUTF8String(1))
    assertTrue(outputStruct1.isNullAt(2), "enabled field should be NULL")

    // Verify second map entry: "key2" -> struct(2, "config2", NULL)
    assertEquals(UTF8String.fromString("key2"), outputKeys.getUTF8String(1))
    val outputStruct2 = outputValues.getStruct(1, 3)
    assertEquals(2, outputStruct2.getInt(0))
    assertEquals(UTF8String.fromString("config2"), outputStruct2.getUTF8String(1))
    assertTrue(outputStruct2.isNullAt(2), "enabled field should be NULL")
  }

  @Test
  def testFilterSchemaByFileSchema_allFieldsPresent(): Unit = {
    // Both schemas have (id, name, age)
    val requestedSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    val fileSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    val filtered = SparkSchemaTransformUtils.filterSchemaByFileSchema(requestedSchema, fileSchema)

    // Verify schema unchanged
    assertEquals(requestedSchema.json, filtered.json)
  }

  @Test
  def testFilterSchemaByFileSchema_topLevelFieldsMissing(): Unit = {
    // Requested: (id, name, age, city)
    val requestedSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("city", StringType)
    ))

    // File: (id, name) - missing age and city
    val fileSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))

    val filtered = SparkSchemaTransformUtils.filterSchemaByFileSchema(requestedSchema, fileSchema)

    // Verify only id and name remain
    assertEquals(2, filtered.fields.length)
    assertEquals("id", filtered.fields(0).name)
    assertEquals("name", filtered.fields(1).name)
    assertEquals(IntegerType, filtered.fields(0).dataType)
    assertEquals(StringType, filtered.fields(1).dataType)
  }

  @Test
  def testFilterSchemaByFileSchema_nestedStructFieldsMissing(): Unit = {
    // Requested: person: struct<name: string, age: int, city: string>
    val requestedSchema = StructType(Seq(
      StructField("person", StructType(Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("city", StringType)
      )))
    ))

    // File: person: struct<name: string, age: int>
    val fileSchema = StructType(Seq(
      StructField("person", StructType(Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )))
    ))

    val filtered = SparkSchemaTransformUtils.filterSchemaByFileSchema(requestedSchema, fileSchema)

    // Verify nested struct filtered correctly
    assertEquals(1, filtered.fields.length)
    assertEquals("person", filtered.fields(0).name)

    val personStruct = filtered.fields(0).dataType.asInstanceOf[StructType]
    assertEquals(2, personStruct.fields.length)
    assertEquals("name", personStruct.fields(0).name)
    assertEquals("age", personStruct.fields(1).name)
    assertEquals(StringType, personStruct.fields(0).dataType)
    assertEquals(IntegerType, personStruct.fields(1).dataType)
  }

  @Test
  def testFilterSchemaByFileSchema_arrayElementFieldsFiltered(): Unit = {
    // Requested: items: array<struct<id: int, name: string, price: double>>
    val requestedSchema = StructType(Seq(
      StructField("items", ArrayType(StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("price", DoubleType)
      ))))
    ))

    // File: items: array<struct<id: int, name: string>>
    val fileSchema = StructType(Seq(
      StructField("items", ArrayType(StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      ))))
    ))

    val filtered = SparkSchemaTransformUtils.filterSchemaByFileSchema(requestedSchema, fileSchema)

    // Verify array element struct filtered correctly
    assertEquals(1, filtered.fields.length)
    assertEquals("items", filtered.fields(0).name)

    val arrayType = filtered.fields(0).dataType.asInstanceOf[ArrayType]
    val elementStruct = arrayType.elementType.asInstanceOf[StructType]
    assertEquals(2, elementStruct.fields.length)
    assertEquals("id", elementStruct.fields(0).name)
    assertEquals("name", elementStruct.fields(1).name)
    assertEquals(IntegerType, elementStruct.fields(0).dataType)
    assertEquals(StringType, elementStruct.fields(1).dataType)
  }
}
