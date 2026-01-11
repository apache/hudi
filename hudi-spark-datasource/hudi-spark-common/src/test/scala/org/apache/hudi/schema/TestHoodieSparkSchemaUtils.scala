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

package org.apache.hudi.schema

import org.apache.hudi.HoodieSchemaUtils
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

/**
 * Tests {@link HoodieSparkSchemaUtils}
 */
class TestHoodieSparkSchemaUtils {

  @Test
  def testGetSchemaField(): Unit = {

    val nestedStructType = StructType(
        StructField("nested_long", LongType, nullable = true) ::
        StructField("nested_int", IntegerType, nullable = true) ::
          StructField("nested_string", StringType, nullable = true) :: Nil)

    val schema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("last_name", StringType, nullable = true) ::
        StructField("nested_field", nestedStructType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    assertFieldType(schema, "first_name", StringType)
    assertFieldType(schema, "timestamp", IntegerType)

    // test nested fields.
    assertFieldType(schema, "nested_field.nested_long", LongType)
    assertFieldType(schema, "nested_field.nested_int", IntegerType)
    assertFieldType(schema, "nested_field.nested_string", StringType)
  }

  @Test
  def testGetNestedFieldWithArraySpark(): Unit = {
    // Create schema with array field using Spark's .array pattern
    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("items", ArrayType(StringType, containsNull = true), nullable = true) :: Nil)

    // Test: items.array should resolve to STRING
    val result = HoodieSchemaUtils.getSchemaForField(schema, "items.array")
    assertEquals("items.array", result.getKey)
    assertEquals("array", result.getValue.name)
    assertEquals(StringType, result.getValue.dataType)
  }

  @Test
  def testGetNestedFieldWithArraySparkAndNesting(): Unit = {
    // Create schema with array of records using Spark's .array pattern
    val nestedStructType = StructType(
      StructField("nested_int", IntegerType, nullable = true) ::
        StructField("nested_string", StringType, nullable = true) :: Nil)

    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("items", ArrayType(nestedStructType, containsNull = true), nullable = true) :: Nil)

    // Test: items.array.nested_int should resolve to INT
    val result = HoodieSchemaUtils.getSchemaForField(schema, "items.array.nested_int")
    assertEquals("items.array.nested_int", result.getKey)
    assertEquals("nested_int", result.getValue.name)
    assertEquals(IntegerType, result.getValue.dataType)
  }

  @Test
  def testGetNestedFieldWithArrayListElement(): Unit = {
    // Create schema with array field using Avro's .list.element pattern
    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("items", ArrayType(StringType, containsNull = true), nullable = true) :: Nil)

    // Test: items.list.element should resolve to STRING
    val result = HoodieSchemaUtils.getSchemaForField(schema, "items.list.element")
    assertEquals("items.list.element", result.getKey)
    assertEquals("element", result.getValue.name)
    assertEquals(StringType, result.getValue.dataType)
  }

  @Test
  def testGetNestedFieldWithArrayListElementAndNesting(): Unit = {
    // Create schema with array of records using Avro's .list.element pattern
    val nestedStructType = StructType(
      StructField("nested_int", IntegerType, nullable = true) ::
        StructField("nested_string", StringType, nullable = true) :: Nil)

    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("items", ArrayType(nestedStructType, containsNull = true), nullable = true) :: Nil)

    // Test: items.list.element.nested_int should resolve to INT
    val result = HoodieSchemaUtils.getSchemaForField(schema, "items.list.element.nested_int")
    assertEquals("items.list.element.nested_int", result.getKey)
    assertEquals("nested_int", result.getValue.name)
    assertEquals(IntegerType, result.getValue.dataType)
  }

  @Test
  def testGetNestedFieldWithMapKeyValueKey(): Unit = {
    // Create schema with map field
    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("metadata", MapType(StringType, IntegerType, valueContainsNull = true), nullable = true) :: Nil)

    // Test: metadata.key_value.key should resolve to STRING (MAP keys are always STRING)
    val result = HoodieSchemaUtils.getSchemaForField(schema, "metadata.key_value.key")
    assertEquals("metadata.key_value.key", result.getKey)
    assertEquals("key", result.getValue.name)
    assertEquals(StringType, result.getValue.dataType)
  }

  @Test
  def testGetNestedFieldWithMapKeyValueValue(): Unit = {
    // Create schema with map field
    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("metadata", MapType(StringType, IntegerType, valueContainsNull = true), nullable = true) :: Nil)

    // Test: metadata.key_value.value should resolve to INT
    val result = HoodieSchemaUtils.getSchemaForField(schema, "metadata.key_value.value")
    assertEquals("metadata.key_value.value", result.getKey)
    assertEquals("value", result.getValue.name)
    assertEquals(IntegerType, result.getValue.dataType)
  }

  @Test
  def testGetNestedFieldWithMapKeyValueValueAndNesting(): Unit = {
    // Create schema with map of records
    val nestedStructType = StructType(
      StructField("nested_int", IntegerType, nullable = true) ::
        StructField("nested_string", StringType, nullable = true) :: Nil)

    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("nested_map", MapType(StringType, nestedStructType, valueContainsNull = true), nullable = true) :: Nil)

    // Test: nested_map.key_value.value.nested_int should resolve to INT
    val result = HoodieSchemaUtils.getSchemaForField(schema, "nested_map.key_value.value.nested_int")
    assertEquals("nested_map.key_value.value.nested_int", result.getKey)
    assertEquals("nested_int", result.getValue.name)
    assertEquals(IntegerType, result.getValue.dataType)
  }

  @Test
  def testGetNestedFieldWithInvalidArrayPath(): Unit = {
    // Create schema with array field
    val schema = StructType(
      StructField("items", ArrayType(StringType, containsNull = true), nullable = true) :: Nil)

    // Test: Invalid array path should throw HoodieException
    assertThrows(classOf[HoodieException], () => {
      HoodieSchemaUtils.getSchemaForField(schema, "items.wrong.path")
    })

    // Test: Missing "element" after "list" should throw HoodieException
    assertThrows(classOf[HoodieException], () => {
      HoodieSchemaUtils.getSchemaForField(schema, "items.list.missing")
    })
  }

  @Test
  def testGetNestedFieldWithInvalidMapPath(): Unit = {
    // Create schema with map field
    val schema = StructType(
      StructField("metadata", MapType(StringType, IntegerType, valueContainsNull = true), nullable = true) :: Nil)

    // Test: Invalid map path should throw HoodieException
    assertThrows(classOf[HoodieException], () => {
      HoodieSchemaUtils.getSchemaForField(schema, "metadata.wrong.path")
    })

    // Test: Cannot navigate beyond map key
    assertThrows(classOf[HoodieException], () => {
      HoodieSchemaUtils.getSchemaForField(schema, "metadata.key_value.key.invalid")
    })
  }

  @Test
  def testGetNestedFieldWithComplexNestedStructure(): Unit = {
    // Create a complex schema with arrays, maps, and nested structs
    val innerStructType = StructType(
      StructField("inner_field", StringType, nullable = true) :: Nil)

    val nestedStructType = StructType(
      StructField("nested_array", ArrayType(innerStructType, containsNull = true), nullable = true) ::
        StructField("nested_map", MapType(StringType, IntegerType, valueContainsNull = true), nullable = true) :: Nil)

    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("top_level_struct", nestedStructType, nullable = true) :: Nil)

    // Test: Access nested array element's field
    val result1 = HoodieSchemaUtils.getSchemaForField(schema, "top_level_struct.nested_array.array.inner_field")
    assertEquals("top_level_struct.nested_array.array.inner_field", result1.getKey)
    assertEquals("inner_field", result1.getValue.name)
    assertEquals(StringType, result1.getValue.dataType)

    // Test: Access nested map value
    val result2 = HoodieSchemaUtils.getSchemaForField(schema, "top_level_struct.nested_map.key_value.value")
    assertEquals("top_level_struct.nested_map.key_value.value", result2.getKey)
    assertEquals("value", result2.getValue.name)
    assertEquals(IntegerType, result2.getValue.dataType)
  }

  def assertFieldType(schema: StructType, fieldName: String, expectedDataType: DataType): Unit = {
    val fieldNameSchemaPair = HoodieSchemaUtils.getSchemaForField(schema, fieldName)
    assertEquals(fieldName, fieldNameSchemaPair.getKey)
    assertEquals(expectedDataType, fieldNameSchemaPair.getValue.dataType)
  }

}
