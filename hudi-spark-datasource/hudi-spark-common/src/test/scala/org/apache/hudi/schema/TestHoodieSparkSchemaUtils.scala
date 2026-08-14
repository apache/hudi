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

import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

/**
 * Tests for {@link HoodieSchemaUtils#getSchemaForField}
 */
class TestHoodieSparkSchemaUtils {

  // Test schemas
  private val simpleSchema = StructType(
    StructField("id", StringType) ::
      StructField("name", StringType) ::
      StructField("count", IntegerType) :: Nil)

  private val nestedSchema = StructType(
    StructField("id", StringType) ::
      StructField("nested", StructType(
        StructField("inner_string", StringType) ::
          StructField("inner_int", IntegerType) :: Nil)) :: Nil)

  private val arraySchema = StructType(
    StructField("id", StringType) ::
      StructField("items", ArrayType(StringType, containsNull = true)) :: Nil)

  private val arrayOfStructSchema = StructType(
    StructField("id", StringType) ::
      StructField("items", ArrayType(StructType(
        StructField("nested_int", IntegerType) ::
          StructField("nested_string", StringType) :: Nil), containsNull = true)) :: Nil)

  private val mapSchema = StructType(
    StructField("id", StringType) ::
      StructField("metadata", MapType(StringType, IntegerType, valueContainsNull = true)) :: Nil)

  private val mapOfStructSchema = StructType(
    StructField("id", StringType) ::
      StructField("nested_map", MapType(StringType, StructType(
        StructField("nested_int", IntegerType) ::
          StructField("nested_string", StringType) :: Nil), valueContainsNull = true)) :: Nil)

  private val complexSchema = StructType(
    StructField("id", StringType) ::
      StructField("top", StructType(
        StructField("nested_array", ArrayType(StructType(
          StructField("inner_field", StringType) :: Nil), containsNull = true)) ::
          StructField("nested_map", MapType(StringType, IntegerType, valueContainsNull = true)) :: Nil)) :: Nil)

  private def getSchema(name: String): StructType = name match {
    case "simple" => simpleSchema
    case "nested" => nestedSchema
    case "array" => arraySchema
    case "arrayOfStruct" => arrayOfStructSchema
    case "map" => mapSchema
    case "mapOfStruct" => mapOfStructSchema
    case "complex" => complexSchema
  }

  @ParameterizedTest
  @CsvSource(Array(
    // Simple field tests
    "simple,       id,                                            id,                                            string",
    "simple,       name,                                          name,                                          string",
    "simple,       count,                                         count,                                         int",
    // Nested struct field tests
    "nested,       nested.inner_string,                           nested.inner_string,                           string",
    "nested,       nested.inner_int,                              nested.inner_int,                              int",
    // Array element access using .list.element
    "array,        items.list.element,                            items.list.element,                            string",
    // Array of struct - access nested fields within array elements
    "arrayOfStruct, items.list.element.nested_int,                items.list.element.nested_int,                 int",
    "arrayOfStruct, items.list.element.nested_string,             items.list.element.nested_string,              string",
    // Map key/value access using .key_value.key and .key_value.value
    "map,          metadata.key_value.key,                        metadata.key_value.key,                        string",
    "map,          metadata.key_value.value,                      metadata.key_value.value,                      int",
    // Map of struct - access nested fields within map values
    "mapOfStruct,  nested_map.key_value.value.nested_int,         nested_map.key_value.value.nested_int,         int",
    "mapOfStruct,  nested_map.key_value.value.nested_string,      nested_map.key_value.value.nested_string,      string",
    // Complex nested: struct -> array -> struct
    "complex,      top.nested_array.list.element.inner_field,     top.nested_array.list.element.inner_field,     string",
    // Complex nested: struct -> map
    "complex,      top.nested_map.key_value.key,                  top.nested_map.key_value.key,                  string",
    "complex,      top.nested_map.key_value.value,                top.nested_map.key_value.value,                int"
  ))
  def testGetSchemaForField(schemaName: String, inputPath: String, expectedKey: String, expectedType: String): Unit = {
    val schema = getSchema(schemaName.trim)
    val result = HoodieSchemaUtils.getSchemaForField(schema, inputPath.trim)
    assertEquals(expectedKey.trim, result.getKey)
    assertEquals(expectedType.trim, result.getValue.dataType.simpleString)
  }

  @Test
  def testInvalidPaths(): Unit = {
    // Invalid array paths
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(arraySchema, "items.wrong.path"))
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(arraySchema, "items.list.missing"))

    // Invalid map paths
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(mapSchema, "metadata.wrong.path"))
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(mapSchema, "metadata.key_value.key.invalid"))

    // Non-existent field
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(simpleSchema, "nonexistent"))
  }
}
