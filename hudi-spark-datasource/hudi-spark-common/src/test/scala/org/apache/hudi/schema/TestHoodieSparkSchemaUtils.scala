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

  // Shared test schemas
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

  // ===========================================
  // Simple and Nested Field Tests
  // ===========================================

  @ParameterizedTest
  @CsvSource(Array(
    "id,        id,        string",
    "name,      name,      string",
    "count,     count,     int"
  ))
  def testSimpleFields(inputPath: String, expectedKey: String, expectedType: String): Unit = {
    val result = HoodieSchemaUtils.getSchemaForField(simpleSchema, inputPath.trim)
    assertEquals(expectedKey.trim, result.getKey)
    assertEquals(expectedType.trim, result.getValue.dataType.simpleString)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "nested.inner_string,  nested.inner_string,  string",
    "nested.inner_int,     nested.inner_int,     int"
  ))
  def testNestedFields(inputPath: String, expectedKey: String, expectedType: String): Unit = {
    val result = HoodieSchemaUtils.getSchemaForField(nestedSchema, inputPath.trim)
    assertEquals(expectedKey.trim, result.getKey)
    assertEquals(expectedType.trim, result.getValue.dataType.simpleString)
  }

  // ===========================================
  // Array Path Tests
  // ===========================================

  @ParameterizedTest
  @CsvSource(Array(
    "items.list.element,       items.list.element,              string"
  ))
  def testArrayPath(inputPath: String, expectedKey: String, expectedType: String): Unit = {
    val result = HoodieSchemaUtils.getSchemaForField(arraySchema, inputPath.trim)
    assertEquals(expectedKey.trim, result.getKey)
    assertEquals(expectedType.trim, result.getValue.dataType.simpleString)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "items.list.element.nested_int,    items.list.element.nested_int,           int",
    "items.list.element.nested_string, items.list.element.nested_string,        string"
  ))
  def testNestedArrayPath(inputPath: String, expectedKey: String, expectedType: String): Unit = {
    val result = HoodieSchemaUtils.getSchemaForField(arrayOfStructSchema, inputPath.trim)
    assertEquals(expectedKey.trim, result.getKey)
    assertEquals(expectedType.trim, result.getValue.dataType.simpleString)
  }

  // ===========================================
  // Map Navigation Tests
  // ===========================================

  @ParameterizedTest
  @CsvSource(Array(
    "metadata.key_value.key,    metadata.key_value.key,    string",
    "metadata.key_value.value,  metadata.key_value.value,  int"
  ))
  def testMapNavigation(inputPath: String, expectedKey: String, expectedType: String): Unit = {
    val result = HoodieSchemaUtils.getSchemaForField(mapSchema, inputPath.trim)
    assertEquals(expectedKey.trim, result.getKey)
    assertEquals(expectedType.trim, result.getValue.dataType.simpleString)
  }

  @ParameterizedTest
  @CsvSource(Array(
    "nested_map.key_value.value.nested_int,    nested_map.key_value.value.nested_int,    int",
    "nested_map.key_value.value.nested_string, nested_map.key_value.value.nested_string, string"
  ))
  def testNestedMapNavigation(inputPath: String, expectedKey: String, expectedType: String): Unit = {
    val result = HoodieSchemaUtils.getSchemaForField(mapOfStructSchema, inputPath.trim)
    assertEquals(expectedKey.trim, result.getKey)
    assertEquals(expectedType.trim, result.getValue.dataType.simpleString)
  }

  // ===========================================
  // Complex Nested Structure Tests
  // ===========================================

  @ParameterizedTest
  @CsvSource(Array(
    // Complex paths with struct -> array -> struct
    "top.nested_array.list.element.inner_field, top.nested_array.list.element.inner_field, string",
    // Complex paths with struct -> map
    "top.nested_map.key_value.key,              top.nested_map.key_value.key,              string",
    "top.nested_map.key_value.value,            top.nested_map.key_value.value,            int"
  ))
  def testComplexNestedPaths(inputPath: String, expectedKey: String, expectedType: String): Unit = {
    val result = HoodieSchemaUtils.getSchemaForField(complexSchema, inputPath.trim)
    assertEquals(expectedKey.trim, result.getKey)
    assertEquals(expectedType.trim, result.getValue.dataType.simpleString)
  }

  // ===========================================
  // Invalid Path Tests
  // ===========================================

  @Test
  def testInvalidArrayPaths(): Unit = {
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(arraySchema, "items.wrong.path"))
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(arraySchema, "items.list.missing"))
  }

  @Test
  def testInvalidMapPaths(): Unit = {
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(mapSchema, "metadata.wrong.path"))
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(mapSchema, "metadata.key_value.key.invalid"))
  }

  @Test
  def testNonExistentField(): Unit = {
    assertThrows(classOf[HoodieException], () =>
      HoodieSchemaUtils.getSchemaForField(simpleSchema, "nonexistent"))
  }
}
