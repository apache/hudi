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

import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertEquals
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

  def assertFieldType(schema: StructType, fieldName: String, expectedDataType: DataType): Unit = {
    val fieldNameSchemaPair = HoodieSchemaUtils.getSchemaForField(schema, fieldName)
    assertEquals(fieldName, fieldNameSchemaPair.getKey)
    assertEquals(expectedDataType, fieldNameSchemaPair.getValue.dataType)
  }

}
