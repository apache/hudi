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

package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.{Assertions, Test}

class TestSparkColumnarFileReader {
  @Test
  def testSchemaTrimming_noRemainingFields(): Unit = {
    val requiredNestedField = new StructType().add("nested_a", "string")
    val dataNestedField = new StructType().add("nested_b", "string")
    val requiredArrayField = new ArrayType(requiredNestedField, true)
    val dataArrayField = new ArrayType(dataNestedField, true)
    val requiredMapField = new MapType(DataTypes.StringType, requiredNestedField, true)
    val dataMapField = new MapType(DataTypes.StringType, dataNestedField, true)
    val requiredSchema = new StructType()
        .add("a", "string")
        .add("b", requiredNestedField)
        .add("c", requiredArrayField)
        .add("d", requiredMapField)
        .add("e", "string")
    val dataSchema = new StructType()
        .add("a", "string")
        .add("b", dataNestedField)
        .add("c", dataArrayField)
        .add("d", dataMapField)
        .add("e", "string")

    val trimmedSchema = SparkColumnarFileReader.trimSchema(requiredSchema, dataSchema)

    val expectedSchema = new StructType()
        .add("a", "string")
        .add("e", "string")

    Assertions.assertEquals(expectedSchema, trimmedSchema)
  }

  @Test
  def testSchemaTrimming_atLeastOneFieldMatches(): Unit = {
    val requiredNestedField = new StructType().add("nested_a", "string").add("nested_b", "string")
    val dataNestedField = new StructType().add("nested_b", "string").add("nested_c", "string")
    val requiredArrayField = new ArrayType(requiredNestedField, true)
    val dataArrayField = new ArrayType(dataNestedField, true)
    val requiredMapField = new MapType(DataTypes.StringType, requiredNestedField, true)
    val dataMapField = new MapType(DataTypes.StringType, dataNestedField, true)
    val requiredSchema = new StructType()
        .add("a", "string")
        .add("b", requiredNestedField)
        .add("c", requiredArrayField)
        .add("d", requiredMapField)
        .add("e", "string")
    val dataSchema = new StructType()
        .add("a", "string")
        .add("b", dataNestedField)
        .add("c", dataArrayField)
        .add("d", dataMapField)

    val trimmedSchema = SparkColumnarFileReader.trimSchema(requiredSchema, dataSchema);

    val expectedSchema = new StructType()
        .add("a", "string")
        .add("b", requiredNestedField)
        .add("c", requiredArrayField)
        .add("d", requiredMapField)
        .add("e", "string")

    Assertions.assertEquals(expectedSchema, trimmedSchema)
  }
}
