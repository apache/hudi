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

package org.apache.hudi.util

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.client.utils.SparkInternalSchemaConverter.collectColNamesFromSparkStruct
import org.apache.hudi.internal.schema.convert.TestAvroInternalSchemaConverter._
import org.apache.hudi.testutils.HoodieSparkClientTestHarness

import org.apache.avro.Schema
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestSparkInternalSchemaConverter extends HoodieSparkClientTestHarness with SparkAdapterSupport {

  private def getStructType(schema: Schema): DataType = {
    sparkAdapter.getAvroSchemaConverters.toSqlType(schema)._1
  }

  @Test
  def testCollectColumnNames(): Unit = {
    val simpleSchema = StructType(Seq(
      StructField("field1", IntegerType, nullable = false, Metadata.empty),
      StructField("field2", StringType, nullable = false, Metadata.empty)))

    assertEquals(getStructType(getSimpleSchema).json, simpleSchema.json)
    var fieldNames =  collectColNamesFromSparkStruct(simpleSchema)
    var expectedOutput = getSimpleSchemaExpectedColumnNames()
    assertEquals(expectedOutput.size(), fieldNames.size())
    assertTrue(fieldNames.containsAll(expectedOutput))

    val simpleSchemaWithNullable = StructType(Seq(
      StructField("field1", IntegerType, nullable = true, Metadata.empty),
      StructField("field2", StringType, nullable = false, Metadata.empty)))

    assertEquals(getStructType(getSimpleSchemaWithNullable).json, simpleSchemaWithNullable.json)
    fieldNames =  collectColNamesFromSparkStruct(simpleSchemaWithNullable)
    expectedOutput = getSimpleSchemaExpectedColumnNames()
    assertEquals(expectedOutput.size(), fieldNames.size())
    assertTrue(fieldNames.containsAll(expectedOutput))

    val complexSchemaSingleLevel = StructType(Seq(
      StructField("field1", StructType(Seq(
        StructField("nested", IntegerType, nullable = false, Metadata.empty)
      )), nullable = false, Metadata.empty),
      StructField("field2", ArrayType(StringType, containsNull = false), nullable = false, Metadata.empty),
      StructField("field3", MapType(StringType, DoubleType, valueContainsNull = false), nullable = false, Metadata.empty)
    ))

    assertEquals(getStructType(getComplexSchemaSingleLevel).json, complexSchemaSingleLevel.json)
    fieldNames =  collectColNamesFromSparkStruct(complexSchemaSingleLevel)
    expectedOutput = getComplexSchemaSingleLevelExpectedColumnNames()
    assertEquals(expectedOutput.size(), fieldNames.size())
    assertTrue(fieldNames.containsAll(expectedOutput))

    val deeplyNestedField = StructType(Seq(
      StructField("field1", IntegerType, nullable = false, Metadata.empty),
      StructField("field2", StructType(Seq(
        StructField("field2nestarray",
          ArrayType(
            StructType(Seq(
              StructField("field21", IntegerType, nullable = true, Metadata.empty),
              StructField("field22", IntegerType, nullable = true, Metadata.empty)
            )), containsNull = true),
          nullable = false)
      )), nullable = false),
      StructField("field3", IntegerType, nullable = true, Metadata.empty)
    ))

    assertEquals(getStructType(getDeeplyNestedFieldSchema).json, deeplyNestedField.json)
    fieldNames =  collectColNamesFromSparkStruct(deeplyNestedField)
    expectedOutput = getDeeplyNestedFieldSchemaExpectedColumnNames()
    assertEquals(expectedOutput.size(), fieldNames.size())
    assertTrue(fieldNames.containsAll(expectedOutput))
  }
}
