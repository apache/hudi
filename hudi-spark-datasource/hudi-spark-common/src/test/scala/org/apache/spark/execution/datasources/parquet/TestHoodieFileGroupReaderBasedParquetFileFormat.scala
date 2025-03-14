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

package org.apache.spark.execution.datasources.parquet

import org.apache.hudi.common.model.HoodiePayloadProps
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedParquetFileFormat
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class TestHoodieFileGroupReaderBasedParquetFileFormat extends AnyFunSuite {
  // Sample schema for testing
  val sampleSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true)
  ))

  // Sample schemas for testing
  val requiredSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true)
  ))

  val partitionSchema: StructType = StructType(Seq(
    StructField("date", StringType, nullable = true),
    StructField("region", StringType, nullable = true)
  ))

  val orderingField: StructField = StructField("timestamp", LongType, nullable = true)

  test("getOrderingField returns null when no options are available") {
    var options: Map[String, String] = Map.empty
    assert(null == HoodieFileGroupReaderBasedParquetFileFormat.getOrderingFieldName(options))
  }

  test("getOrderingField returns null when payload ordering field exists") {
    var options: Map[String, String] = Map(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY -> "orderingField")
    assert("orderingField" == HoodieFileGroupReaderBasedParquetFileFormat.getOrderingFieldName(options))
  }

  test("getOrderingField returns null when write config precombine field exists ") {
    var options: Map[String, String] = Map("hoodie.datasource.write.precombine.field" -> "orderingField")
    assert("orderingField" == HoodieFileGroupReaderBasedParquetFileFormat.getOrderingFieldName(options))
  }

  test("getOrderingField returns table config precombine field exists") {
    var options: Map[String, String] = Map(HoodieTableConfig.PRECOMBINE_FIELD.key -> "orderingField")
    assert("orderingField" == HoodieFileGroupReaderBasedParquetFileFormat.getOrderingFieldName(options))
  }

  test("getOrderingField returns correct field when orderingField exists in schema") {
    val options = Map("hoodie.datasource.write.precombine.field" -> "name")
    val result = HoodieFileGroupReaderBasedParquetFileFormat.getOrderingField(options, sampleSchema)
    assert(result == StructField("name", StringType, nullable = true))
  }

  test("getOrderingField returns null when orderingField does not exist in schema") {
    val options = Map("hoodie.datasource.write.precombine.field" -> "salary")
    val result = HoodieFileGroupReaderBasedParquetFileFormat.getOrderingField(options, sampleSchema)
    assert(result == null)
  }

  test("getOrderingField returns null when orderingField is not provided") {
    val options = Map.empty[String, String]
    val result = HoodieFileGroupReaderBasedParquetFileFormat.getOrderingField(options, sampleSchema)
    assert(result == null)
  }

  test("getOrderingField returns null when orderingField is an empty string") {
    val options = Map("hoodie.datasource.write.precombine.field" -> "")
    val result = HoodieFileGroupReaderBasedParquetFileFormat.getOrderingField(options, sampleSchema)
    assert(result == null)
  }

  test("Includes all fields from requiredSchema") {
    val result = HoodieFileGroupReaderBasedParquetFileFormat
      .getRequestedSchemaFields(requiredSchema, StructType(Nil), Seq.empty, null)
    assert(result.contains(StructField("id", IntegerType, nullable = false)))
    assert(result.contains(StructField("name", StringType, nullable = true)))
  }

  test("Includes mandatory fields from partitionSchema") {
    val result = HoodieFileGroupReaderBasedParquetFileFormat.getRequestedSchemaFields(
      requiredSchema,
      partitionSchema,
      Seq("date"),
      null
    )
    assert(result.contains(StructField("date", StringType, nullable = true)))
    assert(!result.contains(StructField("region", StringType, nullable = true)))
  }

  test("Includes orderingField when it's not null and not already present") {
    val result = HoodieFileGroupReaderBasedParquetFileFormat.getRequestedSchemaFields(
      requiredSchema,
      StructType(Nil),
      Seq.empty,
      orderingField
    )
    assert(result.contains(orderingField))
  }

  test("Does not duplicate orderingField if it's already present") {
    val result = HoodieFileGroupReaderBasedParquetFileFormat.getRequestedSchemaFields(
      requiredSchema,
      StructType(Nil),
      Seq.empty,
      StructField("id", IntegerType, nullable = false)
    )
    assert(result.count(_.name == "id") == 1)
  }

  test("Handles null orderingField gracefully") {
    val result = HoodieFileGroupReaderBasedParquetFileFormat.getRequestedSchemaFields(
      requiredSchema, StructType(Nil), Seq.empty, null)
    assert(!result.exists(_.name == "timestamp"))
  }
}
