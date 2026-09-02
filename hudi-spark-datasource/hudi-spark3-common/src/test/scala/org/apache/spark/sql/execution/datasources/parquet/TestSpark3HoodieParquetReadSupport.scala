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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieException

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.schema.{MessageType, Type, Types}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.parquet.VariantParquetTestFixtures.{shreddedVariant, unshreddedVariant}
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.junit.jupiter.api.{Assertions, Test}

import java.util.{HashMap => JHashMap, Set => JSet}

/**
 * Pins the wiring of [[Spark3HoodieParquetReadSupport]] over a hand-built {@link InitContext}: that
 * `init` reads the catalyst request from SPARK_ROW_REQUESTED_SCHEMA and hands the guard the file
 * schema. The schema walk itself is covered by TestParquetSchemaEvolutionUtils; a slip in either
 * wire here would not fail there, it would fall straight back to the null-value read this class
 * exists to stop. No SparkSession: Spark's own `init` only needs the requested schema in the conf.
 */
class TestSpark3HoodieParquetReadSupport {

  /** A variant column the way a Spark 3.x reader declares it: its two binary members. */
  private val variantRequestedAsStruct =
    new StructType().add("v", new StructType().add("value", BinaryType).add("metadata", BinaryType))

  @Test
  def testInitRejectsShreddedFileForVariantRequestedAsStruct(): Unit = {
    val failure = Assertions.assertThrows(classOf[HoodieException], () =>
      readSupport().init(initContext(variantRequestedAsStruct, fileOf(shreddedVariant("v")))))
    Assertions.assertTrue(
      failure.getMessage.contains("shredded variant") && failure.getMessage.contains("'v'"),
      s"init must fail with the guard's own message naming the column, got: ${failure.getMessage}")
  }

  @Test
  def testInitReadsUnshreddedFileForVariantRequestedAsStruct(): Unit = {
    val context = readSupport().init(initContext(variantRequestedAsStruct, fileOf(unshreddedVariant("v"))))
    Assertions.assertTrue(context.getRequestedSchema.containsField("v"),
      "an unshredded file must initialise as before, with the column in the requested schema")
  }

  /**
   * Why `init` anchors on the file schema: Spark clips the requested parquet schema to the catalyst
   * request, and a two-member request leaves no typed_value in it. Anchoring on the requested schema
   * instead would therefore never fire - which is what the base class, guard-free, demonstrates.
   */
  @Test
  def testClippedRequestCarriesNoTypedValue(): Unit = {
    val base = new HoodieParquetReadSupport(None, false, false, rebaseSpec, rebaseSpec, HOption.empty())
    val context = base.init(initContext(variantRequestedAsStruct, fileOf(shreddedVariant("v"))))
    val requested = context.getRequestedSchema
    Assertions.assertFalse(requested.getType(requested.getFieldIndex("v")).asGroupType().containsField("typed_value"),
      "the clipped request must not carry typed_value, so only the file schema can show the shredding")
  }

  private def readSupport(): Spark3HoodieParquetReadSupport =
    new Spark3HoodieParquetReadSupport(None, false, false, rebaseSpec, rebaseSpec, HOption.empty())

  /** A CORRECTED rebase spec built the version-neutral way the readers themselves use. */
  private def rebaseSpec = DataSourceUtils.datetimeRebaseSpec(_ => null, "CORRECTED")

  private def initContext(requested: StructType, fileSchema: MessageType): InitContext = {
    val conf = new Configuration(false)
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requested.json)
    new InitContext(conf, new JHashMap[String, JSet[String]](), fileSchema)
  }

  private def fileOf(column: Type): MessageType = Types.buildMessage().addField(column).named("file")
}
