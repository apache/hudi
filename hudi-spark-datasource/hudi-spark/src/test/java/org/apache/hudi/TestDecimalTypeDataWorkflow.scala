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

package org.apache.hudi

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class TestDecimalTypeDataWorkflow extends SparkClientFunctionalTestHarness{
  val sparkOpts: Map[String, String] = Map(
    HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet",
    HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName)
  val fgReaderOpts: Map[String, String] = Map(
    HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "true",
    HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key -> "true")
  val opts = sparkOpts ++ fgReaderOpts

  @ParameterizedTest
  @CsvSource(value = Array("10,2", "15,5", "20,10", "38,18", "5,0"))
  def testDecimalInsertUpdateDeleteRead(precision: String, scale: String): Unit = {
    // Create schema
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField(
        "decimal_col",
        DecimalType(Integer.valueOf(precision), Integer.valueOf(scale)),
        nullable = true)))
    // Build data conforming to the schema.
    val tablePath = basePath
    val data: Seq[(Int, Decimal)] = Seq(
      (1, Decimal("123.45")),
      (2, Decimal("987.65")),
      (3, Decimal("-10.23")),
      (4, Decimal("0.01")),
      (5, Decimal("1000.00")))
    val rows = data.map{
      case (id, decimalVal) => Row(id, decimalVal.toJavaBigDecimal)}
    val rddData = spark.sparkContext.parallelize(rows)

    // Insert.
    val insertDf: DataFrame = spark.sqlContext.createDataFrame(rddData, schema)
      .toDF("id", "decimal_col").sort("id")
    insertDf.write.format("hudi")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "decimal_col")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .option(TABLE_NAME.key, "test_table")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Update.
    val update: Seq[(Int, Decimal)] = Seq(
      (1, Decimal("543.21")),
      (2, Decimal("111.11")),
      (6, Decimal("1001.00")))
    val updateRows = update.map {
      case (id, decimalVal) => Row(id, decimalVal.toJavaBigDecimal)
    }
    val rddUpdate = spark.sparkContext.parallelize(updateRows)
    val updateDf: DataFrame = spark.createDataFrame(rddUpdate, schema)
      .toDF("id", "decimal_col").sort("id")
    updateDf.write.format("hudi")
      .option(OPERATION.key(), "upsert")
      .options(opts)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Delete.
    val delete: Seq[(Int, Decimal)] = Seq(
      (3, Decimal("543.21")),
      (4, Decimal("111.11")))
    val deleteRows = delete.map {
      case (id, decimalVal) => Row(id, decimalVal.toJavaBigDecimal)
    }
    val rddDelete = spark.sparkContext.parallelize(deleteRows)
    val deleteDf: DataFrame = spark.createDataFrame(rddDelete, schema)
      .toDF("id", "decimal_col").sort("id")
    deleteDf.write.format("hudi")
      .option(OPERATION.key(), "delete")
      .options(opts)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Asserts
    val actual = spark.read.format("hudi").load(tablePath).select("id", "decimal_col")
    val expected: Seq[(Int, Decimal)] = Seq(
      (1, Decimal("543.21")),
      (2, Decimal("987.65")),
      (5, Decimal("1000.00")),
      (6, Decimal("1001.00")))
    val expectedRows = expected.map {
      case (id, decimalVal) => Row(id, decimalVal.toJavaBigDecimal)
    }
    val rddExpected = spark.sparkContext.parallelize(expectedRows)
    val expectedDf: DataFrame = spark.createDataFrame(rddExpected, schema)
      .toDF("id", "decimal_col").sort("id")
    val expectedMinusActual = expectedDf.except(actual)
    val actualMinusExpected = actual.except(expectedDf)
    expectedDf.show(false)
    actual.show(false)
    expectedMinusActual.show(false)
    actualMinusExpected.show(false)
    assertTrue(expectedMinusActual.isEmpty && actualMinusExpected.isEmpty)
  }
}
