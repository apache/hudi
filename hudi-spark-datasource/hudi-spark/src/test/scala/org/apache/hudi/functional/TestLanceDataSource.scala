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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.DefaultSparkRecordMerger
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.condition.DisabledIfSystemProperty

/**
 * Basic functional tests for Lance file format with Hudi Spark datasource.
 */
@DisabledIfSystemProperty(named = "lance.skip.tests", matches = "true")
class TestLanceDataSource extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    spark = null
  }

  @Test
  def testBasicWriteAndRead(): Unit = {
    val tableName = "test_lance_table"
    val tablePath = s"$basePath/$tableName"

    // Create test data
    val records = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df = spark.createDataFrame(records).toDF("id", "name", "age", "score")

    // Write to Hudi table with Lance base file format
    df.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Read back and verify
  val readDf = spark.read
      .format("hudi")
      .load(tablePath)

    val result = readDf.select("id", "name", "age", "score")
      .orderBy("id")
      .collect()

    assertEquals(3, result.length, "Should read 3 records")
    assertEquals(1, result(0).getInt(0))
    assertEquals("Alice", result(0).getString(1))
    assertEquals(30, result(0).getInt(2))
    assertEquals(95.5, result(0).getDouble(3), 0.01)

    assertEquals(2, result(1).getInt(0))
    assertEquals("Bob", result(1).getString(1))
    assertEquals(25, result(1).getInt(2))
    assertEquals(87.3, result(1).getDouble(3), 0.01)

    assertEquals(3, result(2).getInt(0))
    assertEquals("Charlie", result(2).getString(1))
    assertEquals(35, result(2).getInt(2))
    assertEquals(92.1, result(2).getDouble(3), 0.01)
  }

  @Test
  def testSchemaProjection(): Unit = {
    val tableName = "test_lance_projection"
    val tablePath = s"$basePath/$tableName"

    // Create test data with multiple columns
    val records = Seq(
      (1, "Alice", 30, 95.5, "Engineering"),
      (2, "Bob", 25, 87.3, "Sales"),
      (3, "Charlie", 35, 92.1, "Marketing")
    )
    val df = spark.createDataFrame(records).toDF("id", "name", "age", "score", "department")

    // Write to Hudi table with Lance format
    df.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Read with schema projection - only select subset of columns
    val projectedDf = spark.read
      .format("hudi")
      .load(tablePath)
      .select("id", "name")  // Only read id and name
      .orderBy("id")

    val result = projectedDf.collect()

    assertEquals(3, result.length, "Should read 3 records")
    assertEquals(2, result(0).length, "Should only have 2 columns")

    assertEquals(1, result(0).getInt(0))
    assertEquals("Alice", result(0).getString(1))

    assertEquals(2, result(1).getInt(0))
    assertEquals("Bob", result(1).getString(1))

    assertEquals(3, result(2).getInt(0))
    assertEquals("Charlie", result(2).getString(1))
  }
}
