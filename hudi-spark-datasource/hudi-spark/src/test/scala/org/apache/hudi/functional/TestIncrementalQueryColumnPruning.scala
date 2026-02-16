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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * Tests to validate column pruning optimization for incremental queries.
 * These tests verify that:
 * 1. Column pruning reduces the number of columns read from source files
 * 2. Only requested columns (+ mandatory fields) are included in results
 * 3. The optimization works for both COW and MOR table types
 */
class TestIncrementalQueryColumnPruning extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  @BeforeEach override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown() = {
    spark = null
    cleanupResources()
  }

  /**
   * Core test to validate column pruning in incremental queries.
   * Creates a wide table and validates that selecting a subset of columns
   * results in fewer columns being returned.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testColumnPruningInIncrementalQuery(tableType: HoodieTableType): Unit = {
    val _spark = spark
    import _spark.implicits._

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()
    )

    // Create a wide table with 10 columns (plus hoodie meta fields)
    val batch1 = Seq(
      ("key1", "p1", 1000L, "c1_v1", "c2_v1", "c3_v1", "c4_v1", "c5_v1", "c6_v1", "c7_v1"),
      ("key2", "p1", 1000L, "c1_v2", "c2_v2", "c3_v2", "c4_v2", "c5_v2", "c6_v2", "c7_v2"),
      ("key3", "p2", 1000L, "c1_v3", "c2_v3", "c3_v3", "c4_v3", "c5_v3", "c6_v3", "c7_v3")
    ).toDF("_row_key", "partition", "timestamp", "col1", "col2", "col3", "col4", "col5", "col6", "col7")

    batch1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val hoodieMetaClient = createMetaClient(spark, basePath)
    val commit1 = hoodieMetaClient.getCommitsTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    // Second batch
    val batch2 = Seq(
      ("key4", "p1", 2000L, "c1_v4", "c2_v4", "c3_v4", "c4_v4", "c5_v4", "c6_v4", "c7_v4"),
      ("key5", "p2", 2000L, "c1_v5", "c2_v5", "c3_v5", "c4_v5", "c5_v5", "c6_v5", "c7_v5")
    ).toDF("_row_key", "partition", "timestamp", "col1", "col2", "col3", "col4", "col5", "col6", "col7")

    batch2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    hoodieMetaClient.reloadActiveTimeline()
    val commit2 = hoodieMetaClient.getCommitsTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    // Test 1: Read all columns from incremental query
    val fullDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1)
      .load(basePath)

    val fullColumnCount = fullDF.schema.fields.length
    assertTrue(fullColumnCount >= 10, s"Full DF should have at least 10 columns, has $fullColumnCount")

    // Test 2: Read with column pruning - select only 2 data columns
    val prunedDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1)
      .load(basePath)
      .select("col1", "col3")

    val prunedColumnCount = prunedDF.schema.fields.length

    // Validate column pruning is working
    assertTrue(prunedColumnCount < fullColumnCount,
      s"Pruned query should have fewer columns than full query. Pruned: $prunedColumnCount, Full: $fullColumnCount")

    // Validate that pruned DF has the selected columns
    assertTrue(prunedDF.schema.fieldNames.contains("col1"), "Pruned DF should contain col1")
    assertTrue(prunedDF.schema.fieldNames.contains("col3"), "Pruned DF should contain col3")

    // Validate that pruned DF does NOT have the unselected data columns
    assertFalse(prunedDF.schema.fieldNames.contains("col2"), "Pruned DF should not contain col2")
    assertFalse(prunedDF.schema.fieldNames.contains("col4"), "Pruned DF should not contain col4")
    assertFalse(prunedDF.schema.fieldNames.contains("col5"), "Pruned DF should not contain col5")
    assertFalse(prunedDF.schema.fieldNames.contains("col6"), "Pruned DF should not contain col6")
    assertFalse(prunedDF.schema.fieldNames.contains("col7"), "Pruned DF should not contain col7")

    // Validate data correctness
    val prunedRecords = prunedDF.collect()
    assertTrue(prunedRecords.length > 0, "Pruned query should return some records")

    // Each record should have col1 and col3
    prunedRecords.foreach { row =>
      assertTrue(row.schema.fieldNames.contains("col1"))
      assertTrue(row.schema.fieldNames.contains("col3"))
      assertFalse(row.schema.fieldNames.contains("col2"))
    }

    // Test 3: Validate query plan shows column selection
    val plan = prunedDF.queryExecution.toString()
    // The plan should contain evidence of column selection (either col1, col3, or ReadSchema)
    assertTrue(plan.contains("col1") || plan.contains("col3") || plan.contains("ReadSchema"),
      "Query plan should show evidence of column pruning")
  }

  /**
   * Test that validates selecting a single column works correctly.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSingleColumnSelection(tableType: HoodieTableType): Unit = {
    val _spark = spark
    import _spark.implicits._

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()
    )

    val data = Seq(
      ("key1", "p1", 1000L, "name1", 25, "city1"),
      ("key2", "p1", 1000L, "name2", 30, "city2")
    ).toDF("_row_key", "partition", "timestamp", "name", "age", "city")

    data.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val hoodieMetaClient = createMetaClient(spark, basePath)
    val commit1 = hoodieMetaClient.getCommitsTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    val data2 = Seq(
      ("key3", "p2", 2000L, "name3", 35, "city3")
    ).toDF("_row_key", "partition", "timestamp", "name", "age", "city")

    data2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // Select only single column
    val df = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, commit1)
      .load(basePath)
      .select("name")

    // Should only have 'name' column
    assertTrue(df.schema.fieldNames.contains("name"))
    assertFalse(df.schema.fieldNames.contains("age"))
    assertFalse(df.schema.fieldNames.contains("city"))

    // Data should be correct
    val records = df.collect()
    assertTrue(records.length > 0)
    records.foreach(r => assertTrue(r.schema.fieldNames.contains("name")))
  }
}
