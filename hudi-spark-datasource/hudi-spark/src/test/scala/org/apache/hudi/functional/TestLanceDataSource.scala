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
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters.asScalaIteratorConverter

/**
 * Basic functional tests for Lance file format with Hudi Spark datasource.
 */
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

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testBasicWriteAndRead(tableType: String): Unit = {
    val tableName = s"test_lance_table_${tableType.toLowerCase}"
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
      .option(TABLE_TYPE.key(), tableType)
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

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testSchemaProjection(tableType: String): Unit = {
    val tableName = s"test_lance_projection_${tableType.toLowerCase}"
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
      .option(TABLE_TYPE.key(), tableType)
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

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testWhereClauseFiltering(tableType: String): Unit = {
    val tableName = s"test_lance_where_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Create test data
    val records = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4)
    )
    val df = spark.createDataFrame(records).toDF("id", "name", "age", "score")

    // Write to Hudi table with Lance format
    df.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Test 1: Simple WHERE clause on numeric column
    val filteredByAge = spark.read
      .format("hudi")
      .load(tablePath)
      .where("age > 30")
      .orderBy("id")
      .collect()

    assertEquals(2, filteredByAge.length, "Should filter to 2 records with age > 30")
    assertEquals("Charlie", filteredByAge(0).getAs[String]("name"))
    assertEquals(35, filteredByAge(0).getAs[Int]("age"))
    assertEquals("Eve", filteredByAge(1).getAs[String]("name"))
    assertEquals(32, filteredByAge(1).getAs[Int]("age"))

    // Test 2: WHERE clause on string column
    val filteredByName = spark.read
      .format("hudi")
      .load(tablePath)
      .where("name = 'Bob'")
      .collect()

    assertEquals(1, filteredByName.length, "Should filter to 1 record with name = Bob")
    assertEquals(2, filteredByName(0).getAs[Int]("id"))
    assertEquals(25, filteredByName(0).getAs[Int]("age"))

    // Test 3: Complex WHERE with multiple conditions
    val filteredComplex = spark.read
      .format("hudi")
      .load(tablePath)
      .where("age >= 28 AND score > 90")
      .orderBy("id")
      .collect()

    assertEquals(3, filteredComplex.length, "Should filter to 3 records")
    assertEquals("Alice", filteredComplex(0).getAs[String]("name"))
    assertEquals("Charlie", filteredComplex(1).getAs[String]("name"))
    assertEquals("Eve", filteredComplex(2).getAs[String]("name"))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testMultipleBulkInsertsWithCommitValidation(tableType: String): Unit = {
    val tableName = s"test_lance_multiple_inserts_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = spark.createDataFrame(records1).toDF("id", "name", "age", "score")

    df1.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "bulk_insert")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Second insert - records 4-6
    val records2 = Seq(
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score")

    df2.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "bulk_insert")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Third insert - records 7-9
    val records3 = Seq(
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8)
    )
    val df3 = spark.createDataFrame(records3).toDF("id", "name", "age", "score")

    df3.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "bulk_insert")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Validate number of commits matches number of inserts
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitCount = metaClient.getCommitsTimeline.filterCompletedInstants().countInstants()
    assertEquals(3, commitCount, "Should have 3 completed commits (one per insert)")

    // Verify that all commits are bulk_insert commits
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
    assertEquals(3, commits.size, "Should have exactly 3 commits")

    // Read back all data and verify total record count
    val readDf = spark.read
      .format("hudi")
      .load(tablePath)

    val result = readDf.select("id", "name", "age", "score")
      .orderBy("id")
      .collect()

    // Verify we have all 9 records
    assertEquals(9, result.length, "Should read 9 records from all 3 inserts")

    // Spot check a few records from each insert
    // From first insert
    assertEquals(1, result(0).getInt(0))
    assertEquals("Alice", result(0).getString(1))
    assertEquals(30, result(0).getInt(2))
    assertEquals(95.5, result(0).getDouble(3), 0.01)

    // From second insert
    assertEquals(5, result(4).getInt(0))
    assertEquals("Eve", result(4).getString(1))
    assertEquals(32, result(4).getInt(2))
    assertEquals(91.4, result(4).getDouble(3), 0.01)

    // From third insert
    assertEquals(9, result(8).getInt(0))
    assertEquals("Iris", result(8).getString(1))
    assertEquals(26, result(8).getInt(2))
    assertEquals(94.8, result(8).getDouble(3), 0.01)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testTimeTravel(tableType: String): Unit = {
    val tableName = s"test_lance_time_travel_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = spark.createDataFrame(records1).toDF("id", "name", "age", "score")

    df1.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Second insert - records 4-6
    val records2 = Seq(
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score")

    df2.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Get the commit timestamp after second insert
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
    assertEquals(2, commits.size, "Should have 2 commits after second insert")
    val secondCommitTime = commits(1).requestedTime()

    // Third insert - records 7-9
    val records3 = Seq(
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8)
    )
    val df3 = spark.createDataFrame(records3).toDF("id", "name", "age", "score")

    df3.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Time travel query to second commit (should see data from c1 + c2 only)
    val timeTravelDf = spark.read
      .format("hudi")
      .option("as.of.instant", secondCommitTime)
      .load(tablePath)

    val result = timeTravelDf.select("id", "name", "age", "score")
      .orderBy("id")
      .collect()

    // Verify we have exactly 6 records (from first two commits)
    assertEquals(6, result.length, "Should read 6 records from first two commits")

    // Verify we have records 1-6 (not 7-9 from third commit)
    for (i <- 0 until 6) {
      assertEquals(i + 1, result(i).getInt(0), s"Record $i should have id ${i + 1}")
    }

    // Spot check a couple of records
    assertEquals("Alice", result(0).getString(1))
    assertEquals(95.5, result(0).getDouble(3), 0.01)
    assertEquals("Frank", result(5).getString(1))
    assertEquals(85.7, result(5).getDouble(3), 0.01)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testMultipleRegularInsertsWithCommitValidation(tableType: String): Unit = {
    val tableName = s"test_lance_regular_inserts_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3 using regular insert
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = spark.createDataFrame(records1).toDF("id", "name", "age", "score")

    df1.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "insert")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Second insert - records 4-6 using regular insert
    val records2 = Seq(
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score")

    df2.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "insert")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Validate number of commits matches number of inserts
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitCount = metaClient.getCommitsTimeline.filterCompletedInstants().countInstants()
    assertEquals(2, commitCount, "Should have 2 completed commits (one per insert)")

    // Verify that all commits are insert commits
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
    assertEquals(2, commits.size, "Should have exactly 2 commits")

    // Verify commit action types based on table type
    val expectedAction = if (tableType == "COPY_ON_WRITE") "commit" else "deltacommit"
    commits.foreach { instant =>
      assertEquals(expectedAction, instant.getAction,
        s"Instant ${instant.requestedTime()} should be a $expectedAction action for $tableType table")
    }

    // Read back all data and verify total record count
    val readDf = spark.read
      .format("hudi")
      .load(tablePath)

    val result = readDf.select("id", "name", "age", "score")
      .orderBy("id")
      .collect()

    // Verify we have all 6 records
    assertEquals(6, result.length, "Should read 6 records from both inserts")

    // Spot check records from first insert
    assertEquals(1, result(0).getInt(0))
    assertEquals("Alice", result(0).getString(1))
    assertEquals(30, result(0).getInt(2))
    assertEquals(95.5, result(0).getDouble(3), 0.01)

    assertEquals(3, result(2).getInt(0))
    assertEquals("Charlie", result(2).getString(1))
    assertEquals(35, result(2).getInt(2))
    assertEquals(92.1, result(2).getDouble(3), 0.01)

    // Spot check records from second insert
    assertEquals(4, result(3).getInt(0))
    assertEquals("David", result(3).getString(1))
    assertEquals(28, result(3).getInt(2))
    assertEquals(88.9, result(3).getDouble(3), 0.01)

    assertEquals(6, result(5).getInt(0))
    assertEquals("Frank", result(5).getString(1))
    assertEquals(27, result(5).getInt(2))
    assertEquals(85.7, result(5).getDouble(3), 0.01)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testBasicUpsertModifyExistingRow(tableType: String): Unit = {
    val tableName = s"test_lance_upsert_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Initial insert - 3 records
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = spark.createDataFrame(records1).toDF("id", "name", "age", "score")

    df1.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "insert")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Upsert - modify Bob's record (id=2)
    val records2 = Seq(
      (2, "Bob", 40, 95.0)  // Update Bob: age 25->40, score 87.3->95.0
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score")

    df2.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "upsert")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Second upsert - modify Alice (id=1) and insert David (id=4)
    val records3 = Seq(
      (1, "Alice", 45, 98.5),  // Update Alice: age 30->45, score 95.5->98.5
      (4, "David", 28, 88.0)   // Insert new record
    )
    val df3 = spark.createDataFrame(records3).toDF("id", "name", "age", "score")

    df3.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "upsert")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Validate commits
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitCount = metaClient.getCommitsTimeline.filterCompletedInstants().countInstants()
    assertEquals(3, commitCount, "Should have 3 completed commits (insert + 2 upserts)")

    // Verify commit action types based on table type
    val expectedAction = if (tableType == "COPY_ON_WRITE") "commit" else "deltacommit"
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
    commits.foreach { instant =>
      assertEquals(expectedAction, instant.getAction,
        s"Instant ${instant.requestedTime()} should be a $expectedAction action for $tableType table")
    }

    // Read and verify data
    val readDf = spark.read.format("hudi").load(tablePath)
    val result = readDf.select("id", "name", "age", "score").orderBy("id").collect()

    // Should have exactly 4 records (no duplicates)
    assertEquals(4, result.length, "Should have exactly 4 records after second upsert")

    // Verify Alice was updated
    assertEquals(1, result(0).getInt(0))
    assertEquals("Alice", result(0).getString(1))
    assertEquals(45, result(0).getInt(2), "Alice's age should be updated to 45")
    assertEquals(98.5, result(0).getDouble(3), 0.01, "Alice's score should be updated to 98.5")

    // Verify Bob was updated (from first upsert)
    assertEquals(2, result(1).getInt(0))
    assertEquals("Bob", result(1).getString(1))
    assertEquals(40, result(1).getInt(2), "Bob's age should be updated to 40")
    assertEquals(95.0, result(1).getDouble(3), 0.01, "Bob's score should be updated to 95.0")

    // Verify Charlie unchanged
    assertEquals(3, result(2).getInt(0))
    assertEquals("Charlie", result(2).getString(1))
    assertEquals(35, result(2).getInt(2))
    assertEquals(92.1, result(2).getDouble(3), 0.01)

    // Verify David was inserted
    assertEquals(4, result(3).getInt(0))
    assertEquals("David", result(3).getString(1))
    assertEquals(28, result(3).getInt(2))
    assertEquals(88.0, result(3).getDouble(3), 0.01)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testBasicDeleteOperation(tableType: String): Unit = {
    val tableName = s"test_lance_delete_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Initial insert - 5 records
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.0),
      (5, "Eve", 32, 91.4)
    )
    val df1 = spark.createDataFrame(records1).toDF("id", "name", "age", "score")

    df1.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "insert")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Delete operation - delete Bob (id=2), David (id=4), and a non-existent key (id=99)
    val recordsToDelete = Seq(
      (2, "Bob", 25, 87.3),        // Delete Bob (exists)
      (4, "David", 28, 88.0),      // Delete David (exists)
      (99, "NonExistent", 50, 0.0) // Delete non-existent record (should be no-op)
    )
    val deleteDF = spark.createDataFrame(recordsToDelete).toDF("id", "name", "age", "score")

    deleteDF.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "delete")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Validate commits
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitCount = metaClient.getCommitsTimeline.filterCompletedInstants().countInstants()
    assertEquals(2, commitCount, "Should have 2 completed commits (insert + delete)")

    // Verify commit action types based on table type
    val expectedAction = if (tableType == "COPY_ON_WRITE") "commit" else "deltacommit"
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
    commits.foreach { instant =>
      assertEquals(expectedAction, instant.getAction,
        s"Instant ${instant.requestedTime()} should be a $expectedAction action for $tableType table")
    }

    // Read and verify data
    val readDf = spark.read.format("hudi").load(tablePath)
    val result = readDf.select("id", "name", "age", "score").orderBy("id").collect()

    // Should have exactly 3 records (5 original - 2 deleted)
    assertEquals(3, result.length, "Should have exactly 3 records after delete")

    // Verify Alice is still present
    assertEquals(1, result(0).getInt(0))
    assertEquals("Alice", result(0).getString(1))
    assertEquals(30, result(0).getInt(2))
    assertEquals(95.5, result(0).getDouble(3), 0.01)

    // Verify Bob (id=2) was deleted - Charlie should be second record now
    assertEquals(3, result(1).getInt(0), "Second record should be Charlie (id=3) after Bob was deleted")
    assertEquals("Charlie", result(1).getString(1))
    assertEquals(35, result(1).getInt(2))
    assertEquals(92.1, result(1).getDouble(3), 0.01)

    // Verify David (id=4) was deleted - Eve should be third record now
    assertEquals(5, result(2).getInt(0), "Third record should be Eve (id=5) after David was deleted")
    assertEquals("Eve", result(2).getString(1))
    assertEquals(32, result(2).getInt(2))
    assertEquals(91.4, result(2).getDouble(3), 0.01)

    // Verify Bob and David are NOT in the results
    val ids = result.map(_.getInt(0)).toSet
    assert(!ids.contains(2), "Bob (id=2) should be deleted")
    assert(!ids.contains(4), "David (id=4) should be deleted")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testIncrementalQuery(tableType: String): Unit = {
    val tableName = s"test_lance_incremental_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = spark.createDataFrame(records1).toDF("id", "name", "age", "score")

    df1.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Second insert - records 4-6
    val records2 = Seq(
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score")

    df2.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Get commit timestamps
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitsAfterSecond = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
    assertEquals(2, commitsAfterSecond.size, "Should have 2 commits after second insert")
    val secondCommitTime = commitsAfterSecond(1).getCompletionTime

    // Third insert - records 7-9
    val records3 = Seq(
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8)
    )
    val df3 = spark.createDataFrame(records3).toDF("id", "name", "age", "score")

    df3.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Reload metaClient to get latest commits
    metaClient.reloadActiveTimeline()
    val allCommits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
    assertEquals(3, allCommits.size, "Should have 3 commits after third insert")
    val thirdCommitTime = allCommits(2).getCompletionTime

    // Incremental query from c2 to c3 (should see only data from c3)
    val incrementalDf = spark.read
      .format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", secondCommitTime)
      .option("hoodie.datasource.read.end.instanttime", thirdCommitTime)
      .load(tablePath)

    val result = incrementalDf.select("id", "name", "age", "score")
      .orderBy("id")
      .collect()

    // Verify we have exactly 3 records (only from third commit)
    assertEquals(3, result.length, "Should only read last 3 records from incremental query (c2 to c3)")

    // Verify we have records 7-9 (from third commit only)
    assertEquals(7, result(0).getInt(0))
    assertEquals("Grace", result(0).getString(1))
    assertEquals(29, result(0).getInt(2))
    assertEquals(93.2, result(0).getDouble(3), 0.01)

    assertEquals(8, result(1).getInt(0))
    assertEquals("Henry", result(1).getString(1))
    assertEquals(31, result(1).getInt(2))
    assertEquals(89.6, result(1).getDouble(3), 0.01)

    assertEquals(9, result(2).getInt(0))
    assertEquals("Iris", result(2).getString(1))
    assertEquals(26, result(2).getInt(2))
    assertEquals(94.8, result(2).getDouble(3), 0.01)
  }
}
