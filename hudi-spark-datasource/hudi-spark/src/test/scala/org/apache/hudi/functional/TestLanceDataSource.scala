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
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, DefaultSparkRecordMerger}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.{HoodieCleanConfig, HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConverters._
import scala.util.Random

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

  /**
   * Helper method to count log files in a non-partitioned Hudi table using FileSystemView API.
   * @param tablePath The path to the Hudi table
   * @return The total count of log files across all file slices
   */
  private def countLogFiles(tablePath: String): Int = {
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val completedTimeline = metaClient.getCommitsTimeline.filterCompletedInstants()
    val latestCommit = if (completedTimeline.lastInstant().isPresent) {
      Some(completedTimeline.lastInstant().get().requestedTime())
    } else {
      None
    }

    var fsView: HoodieTableFileSystemView = null

    try {
      fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        context, metaClient, completedTimeline)

      // Get all partition paths (for non-partitioned table, returns list with base path)
      val partitionPaths = FSUtils.getAllPartitionPaths(
        context, metaClient, false).asScala

      var totalLogFiles = 0

      partitionPaths.foreach { partitionPath =>
        // Get file slices as of latest commit
        val fileSlicesStream = latestCommit match {
          case Some(commit) =>
            fsView.getLatestFileSlicesBeforeOrOn(partitionPath, commit, false)
          case None =>
            fsView.getLatestFileSlices(partitionPath)
        }

        fileSlicesStream.iterator().asScala.foreach { fileSlice =>
          totalLogFiles += fileSlice.getLogFiles.count().toInt
        }
      }

      totalLogFiles
    } finally {
      if (fsView != null) {
        fsView.close()
      }
    }
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

  @ParameterizedTest
  @ValueSource(strings = Array("MERGE_ON_READ"))
  def testMORCompactionWithLanceBaseAndAvroLog(tableType: String): Unit = {
    //TODO needed to explicitly set upsert operation in order to trigger compaction
    val tableName = s"test_lance_compaction_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3 (initial base file)
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
      .option(OPERATION.key(), "upsert")
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Second update - update existing records 1-3 (creates log file - delta commit 1)
    val records2 = Seq(
      (1, "Alice", 31, 96.0),
      (2, "Bob", 26, 88.0),
      (3, "Charlie", 36, 93.0)
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
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify Avro log files exist before compaction (after 2nd update, before 3rd)
    val logFilesBeforeCompaction = countLogFiles(tablePath)
    assertTrue(logFilesBeforeCompaction >= 1,
      s"Should have at least 1 Avro log file before compaction, found $logFilesBeforeCompaction")

    // Third update - update existing records 1-3 again (creates another log file - delta commit 2)
    val records3 = Seq(
      (1, "Alice", 32, 97.0),
      (2, "Bob", 27, 89.0),
      (3, "Charlie", 37, 94.0)
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
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Fourth update - update existing records 1-3 again (creates another log file - delta commit 3)
    // This should trigger compaction after 3 delta commits
    val records4 = Seq(
      (1, "Alice", 33, 98.0),
      (2, "Bob", 28, 90.0),
      (3, "Charlie", 38, 95.0)
    )
    val df4 = spark.createDataFrame(records4).toDF("id", "name", "age", "score")

    df4.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "upsert")
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true")
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify final data - should only have 3 rows with latest values
    val result = spark.read
      .format("hudi")
      .load(tablePath)
      .orderBy("id")
      .collect()

    assertEquals(3, result.length, "Should have 3 records (not 12, since we're updating the same keys)")

    // Verify the latest values
    assertEquals(1, result(0).getAs[Int]("id"))
    assertEquals("Alice", result(0).getAs[String]("name"))
    assertEquals(33, result(0).getAs[Int]("age"))
    assertEquals(98.0, result(0).getAs[Double]("score"), 0.01)

    assertEquals(2, result(1).getAs[Int]("id"))
    assertEquals("Bob", result(1).getAs[String]("name"))
    assertEquals(28, result(1).getAs[Int]("age"))
    assertEquals(90.0, result(1).getAs[Double]("score"), 0.01)

    // Reload metaClient to get latest commits
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val allInstants = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList

    // Should have: 1 initial deltacommit + 3 more deltacommits + exactly 1 compaction commit
    // Compaction triggers after 3 delta commits (on the 4th write)
    val deltaCommits = allInstants.filter(_.getAction == "deltacommit")
    val compactionCommits = allInstants.filter(_.getAction == "commit")

    assertEquals(1, compactionCommits.size, s"Should have exactly 1 compaction commit, found ${compactionCommits.size}")
    assertTrue(deltaCommits.size >= 3, s"Should have at least 3 delta commits before compaction, found ${deltaCommits.size}")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testClusteringWithLanceBase(tableType: String): Unit = {
    val tableName = s"test_lance_clustering_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Generate simple test data (avoiding complex nested schemas)
    val records = (1 to 1000).map { i =>
      (s"key_$i", s"rider_$i", s"driver_$i", i.toLong, i * 1.5, "2025-01-01")
    }
    val inputDF = spark.createDataFrame(records)
      .toDF("id", "rider", "driver", "timestamp", "distance", "partition")

    // Write with inline clustering enabled
    inputDF.write.format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "timestamp")
      .option(PARTITIONPATH_FIELD.key(), "partition")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(OPERATION.key(), "bulk_insert")
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "1")
      .option("hoodie.clustering.plan.strategy.sort.columns", "rider,driver")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Verify temp files are cleaned after clustering
    val tempPath = new Path(tablePath, HoodieTableMetaClient.TEMPFOLDER_NAME)
    val fs = tempPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    assertTrue(fs.listStatus(tempPath).isEmpty, "Temp folder should be empty after clustering")

    // Verify data correctness
    val resultDF = spark.read.format("hudi").load(tablePath)
    assertEquals(1000, resultDF.count(), "Should have 1000 records after clustering")

    // Verify table metadata and clustering instant
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
    assertTrue(commits.nonEmpty, "Should have at least one commit after clustering")

    // Verify clustering actually ran by checking for replacecommit action
    val replaceCommits = commits.filter(_.getAction == "replacecommit")
    assertTrue(replaceCommits.nonEmpty, "Should have at least one replacecommit (clustering) on timeline")

    // Verify clustering instant exists
    assertTrue(metaClient.getActiveTimeline.getLastClusteringInstant.isPresent,
      "Should have a clustering instant on timeline")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testSchemaEvolutionAddColumn(tableType: String): Unit = {
    val tableName = s"test_lance_schema_evolution_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert with base schema - columns: (id, name, age, score)
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

    // Schema evolution - add new column "email" and upsert existing records
    val records2 = Seq(
      (1, "Alice", 31, 96.0, "alice@example.com"),
      (2, "Bob", 26, 88.0, "bob@example.com")
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score", "email")

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

    // Insert a new record with the evolved schema
    val records3 = Seq(
      (4, "David", 28, 89.5, "david@example.com")
    )
    val df3 = spark.createDataFrame(records3).toDF("id", "name", "age", "score", "email")

    df3.write
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

    // Read and verify schema evolution
    val result = spark.read
      .format("hudi")
      .load(tablePath)
      .orderBy("id")
      .collect()

    // Verify we have 4 records
    assertEquals(4, result.length, "Should have 4 records after schema evolution")

    // Verify schema includes the new "email" column
    val schema = spark.read.format("hudi").load(tablePath).schema
    assertTrue(schema.fieldNames.contains("email"), "Schema should include 'email' column")

    // Verify record 1 - Alice (updated with email)
    assertEquals(1, result(0).getAs[Int]("id"))
    assertEquals("Alice", result(0).getAs[String]("name"))
    assertEquals(31, result(0).getAs[Int]("age"))
    assertEquals(96.0, result(0).getAs[Double]("score"), 0.01)
    assertEquals("alice@example.com", result(0).getAs[String]("email"))

    // Verify record 2 - Bob (updated with email)
    assertEquals(2, result(1).getAs[Int]("id"))
    assertEquals("Bob", result(1).getAs[String]("name"))
    assertEquals(26, result(1).getAs[Int]("age"))
    assertEquals(88.0, result(1).getAs[Double]("score"), 0.01)
    assertEquals("bob@example.com", result(1).getAs[String]("email"))

    // Verify record 3 - Charlie (original record, email should be null)
    assertEquals(3, result(2).getAs[Int]("id"))
    assertEquals("Charlie", result(2).getAs[String]("name"))
    assertEquals(35, result(2).getAs[Int]("age"))
    assertEquals(92.1, result(2).getAs[Double]("score"), 0.01)
    assertTrue(result(2).isNullAt(result(2).fieldIndex("email")), "Charlie's email should be null")

    // Verify record 4 - David (new record with email)
    assertEquals(4, result(3).getAs[Int]("id"))
    assertEquals("David", result(3).getAs[String]("name"))
    assertEquals(28, result(3).getAs[Int]("age"))
    assertEquals(89.5, result(3).getAs[Double]("score"), 0.01)
    assertEquals("david@example.com", result(3).getAs[String]("email"))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testVectorColumnSupport(tableType: String): Unit = {
    val tableName = s"test_lance_vector_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Create schema with vector column
    val schema = new StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("embeddings", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    // Create test data with vectors
    val rows = (1 to 5).map { i =>
      val vector = (0 until 128).map(j => i * 0.01f + j * 0.001f).toArray
      Row(i, s"item_$i", vector)
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )

    // Write with Lance format
    df.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "id")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .option(HoodieWriteConfig.LANCE_VECTOR_COLUMNS.key(), "embeddings:128")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Read and verify
    val result = spark.read.format("hudi").load(tablePath).orderBy("id").collect()
    assertEquals(5, result.length, "Should have 5 records")

    // Verify first row vector
    val firstRow = result(0)
    assertEquals(1, firstRow.getAs[Int]("id"))
    assertEquals("item_1", firstRow.getAs[String]("name"))

    val embeddings = firstRow.getSeq[Float](firstRow.fieldIndex("embeddings"))
    assertEquals(128, embeddings.size, "Vector should have 128 dimensions")

    // Verify a few vector values
    assertEquals(0.01f, embeddings(0), 0.0001f, "First embedding value should be correct")
    assertEquals(0.011f, embeddings(1), 0.0001f, "Second embedding value should be correct")
    assertEquals(0.137f, embeddings(127), 0.0001f, "Last embedding value should be correct")

    // Verify last row vector
    val lastRow = result(4)
    assertEquals(5, lastRow.getAs[Int]("id"))
    assertEquals("item_5", lastRow.getAs[String]("name"))

    val lastEmbeddings = lastRow.getSeq[Float](lastRow.fieldIndex("embeddings"))
    assertEquals(128, lastEmbeddings.size, "Vector should have 128 dimensions")
    assertEquals(0.05f, lastEmbeddings(0), 0.0001f, "First embedding value of last row should be correct")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testBlobColumnSupport(tableType: String): Unit = {
    val tableName = s"test_lance_blob_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Create schema with blob column
    val schema = new StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("data", BinaryType, nullable = true)
    ))

    // Create test data with large blobs (100KB each)
    val random = new Random(42)
    val rows = (1 to 5).map { i =>
      val largeData = new Array[Byte](100000) // 100KB
      random.nextBytes(largeData)
      Row(i, largeData)
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )

    // Write with Lance format
    df.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "id")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Read and verify
    val result = spark.read.format("hudi").load(tablePath).orderBy("id").collect()
    assertEquals(5, result.length, "Should have 5 records")

    // Verify records can be read successfully
    val firstRow = result(0)
    assertEquals(1, firstRow.getAs[Int]("id"))

    // Verify blob data IS materialized (with LanceFileWriter, blobs are embedded)
    val blobData = firstRow.get(firstRow.fieldIndex("data"))
    assertTrue(blobData != null, "Blob data field should not be null")
    assertTrue(blobData.isInstanceOf[Array[Byte]], "Blob data should be byte array")
    val blobBytes = blobData.asInstanceOf[Array[Byte]]
    assertEquals(100000, blobBytes.length, "Blob data should be materialized (100KB)")

    // Verify all IDs are present
    val ids = result.map(_.getAs[Int]("id")).toSet
    assertEquals(5, ids.size, "Should have 5 unique IDs")
    (1 to 5).foreach { i =>
      assertTrue(ids.contains(i), s"Should contain ID $i")
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testVectorColumnDimensionValidation(tableType: String): Unit = {
    val tableName = s"test_lance_vector_validation_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Create schema with vector column
    val schema = new StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("embeddings", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    // First insert with correct dimension (128)
    val correctRows = (1 to 3).map { i =>
      val vector = (0 until 128).map(j => i * 0.01f + j * 0.001f).toArray
      Row(i, vector)
    }

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(correctRows), schema)

    df1.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "id")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.LANCE_VECTOR_COLUMNS.key(), "embeddings:128")
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Verify correct insert succeeded
    val result1 = spark.read.format("hudi").load(tablePath).collect()
    assertEquals(3, result1.length, "Should have 3 records after initial insert")

    // Verify vectors have correct dimension
    val firstVec = result1(0).getSeq[Float](result1(0).fieldIndex("embeddings"))
    assertEquals(128, firstVec.size, "Initial vectors should have 128 dimensions")

    // Try to insert with WRONG dimension (64 instead of 128) - should fail
    val wrongDimensionRows = Seq(
      Row(4, (0 until 64).map(j => 0.01f * j).toArray) // Only 64 dimensions!
    )

    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(wrongDimensionRows), schema)

    // Expect this to fail with dimension mismatch error
    val exception = assertThrows(classOf[Exception], new org.junit.jupiter.api.function.Executable {
      def execute(): Unit = {
        df2.write
          .format("hudi")
          .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
          .option(TABLE_TYPE.key(), tableType)
          .option(RECORDKEY_FIELD.key(), "id")
          .option(PRECOMBINE_FIELD.key(), "id")
          .option(TABLE_NAME.key(), tableName)
          .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
          .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
          .mode(SaveMode.Append)
          .save(tablePath)
      }
    })

    // Print exception details for debugging
    println(s"Caught expected exception: ${exception.getClass.getName}")
    println(s"Exception message: ${exception.getMessage}")

    // Verify only original 3 records remain (write should have failed)
    val result2 = spark.read.format("hudi").load(tablePath).orderBy("id").collect()
    assertEquals(3, result2.length, "Should still have only 3 records after failed insert attempt")
  }

}