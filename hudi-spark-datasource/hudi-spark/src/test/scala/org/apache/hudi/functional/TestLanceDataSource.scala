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
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, MethodSource}

import scala.collection.JavaConverters._

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

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testBasicWriteAndRead(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_table_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Create test data
    val records = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val expectedDf = createDataFrame(records)

    // Write to Hudi table with Lance base file format
    writeDataframe(tableType, tableName, tablePath, expectedDf, saveMode = SaveMode.Overwrite)

    // Read back and verify
    val readDf = spark.read
      .format("hudi")
      .load(tablePath)

    val actual = readDf.select("id", "name", "age", "score")

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSchemaProjection(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_projection_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Create test data with multiple columns
    val records = Seq(
      (1, "Alice", 30, 95.5, "Engineering"),
      (2, "Bob", 25, 87.3, "Sales"),
      (3, "Charlie", 35, 92.1, "Marketing")
    )
    val inputDf = spark.createDataFrame(records).toDF("id", "name", "age", "score", "department")

    // Write to Hudi table with Lance format
    writeDataframe(tableType, tableName, tablePath, inputDf, saveMode = SaveMode.Overwrite)

    // Read with schema projection - only select subset of columns
    val readDf = spark.read
      .format("hudi")
      .load(tablePath)

    val expectedDf = spark.createDataFrame(Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    )).toDF("id", "name")

    val actual = readDf.select("id", "name")

    // Verify schema projection - should only have 2 columns
    assertEquals(2, actual.schema.fields.length, "Should only have 2 columns")

    // Verify data equality
    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testWhereClauseFiltering(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_where_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Create test data
    val records = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4)
    )
    val df = createDataFrame(records)

    // Write to Hudi table with Lance format
    writeDataframe(tableType, tableName, tablePath, df, saveMode = SaveMode.Overwrite)

    // Test 1: Simple WHERE clause on numeric column
    val filteredByAge = spark.read
      .format("hudi")
      .load(tablePath)
      .where("age > 30")
      .select("id", "name", "age", "score")

    val expectedFilteredByAge = createDataFrame(Seq(
      (3, "Charlie", 35, 92.1),
      (5, "Eve", 32, 91.4)
    ))

    assertTrue(expectedFilteredByAge.except(filteredByAge).isEmpty)
    assertTrue(filteredByAge.except(expectedFilteredByAge).isEmpty)

    // Test 2: WHERE clause on string column
    val filteredByName = spark.read
      .format("hudi")
      .load(tablePath)
      .where("name = 'Bob'")
      .select("id", "name", "age", "score")

    val expectedFilteredByName = createDataFrame(Seq((2, "Bob", 25, 87.3)))

    assertTrue(expectedFilteredByName.except(filteredByName).isEmpty)
    assertTrue(filteredByName.except(expectedFilteredByName).isEmpty)

    // Test 3: Complex WHERE with multiple conditions
    val filteredComplex = spark.read
      .format("hudi")
      .load(tablePath)
      .where("age >= 28 AND score > 90")
      .select("id", "name", "age", "score")

    val expectedFilteredComplex = createDataFrame(Seq(
      (1, "Alice", 30, 95.5),
      (3, "Charlie", 35, 92.1),
      (5, "Eve", 32, 91.4)
    ))

    assertTrue(expectedFilteredComplex.except(filteredComplex).isEmpty)
    assertTrue(filteredComplex.except(expectedFilteredComplex).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testMultipleBulkInsertsWithCommitValidation(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_multiple_inserts_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = createDataFrame(records1)

    writeDataframe(tableType, tableName, tablePath, df1, operation = Some("bulk_insert"))

    // Second insert - records 4-6
    val records2 = Seq(
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    )
    val df2 = createDataFrame(records2)

    writeDataframe(tableType, tableName, tablePath, df2, operation = Some("bulk_insert"))

    // Third insert - records 7-9
    val records3 = Seq(
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8)
    )
    val df3 = createDataFrame(records3)

    writeDataframe(tableType, tableName, tablePath, df3, operation = Some("bulk_insert"))

    // Validate number of commits matches number of inserts
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitCount = metaClient.getCommitsTimeline.filterCompletedInstants().countInstants()
    assertEquals(3, commitCount, "Should have 3 completed commits (one per insert)")

    // Verify that all commits are bulk_insert commits
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.asScala.toList
    assertEquals(3, commits.size, "Should have exactly 3 commits")

    // Read back all data and verify total record count
    val readDf = spark.read
      .format("hudi")
      .load(tablePath)

    val actual = readDf.select("id", "name", "age", "score")

    val expectedDf = createDataFrame(Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7),
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8)
    ))

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testTimeTravel(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_time_travel_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = createDataFrame(records1)

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite)

    // Second insert - records 4-6
    val records2 = Seq(
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    )
    val df2 = createDataFrame(records2)

    writeDataframe(tableType, tableName, tablePath, df2)

    // Get the commit timestamp after second insert
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.asScala.toList
    assertEquals(2, commits.size, "Should have 2 commits after second insert")
    val secondCommitTime = commits(1).requestedTime()

    // Third insert - records 7-9
    val records3 = Seq(
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8)
    )
    val df3 = createDataFrame(records3)

    writeDataframe(tableType, tableName, tablePath, df3)

    // Time travel query to second commit (should see data from c1 + c2 only)
    val timeTravelDf = spark.read
      .format("hudi")
      .option("as.of.instant", secondCommitTime)
      .load(tablePath)

    val actual = timeTravelDf.select("id", "name", "age", "score")

    val expectedDf = createDataFrame(Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    ))

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testMultipleRegularInsertsWithCommitValidation(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_regular_inserts_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3 using regular insert
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = createDataFrame(records1)

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite, operation = Some("insert"))

    // Second insert - records 4-6 using regular insert
    val records2 = Seq(
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    )
    val df2 = createDataFrame(records2)

    writeDataframe(tableType, tableName, tablePath, df2, operation = Some("insert"))

    // Validate number of commits matches number of inserts
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitCount = metaClient.getCommitsTimeline.filterCompletedInstants().countInstants()
    assertEquals(2, commitCount, "Should have 2 completed commits (one per insert)")

    // Verify that all commits are insert commits
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.asScala.toList
    assertEquals(2, commits.size, "Should have exactly 2 commits")

    // Verify commit action types based on table type
    val expectedAction = if (tableType == HoodieTableType.COPY_ON_WRITE) "commit" else "deltacommit"
    commits.foreach { instant =>
      assertEquals(expectedAction, instant.getAction,
        s"Instant ${instant.requestedTime()} should be a $expectedAction action for $tableType table")
    }

    // Read back all data and verify total record count
    val readDf = spark.read
      .format("hudi")
      .load(tablePath)

    val actual = readDf.select("id", "name", "age", "score")

    val expectedDf = createDataFrame(Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    ))

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testBasicUpsertModifyExistingRow(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_upsert_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Initial insert - 3 records
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = createDataFrame(records1)

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite, operation = Some("insert"))

    // Upsert - modify Bob's record (id=2)
    val records2 = Seq(
      (2, "Bob", 40, 95.0)  // Update Bob: age 25->40, score 87.3->95.0
    )
    val df2 = createDataFrame(records2)

    writeDataframe(tableType, tableName, tablePath, df2, operation = Some("upsert"))

    // Second upsert - modify Alice (id=1) and insert David (id=4)
    val records3 = Seq(
      (1, "Alice", 45, 98.5),  // Update Alice: age 30->45, score 95.5->98.5
      (4, "David", 28, 88.0)   // Insert new record
    )
    val df3 = createDataFrame(records3)

    writeDataframe(tableType, tableName, tablePath, df3, operation = Some("upsert"))

    // Validate commits
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitCount = metaClient.getCommitsTimeline.filterCompletedInstants().countInstants()
    assertEquals(3, commitCount, "Should have 3 completed commits (insert + 2 upserts)")

    // Verify commit action types based on table type
    val expectedAction = if (tableType == HoodieTableType.COPY_ON_WRITE) "commit" else "deltacommit"
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.asScala
    commits.foreach { instant =>
      assertEquals(expectedAction, instant.getAction,
        s"Instant ${instant.requestedTime()} should be a $expectedAction action for $tableType table")
    }

    // Read and verify data
    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "name", "age", "score")

    val expectedDf = createDataFrame(Seq(
      (1, "Alice", 45, 98.5),
      (2, "Bob", 40, 95.0),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.0)
    ))

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)

    if (tableType == HoodieTableType.MERGE_ON_READ) {
      // Write one more commit to trigger compaction
      val records4 = Seq(
        (1, "Alice", 50, 98.5),  // Update Alice: age 45->50
        (4, "David", 28, 90.0)   // Update David: score 88.0->90.0
      )
      val df4 = createDataFrame(records4)
      writeDataframe(tableType, tableName, tablePath, df4, operation = Some("upsert"),
        extraOptions = Map("hoodie.compact.inline" -> "true", "hoodie.compact.inline.max.delta.commits" -> "1"))
      val expectedDfAfterCompaction = createDataFrame(Seq(
        (1, "Alice", 50, 98.5),
        (2, "Bob", 40, 95.0),
        (3, "Charlie", 35, 92.1),
        (4, "David", 28, 90.0)
      ))
      // validate compaction commit is present
      val compactionCommits = metaClient.reloadActiveTimeline().filterCompletedInstants().getInstants.asScala
        .filter(instant => instant.getAction == "commit")
      assertTrue(compactionCommits.nonEmpty, "Compaction commit should be present after upsert")
      // Read and verify data after compaction
      val readDfAfterCompaction = spark.read.format("hudi").load(tablePath)
      val actualAfterCompaction = readDfAfterCompaction.select("id", "name", "age", "score")
      assertTrue(expectedDfAfterCompaction.except(actualAfterCompaction).isEmpty)
      assertTrue(actualAfterCompaction.except(expectedDfAfterCompaction).isEmpty)
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testBasicDeleteOperation(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_delete_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Initial insert - 5 records
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.0),
      (5, "Eve", 32, 91.4)
    )
    val df1 = createDataFrame(records1)

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite, operation = Some("insert"))

    // Delete operation - delete Bob (id=2), David (id=4), and a non-existent key (id=99)
    val recordsToDelete = Seq(
      (2, "Bob", 25, 87.3),        // Delete Bob (exists)
      (4, "David", 28, 88.0),      // Delete David (exists)
      (99, "NonExistent", 50, 0.0) // Delete non-existent record (should be no-op)
    )
    val deleteDF = createDataFrame(recordsToDelete)

    writeDataframe(tableType, tableName, tablePath, deleteDF, operation = Some("delete"))

    // Validate commits
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitCount = metaClient.getCommitsTimeline.filterCompletedInstants().countInstants()
    assertEquals(2, commitCount, "Should have 2 completed commits (insert + delete)")

    // Verify commit action types based on table type
    val expectedAction = if (tableType == HoodieTableType.COPY_ON_WRITE) "commit" else "deltacommit"
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.asScala
    commits.foreach { instant =>
      assertEquals(expectedAction, instant.getAction,
        s"Instant ${instant.requestedTime()} should be a $expectedAction action for $tableType table")
    }

    // Read and verify data
    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "name", "age", "score")

    val expectedDf = createDataFrame(Seq(
      (1, "Alice", 30, 95.5),
      (3, "Charlie", 35, 92.1),
      (5, "Eve", 32, 91.4)
    ))

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testIncrementalQuery(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_incremental_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert - records 1-3
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df1 = createDataFrame(records1)

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite)

    // Second insert - records 4-6
    val records2 = Seq(
      (4, "David", 28, 88.9),
      (5, "Eve", 32, 91.4),
      (6, "Frank", 27, 85.7)
    )
    val df2 = createDataFrame(records2)

    writeDataframe(tableType, tableName, tablePath, df2)

    // Get commit timestamps
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val commitsAfterSecond = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.asScala.toList
    assertEquals(2, commitsAfterSecond.size, "Should have 2 commits after second insert")
    val secondCommitTime = commitsAfterSecond(1).getCompletionTime

    // Third insert - records 7-9
    val records3 = Seq(
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8)
    )
    val df3 = createDataFrame(records3)

    writeDataframe(tableType, tableName, tablePath, df3)

    // Reload metaClient to get latest commits
    metaClient.reloadActiveTimeline()
    val allCommits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.asScala.toList
    assertEquals(3, allCommits.size, "Should have 3 commits after third insert")
    val thirdCommitTime = allCommits(2).getCompletionTime

    // Incremental query from c2 to c3 (should see only data from c3)
    val incrementalDf = spark.read
      .format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", secondCommitTime)
      .option("hoodie.datasource.read.end.instanttime", thirdCommitTime)
      .load(tablePath)

    val actual = incrementalDf.select("id", "name", "age", "score")

    val expectedDf = createDataFrame(Seq(
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8)
    ))

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testClustering(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_clustering_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Initial insert - 5 records
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1),
      (4, "David", 28, 88.0),
      (5, "Eve", 32, 91.4)
    )
    val df1 = createDataFrame(records1)

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite, operation = Some("bulk_insert"))

    // Second insert - 5 more records
    val records2 = Seq(
      (6, "Frank", 27, 85.7),
      (7, "Grace", 29, 93.2),
      (8, "Henry", 31, 89.6),
      (9, "Iris", 26, 94.8),
      (10, "Jack", 33, 90.5)
    )
    val df2 = createDataFrame(records2)
    writeDataframe(tableType, tableName, tablePath, df2, operation = Some("bulk_insert"), extraOptions = Map(
      "hoodie.clustering.inline" -> "true",
      "hoodie.clustering.inline.max.commits" -> "1"
    ))

    // Validate that clustering commit is present
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()
    assertTrue(metaClient.getActiveTimeline.getLastClusteringInstant.isPresent, "Clustering commit should be present after inline clustering")

    // Read and verify data
    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "name", "age", "score")

    val expectedDf = createDataFrame(records1 ++ records2)

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  private def createDataFrame(records: Seq[(Int, String, Int, Double)]) = {
    spark.createDataFrame(records).toDF("id", "name", "age", "score").coalesce(1)
  }

  private def writeDataframe(tableType: HoodieTableType, tableName: String, tablePath: String, df: DataFrame,
                             saveMode: SaveMode = SaveMode.Append, operation: Option[String] = None,
                             extraOptions: Map[String, String] = Map.empty): Unit = {
    var writer = df.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType.name())
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "age")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)

    // Add operation if specified
    writer = operation match {
      case Some(op) => writer.option(OPERATION.key(), op)
      case None => writer
    }

    // Add any extra options
    extraOptions.foreach { case (key, value) => writer = writer.option(key, value) }

    writer.mode(saveMode).save(tablePath)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSchemaEvolutionAddColumn(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_schema_evolution_${tableType.name.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // First insert with base schema - columns: (id, name, age, score)
    val records1 = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )

    val df1 = createDataFrame(records1)
    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite, operation = Some("insert"))

    // Schema evolution - add new column "email" and upsert existing records
    val records2 = Seq(
      (1, "Alice", 31, 96.0, "alice@example.com"),
      (2, "Bob", 26, 88.0, "bob@example.com")
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score", "email")
    writeDataframe(tableType, tableName, tablePath, df2, saveMode = SaveMode.Append, operation = Some("upsert"))

    // Insert a new record with the evolved schema
    val records3 = Seq(
      (4, "David", 28, 89.5, "david@example.com")
    )
    val df3 = spark.createDataFrame(records3).toDF("id", "name", "age", "score", "email")
    writeDataframe(tableType, tableName, tablePath, df3, saveMode = SaveMode.Append, operation = Some("upsert"))

    // Read and verify schema evolution
    val actual = spark.read
      .format("hudi")
      .load(tablePath)
      .select("id", "name", "age", "score", "email")

    // Expected data after schema evolution
    val expectedDf = spark.createDataFrame(Seq(
      (1, "Alice", 31, 96.0, "alice@example.com"),
      (2, "Bob", 26, 88.0, "bob@example.com"),
      (3, "Charlie", 35, 92.1, null),
      (4, "David", 28, 89.5, "david@example.com")
    )).toDF("id", "name", "age", "score", "email")

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSchemaEvolutionTypePromotion(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_type_promotion_${tableType.name.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Write initial data with Integer column
    val schema1 = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("value", IntegerType, true)  // This will be promoted to Long
    ))
    val data1 = Seq(
      Row(1, "Alice", 100),
      Row(2, "Bob", 200),
      Row(3, "Charlie", 300)
    )
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema1)

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite)

    val fileSchema1 = spark.read.format("hudi").load(tablePath).schema
    assertEquals(IntegerType, fileSchema1("value").dataType)

    // Write new data with Long column (schema evolution: Int → Long)
    val schema2 = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("value", LongType, true)
    ))
    val data2 = Seq(
      Row(4, "David", 400L)
    )
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)

    writeDataframe(tableType, tableName, tablePath, df2, saveMode = SaveMode.Append)

    // Read with Long schema - should cast Int→Long for old records
    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "name", "value")

    assertEquals(LongType, actual.schema("value").dataType)

    // Verify data - all values should be Long (including casted ones)
    val expected = Seq(
      (1, "Alice", 100L),
      (2, "Bob", 200L),
      (3, "Charlie", 300L),
      (4, "David", 400L)
    )
    val expectedDf = spark.createDataFrame(expected).toDF("id", "name", "value")

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSchemaEvolutionFloatToDouble(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_float_to_double_${tableType.name.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Write initial data with Float column
    val schema1 = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("value", FloatType, true)
    ))
    val data1 = Seq(
      Row(1, "Alice", 1.5f),
      Row(2, "Bob", 2.5f),
      Row(3, "Charlie", 3.5f)
    )
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema1)

    // Write with Lance format
    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite)

    // Verify file schema has FloatType
    val fileSchema1 = spark.read.format("hudi").load(tablePath).schema
    assertEquals(FloatType, fileSchema1("value").dataType)

    // Write new data with Double column (schema evolution: Float → Double)
    val schema2 = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("value", DoubleType, true)
    ))
    val data2 = Seq(
      Row(4, "David", 4.5)
    )
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)

    writeDataframe(tableType, tableName, tablePath, df2, saveMode = SaveMode.Append)

    // Read with Double schema - should cast Float→Double for old records
    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "name", "value")

    // Verify schema has Double type
    assertEquals(DoubleType, actual.schema("value").dataType)

    // Verify data - all values should be Double (including casted ones)
    val expected = Seq(
      (1, "Alice", 1.5),
      (2, "Bob", 2.5),
      (3, "Charlie", 3.5),
      (4, "David", 4.5)
    )
    val expectedDf = spark.createDataFrame(expected).toDF("id", "name", "value")

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSchemaEvolutionNestedStructMinimal(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_nested_struct_minimal_${tableType.name.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Write initial data with base schema: {id: Int, info: {name: String}}
    val schema1 = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("info", StructType(Seq(
        StructField("name", StringType, true)
      )), true)
    ))
    val data1 = Seq(
      Row(1, Row("Alice")),
      Row(2, Row("Bob")),
      Row(3, Row("Charlie"))
    )
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema1)

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite)

    // Write new data with evolved schema: {id: Long, info: {name: String, age: Int}}
    // Tests both type evolution (Int → Long) and nested field addition (info.age)
    val schema2 = StructType(Seq(
      StructField("id", LongType, false),
      StructField("info", StructType(Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )), true)
    ))
    val data2 = Seq(
      Row(4L, Row("David", 28))
    )
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)

    writeDataframe(tableType, tableName, tablePath, df2, saveMode = SaveMode.Append)

    // Read with evolved schema - should handle both type promotion and nested field addition
    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "info")

    // Verify schema has Long type and nested struct has both fields
    assertEquals(LongType, actual.schema("id").dataType)
    val infoSchema = actual.schema("info").dataType.asInstanceOf[StructType]
    assertEquals(2, infoSchema.fields.length)
    assertTrue(infoSchema.fieldNames.contains("name"))
    assertTrue(infoSchema.fieldNames.contains("age"))

    // Create expected DataFrame with evolved schema
    val expectedSchema = StructType(Seq(
      StructField("id", LongType, false),
      StructField("info", StructType(Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )), true)
    ))
    val expectedData = Seq(
      Row(1L, Row("Alice", null)),
      Row(2L, Row("Bob", null)),
      Row(3L, Row("Charlie", null)),
      Row(4L, Row("David", 28))
    )
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)

    // Verify data - old records should have null for info.age, id should be cast to Long
    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

}
