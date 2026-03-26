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
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.view.{FileSystemViewManager, FileSystemViewStorageConfig}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.storage.HoodieSparkLanceReader
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.util.stream.Collectors

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
      (1, "Alice", 30, 95.5, "engineering"),
      (2, "Bob", 25, 87.3, "sales"),
      (3, "Charlie", 35, 92.1, "engineering")
    )
    val df1 = spark.createDataFrame(records1).toDF("id", "name", "age", "score", "department")

    writeDataframe(tableType, tableName, tablePath, df1, saveMode = SaveMode.Overwrite, operation = Some("insert"),
      extraOptions = Map(PARTITIONPATH_FIELD.key() -> "department"))

    // Upsert - modify Bob's record (id=2)
    val records2 = Seq(
      (2, "Bob", 40, 95.0, "sales")  // Update Bob: age 25->40, score 87.3->95.0
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score", "department")

    writeDataframe(tableType, tableName, tablePath, df2, operation = Some("upsert"),
      extraOptions = Map(PARTITIONPATH_FIELD.key() -> "department"))

    // Second upsert - modify Alice (id=1) and insert David (id=4)
    val records3 = Seq(
      (1, "Alice", 45, 98.5, "engineering"),  // Update Alice: age 30->45, score 95.5->98.5
      (4, "David", 28, 88.0, "marketing")   // Insert new record
    )
    val df3 = spark.createDataFrame(records3).toDF("id", "name", "age", "score", "department")

    writeDataframe(tableType, tableName, tablePath, df3, operation = Some("upsert"),
      extraOptions = Map(PARTITIONPATH_FIELD.key() -> "department"))

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
    val actual = readDf.select("id", "name", "age", "score", "department")

    val expectedDf = spark.createDataFrame(Seq(
      (1, "Alice", 45, 98.5, "engineering"),
      (2, "Bob", 40, 95.0, "sales"),
      (3, "Charlie", 35, 92.1, "engineering"),
      (4, "David", 28, 88.0, "marketing")
    )).toDF("id", "name", "age", "score", "department")

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)

    if (tableType == HoodieTableType.MERGE_ON_READ) {
      // Write one more commit to trigger compaction
      val records4 = Seq(
        (1, "Alice", 50, 98.5, "engineering"),  // Update Alice: age 45->50
        (4, "David", 28, 90.0, "marketing")   // Update David: score 88.0->90.0
      )
      val df4 = spark.createDataFrame(records4).toDF("id", "name", "age", "score", "department")
      writeDataframe(tableType, tableName, tablePath, df4, operation = Some("upsert"),
        extraOptions = Map("hoodie.compact.inline" -> "true", "hoodie.compact.inline.max.delta.commits" -> "1",
          PARTITIONPATH_FIELD.key() -> "department"))
      val expectedDfAfterCompaction = spark.createDataFrame(Seq(
        (1, "Alice", 50, 98.5, "engineering"),
        (2, "Bob", 40, 95.0, "sales"),
        (3, "Charlie", 35, 92.1, "engineering"),
        (4, "David", 28, 90.0, "marketing")
      )).toDF("id", "name", "age", "score", "department")
      // validate compaction commit is present
      val compactionCommits = metaClient.reloadActiveTimeline().filterCompletedInstants().getInstants.asScala
        .filter(instant => instant.getAction == "commit")
      assertTrue(compactionCommits.nonEmpty, "Compaction commit should be present after upsert")
      // Read and verify data after compaction
      val readDfAfterCompaction = spark.read.format("hudi").load(tablePath)
      val actualAfterCompaction = readDfAfterCompaction.select("id", "name", "age", "score", "department")
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

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testUpsertWithPopulateMetaFieldsDisabled(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_no_meta_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    val records1 = Seq(
      (101, "Alice", 30, 95.5, "engineering"),
      (102, "Bob", 25, 87.3, "sales"),
      (103, "Charlie", 35, 92.1, "engineering")
    )
    val df1 = spark.createDataFrame(records1).toDF("id", "name", "age", "score", "department")

    // Write with populateMetaFields=false
    writeDataframe(tableType, tableName, tablePath, df1,
      saveMode = SaveMode.Overwrite, operation = Some("insert"),
      extraOptions = Map("hoodie.populate.meta.fields" -> "false", PARTITIONPATH_FIELD.key() -> "department"))

    // Upsert - modify Bob's record
    val records2 = Seq(
      (102, "Bob", 40, 95.0, "sales")
    )
    val df2 = spark.createDataFrame(records2).toDF("id", "name", "age", "score", "department")

    writeDataframe(tableType, tableName, tablePath, df2,
      operation = Some("upsert"),
      extraOptions = Map("hoodie.populate.meta.fields" -> "false", PARTITIONPATH_FIELD.key() -> "department"))

    // Second upsert - modify Alice and insert David
    val records3 = Seq(
      (101, "Alice", 45, 98.5, "engineering"),
      (104, "David", 28, 88.0, "marketing")
    )
    val df3 = spark.createDataFrame(records3).toDF("id", "name", "age", "score", "department")

    writeDataframe(tableType, tableName, tablePath, df3,
      operation = Some("upsert"),
      extraOptions = Map("hoodie.populate.meta.fields" -> "false", PARTITIONPATH_FIELD.key() -> "department"))

    // Verify data
    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "name", "age", "score", "department")

    val expectedDf = spark.createDataFrame(Seq(
      (101, "Alice", 45, 98.5, "engineering"),
      (102, "Bob", 40, 95.0, "sales"),
      (103, "Charlie", 35, 92.1, "engineering"),
      (104, "David", 28, 88.0, "marketing")
    )).toDF("id", "name", "age", "score", "department")

    assertTrue(expectedDf.except(actual).isEmpty)
    assertTrue(actual.except(expectedDf).isEmpty)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testBloomFilterAndMinMaxKeys(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_bloom_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    val records = Seq(
      (1, "Alice", 30, 95.5),
      (2, "Bob", 25, 87.3),
      (3, "Charlie", 35, 92.1)
    )
    val df = createDataFrame(records)

    writeDataframe(tableType, tableName, tablePath, df, saveMode = SaveMode.Overwrite, operation = Some("insert"))

    // Build MetaClient and FileSystemView to find lance base files
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf)
    val metadataConfig = HoodieMetadataConfig.newBuilder.build
    val viewManager = FileSystemViewManager.createViewManager(
      engineContext, metadataConfig, FileSystemViewStorageConfig.newBuilder.build,
      HoodieCommonConfig.newBuilder.build,
      (mc: HoodieTableMetaClient) => metaClient.getTableFormat
        .getMetadataFactory.create(engineContext, mc.getStorage, metadataConfig, tablePath))
    val fsView = viewManager.getFileSystemView(metaClient)

    // Get all lance base files across all partitions
    val baseFiles = fsView.getLatestBaseFiles("").collect(Collectors.toList[org.apache.hudi.common.model.HoodieBaseFile])
    assertTrue(baseFiles.size() > 0, "Should have at least one base file")

    baseFiles.asScala.foreach { baseFile =>
      val reader = new HoodieSparkLanceReader(new StoragePath(baseFile.getPath))
      try {
        // Verify bloom filter
        val bloomFilter = reader.readBloomFilter()
        assertNotNull(bloomFilter, "Bloom filter should not be null")

        // All written record keys should be present
        // Record keys are the "id" field values as strings
        Seq("1", "2", "3").foreach { key =>
          assertTrue(bloomFilter.mightContain(key), s"Bloom filter should contain key $key")
        }
        // Non-existent key should (very likely) not be present
        assertFalse(bloomFilter.mightContain("nonexistent_key"), "Bloom filter should not contain nonexistent key")

        // Verify min/max record keys
        val minMaxKeys = reader.readMinMaxRecordKeys()
        assertEquals("1", minMaxKeys(0), "Min key should be '1'")
        assertEquals("3", minMaxKeys(1), "Max key should be '3'")
      } finally {
        reader.close()
      }
    }
    fsView.close()
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
}
