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
import org.apache.hudi.blob.BlobTestHelpers
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieTableType}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.view.{FileSystemViewManager, FileSystemViewStorageConfig}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.storage.HoodieSparkLanceReader
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource}
import org.lance.file.LanceFileReader

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger
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

  private val tableId = new AtomicInteger(0)

  // Mirrors the Lance metadata key on the BLOB `data` Arrow field; kept here so the test asserts
  // the on-disk invariant against a constant rather than a literal.
  private val LANCE_BLOB_ENCODING_KEY = "lance-encoding:blob"

  private def generateTableName: String = {
    s"lance_sql_${tableId.incrementAndGet()}"
  }

  private def checkAnswer(sql: String)(expects: Seq[Any]*): Unit = {
    val result = spark.sql(sql).collect()
    val expectedRows = expects.map(row => Row(row: _*)).toArray
    assertEquals(expectedRows.length, result.length, "Row count mismatch")
    expectedRows.zip(result).foreach { case (expected, actual) =>
      assertEquals(expected, actual)
    }
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

    assertEquals(expectedDf.collect().length, actual.collect().length, "Row count mismatch - possible duplicates")
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

  /**
   * INLINE BLOB round-trip on Lance: writes rows with inline bytes, reads them back through
   * Lance's CONTENT mode, and asserts the bytes survive both the raw `data` column and
   * `read_blob(payload)`. Also verifies the Lance file activated blob encoding on write.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testBlobInlineRoundTrip(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_blob_inline_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // Deterministic payloads: row i -> bytes of length 2048 with pattern (i+j) % 256.
    val payloadLen = 2048
    val numRows = 5
    val expectedPayloads: Seq[Array[Byte]] = (0 until numRows).map { i =>
      (0 until payloadLen).map(j => ((i + j) % 256).toByte).toArray
    }
    val sparkSess = spark
    import sparkSess.implicits._
    val baseDf = expectedPayloads.zipWithIndex.map { case (bytes, i) => (i, bytes) }
      .toDF("id", "bytes")
    val rawDf = baseDf.select($"id",
      BlobTestHelpers.inlineBlobStructCol("payload", $"bytes"))
    // Coerce the helper-built schema to the canonical BLOB schema so
    // HoodieSparkSchemaConverters.validateBlobStructure (exact equals) accepts it.
    val canonicalSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("payload", BlobType().asInstanceOf[StructType], nullable = true,
        BlobTestHelpers.blobMetadata)
    ))
    val df = spark.createDataFrame(rawDf.rdd, canonicalSchema)

    writeDataframe(tableType, tableName, tablePath, df, saveMode = SaveMode.Overwrite,
      operation = Some("bulk_insert"),
      extraOptions = Map(PRECOMBINE_FIELD.key() -> "id"))

    // Writer-side: prove the bytes actually routed through Lance's dedicated blob writer.
    assertLanceBlobEncoding(tablePath)

    // Reader-side: in CONTENT mode the INLINE bytes come back directly in `data`.
    val readRows = spark.read.format("hudi").load(tablePath)
      .select($"id", $"payload")
      .orderBy($"id")
      .collect()
    assertEquals(numRows, readRows.length)
    readRows.zipWithIndex.foreach { case (row, i) =>
      assertEquals(i, row.getInt(row.fieldIndex("id")))
      val payload = row.getStruct(row.fieldIndex("payload"))
      assertEquals(HoodieSchema.Blob.INLINE,
        payload.getString(payload.fieldIndex(HoodieSchema.Blob.TYPE)))
      val data = payload.getAs[Array[Byte]](HoodieSchema.Blob.INLINE_DATA_FIELD)
      assertArrayEquals(expectedPayloads(i), data,
        s"Inline data bytes mismatch for id=$i")
      // Lance materializes the `reference` struct as non-null with all-null leaves for INLINE
      // rows (rather than a null struct). The canonical INLINE signal is `type`; downstream
      // read_blob() dispatches on `type`, so the reference shape does not affect correctness.
      val refIdx = payload.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)
      if (!payload.isNullAt(refIdx)) {
        val ref = payload.getStruct(refIdx)
        assertTrue(ref.isNullAt(ref.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH)),
          s"reference.external_path should be null for INLINE blob (id=$i)")
      }
    }

    // read_blob() resolution path: INLINE payloads resolve to the same bytes.
    val viewName = s"${tableName}_view"
    spark.read.format("hudi").load(tablePath).createOrReplaceTempView(viewName)
    val materialized = spark.sql(
      s"SELECT id, read_blob(payload) AS bytes FROM $viewName ORDER BY id").collect()
    assertEquals(numRows, materialized.length)
    materialized.zipWithIndex.foreach { case (row, i) =>
      val bytes = row.getAs[Array[Byte]]("bytes")
      assertArrayEquals(expectedPayloads(i), bytes,
        s"read_blob() bytes mismatch for id=$i")
    }
  }

  @ParameterizedTest
  @MethodSource(Array("blobOutOfLineParams"))
  def testBlobOutOfLine(tableType: HoodieTableType, readMode: String): Unit = {
    verifyBlobOutOfLine(tableType, Option(readMode))
  }

  /**
   * DESCRIPTOR mode on INLINE rows: user writes `data` bytes; on read with
   * `hoodie.read.blob.inline.mode=DESCRIPTOR` each row comes back with type still set to
   * {@code INLINE} (preserving the original storage mode) but with {@code data=null} and a
   * populated {@code reference} pointing at the Lance file. {@code read_blob()} then preads
   * the bytes back from the .lance file via the reference.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testBlobInlineDescriptorMode(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_blob_inline_desc_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    val payloadLen = 2048
    val numRows = 5
    val expectedPayloads: Seq[Array[Byte]] = (0 until numRows).map { i =>
      (0 until payloadLen).map(j => ((i + j) % 256).toByte).toArray
    }
    val sparkSess = spark
    import sparkSess.implicits._
    val baseDf = expectedPayloads.zipWithIndex.map { case (bytes, i) => (i, bytes) }
      .toDF("id", "bytes")
    val rawDf = baseDf.select($"id",
      BlobTestHelpers.inlineBlobStructCol("payload", $"bytes"))
    val canonicalSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("payload", BlobType().asInstanceOf[StructType], nullable = true,
        BlobTestHelpers.blobMetadata)
    ))
    val df = spark.createDataFrame(rawDf.rdd, canonicalSchema)

    writeDataframe(tableType, tableName, tablePath, df, saveMode = SaveMode.Overwrite,
      operation = Some("bulk_insert"),
      extraOptions = Map(PRECOMBINE_FIELD.key() -> "id"))

    assertLanceBlobEncoding(tablePath)

    val modeKey = "hoodie.read.blob.inline.mode"
    val readRows = spark.read.format("hudi")
      .option(modeKey, "DESCRIPTOR")
      .load(tablePath)
      .select($"id", $"payload")
      .orderBy($"id")
      .collect()
    assertEquals(numRows, readRows.length)
    readRows.zipWithIndex.foreach { case (row, i) =>
      assertEquals(i, row.getInt(row.fieldIndex("id")))
      val payload = row.getStruct(row.fieldIndex("payload"))
      assertEquals(HoodieSchema.Blob.INLINE,
        payload.getString(payload.fieldIndex(HoodieSchema.Blob.TYPE)),
        s"DESCRIPTOR mode should preserve INLINE type (id=$i)")
      assertTrue(payload.isNullAt(payload.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)),
        s"data should be null in DESCRIPTOR mode (id=$i)")
      val ref = payload.getStruct(payload.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE))
      assertNotNull(ref, s"reference struct should be populated (id=$i)")
      val extPath = ref.getString(ref.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH))
      assertTrue(extPath.endsWith(".lance"),
        s"external_path should point at a .lance file, got: $extPath (id=$i)")
      assertEquals(payloadLen.toLong,
        ref.getLong(ref.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH)),
        s"length should equal the written payload length (id=$i)")
      assertTrue(ref.getBoolean(ref.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED)),
        s"synthetic reference to the Lance file should be flagged managed (id=$i)")
    }

    // read_blob() materializes bytes via BatchedBlobReader, which always reads with CONTENT
    // mode (actual bytes) regardless of the user's inline read mode setting.
    val viewName = s"${tableName}_view"
    spark.read.format("hudi").load(tablePath).createOrReplaceTempView(viewName)
    val materialized = spark.sql(
      s"SELECT id, read_blob(payload) AS bytes FROM $viewName ORDER BY id").collect()
    assertEquals(numRows, materialized.length)
    materialized.zipWithIndex.foreach { case (row, i) =>
      val bytes = row.getAs[Array[Byte]]("bytes")
      assertArrayEquals(expectedPayloads(i), bytes,
        s"read_blob() bytes mismatch for id=$i")
    }
  }

  /**
   * Shared implementation for OUT_OF_LINE blob tests. Writes rows with external references,
   * reads them back (optionally with a specific read mode), and asserts the reference survives
   * unchanged and {@code read_blob()} resolves the correct bytes.
   */
  private def verifyBlobOutOfLine(tableType: HoodieTableType, readMode: Option[String]): Unit = {
    val modeSuffix = readMode.map(m => s"_${m.toLowerCase}").getOrElse("")
    val tableName = s"test_lance_blob_out_of_line${modeSuffix}_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    val externalDir = Files.createDirectories(
      Paths.get(s"$basePath/_blob_ext${modeSuffix}_${tableType.name().toLowerCase}"))
    val filePath1 = BlobTestHelpers.createTestFile(externalDir, "blob_file_1.bin", 1024)
    val filePath2 = BlobTestHelpers.createTestFile(externalDir, "blob_file_2.bin", 1024)

    val sparkSess = spark
    import sparkSess.implicits._
    val baseDf = Seq(
      (1, filePath1, 0L, 256L),
      (2, filePath1, 256L, 256L),
      (3, filePath2, 0L, 1024L),
      (4, filePath2, 0L, 512L)
    ).toDF("id", "path", "offset", "length")
    val rawDf = baseDf.select($"id",
      BlobTestHelpers.blobStructCol("payload", $"path", $"offset", $"length"))
    val canonicalSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("payload", BlobType().asInstanceOf[StructType], nullable = true,
        BlobTestHelpers.blobMetadata)
    ))
    val df = spark.createDataFrame(rawDf.rdd, canonicalSchema)

    writeDataframe(tableType, tableName, tablePath, df, saveMode = SaveMode.Overwrite,
      operation = Some("bulk_insert"),
      extraOptions = Map(PRECOMBINE_FIELD.key() -> "id"))

    val modeKey = "hoodie.read.blob.inline.mode"
    val reader = readMode.foldLeft(spark.read.format("hudi"))((r, m) => r.option(modeKey, m))

    val rowsBack = reader.load(tablePath)
      .select("id", "payload")
      .orderBy("id")
      .collect()
    assertEquals(4, rowsBack.length)

    val expectedFiles = Map(1 -> "blob_file_1.bin", 2 -> "blob_file_1.bin",
      3 -> "blob_file_2.bin", 4 -> "blob_file_2.bin")
    rowsBack.foreach { row =>
      val id = row.getInt(row.fieldIndex("id"))
      val payload = row.getStruct(row.fieldIndex("payload"))
      assertEquals(HoodieSchema.Blob.OUT_OF_LINE,
        payload.getString(payload.fieldIndex(HoodieSchema.Blob.TYPE)))
      assertTrue(payload.isNullAt(payload.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)),
        s"Inline data must be null for OUT_OF_LINE blob (id=$id)")
      val ref = payload.getStruct(payload.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE))
      val extPath = ref.getString(ref.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH))
      assertTrue(extPath.endsWith(expectedFiles(id)),
        s"Unexpected external_path for id=$id: $extPath")
    }

    val viewName = s"${tableName}_view"
    readMode.foldLeft(spark.read.format("hudi"))((r, m) => r.option(modeKey, m))
      .load(tablePath).createOrReplaceTempView(viewName)
    val materialized = spark.sql(
      s"SELECT id, read_blob(payload) AS bytes FROM $viewName ORDER BY id").collect()
    assertEquals(4, materialized.length)

    val expectedRanges = Map(1 -> 0L, 2 -> 256L, 3 -> 0L, 4 -> 0L)
    val expectedLengths = Map(1 -> 256, 2 -> 256, 3 -> 1024, 4 -> 512)
    materialized.foreach { row =>
      val id = row.getInt(row.fieldIndex("id"))
      val bytes = row.getAs[Array[Byte]]("bytes")
      assertEquals(expectedLengths(id), bytes.length,
        s"Unexpected blob length for id=$id")
      BlobTestHelpers.assertBytesContent(bytes, expectedOffset = expectedRanges(id).toInt)
    }
  }

  /**
   * Asserts that the nested `data` child of the BLOB struct in a Lance base file under
   * `tablePath` is stored as LargeBinary with `lance-encoding:blob=true` — i.e. Hudi told
   * Lance to use its blob writer for that column. One file is enough: the schema is
   * writer-controlled and identical across base files of the same table.
   */
  private def assertLanceBlobEncoding(tablePath: String): Unit = {
    val lanceFile = Files.walk(Paths.get(tablePath))
      .filter(p => p.toString.endsWith(".lance"))
      .findFirst()
      .orElseThrow(() => new AssertionError(s"No .lance files found under $tablePath"))

    val allocator = new RootAllocator(64L * 1024 * 1024)
    try {
      val reader = LanceFileReader.open(lanceFile.toString, allocator)
      try {
        val dataField = reader.schema().getFields.asScala
          .flatMap(_.getChildren.asScala)
          .find(_.getName == HoodieSchema.Blob.INLINE_DATA_FIELD)
          .getOrElse(throw new AssertionError(
            s"No nested '${HoodieSchema.Blob.INLINE_DATA_FIELD}' field in $lanceFile"))
        val md = dataField.getMetadata
        assertTrue(md != null && "true".equalsIgnoreCase(md.get(LANCE_BLOB_ENCODING_KEY)),
          s"Lance blob-encoding metadata missing on ${dataField.getName}: $md")
        assertTrue(dataField.getType.isInstanceOf[ArrowType.LargeBinary],
          s"Expected LargeBinary for ${dataField.getName}, got ${dataField.getType}")
      } finally {
        reader.close()
      }
    } finally {
      allocator.close()
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testSqlCommands(tableType: HoodieTableType): Unit = {
    Seq(true, false).foreach { isPartitioned =>
      val tableName = generateTableName
      val tablePath = s"$basePath/$tableName"
      testSqlCommands(tableType, tableName, tablePath, isPartitioned)
    }
  }

  private def testSqlCommands(tableType: HoodieTableType,
                              tableName: String,
                              tablePath: String,
                              isPartitioned: Boolean): Unit = {
    val createTablePartitionClause = if (isPartitioned) "partitioned by (dt)" else ""

    // CREATE TABLE with Lance configuration
    // Lance format requires Spark record merger for writing
    spark.sql(s"""
      create table $tableName (
        id int,
        dt string,
        name string,
        age int,
        score double
      ) using hudi
      tblproperties (
        hoodie.table.base.file.format = 'LANCE',
        type = '${tableType.name()}',
        primaryKey = 'id',
        hoodie.datasource.write.record.merger.impls = '${classOf[DefaultSparkRecordMerger].getName}'
      )
      $createTablePartitionClause
      location '$tablePath'
    """.stripMargin)

    // Test 1: INSERT with all columns in schema order
    spark.sql(s"""
      insert into $tableName (id, name, age, score, dt)
      values (1, 'Alice', 30, 95.5, '2025-01-01'),
             (2, 'Bob', 25, 87.3, '2025-01-02')
    """.stripMargin)

    checkAnswer(s"select id, name, age, score, dt from $tableName order by id")(
      Seq(1, "Alice", 30, 95.5, "2025-01-01"),
      Seq(2, "Bob", 25, 87.3, "2025-01-02")
    )

    // Test 2: INSERT with reordered columns
    spark.sql(s"""
      insert into $tableName (dt, name, id, age, score)
      values ('2025-01-02', 'Charlie', 3, 35, 92.1)
    """.stripMargin)

    checkAnswer(s"select id, name, age, score, dt from $tableName order by id")(
      Seq(1, "Alice", 30, 95.5, "2025-01-01"),
      Seq(2, "Bob", 25, 87.3, "2025-01-02"),
      Seq(3, "Charlie", 35, 92.1, "2025-01-02")
    )

    // Disable small file handling so the next insert creates a new file group
    // and updates in MOR generate log file(s)
    spark.sql(s"alter table $tableName set tblproperties ('hoodie.merge.small.file.group.candidates.limit' = '0')")

    // Test 3: INSERT with subset of columns (null handling)
    spark.sql(s"""
      insert into $tableName (dt, age, name, id)
      values ('2025-01-01', 40, 'Diana', 4)
    """.stripMargin)

    checkAnswer(s"select id, name, age, score, dt from $tableName order by id")(
      Seq(1, "Alice", 30, 95.5, "2025-01-01"),
      Seq(2, "Bob", 25, 87.3, "2025-01-02"),
      Seq(3, "Charlie", 35, 92.1, "2025-01-02"),
      Seq(4, "Diana", 40, null, "2025-01-01")
    )

    // Test 4: UPDATE existing row
    spark.sql(s"update $tableName set score = 99.9, age = 31 where id = 1")

    checkAnswer(s"select id, name, age, score, dt from $tableName order by id")(
      Seq(1, "Alice", 31, 99.9, "2025-01-01"),
      Seq(2, "Bob", 25, 87.3, "2025-01-02"),
      Seq(3, "Charlie", 35, 92.1, "2025-01-02"),
      Seq(4, "Diana", 40, null, "2025-01-01")
    )

    // Test 5: DELETE a row
    // TODO(#18558): test DELETE with MOR table type once the bug is fixed
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      spark.sql(s"delete from $tableName where id = 3")

      checkAnswer(s"select id, name, age, score, dt from $tableName order by id")(
        Seq(1, "Alice", 31, 99.9, "2025-01-01"),
        Seq(2, "Bob", 25, 87.3, "2025-01-02"),
        Seq(4, "Diana", 40, null, "2025-01-01")
      )
    }

    // Test 6: INSERT with static partition (only for partitioned tables)
    if (isPartitioned) {
      spark.sql(s"""
        insert into $tableName partition(dt='2025-01-05') (age, id, name)
        values (28, 5, 'Eve')
      """.stripMargin)

      if (tableType == HoodieTableType.COPY_ON_WRITE) {
        checkAnswer(s"select id, name, age, score, dt from $tableName order by id")(
          Seq(1, "Alice", 31, 99.9, "2025-01-01"),
          Seq(2, "Bob", 25, 87.3, "2025-01-02"),
          Seq(4, "Diana", 40, null, "2025-01-01"),
          Seq(5, "Eve", 28, null, "2025-01-05")
        )
      } else {
        checkAnswer(s"select id, name, age, score, dt from $tableName order by id")(
          Seq(1, "Alice", 31, 99.9, "2025-01-01"),
          Seq(2, "Bob", 25, 87.3, "2025-01-02"),
          Seq(3, "Charlie", 35, 92.1, "2025-01-02"),
          Seq(4, "Diana", 40, null, "2025-01-01"),
          Seq(5, "Eve", 28, null, "2025-01-05")
        )
      }
    }

    // Verify Lance files were created
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(tablePath)
      .build()

    val baseFileFormat = metaClient.getTableConfig.getBaseFileFormat
    assertEquals(HoodieFileFormat.LANCE, baseFileFormat,
                 "Table should use Lance base file format")
  }

  /**
   * Vector round-trip test parameterized over COW + MOR. Covers two non-null VECTOR
   * columns of different element types (FLOAT, DOUBLE) and dimensions, and exercises
   * the upsert path (MOR log-merge on MOR, file rewrite on COW).
   *
   * <p>Nullable-vector coverage lives in {@code testNullableVectorRoundTrip} because
   * merging a null-valued vector through the upsert path currently errors out in
   * the Lance reader; tracked as a separate follow-up.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testMultipleVectorColumns(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_vec_multi_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    val embeddingDim = 3
    val featuresDim = 2
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("embedding",
        ArrayType(FloatType, containsNull = false),
        nullable = false,
        vectorMetadata(s"VECTOR($embeddingDim)")),
      StructField("features",
        ArrayType(DoubleType, containsNull = false),
        nullable = false,
        vectorMetadata(s"VECTOR($featuresDim, DOUBLE)"))
    ))

    // Initial insert.
    val data1 = Seq(
      Row(1, "Alice", 30, Array(1.0f, 2.0f, 3.0f), Array(10.0d, 20.0d)),
      Row(2, "Bob", 25, Array(4.0f, 5.0f, 6.0f), Array(30.0d, 40.0d)),
      Row(3, "Charlie", 35, Array(7.0f, 8.0f, 9.0f), Array(50.0d, 60.0d))
    )
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema).coalesce(1)
    writeDataframe(tableType, tableName, tablePath, df1,
      saveMode = SaveMode.Overwrite, operation = Some("insert"))

    // Upsert — update Bob's embedding, age, and features.
    val data2 = Seq(
      Row(2, "Bob", 40, Array(10.0f, 20.0f, 30.0f), Array(70.0d, 80.0d))
    )
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema).coalesce(1)
    writeDataframe(tableType, tableName, tablePath, df2, operation = Some("upsert"))

    val readDf = spark.read.format("hudi").load(tablePath)
      .select("id", "name", "age", "embedding", "features")
    assertEquals(
      ArrayType(FloatType, containsNull = false),
      readDf.schema("embedding").dataType)
    assertEquals(
      ArrayType(DoubleType, containsNull = false),
      readDf.schema("features").dataType)
    assertHudiTypeMetadata(readDf.schema("embedding"), s"VECTOR($embeddingDim)")
    assertHudiTypeMetadata(readDf.schema("features"), s"VECTOR($featuresDim, DOUBLE)")

    val rows = readDf.collect().sortBy(_.getInt(0))
    assertEquals(3, rows.length)
    // Alice unchanged.
    assertEquals(Seq(1.0f, 2.0f, 3.0f), rows(0).getSeq[Float](3).toSeq)
    assertEquals(Seq(10.0d, 20.0d), rows(0).getSeq[Double](4).toSeq)
    // Bob upserted.
    assertEquals(40, rows(1).getInt(2), "Bob's age should be updated to 40")
    assertEquals(Seq(10.0f, 20.0f, 30.0f), rows(1).getSeq[Float](3).toSeq)
    assertEquals(Seq(70.0d, 80.0d), rows(1).getSeq[Double](4).toSeq)
    // Charlie unchanged.
    assertEquals(Seq(7.0f, 8.0f, 9.0f), rows(2).getSeq[Float](3).toSeq)
    assertEquals(Seq(50.0d, 60.0d), rows(2).getSeq[Double](4).toSeq)

    // Validate Lance file physical schema + footer on the base files produced.
    assertLanceFieldIsFixedSizeList(tablePath, "embedding", embeddingDim)
    assertLanceFieldIsFixedSizeList(tablePath, "features", featuresDim)
    assertLanceFooterHasVectorColumns(tablePath,
      s"embedding:VECTOR($embeddingDim),features:VECTOR($featuresDim, DOUBLE)")
  }

  /**
   * Nullable-vector coverage — kept separate from {@code testMultipleVectorColumns}
   * because folding this case into the upsert/merge path errors out in the current
   * Lance reader (null vector element read).
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testNullableVectorRoundTrip(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_vec_nullable_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    val dim = 3
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("embedding",
        ArrayType(FloatType, containsNull = false),
        nullable = true,
        vectorMetadata(s"VECTOR($dim)"))
    ))
    val data = Seq(
      Row(1, Array(1.0f, 2.0f, 3.0f)),
      Row(2, null),
      Row(3, Array(7.0f, 8.0f, 9.0f))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)

    writeDataframe(tableType, tableName, tablePath, df, saveMode = SaveMode.Overwrite)

    val readDf = spark.read.format("hudi").load(tablePath).select("id", "embedding")
    assertHudiTypeMetadata(readDf.schema("embedding"), s"VECTOR($dim)")

    val rows = readDf.collect().sortBy(_.getInt(0))
    assertEquals(3, rows.length)
    assertEquals(Seq(1.0f, 2.0f, 3.0f), rows(0).getSeq[Float](1).toSeq)
    assertTrue(rows(1).isNullAt(1), "Row with id=2 should have null embedding")
    assertEquals(Seq(7.0f, 8.0f, 9.0f), rows(2).getSeq[Float](1).toSeq)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testVectorProjection(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_vec_proj_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    val dim = 4
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("embedding",
        ArrayType(FloatType, containsNull = false),
        nullable = false,
        vectorMetadata(s"VECTOR($dim)"))
    ))
    val data = Seq(
      Row(1, "Alice", Array(1.0f, 2.0f, 3.0f, 4.0f)),
      Row(2, "Bob", Array(5.0f, 6.0f, 7.0f, 8.0f))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
    writeDataframe(tableType, tableName, tablePath, df, saveMode = SaveMode.Overwrite)

    // Project only the vector column
    val vecOnly = spark.read.format("hudi").load(tablePath).select("embedding")
    assertEquals(1, vecOnly.schema.fields.length)
    assertHudiTypeMetadata(vecOnly.schema("embedding"), s"VECTOR($dim)")
    val vecRows = vecOnly.collect().map(_.getSeq[Float](0).toSeq).toSet
    assertEquals(Set(Seq(1.0f, 2.0f, 3.0f, 4.0f), Seq(5.0f, 6.0f, 7.0f, 8.0f)), vecRows)

    // Project vector alongside Hudi metadata columns
    val withMeta = spark.read.format("hudi").load(tablePath)
      .select("_hoodie_record_key", "embedding")
    assertEquals(2, withMeta.schema.fields.length)
    assertHudiTypeMetadata(withMeta.schema("embedding"), s"VECTOR($dim)")
    val metaRows = withMeta.collect().map(r =>
      r.getString(0) -> r.getSeq[Float](1).toSeq).toMap
    assertEquals(Seq(1.0f, 2.0f, 3.0f, 4.0f), metaRows("1"))
    assertEquals(Seq(5.0f, 6.0f, 7.0f, 8.0f), metaRows("2"))
  }

  private def assertHudiTypeMetadata(field: StructField, expectedDescriptor: String): Unit = {
    assertTrue(field.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD),
      s"Expected field ${field.name} to carry ${HoodieSchema.TYPE_METADATA_FIELD} metadata after read")
    assertEquals(expectedDescriptor, field.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD),
      s"Expected ${HoodieSchema.TYPE_METADATA_FIELD}=$expectedDescriptor on field ${field.name}")
  }

  private def vectorMetadata(descriptor: String): Metadata =
    new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, descriptor).build()

  /** Runs `check` against each Lance base file's Arrow schema. */
  private def validateLanceFileSchema(tablePath: String)(check: (org.apache.arrow.vector.types.pojo.Schema, String) => Unit): Unit = {
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
    try {
      val baseFiles = fsView.getLatestBaseFiles("")
        .collect(Collectors.toList[org.apache.hudi.common.model.HoodieBaseFile])
      assertTrue(baseFiles.size() > 0, "Expected at least one Lance base file")
      val allocator = new RootAllocator()
      try {
        baseFiles.asScala.foreach { bf =>
          val reader = LanceFileReader.open(bf.getPath, allocator)
          try {
            check(reader.schema(), bf.getPath)
          } finally {
            reader.close()
          }
        }
      } finally {
        allocator.close()
      }
    } finally {
      fsView.close()
    }
  }

  private def assertLanceFooterHasVectorColumns(tablePath: String, expected: String): Unit = {
    validateLanceFileSchema(tablePath) { (schema, path) =>
      val meta = schema.getCustomMetadata
      assertNotNull(meta, s"Lance footer metadata null for $path")
      val key = HoodieSchema.VECTOR_COLUMNS_METADATA_KEY
      assertTrue(meta.containsKey(key),
        s"Lance file $path should have footer key $key, got keys ${meta.keySet()}")
      assertEquals(expected, meta.get(key), s"Lance file $path footer $key mismatch")
    }
  }

  private def assertLanceFieldIsFixedSizeList(tablePath: String, fieldName: String, expectedDim: Int): Unit = {
    validateLanceFileSchema(tablePath) { (schema, path) =>
      val field = schema.findField(fieldName)
      assertNotNull(field, s"Field $fieldName not found in Lance schema for $path")
      field.getType match {
        case fsl: ArrowType.FixedSizeList =>
          assertEquals(expectedDim, fsl.getListSize,
            s"Lance field $fieldName in $path should be FixedSizeList of size $expectedDim")
        case other =>
          throw new AssertionError(
            s"Lance field $fieldName in $path should be FixedSizeList but was $other")
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testLanceReadNoDuplicateRows(tableType: HoodieTableType): Unit = {
    val tableName = s"test_lance_no_dup_${tableType.name().toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    val records = (1 to 100).map(i => (i, s"name_$i", 20 + i, i * 1.1))
    val inputDf = createDataFrame(records)

    writeDataframe(tableType, tableName, tablePath, inputDf, saveMode = SaveMode.Overwrite)

    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "name", "age", "score")
    // Use collect().length instead of count() — Spark's count optimization pushes down an
    // empty schema which SparkLanceReaderBase short-circuits to Iterator.empty (separate bug).
    val total = actual.collect().length
    val distinct = actual.select("id").distinct().count()
    assertEquals(100, total, "Lance read should not produce duplicate rows")
    assertEquals(100, distinct, "All record keys should be unique")
    assertTrue(inputDf.except(actual).isEmpty)
    assertTrue(actual.except(inputDf).isEmpty)
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

object TestLanceDataSource {
  /** Cross-product of table types and blob read modes for out-of-line blob tests. */
  def blobOutOfLineParams(): java.util.stream.Stream[Arguments] = {
    val params = for {
      tableType <- HoodieTableType.values()
      readMode <- Array(null, "CONTENT", "DESCRIPTOR")
    } yield Arguments.of(tableType, readMode: java.lang.String)
    java.util.stream.Stream.of(params: _*)
  }
}
