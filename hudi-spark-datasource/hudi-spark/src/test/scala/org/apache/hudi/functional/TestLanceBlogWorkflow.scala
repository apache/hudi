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
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.sql.Timestamp

/**
 * This test demonstrates using Hudi with Lance file format through the DataFrame API
 * to perform common operations including table creation, querying, appending, and aggregations.
 */
class TestLanceBlogWorkflow extends HoodieSparkClientTestBase {

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
   * Complete blog workflow test that demonstrates:
   * 1. Table creation with embeddings (Array[Float])
   * 2. Initial data insertion
   * 3. Querying with filtering and column projection
   * 4. Append operations
   * 5. Aggregations on the data
   */
  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testCompleteBlogWorkflow(tableType: String): Unit = {
    val tableName = s"test_lance_blog_workflow_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"

    // ========================================
    // Phase 1: Table Creation & Initial Data Insertion
    // ========================================
    println(s"\n=== Phase 1: Creating table with embeddings ===")

    // Create initial dataset with embeddings similar to the blog example
    // Schema: id (Long), text (String), embedding (Array[Float]), created_at (Timestamp)
    val initialRecords = Seq(
      (1L, "deep learning", Array(0.1f, 0.2f, 0.3f), Timestamp.valueOf("2025-01-01 10:00:00")),
      (2L, "machine learning", Array(0.15f, 0.25f, 0.35f), Timestamp.valueOf("2025-01-01 11:00:00")),
      (3L, "neural networks", Array(0.2f, 0.3f, 0.4f), Timestamp.valueOf("2025-01-01 12:00:00"))
    )
    val initialDF = spark.createDataFrame(initialRecords)
      .toDF("id", "text", "embedding", "created_at")

    println(s"Initial dataset schema:")
    initialDF.printSchema()
    println(s"Initial dataset (${initialDF.count()} records):")
    initialDF.show(truncate = false)

    // Write to Hudi table with Lance base file format
    initialDF.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "created_at")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    println(s"✓ Table created and initial data written")

    // ========================================
    // Phase 2: Data Querying with Filtering
    // ========================================
    println(s"\n=== Phase 2: Querying with filtering and projection ===")

    // Read table and apply filters (similar to blog's WHERE clause examples)
    val queriedDF = spark.read
      .format("hudi")
      .load(tablePath)
      .filter("id > 0")  // Similar to blog's "WHERE id > 0"
      .select(
        col("id"),
        col("text"),
        size(col("embedding")).as("dim")  // Get embedding dimension like in the blog
      )
      .orderBy("id")

    println("Query results (id > 0) with embedding dimensions:")
    val queryResults = queriedDF.collect()
    queriedDF.show(truncate = false)

    // Verify query results
    assertEquals(3, queryResults.length, "Should have 3 records after filtering")

    // Verify first record
    assertEquals(1L, queryResults(0).getAs[Long]("id"))
    assertEquals("deep learning", queryResults(0).getAs[String]("text"))
    assertEquals(3, queryResults(0).getAs[Int]("dim"), "Embedding dimension should be 3")

    // Verify second record
    assertEquals(2L, queryResults(1).getAs[Long]("id"))
    assertEquals("machine learning", queryResults(1).getAs[String]("text"))
    assertEquals(3, queryResults(1).getAs[Int]("dim"))

    // Verify third record
    assertEquals(3L, queryResults(2).getAs[Long]("id"))
    assertEquals("neural networks", queryResults(2).getAs[String]("text"))
    assertEquals(3, queryResults(2).getAs[Int]("dim"))

    println(s"✓ Query filtering and projection verified")

    // ========================================
    // Phase 3: Append Operations (ML Integration)
    // ========================================
    println(s"\n=== Phase 3: Appending new embeddings ===")

    // Append new records with embeddings (similar to blog's ML integration example)
    val appendRecords = Seq(
      (4L, "artificial intelligence", Array(0.25f, 0.35f, 0.45f), Timestamp.valueOf("2025-01-01 13:00:00")),
      (5L, "computer vision", Array(0.3f, 0.4f, 0.5f), Timestamp.valueOf("2025-01-01 14:00:00"))
    )
    val appendDF = spark.createDataFrame(appendRecords)
      .toDF("id", "text", "embedding", "created_at")

    println(s"Appending ${appendDF.count()} new records:")
    appendDF.show(truncate = false)

    // Append to the table
    appendDF.write
      .format("hudi")
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE")
      .option(TABLE_TYPE.key(), tableType)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "created_at")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify append
    val afterAppendDF = spark.read
      .format("hudi")
      .load(tablePath)

    val afterAppendResults = afterAppendDF.collect()
    val totalCount = afterAppendResults.length
    println(s"Total records after append: $totalCount")
    assertEquals(5, totalCount, "Should have 5 records after append")

    println(s"✓ Append operation verified")

    // ========================================
    // Phase 4: Aggregations (Computing Statistics) - Matching Blog Workflow
    // ========================================
    println(s"\n=== Phase 4: Computing aggregations (matching blog) ===")

    // Replicate the blog's SQL query using DataFrame API:
    // SELECT COUNT(*) as total_records,
    //        ROUND(AVG(aggregate(embedding, 0D, (acc, x) -> acc + x * x)), 3) as avg_l2_norm,
    //        ROUND(MIN(embedding[0]), 2) as min_first_dim,
    //        ROUND(MAX(embedding[0]), 2) as max_first_dim
    // FROM embeddings

    val blogStatsDF = spark.read
      .format("hudi")
      .load(tablePath)
      .select(
        // Compute L2 norm squared for each embedding vector
        aggregate(col("embedding"), lit(0.0), (acc, x) => acc + x * x).as("l2_norm_squared"),
        // Access first dimension of embedding and cast to Double
        element_at(col("embedding"), 1).cast("double").as("first_dim")
      )
      .agg(
        count("*").as("total_records"),
        round(avg("l2_norm_squared"), 3).as("avg_l2_norm"),
        round(min("first_dim"), 2).as("min_first_dim"),
        round(max("first_dim"), 2).as("max_first_dim")
      )

    println("Computed statistics (matching blog workflow):")
    blogStatsDF.show(truncate = false)

    val stats = blogStatsDF.collect()(0)
    assertEquals(5L, stats.getAs[Long]("total_records"), "Should have 5 total records")

    // Verify L2 norm and embedding dimension statistics are computed
    val avgL2Norm = stats.getAs[Double]("avg_l2_norm")
    val minFirstDim = stats.getAs[Double]("min_first_dim")
    val maxFirstDim = stats.getAs[Double]("max_first_dim")

    assertNotNull(avgL2Norm, "avg_l2_norm should not be null")
    assertNotNull(minFirstDim, "min_first_dim should not be null")
    assertNotNull(maxFirstDim, "max_first_dim should not be null")

    // L2 norm should be positive since we have non-zero embeddings
    assertTrue(avgL2Norm > 0, s"Average L2 norm should be positive, got $avgL2Norm")

    println(s"  Total records: ${stats.getAs[Long]("total_records")}")
    println(s"  Average L2 norm: $avgL2Norm")
    println(s"  Min first dimension: $minFirstDim")
    println(s"  Max first dimension: $maxFirstDim")

    println(s"✓ Blog-style aggregations verified")

    // ========================================
    // Final Verification: Complete Data
    // ========================================
    println(s"\n=== Final Verification ===")

    val finalDF = spark.read
      .format("hudi")
      .load(tablePath)
      .select("id", "text", "embedding", "created_at")
      .orderBy("id")

    println("Final dataset:")
    finalDF.show(truncate = false)

    val finalResults = finalDF.collect()
    assertEquals(5, finalResults.length, "Final dataset should have 5 records")

    // Verify all records are present
    val expectedTexts = Array("deep learning", "machine learning", "neural networks",
                               "artificial intelligence", "computer vision")
    for (i <- 0 until 5) {
      assertEquals(i + 1, finalResults(i).getAs[Long]("id").toInt)
      assertEquals(expectedTexts(i), finalResults(i).getAs[String]("text"))
      val embedding = finalResults(i).getAs[Seq[Float]]("embedding")
      assertNotNull(embedding, s"Embedding for record ${i + 1} should not be null")
      assertEquals(3, embedding.length, s"Embedding for record ${i + 1} should have dimension 3")
    }

    println(s"✓ Complete blog workflow test passed for $tableType table type!")
  }
}