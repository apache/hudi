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

import org.apache.hudi.{DataSourceWriteOptions, ScalaAssertionSupport}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

/**
 * Tests for the SELECT count(*) footer-only fast path added in
 * HoodieFileGroupReaderBasedFileFormat (apache/hudi#18769 / #18770).
 *
 * The fast path is gated on `isCount = requiredSchema.isEmpty && !isMOR && !isIncremental`
 * AND `hoodieFileFormat == PARQUET && !isMultipleBaseFileFormatsEnabled`. When all conditions
 * are met, the reader sums `BlockMetaData.getRowCount()` from the parquet footer
 * (filtered to the current split's [start, start+length) range) instead of opening the
 * vectorized reader.
 *
 * Tests below cover:
 *   - Basic correctness on partitioned and unpartitioned COW tables.
 *   - Split correctness when files are large enough to span multiple PartitionedFile splits.
 *   - Fallback correctness when a predicate disqualifies the fast path.
 *   - Direct proof that the fast path is exercised: count(*) reads substantially fewer
 *     bytes than an equivalent full scan on the same table.
 */
class TestCountStarFastPath extends HoodieSparkClientTestBase with ScalaAssertionSupport {

  var spark: SparkSession = null

  private val commonOpts: Map[String, String] = Map(
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "p",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts",
    DataSourceWriteOptions.OPERATION.key -> "insert",
    DataSourceWriteOptions.TABLE_TYPE.key -> "COPY_ON_WRITE",
    HoodieTableConfig.NAME.key -> "test_count_star",
    HoodieWriteConfig.TBL_NAME.key -> "test_count_star",
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
    "hoodie.parquet.small.file.limit" -> "0",
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2"
  )

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(Some(JFunction.toJavaConsumer((receiver: SparkSessionExtensions) =>
      new HoodieSparkSessionExtension().apply(receiver))))

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  /**
   * Captures bytesRead across all tasks of a single query. Used to prove the fast path
   * reads less than the full-scan path.
   */
  private class BytesReadListener extends SparkListener {
    val total = new AtomicLong(0L)
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): scala.Unit = {
      total.addAndGet(taskEnd.taskMetrics.inputMetrics.bytesRead)
    }
  }

  private def buildPartitionedTable(path: String, n: Int, partitions: Int): scala.Unit = {
    val sparkLocal = spark; import sparkLocal.implicits._
    val df = spark.range(n)
      .withColumn("p", ($"id" % partitions).cast("string"))
      .withColumn("ts", lit(1L))
    df.write.format("hudi").options(commonOpts).mode(SaveMode.Overwrite).save(path)
  }

  private def buildUnpartitionedTable(path: String, n: Int): scala.Unit = {
    val sparkLocal = spark; import sparkLocal.implicits._
    val df = spark.range(n)
      .withColumn("p", lit("default"))
      .withColumn("ts", lit(1L))
    df.write.format("hudi")
      .options(commonOpts +
        (DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> ""))
      .mode(SaveMode.Overwrite).save(path)
  }

  @Test
  def testCountStarPartitionedCOW(): scala.Unit = {
    val n = 10000
    val path = basePath + "/partitioned"
    buildPartitionedTable(path, n, partitions = 5)
    val count = spark.read.format("hudi").load(path).count()
    assertEquals(n.toLong, count,
      "count(*) on partitioned COW table must return the inserted row count")
  }

  @Test
  def testCountStarUnpartitionedCOW(): scala.Unit = {
    val n = 5000
    val path = basePath + "/unpartitioned"
    buildUnpartitionedTable(path, n)
    val count = spark.read.format("hudi").load(path).count()
    assertEquals(n.toLong, count,
      "count(*) on unpartitioned COW table must return the inserted row count")
  }

  /**
   * Regression test for the split-aware row-group filter (apache/hudi#18770 review).
   *
   * Spark splits parquet files into multiple PartitionedFile ranges when file size
   * exceeds spark.sql.files.maxPartitionBytes and the format is splittable (COW parquet
   * is). The fast path must only count row groups whose getStartingPos falls inside the
   * current split, or it will over-count by the split factor.
   *
   * Force splits via small maxPartitionBytes + small parquet block size so each base
   * file has multiple row groups and each is handed to the reader as multiple
   * PartitionedFile splits.
   */
  @Test
  def testCountStarOnSplittableFiles(): scala.Unit = {
    val n = 20000
    val path = basePath + "/splittable"

    val optsWithTinyBlocks = commonOpts +
      ("hoodie.parquet.block.size" -> "65536") +   // 64 KB row groups → multiple per file
      ("hoodie.parquet.page.size" -> "8192")
    val sparkLocal = spark; import sparkLocal.implicits._
    val df = spark.range(n)
      .withColumn("p", lit("only"))
      .withColumn("ts", lit(1L))
    df.write.format("hudi").options(optsWithTinyBlocks).mode(SaveMode.Overwrite).save(path)

    // Lower the per-Spark-partition byte budget so the file gets handed to the reader
    // as multiple PartitionedFile splits. Reset after the test to avoid polluting the
    // session for other tests.
    val originalMaxPartitionBytes = spark.conf.get("spark.sql.files.maxPartitionBytes")
    try {
      spark.conf.set("spark.sql.files.maxPartitionBytes", "65536") // 64 KB
      val df = spark.read.format("hudi").load(path)
      // Sanity: confirm Spark actually split (more partitions than files)
      val numScanPartitions = df.rdd.getNumPartitions
      assertTrue(numScanPartitions > 1,
        s"expected the file to be split into >1 Spark partition, got $numScanPartitions; " +
        s"the test cannot validate the split-aware filter without splits")
      val count = df.count()
      assertEquals(n.toLong, count,
        "count(*) must return the inserted row count even when files are split across " +
        "multiple PartitionedFile ranges (split-aware row-group filter)")
    } finally {
      spark.conf.set("spark.sql.files.maxPartitionBytes", originalMaxPartitionBytes)
    }
  }

  /**
   * The fast path is gated on `requiredSchema.isEmpty`. A predicate like
   * `WHERE val > X` makes requiredSchema non-empty, so this query routes through
   * the regular read path. Verify the count is still correct.
   */
  @Test
  def testCountStarWithFilterRoutesThroughSlowPath(): scala.Unit = {
    val n = 1000
    val path = basePath + "/filtered"
    val sparkLocal = spark; import sparkLocal.implicits._
    val df = spark.range(n)
      .withColumn("p", ($"id" % 4).cast("string"))
      .withColumn("ts", lit(1L))
    df.write.format("hudi").options(commonOpts).mode(SaveMode.Overwrite).save(path)

    val filtered = spark.read.format("hudi").load(path).filter("id >= 500").count()
    assertEquals(500L, filtered,
      "count(*) with a filter must still return the correct count via the regular " +
      "read path")
  }

  /**
   * Direct proof the fast path is exercised: count(*) reads substantially fewer bytes
   * than the equivalent full SELECT * on the same table.
   *
   * If the fast path were NOT taken, count(*) would route through readBaseFile and read
   * comparable bytes to SELECT *. The footer-only path reads ~few KB per file (footer
   * thrift) rather than the whole file body.
   *
   * We assert count(*) reads less than half of SELECT *. The actual ratio is typically
   * ~10× smaller, but we keep the threshold loose to absorb MDT-setup noise.
   */
  @Test
  def testCountStarFastPathReadsLessThanFullScan(): scala.Unit = {
    val n = 50000
    val path = basePath + "/proof"
    val sparkLocal = spark; import sparkLocal.implicits._
    val df = spark.range(n)
      .withColumn("p", ($"id" % 10).cast("string"))
      .withColumn("ts", lit(1L))
    df.write.format("hudi").options(commonOpts).mode(SaveMode.Overwrite).save(path)

    val rdf = spark.read.format("hudi").load(path)
    rdf.createOrReplaceTempView("vproof")

    // Warm-up: prime Spark's session state so the listener measurements don't include
    // first-time-relation-build overhead.
    spark.sql("SELECT count(*) FROM vproof").collect()
    spark.sql("SELECT * FROM vproof").collect()

    val countListener = new BytesReadListener
    spark.sparkContext.addSparkListener(countListener)
    val countResult = spark.sql("SELECT count(*) FROM vproof").collect()(0).getLong(0)
    spark.sparkContext.removeSparkListener(countListener)

    val fullListener = new BytesReadListener
    spark.sparkContext.addSparkListener(fullListener)
    spark.sql("SELECT * FROM vproof").collect()
    spark.sparkContext.removeSparkListener(fullListener)

    assertEquals(n.toLong, countResult,
      "count(*) must return the correct row count")

    val countBytes = countListener.total.get()
    val fullBytes = fullListener.total.get()
    assertTrue(fullBytes > 0,
      s"sanity: full scan should read >0 bytes; got $fullBytes")
    assertTrue(countBytes * 2 < fullBytes,
      s"fast path should read <half the bytes of a full scan, but count(*) " +
      s"read $countBytes bytes vs SELECT * $fullBytes bytes — this suggests the " +
      s"fast path was not exercised")
  }
}
