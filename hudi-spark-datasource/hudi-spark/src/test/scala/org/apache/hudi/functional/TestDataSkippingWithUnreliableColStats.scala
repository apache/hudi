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

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test}
import org.junit.jupiter.api.Assertions.assertEquals

/**
 * End-to-end correctness tests for the col-stats unreliability fix
 * (apache/hudi#18754 and #18755).
 *
 * Each test writes a Hudi table whose col-stats would be poisoned by the
 * pre-fix writer (a NaN in a DOUBLE column, or a string value larger than
 * parquet-mr's stats truncation threshold), then asserts that queries against
 * the table return the same rows with data-skipping enabled as with it
 * disabled. Data-skipping is supposed to be a transparent performance
 * optimization — any divergence between ON and OFF is silent data loss.
 */
@Tag("functional")
class TestDataSkippingWithUnreliableColStats extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  private val schema = StructType(Seq(
    StructField("rk", IntegerType, nullable = false),
    StructField("p",  StringType,  nullable = false),
    StructField("s_str",    StringType, nullable = true),
    StructField("d_double", DoubleType, nullable = true)
  ))

  private val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    RECORDKEY_FIELD.key -> "rk",
    PARTITIONPATH_FIELD.key -> "p",
    HoodieTableConfig.ORDERING_FIELDS.key -> "rk",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_unreliable_colstats",
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
    // Index the data columns so the data-skipping path is exercised.
    HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "s_str,d_double",
    // Disable small-file batching so each write produces its own file (we want
    // multiple files in the same partition to exercise both file-level and
    // partition-level pruning).
    "hoodie.parquet.small.file.limit" -> "0"
  )

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  /**
   * #18754 — a single NaN value in a DOUBLE column would silently drop all
   * matching rows from `WHERE d_double > N` queries (and predicates on
   * unrelated columns of the same partition, via partition-stats aggregation).
   * After the fix, ON and OFF results must match.
   */
  @Test
  def testNaNInDoubleColumnDoesNotSilentlyPruneRows(): Unit = {
    // Four files in partition "P". File 3 has 10 numeric values + 1 NaN row.
    val fileBatches = Seq(
      (0 until 10).map(k => Row(10000 + k, "P", "a_" + "%02d".format(k), k.toDouble)),
      (0 until 10).map(k => Row(20000 + k, "P", "b_" + "%02d".format(k), 100.0 + k)),
      (0 until 10).map(k => Row(30000 + k, "P", "c_" + "%02d".format(k), 200.0 + k)),
      (0 until 10).map(k => Row(40000 + k, "P", "d_" + "%02d".format(k), 300.0 + k)) :+
        Row(40100, "P", "m_nan", Double.NaN)
    )
    writeBatches(fileBatches)

    // The NaN row is excluded from `> 250` in standard SQL semantics — but Spark's
    // DOUBLE comparison treats NaN as greater than every other value, so it IS
    // included. Either way, ON and OFF must agree.
    assertSkippingNeverDropsRows("d_double > 250")
    assertSkippingNeverDropsRows("d_double >= 300")
    // Predicates on other columns of the table must also be unaffected by file 3's NaN.
    assertSkippingNeverDropsRows("s_str = 'a_05'")
    assertSkippingNeverDropsRows("s_str IN ('a_05','b_05','c_05','d_05')")
  }

  /**
   * #18755 — when a Parquet column omits stats entirely (one value exceeds
   * parquet-mr's stats truncation threshold ~1KB), the pre-fix writer recorded
   * nullCount = valueCount, claiming the file was all-null. Data-skipping then
   * pruned the file silently. After the fix, ON and OFF results must match.
   */
  @Test
  def testTruncatedStringStatsDoNotSilentlyPruneRows(): Unit = {
    val longValue = "Y" * 4096  // exceeds parquet-mr's stats truncation threshold

    val fileBatches = Seq(
      // File 0: only short strings — stats are reliable.
      (0 until 5).map(k => Row(100 + k, "P", "short_" + k, k.toDouble)),
      // File 1: mix of short and one long value — parquet omits stats for s_str.
      Seq(Row(200, "P", "Y", 1.0),
          Row(201, "P", longValue, 2.0))
    )
    writeBatches(fileBatches)

    // The short value "Y" lives in file 1, but pre-fix data-skipping would prune
    // file 1 entirely because the col-stats record incorrectly claimed all
    // values were null. After the fix, the file is scanned and the row returned.
    assertSkippingNeverDropsRows("s_str = 'Y'")
    assertSkippingNeverDropsRows("s_str = 'short_2'")
  }

  /** Writes successive batches as separate Hudi commits, producing distinct files. */
  private def writeBatches(fileBatches: Seq[Seq[Row]]): Unit = {
    fileBatches.zipWithIndex.foreach { case (rows, i) =>
      spark.createDataFrame(rows.asJava(), schema).write.format("org.apache.hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(if (i == 0) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }
  }

  /**
   * Asserts the predicate returns the same set of rows with data-skipping
   * enabled as with it disabled. Skipping is a performance optimization;
   * silent divergence is the data-loss signature of #18754 / #18755.
   */
  private def assertSkippingNeverDropsRows(predicateSql: String): Unit = {
    val skippingOn = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key, "true").load(basePath)
      .where(predicateSql).select("rk").collect().map(_.getInt(0)).toSet
    val skippingOff = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key, "false").load(basePath)
      .where(predicateSql).select("rk").collect().map(_.getInt(0)).toSet
    assertEquals(skippingOff, skippingOn,
      s"Data-skipping ON returned a different result set than OFF for predicate `$predicateSql`. " +
        s"Skipping ON: $skippingOn  ;  Skipping OFF: $skippingOff")
  }

  // Helper to convert Scala Seq to Java List for createDataFrame(java.util.List[Row], schema).
  private implicit class SeqOps(s: Seq[Row]) {
    def asJava(): java.util.List[Row] = {
      val list = new java.util.ArrayList[Row](s.size)
      s.foreach(list.add)
      list
    }
  }
}
