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

import org.apache.hudi.{BaseFileOnlyRelation, DataSourceReadOptions, DataSourceWriteOptions, IncrementalRelationV1, IncrementalRelationV2}
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

/** Row shape written by these tests. A nested struct and an array are included so the legacy
 * parquet read path is exercised on complex types -- the historically fragile vectorized
 * nested-column branch (e.g. HUDI-7190), not just flat scalar columns. */
private case class LegacyNested(a: Int, b: String)

private case class LegacyTestRow(id: String,
                                 ts: Long,
                                 value: Long,
                                 partition: String,
                                 nested: LegacyNested,
                                 tags: Seq[Int])

/**
 * Functional tests for the legacy (pre-file-group-reader) Spark read path:
 * [[BaseFileOnlyRelation]], [[IncrementalRelationV1]], [[IncrementalRelationV2]] and the
 * per-Spark-version legacy Hudi parquet file format created via
 * `sparkAdapter.createLegacyHoodieParquetFileFormat`.
 *
 * In the batch datasource, `DefaultSource` routes normal reads to the file-group-reader-based
 * relations regardless of `hoodie.file.group.reader.enabled`; the legacy relations still run in
 * production for metadata-table reads and for streaming reads with the flag disabled. To exercise
 * them functionally here, the legacy relations are constructed directly (with the flag set to
 * false in their options, matching how the streaming sources invoke them) and their results are
 * compared row-by-row against the file-group-reader-enabled reads of the same table.
 */
class TestLegacyParquetReadPath extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  private val writeOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "true",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    HoodieWriteConfig.TBL_NAME.key -> "legacy_read_path_tbl"
  )

  // Columns compared across the read paths; meta fields are persisted in the base files, so the
  // record key and commit time must match exactly between the legacy and new readers. `nested` and
  // `tags` force the legacy parquet reader through its complex-type (struct / array) branch.
  private val comparedCols =
    Seq("_hoodie_commit_time", "_hoodie_record_key", "id", "ts", "value", "partition", "nested", "tags")

  @BeforeEach override def setUp(): Unit = {
    setTableName("legacy_read_path_tbl")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupResources()
    spark = null
  }

  private def makeRows(ids: Seq[Int], ts: Long, valueFn: Int => Long): Seq[LegacyTestRow] =
    ids.map(i => LegacyTestRow(i.toString, ts, valueFn(i), "p" + (i % 3),
      LegacyNested(i, "v" + valueFn(i)), Seq(i, ts.toInt)))

  private def writeBatch(rows: Seq[LegacyTestRow], operation: String): Unit = {
    spark.createDataFrame(rows)
      .write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  /** Commit 1: insert 30 rows; commit 2: upsert rows 1-10 with new values. */
  private def writeTwoCommits(): Unit = {
    writeBatch(makeRows(1 to 30, ts = 1L, i => i * 10L), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    writeBatch(makeRows(1 to 10, ts = 2L, i => i * 100L), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
  }

  private def fgReaderDf(extraOpts: Map[String, String] = Map.empty): DataFrame =
    spark.read.format("hudi")
      .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key, "true")
      .options(extraOpts)
      .load(basePath)

  private def legacyReadOpts(extraOpts: Map[String, String]): Map[String, String] =
    Map(
      "path" -> basePath,
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "false"
    ) ++ extraOpts

  /**
   * Legacy scan through [[BaseFileOnlyRelation]]'s own `PrunedFilteredScan` implementation
   * (`HoodieBaseRelation.buildScan` -> base-file readers built on the legacy parquet format).
   */
  private def legacyRelationDf(extraOpts: Map[String, String] = Map.empty): DataFrame = {
    val metaClient = createMetaClient(spark, basePath)
    spark.baseRelationToDataFrame(
      BaseFileOnlyRelation(sqlContext, metaClient, legacyReadOpts(extraOpts), None))
  }

  /**
   * Legacy scan through the `HadoopFsRelation` conversion that
   * `DefaultSource#resolveBaseFileOnlyRelation` applies, executing the per-Spark-version
   * legacy Hudi parquet file format inside a regular file-source scan.
   */
  private def legacyFileFormatDf(extraOpts: Map[String, String] = Map.empty): DataFrame = {
    val metaClient = createMetaClient(spark, basePath)
    val hadoopFsRelation =
      BaseFileOnlyRelation(sqlContext, metaClient, legacyReadOpts(extraOpts), None).toHadoopFsRelation
    assertTrue(hadoopFsRelation.fileFormat.getClass.getSimpleName.contains("LegacyHoodieParquetFileFormat"),
      s"Expected the legacy parquet file format but got ${hadoopFsRelation.fileFormat.getClass.getName}")
    spark.baseRelationToDataFrame(hadoopFsRelation)
  }

  private def collectSorted(df: DataFrame): Seq[Row] =
    df.select(comparedCols.map(col): _*).collect().toSeq.sortBy(_.getAs[String]("id").toInt)

  private def assertSameRows(expected: DataFrame, actual: DataFrame): Unit = {
    val expectedRows = collectSorted(expected)
    assertTrue(expectedRows.nonEmpty, "Comparison must cover a non-empty result")
    assertEquals(expectedRows, collectSorted(actual))
  }

  @Test
  def testCowSnapshotReadEqualsFileGroupReader(): Unit = {
    writeTwoCommits()

    val newReaderDf = fgReaderDf()
    assertEquals(30, newReaderDf.count())

    Seq(legacyRelationDf(), legacyFileFormatDf()).foreach { legacyDf =>
      assertSameRows(newReaderDf, legacyDf)
      // The upserts from the second commit must be visible through the legacy path.
      val updatedValues = legacyDf.filter(col("ts") === 2L)
        .collect().map(_.getAs[Long]("value")).sorted.toSeq
      assertEquals((1 to 10).map(_ * 100L), updatedValues)
    }
  }

  @Test
  def testCowSnapshotReadWithoutVectorizedReader(): Unit = {
    writeTwoCommits()

    val vectorizedKey = "spark.sql.parquet.enableVectorizedReader"
    val previous = spark.conf.get(vectorizedKey, "true")
    spark.conf.set(vectorizedKey, "false")
    try {
      // Row-based (non-batch) branch of the legacy parquet file format.
      val newReaderDf = fgReaderDf()
      assertSameRows(newReaderDf, legacyFileFormatDf())
      assertSameRows(newReaderDf, legacyRelationDf())
    } finally {
      spark.conf.set(vectorizedKey, previous)
    }
  }

  @Test
  def testCowSnapshotReadWithPartitionValuesExtractedFromPath(): Unit = {
    writeTwoCommits()

    // BaseFileOnlyRelation always appends partition values parsed from the (hive-style)
    // partition path; enabling the same extraction on the new reader must yield equal rows.
    val extractOpts = Map(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key -> "true")
    val newReaderDf = fgReaderDf(extractOpts)
    assertSameRows(newReaderDf, legacyFileFormatDf(extractOpts))
    assertSameRows(newReaderDf, legacyRelationDf(extractOpts))

    val partitions = legacyFileFormatDf(extractOpts)
      .select("partition").distinct().collect().map(_.getString(0)).sorted.toSeq
    assertEquals(Seq("p0", "p1", "p2"), partitions)
  }

  @Test
  def testPartitionAndDataFilterPushdown(): Unit = {
    writeTwoCommits()

    def applyFilters(df: DataFrame): DataFrame =
      df.filter(col("partition") === "p1" && col("value") > 100L)

    // Partition p1 holds ids with id % 3 == 1: updated ids {4, 7, 10} have value > 100
    // (id 1 has value exactly 100) and untouched ids {13, 16, 19, 22, 25, 28} do as well.
    val newReaderDf = applyFilters(fgReaderDf())
    assertEquals(9, newReaderDf.count())
    assertSameRows(newReaderDf, applyFilters(legacyFileFormatDf()))
    assertSameRows(newReaderDf, applyFilters(legacyRelationDf()))
  }

  @Test
  def testCowIncrementalReadEqualsFileGroupReader(): Unit = {
    writeTwoCommits()
    writeBatch(makeRows(11 to 15, ts = 3L, i => i * 1000L), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)

    val metaClient = createMetaClient(spark, basePath)
    val firstInstant = metaClient.getCommitsTimeline.filterCompletedInstants.firstInstant.get

    // New-reader incremental query; on current table versions the start bound is a completion
    // time and the range is start-exclusive, so this returns rows written by commits 2 and 3.
    val newReaderDf = spark.read.format("hudi")
      .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key, "true")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, firstInstant.getCompletionTime)
      .load(basePath)

    // V1 slices the timeline by instant time, V2 by completion time; both are start-exclusive.
    val incOptsV1 = Map(
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.START_COMMIT.key -> firstInstant.requestedTime,
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "false")
    val incOptsV2 = incOptsV1.updated(DataSourceReadOptions.START_COMMIT.key, firstInstant.getCompletionTime)

    val legacyV1Df = spark.baseRelationToDataFrame(
      new IncrementalRelationV1(sqlContext, incOptsV1, None, metaClient))
    val legacyV2Df = spark.baseRelationToDataFrame(
      new IncrementalRelationV2(sqlContext, incOptsV2, None, metaClient, RangeType.OPEN_CLOSED))

    // COW upserts preserve the original commit time of untouched rows in rewritten files, so the
    // incremental result is exactly the rows written by commits 2 and 3.
    val expected = ((1 to 10).map(i => (i.toString, i * 100L)) ++ (11 to 15).map(i => (i.toString, i * 1000L)))
      .sortBy(_._1.toInt)
    Seq(newReaderDf, legacyV1Df, legacyV2Df).foreach { df =>
      val actual = df.select("id", "value").collect()
        .map(r => (r.getString(0), r.getLong(1))).toSeq.sortBy(_._1.toInt)
      assertEquals(expected, actual)
    }

    assertSameRows(newReaderDf, legacyV1Df)
    assertSameRows(newReaderDf, legacyV2Df)
  }
}
