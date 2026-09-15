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

import org.apache.hudi.{BaseFileOnlyRelation, DataSourceReadOptions, DataSourceWriteOptions, IncrementalRelationV1, IncrementalRelationV2, ScalaAssertionSupport}
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

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
 *
 * `DefaultSource#resolveBaseFileOnlyRelation` returns [[BaseFileOnlyRelation]] itself only under
 * schema-on-read and converts it to a `HadoopFsRelation` otherwise, so the relation's own
 * `buildScan` ships only in the schema-on-read shape; both scan shapes are exercised below.
 */
class TestLegacyParquetReadPath extends HoodieSparkClientTestBase with ScalaAssertionSupport {

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
    // The test schema carries a nested struct and an array. On the legacy parquet read path, batch
    // (vectorized) support for such complex types is additionally gated on nested-column
    // vectorization, which defaults off on spark3.3 and on only from spark3.4. Enable it so the
    // vectorized nested-column branch -- the one HUDI-7190 fixed -- is exercised on every Spark
    // profile instead of silently falling back to parquet-mr on 3.3 (which would make the vectorized
    // and non-vectorized cases below collapse onto the same row-based path there).
    spark.conf.set("spark.sql.parquet.enableNestedColumnVectorizedReader", "true")
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupResources()
    spark = null
  }

  private def makeRows(ids: Seq[Int], ts: Long, valueFn: Int => Long): Seq[LegacyTestRow] =
    ids.map(i => LegacyTestRow(i.toString, ts, valueFn(i), "p" + (i % 3),
      LegacyNested(i, "v" + valueFn(i)), Seq(i, ts.toInt)))

  private def writeBatch(rows: Seq[LegacyTestRow], operation: String,
                         extraWriteOpts: Map[String, String] = Map.empty): Unit = {
    spark.createDataFrame(rows)
      .write.format("hudi")
      .options(writeOpts ++ extraWriteOpts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  /** Commit 1: insert 30 rows; commit 2: upsert rows 1-10 with new values. */
  private def writeTwoCommits(): Unit = {
    writeBatch(makeRows(1 to 30, ts = 1L, i => i * 10L), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    writeBatch(makeRows(1 to 10, ts = 2L, i => i * 100L), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
  }

  // > Int.MaxValue, so a widened value is representable only as a long
  private val widenedBase = 10000000000L

  /**
   * Commit 1: insert ids 1..30 with `value` written as INT32; commit 2: upsert only the p0 rows
   * (id % 3 == 0) with a LONG `value` too large for an int, promoting the table schema to long.
   * In COW, commit 2 rewrites just the p0 file group, so the p1/p2 base files keep the narrower
   * physical int while the table schema is now long -- reading them exercises the legacy format's
   * type-change reconciliation. Updating a single partition is deliberate: upserting across all
   * partitions would rewrite every file group and leave no narrow base files behind.
   */
  private def writeIntToLongCommits(extraWriteOpts: Map[String, String] = Map.empty): Unit = {
    spark.createDataFrame(makeRows(1 to 30, ts = 1L, i => i.toLong))
      .withColumn("value", col("value").cast(IntegerType))
      .write.format("hudi")
      .options(writeOpts ++ extraWriteOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    writeBatch(makeRows((1 to 30).filter(_ % 3 == 0), ts = 2L, i => widenedBase + i),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, extraWriteOpts)
  }

  /**
   * Asserts the promoted `value` column of a [[writeIntToLongCommits]] table: p1/p2 ids come from
   * int base files widened on read, p0 ids carry the large long values written in commit 2.
   */
  private def assertWidenedValues(df: DataFrame): Unit = {
    assertEquals(LongType, df.schema("value").dataType,
      "Reading the promoted table must surface `value` as long")
    val actual = df.select("id", "value").collect()
      .map(r => (r.getString(0), r.getLong(1))).toSeq.sortBy(_._1.toInt)
    val expected = (1 to 30).map(i => (i.toString, if (i % 3 == 0) widenedBase + i else i.toLong))
    assertEquals(expected, actual)
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

  /**
   * Whether the legacy parquet format engages its vectorized (batch) reader for the table's
   * schema. Because the schema carries a nested struct and an array, batch support additionally
   * requires nested-column vectorization; asserting on this pins which branch of the reader a test
   * exercises so the vectorized and row-based cases cannot silently collapse onto one path (e.g. if
   * a Spark default change dropped nested-column vectorization on some profile).
   */
  private def legacyFormatSupportsBatch: Boolean = {
    val metaClient = createMetaClient(spark, basePath)
    val hadoopFsRelation =
      BaseFileOnlyRelation(sqlContext, metaClient, legacyReadOpts(Map.empty), None).toHadoopFsRelation
    hadoopFsRelation.fileFormat.supportBatch(spark, hadoopFsRelation.schema)
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

    // With nested-column vectorization enabled (see setUp), the legacy format must take its
    // vectorized branch on every profile; otherwise this would degenerate to the same row-based
    // path as testCowSnapshotReadWithoutVectorizedReader.
    assertTrue(legacyFormatSupportsBatch,
      "Legacy parquet format must engage the vectorized reader on the nested-column schema")

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
      // Row-based (non-batch) branch of the legacy parquet file format. Pin that the disabled
      // vectorized reader really forces the fallback path, so this case stays distinct from the
      // vectorized one above on every profile.
      assertFalse(legacyFormatSupportsBatch,
        "Disabling the vectorized reader must force the legacy parquet format onto the row-based path")
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

  @Test
  def testCowSnapshotReadWithImplicitTypeChange(): Unit = {
    // The Hudi-specific reason these per-version parquet formats exist (vs stock ParquetFileFormat)
    // is on-read type reconciliation: when a base file's physical column type is narrower than the
    // table schema, HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo records the change
    // and the vectorized read runs through Hudi's HoodieVectorizedParquetRecordReader (which widens
    // the column vector) instead of Spark's stock VectorizedParquetRecordReader.
    writeIntToLongCommits()

    // Vectorization must be on so the read takes the HoodieVectorizedParquetRecordReader branch
    // rather than the row-based parquet-mr fallback (which reconciles types via a different path).
    assertTrue(legacyFormatSupportsBatch,
      "Vectorized reader must be engaged so the implicit type change runs through " +
        "HoodieVectorizedParquetRecordReader")

    val newReaderDf = fgReaderDf()
    Seq(legacyRelationDf(), legacyFileFormatDf()).foreach { legacyDf =>
      assertWidenedValues(legacyDf)
      // The legacy path must still agree with the file-group reader on the promoted column.
      assertSameRows(newReaderDf, legacyDf)
    }
  }

  @Test
  def testCowSnapshotReadWithImplicitTypeChangeWithoutVectorizedReader(): Unit = {
    writeIntToLongCommits()

    val vectorizedKey = "spark.sql.parquet.enableVectorizedReader"
    val previous = spark.conf.get(vectorizedKey, "true")
    spark.conf.set(vectorizedKey, "false")
    try {
      // With the vectorized reader off, the legacy format reconciles the type change on its
      // row-based branch instead: a Cast from the file's narrower type compiled into a
      // GenerateUnsafeProjection -- an implementation separate from
      // HoodieVectorizedParquetRecordReader, so it needs its own coverage.
      assertFalse(legacyFormatSupportsBatch,
        "Disabling the vectorized reader must force the legacy parquet format onto the row-based path")
      val newReaderDf = fgReaderDf()
      Seq(legacyRelationDf(), legacyFileFormatDf()).foreach { legacyDf =>
        assertWidenedValues(legacyDf)
        assertSameRows(newReaderDf, legacyDf)
      }
    } finally {
      spark.conf.set(vectorizedKey, previous)
    }
  }

  @Test
  def testCowSnapshotReadWithNestedTypeChange(): Unit = {
    // Same int->long promotion, but inside the `nested` struct. The changed top-level column is
    // then non-atomic, which the legacy format cannot reconcile in vectorized mode: it must fail
    // fast with the documented IllegalArgumentException instead of returning corrupt columns, and
    // the workaround the exception advertises (disabling the vectorized reader) must actually read
    // the promoted struct correctly through the row-based Cast branch.
    writeBatch(makeRows(1 to 30, ts = 1L, i => i * 10L), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    spark.createDataFrame(makeRows((1 to 30).filter(_ % 3 == 0), ts = 2L, i => i * 100L))
      .withColumn("nested",
        struct((col("nested.a") + lit(widenedBase)).as("a"), col("nested.b").as("b")))
      .write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    assertTrue(legacyFormatSupportsBatch,
      "Vectorized reader must be engaged so the non-atomic type change hits the legacy format's rejection")
    Seq(legacyRelationDf(), legacyFileFormatDf()).foreach { legacyDf =>
      val thrown = assertThrows(classOf[Throwable]) {
        legacyDf.collect()
      }
      val causes = Iterator.iterate(thrown: Throwable)(_.getCause).takeWhile(_ != null).take(10).toSeq
      assertTrue(causes.exists(c => c.isInstanceOf[IllegalArgumentException]
        && String.valueOf(c.getMessage).contains("cannot be read in vectorized mode")),
        s"Expected the non-atomic type-change rejection but got: $thrown")
    }

    val vectorizedKey = "spark.sql.parquet.enableVectorizedReader"
    val previous = spark.conf.get(vectorizedKey, "true")
    spark.conf.set(vectorizedKey, "false")
    try {
      assertFalse(legacyFormatSupportsBatch,
        "Disabling the vectorized reader must force the legacy parquet format onto the row-based path")
      val newReaderDf = fgReaderDf()
      Seq(legacyRelationDf(), legacyFileFormatDf()).foreach { legacyDf =>
        val actual = legacyDf.select("id", "nested").collect()
          .map(r => (r.getString(0), r.getStruct(1).getLong(0))).toSeq.sortBy(_._1.toInt)
        val expected = (1 to 30).map(i => (i.toString, if (i % 3 == 0) widenedBase + i else i.toLong))
        assertEquals(expected, actual)
        assertSameRows(newReaderDf, legacyDf)
      }
    } finally {
      spark.conf.set(vectorizedKey, previous)
    }
  }

  @Test
  def testCowSnapshotReadWithSchemaOnRead(): Unit = {
    // Schema-on-read is the one production shape in which DefaultSource#resolveBaseFileOnlyRelation
    // returns BaseFileOnlyRelation itself instead of converting it to a HadoopFsRelation, making
    // the relation's own buildScan the shipped scan path. It also flips
    // BaseFileOnlyRelation.shouldExtractPartitionValuesFromPartitionPath (defined as
    // internalSchemaOpt.isEmpty) to false -- the only way the legacy parquet format is constructed
    // with shouldAppendPartitionValues = false -- and drives the format's explicit internal-schema
    // branch (InternalSchemaCache lookup + InternalSchemaMerger) instead of the implicit
    // footer-based reconciliation.
    //
    // No ALTER TABLE is needed to get an InternalSchema into commit metadata: with
    // hoodie.schema.on.read.enable plus hoodie.datasource.write.reconcile.schema on the writes,
    // HoodieSparkSqlWriter seeds the internal schema on the first commit and evolves it with the
    // int->long promotion on the second.
    writeIntToLongCommits(Map(
      DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key -> "true",
      DataSourceWriteOptions.RECONCILE_SCHEMA.key -> "true"))

    val readOpts = Map(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key -> "true")
    val metaClient = createMetaClient(spark, basePath)
    val schemaOnReadRelation = BaseFileOnlyRelation(sqlContext, metaClient, legacyReadOpts(readOpts), None)
    assertTrue(schemaOnReadRelation.hasSchemaOnRead,
      "The writes must have recorded an InternalSchema in commit metadata for schema-on-read to engage")
    // Pin the flipped partition-values branch: under schema-on-read the relation reads partition
    // columns from the data files (empty partition schema) instead of re-appending them from the
    // partition path, unlike every other case in this suite.
    assertTrue(schemaOnReadRelation.toHadoopFsRelation.partitionSchema.isEmpty,
      "Schema-on-read must flip shouldExtractPartitionValuesFromPartitionPath off")
    assertTrue(BaseFileOnlyRelation(sqlContext, metaClient, legacyReadOpts(Map.empty), None)
      .toHadoopFsRelation.partitionSchema.nonEmpty,
      "Without schema-on-read the converted relation appends partition values from the path")

    assertTrue(legacyFormatSupportsBatch,
      "Vectorized reader must be engaged so the explicit type change runs through " +
        "HoodieVectorizedParquetRecordReader")

    val newReaderDf = fgReaderDf(readOpts)
    Seq(legacyRelationDf(readOpts), legacyFileFormatDf(readOpts)).foreach { legacyDf =>
      assertWidenedValues(legacyDf)
      assertSameRows(newReaderDf, legacyDf)
    }
  }
}
