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

import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkUtils, ScalaAssertionSupport}
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.functional.TestVectorizedParquetReadPerScan.{fileScan, rowClassesByPartition}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.{FileSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.MetadataBuilder
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue

/**
 * The file-group reader format decides vectorized decoding per scan, as Spark's ParquetFileFormat
 * does: base files decode vectorized whenever the schema allows it, batches are returned only when
 * the scan asks for them, and the session conf is never written. Spark only asks the format about
 * batches for scans of at most spark.sql.codegen.maxFields leaf fields, so the wide tables here
 * have more leaves than that.
 *
 * Which reader decoded a row is read off its class: the vectorized reader returns views over a
 * column batch, the row-based reader and the file-group reader return UnsafeRows.
 */
class TestVectorizedParquetReadPerScan extends HoodieSparkClientTestBase with ScalaAssertionSupport {

  private val vectorizedKey = "spark.sql.parquet.enableVectorizedReader"
  private val numStructColumns = 40
  private val unsafeRow = classOf[UnsafeRow].getSimpleName

  var spark: SparkSession = _

  private val writeOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    HoodieWriteConfig.RECORD_MERGE_MODE.key -> RecordMergeMode.COMMIT_TIME_ORDERING.name,
    HoodieWriteConfig.TBL_NAME.key -> "vectorized_per_scan_tbl")

  private val morWriteOpts = writeOpts ++ Map(
    DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
    HoodieCompactionConfig.INLINE_COMPACT.key -> "false")

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    // Pinned rather than assumed: the nested vectorized reader is off by default on Spark 3.3.
    spark.conf.set(vectorizedKey, "true")
    spark.conf.set("spark.sql.parquet.enableNestedColumnVectorizedReader", "true")
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupResources()
    spark = null
  }

  /**
   * A wide scan leaves the session conf alone, so later narrow Hudi scans and plain Parquet scans
   * in the same session stay columnar.
   */
  @Test
  def testWideScanKeepsLaterScansColumnar(): Unit = {
    val source = wideSource(0 until 20, i => s"$i")
    writeHudi(source, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    val parquetPath = tempDir.resolve("plain_parquet").toAbsolutePath.toString
    spark.range(10).selectExpr("id", "named_struct('a', id, 'b', cast(id as string)) as s")
      .write.parquet(parquetPath)

    val wide = spark.read.format("hudi").load(basePath)
    assertWide(wide)
    assertEquals(20, wide.collect().length)
    assertFalse(fileScan(wide).supportsColumnar, "A scan over maxFields leaves is row-based")

    val narrowHudi = spark.read.format("hudi").load(basePath).select("id", "s1")
    assertEquals(20, narrowHudi.collect().length)
    val plainParquet = spark.read.parquet(parquetPath).select("id", "s")
    assertEquals(10, plainParquet.collect().length)

    assertEquals(
      Map("session conf" -> "true", "narrow Hudi scan columnar" -> "true", "plain parquet scan columnar" -> "true"),
      Map("session conf" -> spark.conf.get(vectorizedKey),
        "narrow Hudi scan columnar" -> fileScan(narrowHudi).supportsColumnar.toString,
        "plain parquet scan columnar" -> fileScan(plainParquet).supportsColumnar.toString))
  }

  /**
   * A scan planned for batches keeps returning batches when the conf changes before it runs.
   */
  @Test
  def testPlannedBatchesSurviveConfChange(): Unit = {
    writeHudi(wideSource(0 until 20, i => s"$i"), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    val narrow = spark.read.format("hudi").load(basePath).select("id", "s1")
    assertTrue(fileScan(narrow).supportsColumnar, "The narrow COW scan is planned for batches")
    spark.conf.set(vectorizedKey, "false")
    try {
      val ids = narrow.collect().map(_.getString(0).toInt).toSeq.sorted
      assertEquals(0 until 20, ids)
    } finally {
      spark.conf.set(vectorizedKey, "true")
    }
  }

  /**
   * A wide scan returns rows but decodes them with the vectorized reader, and the rows are the ones
   * written.
   */
  @Test
  def testWideScanDecodesVectorized(): Unit = {
    val source = wideSource(0 until 20, i => s"$i")
    writeHudi(source, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)

    val wide = spark.read.format("hudi").load(basePath).select(source.columns.map(col): _*)
    assertWide(wide)
    val scan = fileScan(wide)
    assertTrue(scan.relation.fileFormat.isInstanceOf[HoodieFileGroupReaderBasedFileFormat])
    assertFalse(scan.supportsColumnar, "A scan over maxFields leaves is row-based")
    val rowClasses = rowClassesByPartition(wide).values.flatten.toSet
    assertFalse(rowClasses.isEmpty)
    assertFalse(rowClasses.contains(unsafeRow),
      s"The wide scan must decode with the vectorized reader, got rows of $rowClasses")

    assertSameRows(source, wide)
    assertSameRows(source, withVectorizedReaderOff(
      spark.read.format("hudi").load(basePath).select(source.columns.map(col): _*)))
  }

  /**
   * A file whose nested column changed type cannot be decoded vectorized. A scan that returns rows
   * reads that file row-based and the other files vectorized; a scan that returns batches cannot
   * mix the two and still fails as before.
   */
  @Test
  def testNestedTypeChangeReadsRowBasedPerFile(): Unit = {
    val widenedBase = 10000000000L
    writeHudi(wideSource(0 until 30, i => s"$i"), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    // Rewrites only the p0 file group with `nested.a` as a long; the p1 and p2 files keep the int.
    writeHudi(wideSource((0 until 30).filter(_ % 3 == 0), i => s"${widenedBase + i}L"),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
    val expected = (0 until 30).map(i => (s"$i", if (i % 3 == 0) widenedBase + i else i.toLong))

    val wide = spark.read.format("hudi").load(basePath)
    assertWide(wide)
    val actual = wide.collect().map(r => (r.getAs[String]("id"), r.getAs[Row]("nested").getLong(0)))
      .toSeq.sortBy(_._1.toInt)
    assertEquals(expected, actual)
    val classes = rowClassesByPartition(wide)
    assertFalse(classes("p0").contains(unsafeRow), s"p0 has no type change and decodes vectorized: $classes")
    assertEquals(Set(unsafeRow), classes("p1"), s"p1 falls back to the row-based reader: $classes")
    assertEquals(Set(unsafeRow), classes("p2"), s"p2 falls back to the row-based reader: $classes")

    val narrow = spark.read.format("hudi").load(basePath).select("id", "nested")
    assertTrue(fileScan(narrow).supportsColumnar, "The narrow COW scan returns batches")
    val thrown = assertThrows(classOf[Throwable]) {
      narrow.collect()
    }
    val causes = Iterator.iterate(thrown: Throwable)(_.getCause).takeWhile(_ != null).take(10).toSeq
    assertTrue(causes.exists(c => c.isInstanceOf[IllegalArgumentException]
      && String.valueOf(c.getMessage).contains("cannot be read in vectorized mode")),
      s"Expected the non-atomic type-change rejection but got: $thrown")
  }

  /**
   * A wide MOR scan decodes base-only slices vectorized and merges slices with log files through
   * the file-group reader, row-based, in the same scan.
   */
  @Test
  def testWideMorScanMixesVectorizedBaseFilesAndMergedSlices(): Unit = {
    writeHudi(wideSource(0 until 30, i => s"$i"), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, morWriteOpts)
    // Only p0 gets a log file.
    writeHudi(wideSource((0 until 30).filter(_ % 3 == 0), i => s"${i + 1000}"),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, morWriteOpts)
    val expected = wideSource(0 until 30, i => if (i % 3 == 0) s"${i + 1000}" else s"$i")

    val wide = spark.read.format("hudi").load(basePath).select(expected.columns.map(col): _*)
    assertWide(wide)
    assertFalse(fileScan(wide).supportsColumnar, "A MOR scan is row-based")
    val classes = rowClassesByPartition(wide)
    assertEquals(Set(unsafeRow), classes("p0"), s"The merged p0 slice goes through the file-group reader: $classes")
    assertFalse(classes("p1").contains(unsafeRow), s"The base-only p1 slice decodes vectorized: $classes")
    assertFalse(classes("p2").contains(unsafeRow), s"The base-only p2 slice decodes vectorized: $classes")

    assertSameRows(expected, wide)
    assertSameRows(expected, withVectorizedReaderOff(
      spark.read.format("hudi").load(basePath).select(expected.columns.map(col): _*)))
  }

  /**
   * Vector columns need row access to turn their binary values back into arrays, so a wide scan
   * over them stays row-based even though it returns rows.
   */
  @Test
  def testWideScanWithVectorColumnStaysRowBased(): Unit = {
    val vectorMetadata = new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)").build()
    val source = wideSource(0 until 20, i => s"$i").withColumn("embedding",
      // coalesce keeps the elements non-null, which VECTOR requires.
      expr("array(coalesce(cast(id as float), cast(0 as float)), cast(1 as float), cast(2 as float))")
        .as("embedding", vectorMetadata))
    writeHudi(source, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)

    val wide = spark.read.format("hudi").load(basePath).select(source.columns.map(col): _*)
    assertWide(wide)
    val rowClasses = rowClassesByPartition(wide).values.flatten.toSet
    assertEquals(Set(unsafeRow), rowClasses)
    val embeddings = wide.collect().map(r => r.getAs[String]("id").toInt -> r.getAs[Seq[Float]]("embedding")).toMap
    assertEquals((0 until 20).map(i => i -> Seq(i.toFloat, 1f, 2f)).toMap, embeddings)
  }

  /**
   * Spark 4.1+ variants stay row-based on a wide scan too, not only when the scan asks for batches.
   */
  @Test
  def testWideScanWithVariantStaysRowBased(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_1, "Variant reads are vetoed on Spark 4.1+ only")
    val source = wideSource(0 until 20, i => s"$i").withColumn("v", expr("parse_json(concat('{\"k\":', id, '}'))"))
    writeHudi(source, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)

    val wide = spark.read.format("hudi").load(basePath).select(source.columns.map(col): _*)
    assertWide(wide)
    assertEquals(Set(unsafeRow), rowClassesByPartition(wide).values.flatten.toSet)
    val values = wide.selectExpr("id", "to_json(v)").collect().map(r => r.getString(0) -> r.getString(1)).toMap
    assertEquals((0 until 20).map(i => s"$i" -> s"""{"k":$i}""").toMap, values)
  }

  /**
   * Rows keyed by `id` over three partitions, with a `nested` struct whose `a` field is
   * `nestedA(i)` and enough struct columns for the scan to exceed spark.sql.codegen.maxFields.
   */
  private def wideSource(ids: Seq[Int], nestedA: Int => String): DataFrame = {
    val rows = ids.map(i => s"($i, ${nestedA(i)})").mkString(", ")
    val structColumns = (1 to numStructColumns).map(c =>
      s"named_struct('a', cast(i as int) + $c, 'b', concat('v', i, '_$c'), 'c', array(cast(i as int), $c)) as s$c")
    spark.sql(s"select * from values $rows as t(i, na)")
      .selectExpr(Seq(
        "cast(i as string) as id",
        "cast(i as long) as ts",
        "concat('p', i % 3) as partition",
        "named_struct('a', na, 'b', concat('n', i)) as nested") ++ structColumns: _*)
  }

  private def writeHudi(df: DataFrame, operation: String, opts: Map[String, String] = writeOpts): Unit = {
    df.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  private def assertWide(df: DataFrame): Unit =
    assertTrue(WholeStageCodegenExec.isTooManyFields(spark.sessionState.conf, df.schema),
      "The scan must have more leaf fields than spark.sql.codegen.maxFields")

  /** Collects `df` with the vectorized reader off, as the reference result. */
  private def withVectorizedReaderOff(df: => DataFrame): DataFrame = {
    spark.conf.set(vectorizedKey, "false")
    try {
      spark.createDataFrame(spark.sparkContext.parallelize(df.collect().toSeq), df.schema)
    } finally {
      spark.conf.set(vectorizedKey, "true")
    }
  }

  private def assertSameRows(expected: DataFrame, actual: DataFrame): Unit = {
    def sorted(df: DataFrame): Seq[Row] = df.collect().toSeq.sortBy(_.getAs[String]("id"))
    assertEquals(sorted(expected), sorted(actual))
  }
}

object TestVectorizedParquetReadPerScan extends AdaptiveSparkPlanHelper {

  def fileScan(df: DataFrame): FileSourceScanExec = {
    val scans = collect(df.queryExecution.executedPlan) { case s: FileSourceScanExec => s }
    assertEquals(1, scans.size, s"Expected one file scan in ${df.queryExecution.executedPlan}")
    scans.head
  }

  /**
   * The classes of the rows the scan's reader returns, per value of the `partition` column. Only
   * for a scan that returns rows.
   */
  def rowClassesByPartition(df: DataFrame): Map[String, Set[String]] = {
    val scan = fileScan(df)
    assertFalse(scan.supportsColumnar, "The row-class probe needs a scan that returns rows")
    val partitionIndex = scan.output.indexWhere(_.name == "partition")
    assertTrue(partitionIndex >= 0, s"No partition column in ${scan.output}")
    scan.inputRDDs().head
      .mapPartitions(_.map(r => (r.getUTF8String(partitionIndex).toString, r.getClass.getSimpleName)))
      .distinct().collect().toSeq
      .groupBy(_._1).map { case (partition, pairs) => partition -> pairs.map(_._2).toSet }
  }
}
