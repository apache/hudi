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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieSchemaConversionUtils, ScalaAssertionSupport, SparkAdapterSupport}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.config.{HoodieCommonConfig, RecordMergeMode}
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, TableSchemaResolver}
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.exception.SchemaCompatibilityException
import org.apache.hudi.functional.TestBasicSchemaEvolution.{dropColumn, injectColumnAt}
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieSparkClientTestBase}
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{functions, DataFrame, Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}

import java.util.function.Consumer

import scala.collection.JavaConverters._

class TestBasicSchemaEvolution extends HoodieSparkClientTestBase with ScalaAssertionSupport with SparkAdapterSupport {

  var spark: SparkSession = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key() -> "true",
    HoodieWriteConfig.RECORD_MERGE_MODE.key() -> RecordMergeMode.COMMIT_TIME_ORDERING.name(),
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,bulk_insert,true",
    "COPY_ON_WRITE,bulk_insert,false",
    "COPY_ON_WRITE,insert,true",
    "COPY_ON_WRITE,insert,false",
    "COPY_ON_WRITE,upsert,true",
    "COPY_ON_WRITE,upsert,false",
    "MERGE_ON_READ,bulk_insert,true",
    "MERGE_ON_READ,bulk_insert,false",
    "MERGE_ON_READ,insert,true",
    "MERGE_ON_READ,insert,false",
    "MERGE_ON_READ,upsert,true",
    "MERGE_ON_READ,upsert,false"
  ))
  def testBasicSchemaEvolution(tableType: HoodieTableType, opType: String, shouldReconcileSchema: Boolean): Unit = {
    // open the schema validate
    val opts = commonOpts ++
      Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
        HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key -> "true",
        DataSourceWriteOptions.RECONCILE_SCHEMA.key -> shouldReconcileSchema.toString,
        DataSourceWriteOptions.OPERATION.key -> opType
      )

    def appendData(schema: StructType, batch: Seq[Row], shouldAllowDroppedColumns: Boolean = false): Unit = {
      sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, batch, schema)
        .write
        .format("org.apache.hudi")
        .options(opts ++ Map(HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key -> shouldAllowDroppedColumns.toString))
        .mode(SaveMode.Append)
        .save(basePath)
    }

    def loadTable(): (StructType, Seq[Row]) = {
      val tableMetaClient = createMetaClient(spark, basePath)

      tableMetaClient.reloadActiveTimeline()

      val resolver = new TableSchemaResolver(tableMetaClient)
      val latestTableSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(resolver.getTableSchema(false))

      val df =
        spark.read.format("org.apache.hudi")
          .load(basePath)
          .drop(HoodieRecord.HOODIE_META_COLUMNS.asScala.toSeq: _*)
          .orderBy(functions.col("_row_key").cast(IntegerType))

      (latestTableSchema, df.collectAsList.asScala.toSeq)
    }

    //
    // 1. Write 1st batch with schema A
    //

    val firstSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("last_name", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val firstBatch = Seq(
      Row("1", "Andy", "Cooper", 1, 1),
      Row("2", "Lisi", "Wallace", 1, 1),
      Row("3", "Zhangsan", "Shu", 1, 1))

    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, firstBatch, firstSchema)
      .write
      .format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    //
    // 2. Write 2d batch with another schema (added column `age`)
    //

    val secondSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("last_name", StringType, nullable = true) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val secondSchemaWithOrdering = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("last_name", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) ::
        StructField("age", StringType, nullable = true) :: Nil)

    val secondBatch = Seq(
      Row("4", "John", "Green", "10", 1, 1),
      Row("5", "Jack", "Sparrow", "13", 1, 1),
      Row("6", "Jill", "Fiorella", "12", 1, 1))

    appendData(secondSchema, secondBatch)
    val (tableSchemaAfterSecondBatch, rowsAfterSecondBatch) = loadTable()

    // NOTE: In case schema reconciliation is ENABLED, Hudi would prefer the new batch's schema (since it's adding a
    //       new column, compared w/ the table's one), therefore this case would be identical to reconciliation
    //       being DISABLED
    //
    //       In case schema reconciliation is DISABLED, table will be overwritten in the batch's schema,
    //       entailing that the data in the added columns for table's existing records will be added w/ nulls,
    //       in case new column is nullable, and would fail otherwise
    if (true) {
      if (shouldReconcileSchema) {
        assertEquals(secondSchema, tableSchemaAfterSecondBatch)
        val ageColOrd = secondSchema.indexWhere(_.name == "age")
        val rowsToAdd = secondBatch

        val expectedRows = injectColumnAt(firstBatch, ageColOrd, null) ++ rowsToAdd
        assertEquals(expectedRows, rowsAfterSecondBatch)
      } else {
        // Second schema for the table is expected to reconcile ordering if enabled

        // Reorder batch based on the expected schema
        val secondBatchWithProperOrder = Seq(
          Row("4", "John", "Green", 1, 1, "10"),
          Row("5", "Jack", "Sparrow", 1, 1, "13"),
          Row("6", "Jill", "Fiorella", 1, 1, "12"))

        assertEquals(secondSchemaWithOrdering, tableSchemaAfterSecondBatch)
        val ageColOrd = secondSchemaWithOrdering.indexWhere(_.name == "age")
        val rowsToAdd = secondBatchWithProperOrder
        val expectedRows = injectColumnAt(firstBatch, ageColOrd, null) ++ rowsToAdd
        assertEquals(expectedRows, rowsAfterSecondBatch)
      }
    }

    //
    // 3. Write 3d batch with another schema (w/ omitted a _nullable_ column `second_name`, expected to succeed if
    // col drop is enabled)
    //

    val thirdSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val thirdSchemaWithOrdering = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) ::
        StructField("age", StringType, nullable = true) :: Nil)

    val thirdBatch = Seq(
      Row("7", "Harry", "15", 1, 1),
      Row("8", "Ron", "14", 1, 1),
      Row("9", "Germiona", "16", 1, 1))

    if (shouldReconcileSchema) {
      appendData(thirdSchema, thirdBatch)
    } else {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(thirdSchema, thirdBatch)
      }
      appendData(thirdSchema, thirdBatch, shouldAllowDroppedColumns = true)
    }
    val (tableSchemaAfterThirdBatch, rowsAfterThirdBatch) = loadTable()

    // NOTE: In case schema reconciliation is ENABLED, Hudi would prefer the table's schema over the new batch
    //       schema (since we drop the column in the new batch), therefore table's schema after commit will actually
    //       stay the same, adding back (dropped) columns to the records in the batch (setting them as null).
    //
    //       In case schema reconciliation is DISABLED, table will be overwritten in the batch's schema,
    //       entailing that the data in the dropped columns for table's existing records will be dropped.
    if (shouldReconcileSchema) {
      assertEquals(secondSchema, tableSchemaAfterThirdBatch)

      val lastNameColOrd = firstSchema.indexWhere(_.name == "last_name")
      val expectedRows = rowsAfterSecondBatch ++ injectColumnAt(thirdBatch, lastNameColOrd, null)

      assertEquals(expectedRows, rowsAfterThirdBatch)
    } else {
      assertEquals(thirdSchemaWithOrdering, tableSchemaAfterThirdBatch)

      val lastNameColOrd = secondSchemaWithOrdering.indexWhere(_.name == "last_name")
      // properly maintain order of columns
      val rowsToAdd = Seq(
        Row("7", "Harry", 1, 1, "15"),
        Row("8", "Ron", 1, 1, "14"),
        Row("9", "Germiona", 1, 1, "16"))
      val expectedRows = dropColumn(rowsAfterSecondBatch, lastNameColOrd) ++ rowsToAdd

      assertEquals(expectedRows, rowsAfterThirdBatch)
    }

    //
    // 4. Write 4th batch with another schema (w/ omitted a _non-nullable_ column `first_name`, expected to fail
    //    in case when schema reconciliation is enabled, expected to succeed otherwise)
    //

    val fourthSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val fourthBatch = Seq(
      Row("10", "15", 1, 1),
      Row("11", "14", 1, 1),
      Row("12", "16", 1, 1))

    // NOTE: In case schema reconciliation is ENABLED, Hudi would prefer the table's schema over the new batch
    //       schema, therefore table's schema after commit will actually stay the same, adding back (dropped) columns
    //       to the records in the batch. Since batch omits column that is designated as non-null, write is expected
    //       to fail (being unable to set the missing column values to null).
    //
    //       In case schema reconciliation is DISABLED, table will be overwritten in the batch's schema,
    //       entailing that the data in the dropped columns for table's existing records will be dropped.
    if (shouldReconcileSchema) {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(fourthSchema, fourthBatch)
      }
    } else {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(fourthSchema, fourthBatch)
      }
      appendData(fourthSchema, fourthBatch, shouldAllowDroppedColumns = true)
      val (latestTableSchema, rows) = loadTable()

      val fourthSchemaWithOrdering = StructType(
        StructField("_row_key", StringType, nullable = true) ::
          StructField("timestamp", IntegerType, nullable = true) ::
          StructField("partition", IntegerType, nullable = true) ::
          StructField("age", StringType, nullable = true) :: Nil)
      assertEquals(fourthSchemaWithOrdering, latestTableSchema)

      val firstNameColOrd = thirdSchemaWithOrdering.indexWhere(_.name == "first_name")

      // Order the columns
      val rowsToAdd = Seq(
        Row("10", 1, 1, "15"),
        Row("11", 1, 1, "14"),
        Row("12", 1, 1, "16"))
      val expectedRecords =
        dropColumn(rowsAfterThirdBatch, firstNameColOrd) ++ rowsToAdd

      assertEquals(expectedRecords, rows)
    }

    //
    // 5. Write 5th batch with another schema w/ data-type changing for a column `timestamp`;
    //      - Expected to succeed when reconciliation is off, and
    //      - Expected to fail when reconciliation is on (b/c we can't down-cast Long to Int)
    //

    val fifthSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", LongType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val fifthBatch = Seq(
      Row("10", "15", 9876543210L, 1),
      Row("11", "14", 9876543211L, 1),
      Row("12", "16", 9876543212L, 1))

    if (shouldReconcileSchema) {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(fifthSchema, fifthBatch)
      }
    } else {
      appendData(fifthSchema, fifthBatch)

      // TODO(SPARK-40876) this is disabled, until primitive-type promotions are properly supported
      //                   w/in Spark's vectorized reader
      //val (latestTableSchema, rows) = loadTable()
    }

    //
    // 6. Write 6th batch with another schema (w/ data-type changed for a column `timestamp`, expected to fail)
    //

    val sixthSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", StringType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val sixthBatch = Seq(
      Row("10", "15", "1", 1),
      Row("11", "14", "1", 1),
      Row("12", "16", "1", 1))

    // Now, only fails for reconcile
    if (shouldReconcileSchema) {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(sixthSchema, sixthBatch)
      }
    } else {
      appendData(sixthSchema, sixthBatch)
    }


    // TODO add test w/ overlapping updates
  }

  private def schemaOnReadOpts(tableType: HoodieTableType): Map[String, String] = commonOpts ++ Map(
    DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
    HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key -> "true",
    // HoodieSparkSqlWriter turns inline compaction on for batch MOR writes; keep commit 2 in a log file
    HoodieCompactionConfig.INLINE_COMPACT.key -> "false")

  /**
   * Add-column evolution under schema-on-read, read through the file-group reader. Commit 2 only
   * touches partition p1: on COW that rewrites the p1 file group with the new column while the p2
   * base file keeps the old schema; on MOR p1 gains a log file and the base files of both partitions
   * keep the old schema. The snapshot read must therefore fill `bonus` with null for rows served from
   * an old-schema base file, and a pushed-down filter over `bonus` must be dropped for a file that
   * lacks the column (InternalSchemaUtils.reBuildFilterName's "added column" branch). The incremental
   * read after the evolution must return exactly the commit-2 records with their `bonus` values.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSchemaOnReadAddColumnSnapshotAndIncrementalRead(tableType: HoodieTableType): Unit = {
    val _spark = spark
    import _spark.implicits._
    val opts = schemaOnReadOpts(tableType)

    // commit 1: ages 10..17, even ids in p1, odd ids in p2
    val v1 = (0 until 8).map(i => (s"id$i", s"n$i", 10 + i, 1L, if (i % 2 == 0) "p1" else "p2"))
      .toDF("_row_key", "name", "age", "timestamp", "partition")
    v1.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val firstCompletion = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

    // commit 2, p1 only: update id2 and insert id8, both carrying the new nullable `bonus` column
    val v2 = Seq[(String, String, Int, Long, String, scala.Option[Double])](
      ("id2", "n2u", 12, 2L, "p1", Some(100.0d)),
      ("id8", "n8", 20, 2L, "p1", Some(300.0d)))
      .toDF("_row_key", "name", "age", "timestamp", "partition", "bonus")
    v2.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot = spark.read.format("hudi")
      .option(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key, "true")
      .load(basePath)
    assertEquals(DoubleType, snapshot.schema("bonus").dataType)
    assertEquals(9, snapshot.count())
    assertEquals(2, snapshot.filter("bonus is not null").count())
    // ages are now {10..17, 20}
    assertEquals(7, snapshot.filter("age >= 12").count())
    assertEquals(2, snapshot.filter("age >= 12 AND bonus is not null").count())
    // the `bonus` predicate is evaluated against p2's file, which does not contain the column
    assertEquals(0, snapshot.filter("partition = 'p2' AND bonus is not null").count())
    assertEquals(4, snapshot.filter("partition = 'p2' AND bonus is null").count())

    val byId = snapshot.select("_row_key", "name", "age", "bonus").collect().map(r => r.getString(0) -> r).toMap
    assertEquals("n2u", byId("id2").getString(1))
    assertEquals(100.0d, byId("id2").getDouble(3))
    // id1 is served from the untouched p2 base file that lacks `bonus`; id0 from the rewritten p1
    // file group (COW) or the merged p1 base+log slice (MOR)
    assertEquals(11, byId("id1").getInt(2))
    assertTrue(byId("id1").isNullAt(3))
    assertTrue(byId("id0").isNullAt(3))

    val incremental = spark.read.format("hudi")
      .option(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key, "true")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, firstCompletion)
      .load(basePath)
    val incRows = incremental.select("_row_key", "bonus").collect().map(r => r.getString(0) -> r.getDouble(1)).toMap
    assertEquals(Map("id2" -> 100.0d, "id8" -> 300.0d), incRows)
  }

  /**
   * int->long promotion under schema-on-read on a MOR table. Commit 2 only touches p1, so the read
   * spans a base-only int file (p2) and an int base file merged with a long log file (p1). The
   * top-level `age` promotion is atomic and is read with the vectorized parquet reader on. When the
   * same promotion is applied inside the `nested` struct the changed top-level column is no longer
   * atomic: ParquetSchemaEvolutionUtils.getHadoopConfClone must reject it fast on the base slice
   * instead of returning corrupt columns, and the workaround it advertises (disabling the vectorized
   * reader) must actually widen `nested.a` across both shapes. COW is covered by
   * TestLegacyParquetReadPath#testCowSnapshotReadWithNestedTypeChange.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testSchemaOnReadTypePromotionOnMorBaseAndLogMerge(promoteNested: Boolean): Unit = {
    val _spark = spark
    import _spark.implicits._
    val opts = schemaOnReadOpts(HoodieTableType.MERGE_ON_READ)
    val widenedBase = 10000000000L
    // id4's `nested.a` only leaves the int range in the promoting arm
    val expectedNestedA4 = if (promoteNested) widenedBase + 4 else 4L

    // commit 1: `age` and `nested.a` are int; even ids in p1, odd ids in p2
    val v1 = (0 until 6).map(i => (s"id$i", s"n$i", 10 + i, i, s"v$i", 1L, if (i % 2 == 0) "p1" else "p2"))
      .toDF("_row_key", "name", "age", "a", "b", "timestamp", "partition")
      .withColumn("nested", functions.struct(functions.col("a"), functions.col("b")))
      .drop("a", "b")
    v1.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // commit 2, p1 only: `age` is long now; `nested.a` is promoted to long only in the second arm
    val v2Raw = Seq(
      ("id2", "n2u", 12L, 2L, "v2u", 2L, "p1"),
      ("id4", "n4u", widenedBase + 14, expectedNestedA4, "v4u", 2L, "p1"),
      ("id6", "n6", 42L, 6L, "v6", 2L, "p1"))
      .toDF("_row_key", "name", "age", "a", "b", "timestamp", "partition")
    val v2 = (if (promoteNested) v2Raw else v2Raw.withColumn("a", functions.col("a").cast(IntegerType)))
      .withColumn("nested", functions.struct(functions.col("a"), functions.col("b")))
      .drop("a", "b")
    v2.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    def loadSnapshot(): DataFrame = spark.read.format("hudi")
      .option(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key, "true")
      .load(basePath)

    def assertPromotedRows(df: DataFrame): Unit = {
      assertEquals(7, df.count())
      val byId = df.selectExpr("_row_key", "name", "age", "cast(nested.a as long)", "nested.b").collect()
        .map(r => r.getString(0) -> (r.getString(1), r.getLong(2), r.getLong(3), r.getString(4))).toMap
      // id1: untouched p2 base file, int on disk, widened on read
      assertEquals(("n1", 11L, 1L, "v1"), byId("id1"))
      // id0: p1 int base file merged with the p1 log file, record itself untouched
      assertEquals(("n0", 10L, 0L, "v0"), byId("id0"))
      // id4: updated in the log with values outside the int range
      assertEquals(("n4u", widenedBase + 14, expectedNestedA4, "v4u"), byId("id4"))
      assertEquals(("n6", 42L, 6L, "v6"), byId("id6"))
      // filters over the promoted columns across int and long files: ages {10, 11, 12, 13, widened+14, 15, 42}
      assertEquals(2, df.filter("age > 40").count())
      assertEquals(1, df.filter("age > 1000000000").count())
      assertEquals(if (promoteNested) 1 else 0, df.filter("nested.a > 1000000000").count())
    }

    if (!promoteNested) {
      val snapshot = loadSnapshot()
      assertEquals(LongType, snapshot.schema("age").dataType)
      assertEquals(IntegerType, snapshot.schema("nested").dataType.asInstanceOf[StructType]("a").dataType)
      assertPromotedRows(snapshot)
    } else {
      // the non-atomic type change must fail fast in vectorized mode rather than return corrupt columns.
      // `nested` has to be projected for the guard to engage: a bare count() prunes it away and passes.
      val thrown = assertThrows(classOf[Throwable]) {
        loadSnapshot().select("_row_key", "nested").collect()
      }
      val causes = Iterator.iterate(thrown: Throwable)(_.getCause).takeWhile(_ != null).take(10).toSeq
      assertTrue(causes.exists(c => c.isInstanceOf[IllegalArgumentException]
        && String.valueOf(c.getMessage).contains("cannot be read in vectorized mode")),
        s"Expected the non-atomic type-change rejection but got: $thrown")

      val vectorizedKey = "spark.sql.parquet.enableVectorizedReader"
      val previous = spark.conf.get(vectorizedKey, "true")
      spark.conf.set(vectorizedKey, "false")
      try {
        val snapshot = loadSnapshot()
        assertEquals(LongType, snapshot.schema("age").dataType)
        assertEquals(LongType, snapshot.schema("nested").dataType.asInstanceOf[StructType]("a").dataType)
        assertPromotedRows(snapshot)
      } finally {
        spark.conf.set(vectorizedKey, previous)
      }
    }
  }
}

object TestBasicSchemaEvolution {

  def dropColumn(rows: Seq[Row], idx: Int): Seq[Row] =
    rows.map { r =>
      val values = r.toSeq.zipWithIndex
        .filterNot { case (_, cidx) => cidx == idx }
        .map { case (c, _) => c }
      Row(values: _*)
    }

  def injectColumnAt(rows: Seq[Row], idx: Int, value: Any): Seq[Row] =
    rows.map { r =>
      val (left, right) = r.toSeq.splitAt(idx)
      val values = (left :+ value) ++ right
      Row(values: _*)
    }

}
