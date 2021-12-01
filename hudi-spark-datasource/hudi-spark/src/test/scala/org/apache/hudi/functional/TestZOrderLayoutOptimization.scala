/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.index.zorder.ZOrderingIndexHelper
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.sql.{Date, Timestamp}
import scala.collection.JavaConversions._
import scala.util.Random

@Tag("functional")
class TestZOrderLayoutOptimization extends HoodieClientTestBase {
  var spark: SparkSession = _

  val sourceTableSchema =
    new StructType()
      .add("c1", IntegerType)
      .add("c2", StringType)
      .add("c3", DecimalType(9,3))
      .add("c4", TimestampType)
      .add("c5", ShortType)
      .add("c6", DateType)
      .add("c7", BinaryType)
      .add("c8", ByteType)

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach
  override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testZOrderingLayoutClustering(tableType: String): Unit = {
    val targetRecordsCount = 10000
    // Bulk Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("001", targetRecordsCount)).toList
    val writeDf: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records, 2))

    writeDf.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false")
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), tableType)
      // option for clustering
      .option("hoodie.parquet.small.file.limit", "0")
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "1")
      .option("hoodie.clustering.plan.strategy.target.file.max.bytes", "1073741824")
      .option("hoodie.clustering.plan.strategy.small.file.limit", "629145600")
      .option("hoodie.clustering.plan.strategy.max.bytes.per.group", Long.MaxValue.toString)
      .option("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(64 * 1024 * 1024L))
      .option(HoodieClusteringConfig.LAYOUT_OPTIMIZE_ENABLE.key, "true")
      .option(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key, "begin_lat, begin_lon")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val hudiMetaClient = HoodieTableMetaClient.builder
      .setConf(hadoopConf)
      .setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true)
      .build

    val lastCommit = hudiMetaClient.getActiveTimeline.getAllCommitsTimeline.lastInstant().get()

    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, lastCommit.getAction)
    assertEquals(HoodieInstant.State.COMPLETED, lastCommit.getState)

    val readDf =
      spark.read
        .format("hudi")
        .load(basePath)

    val readDfSkip =
      spark.read
        .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key(), "true")
        .format("hudi")
        .load(basePath)

    assertEquals(targetRecordsCount, readDf.count())
    assertEquals(targetRecordsCount, readDfSkip.count())

    readDf.createOrReplaceTempView("hudi_snapshot_raw")
    readDfSkip.createOrReplaceTempView("hudi_snapshot_skipping")

    def select(tableName: String) =
      spark.sql(s"SELECT * FROM $tableName WHERE begin_lat >= 0.49 AND begin_lat < 0.51 AND begin_lon >= 0.49 AND begin_lon < 0.51")

    assertRowsMatch(
      select("hudi_snapshot_raw"),
      select("hudi_snapshot_skipping")
    )
  }

  @Test
  @Disabled
  def testZIndexTableComposition(): Unit = {
    val inputDf =
      // NOTE: Schema here is provided for validation that the input date is in the appropriate format
      spark.read
        .schema(sourceTableSchema)
        .parquet(
          getClass.getClassLoader.getResource("index/zorder/input-table").toString
        )

    val zorderedCols = Seq("c1", "c2", "c3", "c5", "c6", "c7", "c8")
    val zorderedColsSchemaFields = inputDf.schema.fields.filter(f => zorderedCols.contains(f.name)).toSeq

    // {@link TimestampType} is not supported, and will throw -- hence skipping "c4"
    val newZIndexTableDf =
      ZOrderingIndexHelper.buildZIndexTableFor(
        inputDf.sparkSession,
        inputDf.inputFiles.toSeq,
        zorderedColsSchemaFields
      )

    val indexSchema =
      ZOrderingIndexHelper.composeIndexSchema(
        sourceTableSchema.fields.filter(f => zorderedCols.contains(f.name)).toSeq
      )

    // Collect Z-index stats manually (reading individual Parquet files)
    val manualZIndexTableDf =
      buildZIndexTableManually(
        getClass.getClassLoader.getResource("index/zorder/input-table").toString,
        zorderedCols,
        indexSchema
      )

    // NOTE: Z-index is built against stats collected w/in Parquet footers, which will be
    //       represented w/ corresponding Parquet schema (INT, INT64, INT96, etc).
    //
    //       When stats are collected manually, produced Z-index table is inherently coerced into the
    //       schema of the original source Parquet base-file and therefore we have to similarly coerce newly
    //       built Z-index table (built off Parquet footers) into the canonical index schema (built off the
    //       original source file schema)
    assertEquals(asJson(sort(manualZIndexTableDf)), asJson(sort(newZIndexTableDf)))

    // Match against expected Z-index table
    val expectedZIndexTableDf =
      spark.read
        .schema(indexSchema)
        .json(getClass.getClassLoader.getResource("index/zorder/z-index-table.json").toString)

    assertEquals(asJson(sort(expectedZIndexTableDf)), asJson(sort(newZIndexTableDf)))
  }

  @Test
  @Disabled
  def testZIndexTableMerge(): Unit = {
    val testZIndexPath = new Path(basePath, "zindex")

    val zorderedCols = Seq("c1", "c2", "c3", "c5", "c6", "c7", "c8")
    val indexSchema =
      ZOrderingIndexHelper.composeIndexSchema(
        sourceTableSchema.fields.filter(f => zorderedCols.contains(f.name)).toSeq
      )

    //
    // Bootstrap Z-index table
    //

    val firstCommitInstance = "0"
    val firstInputDf =
      spark.read.parquet(
        getClass.getClassLoader.getResource("index/zorder/input-table").toString
      )

    ZOrderingIndexHelper.updateZIndexFor(
      firstInputDf.sparkSession,
      sourceTableSchema,
      firstInputDf.inputFiles.toSeq,
      zorderedCols.toSeq,
      testZIndexPath.toString,
      firstCommitInstance,
      Seq()
    )

    // NOTE: We don't need to provide schema upon reading from Parquet, since Spark will be able
    //       to reliably retrieve it
    val initialZIndexTable =
      spark.read
        .parquet(new Path(testZIndexPath, firstCommitInstance).toString)

    val expectedInitialZIndexTableDf =
        spark.read
          .schema(indexSchema)
          .json(getClass.getClassLoader.getResource("index/zorder/z-index-table.json").toString)

    assertEquals(asJson(sort(expectedInitialZIndexTableDf)), asJson(sort(initialZIndexTable)))

    val secondCommitInstance = "1"
    val secondInputDf =
      spark.read
        .schema(sourceTableSchema)
        .parquet(
          getClass.getClassLoader.getResource("index/zorder/another-input-table").toString
        )

    //
    // Update Z-index table
    //

    ZOrderingIndexHelper.updateZIndexFor(
      secondInputDf.sparkSession,
      sourceTableSchema,
      secondInputDf.inputFiles.toSeq,
      zorderedCols.toSeq,
      testZIndexPath.toString,
      secondCommitInstance,
      Seq(firstCommitInstance)
    )

    // NOTE: We don't need to provide schema upon reading from Parquet, since Spark will be able
    //       to reliably retrieve it
    val mergedZIndexTable =
      spark.read
        .parquet(new Path(testZIndexPath, secondCommitInstance).toString)

    val expectedMergedZIndexTableDf =
      spark.read
        .schema(indexSchema)
        .json(getClass.getClassLoader.getResource("index/zorder/z-index-table-merged.json").toString)

    assertEquals(asJson(sort(expectedMergedZIndexTableDf)), asJson(sort(mergedZIndexTable)))
  }

  @Test
  @Disabled
  def testZIndexTablesGarbageCollection(): Unit = {
    val testZIndexPath = new Path(System.getProperty("java.io.tmpdir"), "zindex")
    val fs = testZIndexPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val inputDf =
      spark.read.parquet(
        getClass.getClassLoader.getResource("index/zorder/input-table").toString
      )

    // Try to save statistics
    ZOrderingIndexHelper.updateZIndexFor(
      inputDf.sparkSession,
      sourceTableSchema,
      inputDf.inputFiles.toSeq,
      Seq("c1","c2","c3","c5","c6","c7","c8"),
      testZIndexPath.toString,
      "2",
      Seq("0", "1")
    )

    // Save again
    ZOrderingIndexHelper.updateZIndexFor(
      inputDf.sparkSession,
      sourceTableSchema,
      inputDf.inputFiles.toSeq,
      Seq("c1","c2","c3","c5","c6","c7","c8"),
      testZIndexPath.toString,
      "3",
      Seq("0", "1", "2")
    )

    // Test old index table being cleaned up
    ZOrderingIndexHelper.updateZIndexFor(
      inputDf.sparkSession,
      sourceTableSchema,
      inputDf.inputFiles.toSeq,
      Seq("c1","c2","c3","c5","c6","c7","c8"),
      testZIndexPath.toString,
      "4",
      Seq("0", "1", "3")
    )

    assertEquals(!fs.exists(new Path(testZIndexPath, "2")), true)
    assertEquals(!fs.exists(new Path(testZIndexPath, "3")), true)
    assertEquals(fs.exists(new Path(testZIndexPath, "4")), true)
  }

  private def buildZIndexTableManually(tablePath: String, zorderedCols: Seq[String], indexSchema: StructType) = {
    val files = {
      val it = fs.listFiles(new Path(tablePath), true)
      var seq = Seq[LocatedFileStatus]()
      while (it.hasNext) {
        seq = seq :+ it.next()
      }
      seq
    }

    spark.createDataFrame(
      files.flatMap(file => {
        val df = spark.read.schema(sourceTableSchema).parquet(file.getPath.toString)
        val exprs: Seq[String] =
          s"'${typedLit(file.getPath.getName)}' AS file" +:
          df.columns
            .filter(col => zorderedCols.contains(col))
            .flatMap(col => {
              val minColName = s"${col}_minValue"
              val maxColName = s"${col}_maxValue"
              Seq(
                s"min($col) AS $minColName",
                s"max($col) AS $maxColName",
                s"sum(cast(isnull($col) AS long)) AS ${col}_num_nulls"
              )
            })

        df.selectExpr(exprs: _*)
          .collect()
      }),
      indexSchema
    )
  }

  private def asJson(df: DataFrame) =
    df.toJSON
      .select("value")
      .collect()
      .toSeq
      .map(_.getString(0))
      .mkString("\n")

  private def assertRowsMatch(one: DataFrame, other: DataFrame) = {
    val rows = one.count()
    assert(rows == other.count() && one.intersect(other).count() == rows)
  }

  private def sort(df: DataFrame): DataFrame = {
    // Since upon parsing JSON, Spark re-order columns in lexicographical order
    // of their names, we have to shuffle new Z-index table columns order to match
    // Rows are sorted by filename as well to avoid
    val sortedCols = df.columns.sorted
    df.select(sortedCols.head, sortedCols.tail: _*)
      .sort("file")
  }

  def createComplexDataFrame(spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext.parallelize(0 to 1000, 1).map { item =>
      val c1 = Integer.valueOf(item)
      val c2 = s" ${item}sdc"
      val c3 = new java.math.BigDecimal(s"${Random.nextInt(1000)}.${item}")
      val c4 = new Timestamp(System.currentTimeMillis())
      val c5 = java.lang.Short.valueOf(s"${(item + 16) /10}")
      val c6 = Date.valueOf(s"${2020}-${item % 11  +  1}-${item % 28  + 1}")
      val c7 = Array(item).map(_.toByte)
      val c8 = java.lang.Byte.valueOf("9")

      RowFactory.create(c1, c2, c3, c4, c5, c6, c7, c8)
    }
    spark.createDataFrame(rdd, sourceTableSchema)
  }
}
