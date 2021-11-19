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

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.index.zorder.ZOrderingIndexHelper
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.sql.{Date, Timestamp}
import scala.collection.JavaConversions._
import scala.util.Random

@Tag("functional")
class TestTableLayoutOptimization extends HoodieClientTestBase {
  var spark: SparkSession = _

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
  def testZIndexTableComposition(): Unit = {
    val inputDf =
      spark.read.parquet(
        getClass.getClassLoader.getResource("index/zorder/input-table").toString
      )

    val zorderedCols = Set("c1", "c2", "c3", "c5", "c6", "c7", "c8")
    val zorderedColsSchemaFields = inputDf.schema.fields.filter(f => zorderedCols.contains(f.name)).toSeq

    // {@link TimestampType} is not supported, and will throw -- hence skipping "c4"
    val newZIndexTableDf =
      ZOrderingIndexHelper.buildZIndexTableFor(
        inputDf.sparkSession,
        inputDf.inputFiles.toSeq,
        zorderedColsSchemaFields
      )
    newZIndexTableDf.cache()

    val expectedZIndexTableDf =
      spark.read
        .json(getClass.getClassLoader.getResource("index/zorder/z-index-table.json").toString)

    assertRowsMatch(expectedZIndexTableDf, newZIndexTableDf)
  }

  @Test
  def testZIndexTableMerge(): Unit = {
    // TODO
  }

  @Test
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
      inputDf.inputFiles.toSeq,
      Seq("c1","c2","c3","c5","c6","c7","c8"),
      testZIndexPath.toString,
      "2",
      Seq("0", "1")
    )

    // Save again
    ZOrderingIndexHelper.updateZIndexFor(
      inputDf.sparkSession,
      inputDf.inputFiles.toSeq,
      Seq("c1","c2","c3","c5","c6","c7","c8"),
      testZIndexPath.toString,
      "3",
      Seq("0", "1", "2")
    )

    // Test old index table being cleaned up
    ZOrderingIndexHelper.updateZIndexFor(
      inputDf.sparkSession,
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

  private def assertRowsMatch(one: DataFrame, other: DataFrame) = {
    val rows = one.count()
    assert(rows == other.count() && one.intersect(other).count() == rows)
  }

  def createComplexDataFrame(spark: SparkSession): DataFrame = {
    val schema = new StructType()
      .add("c1", IntegerType)
      .add("c2", StringType)
      .add("c3", DecimalType(9,3))
      .add("c4", TimestampType)
      .add("c5", ShortType)
      .add("c6", DateType)
      .add("c7", BinaryType)
      .add("c8", ByteType)

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
    spark.createDataFrame(rdd, schema)
  }
}
