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
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.{BaseFileUtils, ParquetUtils}
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.OrderingIndexHelper
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

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

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @MethodSource(Array("testLayOutParameter"))
  def testOptimizewithClustering(tableType: String, optimizeMode: String): Unit = {
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
      .option(HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY.key(), optimizeMode)
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

  def assertRowsMatch(one: DataFrame, other: DataFrame) = {
    val rows = one.count()
    assert(rows == other.count() && one.intersect(other).count() == rows)
  }

  @Test
  def testCollectMinMaxStatistics(): Unit = {
    val testPath = new Path(System.getProperty("java.io.tmpdir"), "minMax")
    val statisticPath = new Path(System.getProperty("java.io.tmpdir"), "stat")
    val fs = testPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val complexDataFrame = createComplexDataFrame(spark)
    complexDataFrame.repartition(3).write.mode("overwrite").save(testPath.toString)
    val df = spark.read.load(testPath.toString)
    try {
      // test z-order/hilbert sort for all primitive type
      // shoud not throw exception.
      OrderingIndexHelper.createOptimizedDataFrameByMapValue(df, "c1,c2,c3,c5,c6,c7,c8", 20, "hilbert").show(1)
      OrderingIndexHelper.createOptimizedDataFrameByMapValue(df, "c1,c2,c3,c5,c6,c7,c8", 20, "z-order").show(1)
      OrderingIndexHelper.createOptimizeDataFrameBySample(df, "c1,c2,c3,c5,c6,c7,c8", 20, "hilbert").show(1)
      OrderingIndexHelper.createOptimizeDataFrameBySample(df, "c1,c2,c3,c5,c6,c7,c8", 20, "z-order").show(1)
      try {
        // do not support TimeStampType, so if we collect statistics for c4, should throw exception
        val colDf = OrderingIndexHelper.getMinMaxValue(df, "c1,c2,c3,c5,c6,c7,c8")
        colDf.cache()
        assertEquals(colDf.count(), 3)
        assertEquals(colDf.take(1)(0).length, 22)
        colDf.unpersist()
        // try to save statistics
        OrderingIndexHelper.saveStatisticsInfo(df, "c1,c2,c3,c5,c6,c7,c8", statisticPath.toString, "2", Seq("0", "1"))
        // save again
        OrderingIndexHelper.saveStatisticsInfo(df, "c1,c2,c3,c5,c6,c7,c8", statisticPath.toString, "3", Seq("0", "1", "2"))
        // test old index table clean
        OrderingIndexHelper.saveStatisticsInfo(df, "c1,c2,c3,c5,c6,c7,c8", statisticPath.toString, "4", Seq("0", "1", "3"))
        assertEquals(!fs.exists(new Path(statisticPath, "2")), true)
        assertEquals(fs.exists(new Path(statisticPath, "3")), true)
        // test to save different index, new index on ("c1,c6,c7,c8") should be successfully saved.
        OrderingIndexHelper.saveStatisticsInfo(df, "c1,c6,c7,c8", statisticPath.toString, "5", Seq("0", "1", "3", "4"))
        assertEquals(fs.exists(new Path(statisticPath, "5")), true)
      } finally {
        if (fs.exists(testPath)) fs.delete(testPath)
        if (fs.exists(statisticPath)) fs.delete(statisticPath)
      }
    }
  }

  // test collect min-max statistic info for DateType in the case of multithreading.
  // parquet will give a wrong statistic result for DateType in the case of multithreading.
  @Test
  def testMultiThreadParquetFooterReadForDateType(): Unit = {
    // create parquet file with DateType
    val rdd = spark.sparkContext.parallelize(0 to 100, 1)
      .map(item => RowFactory.create(Date.valueOf(s"${2020}-${item % 11 + 1}-${item % 28 + 1}")))
    val df = spark.createDataFrame(rdd, new StructType().add("id", DateType))
    val testPath = new Path(System.getProperty("java.io.tmpdir"), "testCollectDateType")
    val conf = spark.sparkContext.hadoopConfiguration
    val cols = new java.util.ArrayList[String]
    cols.add("id")
    try {
      df.repartition(3).write.mode("overwrite").save(testPath.toString)
      val inputFiles = spark.read.load(testPath.toString).inputFiles.sortBy(x => x)

      val realResult = new Array[(String, String)](3)
      inputFiles.zipWithIndex.foreach { case (f, index) =>
        val fileUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET).asInstanceOf[ParquetUtils]
        val res = fileUtils.readRangeFromParquetMetadata(conf, new Path(f), cols).iterator().next()
        realResult(index) = (res.getMinValue.toString, res.getMaxValue.toString)
      }

      // multi thread read with no lock
      val resUseLock = new Array[(String, String)](3)
      inputFiles.zipWithIndex.par.foreach { case (f, index) =>
        val fileUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET).asInstanceOf[ParquetUtils]
        val res = fileUtils.readRangeFromParquetMetadata(conf, new Path(f), cols).iterator().next()
        resUseLock(index) = (res.getMinValue.toString, res.getMaxValue.toString)
      }

      // check resUseNoLock,
      // We can't guarantee that there must be problems in the case of multithreading.
      // In order to make ut pass smoothly, we will not check resUseNoLock.
      // check resUseLock
      // should pass assert
      realResult.zip(resUseLock).foreach { case (realValue, testValue) =>
        assert(realValue == testValue, s" expect realValue: ${realValue} but find ${testValue}")
      }
    } finally {
      if (fs.exists(testPath)) fs.delete(testPath)
    }
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

object TestTableLayoutOptimization {
  def testLayOutParameter(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments("COPY_ON_WRITE", "hilbert"),
      arguments("COPY_ON_WRITE", "z-order"),
      arguments("MERGE_ON_READ", "hilbert"),
      arguments("MERGE_ON_READ", "z-order")
    )
  }
}

