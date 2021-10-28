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

import java.sql.{Date, Timestamp}

import org.apache.hadoop.fs.Path
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.ZCurveOptimizeHelper
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConversions._
import scala.util.Random

class TestOptimizeTable extends HoodieClientTestBase {
  var spark: SparkSession = null

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
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testOptimizewithClustering(tableType: String): Unit = {
    // Bulk Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("001", 1000)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
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
      .option("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(64 *1024 * 1024L))
      .option(HoodieClusteringConfig.LAYOUT_OPTIMIZE_ENABLE.key, "true")
      .option(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key, "begin_lat, begin_lon")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertEquals(1000, spark.read.format("hudi").load(basePath).count())
    assertEquals(1000,
      spark.read.option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key(), "true").format("hudi").load(basePath).count())
  }

  @Test
  def testCollectMinMaxStatistics(): Unit = {
    val testPath = new Path(System.getProperty("java.io.tmpdir"), "minMax")
    val statisticPath = new Path(System.getProperty("java.io.tmpdir"), "stat")
    val fs = testPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    try {
      val complexDataFrame = createComplexDataFrame(spark)
      complexDataFrame.repartition(3).write.mode("overwrite").save(testPath.toString)
      val df = spark.read.load(testPath.toString)
      // do not support TimeStampType, so if we collect statistics for c4, should throw exception
      val colDf = ZCurveOptimizeHelper.getMinMaxValue(df, "c1,c2,c3,c5,c6,c7,c8")
      colDf.cache()
      assertEquals(colDf.count(), 3)
      assertEquals(colDf.take(1)(0).length, 22)
      colDf.unpersist()
      // try to save statistics
      ZCurveOptimizeHelper.saveStatisticsInfo(df, "c1,c2,c3,c5,c6,c7,c8", statisticPath.toString, "2", Seq("0", "1"))
      // save again
      ZCurveOptimizeHelper.saveStatisticsInfo(df, "c1,c2,c3,c5,c6,c7,c8", statisticPath.toString, "3", Seq("0", "1", "2"))
      // test old index table clean
      ZCurveOptimizeHelper.saveStatisticsInfo(df, "c1,c2,c3,c5,c6,c7,c8", statisticPath.toString, "4", Seq("0", "1", "3"))
      assertEquals(!fs.exists(new Path(statisticPath, "2")), true)
      assertEquals(fs.exists(new Path(statisticPath, "3")), true)
    } finally {
      if (fs.exists(testPath)) fs.delete(testPath)
      if (fs.exists(statisticPath)) fs.delete(statisticPath)
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
