/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hadoop.fs.FileSystem
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.{DataSourceWriteOptions, PrecombineBasedSparkRecordMerger}
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieRecordMerger
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.function.Consumer

class TestPositionBasedMergingFallback extends HoodieSparkClientTestBase {
  var spark: SparkSession = null

  override def getSparkSessionExtensionsInjector: util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
    FileSystem.closeAll()
    System.gc()
  }

  @ParameterizedTest
  @MethodSource(Array("testDefaultMergerArgs"))
  def testDefaultMergerArgs(updateWithRecordPositions: String, deleteWithRecordPositions: String, secondUpdateWithPositions: String): Unit = {
    val columns = Seq("ts", "key", "rider", "driver", "fare", "number")
    val data = Seq((10, "1", "rider-A", "driver-A", 19.10, 7),
      (10, "2", "rider-B", "driver-B", 27.70, 1),
      (10, "3", "rider-C", "driver-C", 33.90, 10),
      (10, "4", "rider-D", "driver-D", 34.15, 6),
      (10, "5", "rider-E", "driver-E", 17.85, 10))

    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), "org.apache.hudi.HoodieSparkRecordMerger").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), "true").
      mode(Overwrite).
      save(basePath)

    val updateData = Seq((11, "1", "rider-X", "driver-X", 19.10, 9),
      (9, "2", "rider-Y", "driver-Y", 27.70, 7))

    val updates = spark.createDataFrame(updateData).toDF(columns: _*)

    updates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), "org.apache.hudi.HoodieSparkRecordMerger").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), updateWithRecordPositions).
      mode(Append).
      save(basePath)

    val deletesData = Seq((10, "4", "rider-D", "driver-D", 34.15, 6))

    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(OPERATION.key(), "delete").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), "org.apache.hudi.HoodieSparkRecordMerger").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), deleteWithRecordPositions).
      mode(Append).
      save(basePath)


    val secondUpdateData = Seq((14, "5", "rider-Z", "driver-Z", 17.85, 3))
    val secondUpdates = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), "org.apache.hudi.HoodieSparkRecordMerger").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), secondUpdateWithPositions).
      mode(Append).
      save(basePath)

    val df = spark.read.format("hudi").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), "org.apache.hudi.HoodieSparkRecordMerger").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), "true").load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "number")


    val finalExpectedData = Seq((11, "1", "rider-X", "driver-X", 19.10, 9),
      (10, "2", "rider-B", "driver-B", 27.70, 1),
      (10, "3", "rider-C", "driver-C", 33.90, 10),
      (14, "5", "rider-Z", "driver-Z", 17.85, 3))

    val expectedDf = spark.createDataFrame(finalExpectedData).toDF(columns: _*)

    assertEquals(0, finalDf.except(expectedDf).count())
    assertEquals(0, expectedDf.except(finalDf).count())

    //test filter pushdown
    //if the filter is pushed down, then record 2 will be filtered from the base file
    //but record 2 in the log file won't be. Since precombine is larger in the base file,
    //that record is the "winner" of merging, and therefore the record should not be
    //in the output
    sqlContext.clearCache()
    val finalFilterDf = finalDf.filter("rider != 'rider-B'")
    val finalExpectedFilterData = Seq((11, "1", "rider-X", "driver-X", 19.10, 9),
      (10, "3", "rider-C", "driver-C", 33.90, 10),
      (14, "5", "rider-Z", "driver-Z", 17.85, 3))

    val expectedFilterDf = spark.createDataFrame(finalExpectedFilterData).toDF(columns: _*)
    assertEquals(0, finalFilterDf.except(expectedFilterDf).count())
    assertEquals(0, expectedFilterDf.except(finalFilterDf).count())
  }


  @ParameterizedTest
  @MethodSource(Array("testOtherMergerArgs"))
  def testWithOtherMerger(updateWithRecordPositions: String, deleteWithRecordPositions: String,
                          secondUpdateWithPositions: String, deletePrecombineLess: String): Unit = {
    val columns = Seq("ts", "key", "rider", "driver", "fare", "number")
    val data = Seq((10, "1", "rider-A", "driver-A", 19.10, 7),
      (10, "2", "rider-B", "driver-B", 27.70, 1),
      (10, "3", "rider-C", "driver-C", 33.90, 10),
      (10, "4", "rider-D", "driver-D", 34.15, 6),
      (10, "5", "rider-E", "driver-E", 17.85, 10))

    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.PRECOMBINE_BASED_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), classOf[PrecombineBasedSparkRecordMerger].getName).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), "true").
      mode(Overwrite).
      save(basePath)

    val updateData = Seq((11, "1", "rider-X", "driver-X", 19.10, 9),
      (9, "2", "rider-Y", "driver-Y", 27.70, 7))

    val updates = spark.createDataFrame(updateData).toDF(columns: _*)

    updates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.PRECOMBINE_BASED_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), classOf[PrecombineBasedSparkRecordMerger].getName).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), updateWithRecordPositions).
      mode(Append).
      save(basePath)

    val deletesData = if (deletePrecombineLess.toBoolean) {
      Seq((9, "4", "rider-D", "driver-D", 34.15, 6))
    } else {
      Seq((11, "4", "rider-D", "driver-D", 34.15, 6))
    }

    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(OPERATION.key(), "delete").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.PRECOMBINE_BASED_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), classOf[PrecombineBasedSparkRecordMerger].getName).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), deleteWithRecordPositions).
      mode(Append).
      save(basePath)


    val secondUpdateData = Seq((14, "5", "rider-Z", "driver-Z", 17.85, 3),(10, "4", "rider-DD", "driver-DD", 34.15, 5))
    val secondUpdates = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.PRECOMBINE_BASED_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), classOf[PrecombineBasedSparkRecordMerger].getName).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), secondUpdateWithPositions).
      mode(Append).
      save(basePath)

    val df = spark.read.format("hudi").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.PRECOMBINE_BASED_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), classOf[PrecombineBasedSparkRecordMerger].getName).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), "false").load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "number")


    val finalExpectedData = if (deletePrecombineLess.toBoolean) {
      Seq((11, "1", "rider-X", "driver-X", 19.10, 9),
        (10, "2", "rider-B", "driver-B", 27.70, 1),
        (10, "3", "rider-C", "driver-C", 33.90, 10),
        (10, "4", "rider-D", "driver-D", 34.15, 6),
        (14, "5", "rider-Z", "driver-Z", 17.85, 3))
    } else {
      Seq((11, "1", "rider-X", "driver-X", 19.10, 9),
        (10, "2", "rider-B", "driver-B", 27.70, 1),
        (10, "3", "rider-C", "driver-C", 33.90, 10),
        (14, "5", "rider-Z", "driver-Z", 17.85, 3))
    }

    val expectedDf = spark.createDataFrame(finalExpectedData).toDF(columns: _*)

    assertEquals(0, finalDf.except(expectedDf).count())
    assertEquals(0, expectedDf.except(finalDf).count())
  }
}

object TestPositionBasedMergingFallback {
  def testDefaultMergerArgs: java.util.stream.Stream[Arguments] = {
    val scenarios = Array(
      Seq("true","true","true"),
      Seq("false","true","true"),
      Seq("true","false","true"),
      Seq("false","false","true"),
      Seq("true","true","false"),
      Seq("false","true","false"),
      Seq("true","false","false"),
      Seq("false","false","false")
    )
    java.util.Arrays.stream(scenarios.map(as => Arguments.arguments(as.map(_.asInstanceOf[AnyRef]):_*)))
  }

  def testOtherMergerArgs: java.util.stream.Stream[Arguments] = {
    val scenarios = Array(
//      Seq("true", "true", "true", "true"),
//      Seq("false", "true", "true", "true"),
//      Seq("true", "false", "true", "true"),
//      Seq("false", "false", "true", "true"),
//      Seq("true", "true", "false", "true"),
//      Seq("false", "true", "false", "true"),
//      Seq("true", "false", "false", "true"),
//      Seq("false", "false", "false", "true"),
//      Seq("true", "true", "true", "false"),
//      Seq("false", "true", "true", "false"),
//      Seq("true", "false", "true", "false"),
//      Seq("false", "false", "true", "false"),
//      Seq("true", "true", "false", "false"),
//      Seq("false", "true", "false", "false"),
//      Seq("true", "false", "false", "false"),
      Seq("false", "false", "false", "true")
    )
    java.util.Arrays.stream(scenarios.map(as => Arguments.arguments(as.map(_.asInstanceOf[AnyRef]): _*)))
  }
}
