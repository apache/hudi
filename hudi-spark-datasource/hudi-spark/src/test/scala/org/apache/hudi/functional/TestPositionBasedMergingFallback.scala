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
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieRecordMerger
import org.apache.hudi.common.util
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
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
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
    FileSystem.closeAll()
    System.gc()
  }

  @ParameterizedTest
  @MethodSource(Array("testArgs"))
  def testPositionFallback(updateWithRecordPositions: String, deleteWithRecordPositions: String, secondUpdateWithPositions: String): Unit = {
    val columns = Seq("ts", "key", "name", "_hoodie_is_deleted")
    val data = Seq(
      (10, "1", "A", false),
      (10, "2", "B", false),
      (10, "3", "C", false),
      (10, "4", "D", false),
      (10, "5", "E", false))

    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option("hoodie.table.name", "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), "org.apache.hudi.HoodieSparkRecordMerger").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), "true").
      mode(Overwrite).
      save(basePath)

    val updateData = Seq((11, "1", "A_1", false), (9, "2", "B_1", false))

    val updates = spark.createDataFrame(updateData).toDF(columns: _*)

    updates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option("hoodie.table.name", "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), "org.apache.hudi.HoodieSparkRecordMerger").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), updateWithRecordPositions).
      mode(Append).
      save(basePath)

    val deletesData = Seq((10, "4", "D",  true), (10, "3", "C", true))

    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option("hoodie.table.name", "test_table").
      option(TABLE_TYPE.key(), "MERGE_ON_READ").
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(DataSourceWriteOptions.RECORD_MERGER_STRATEGY.key(), HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID).
      option(DataSourceWriteOptions.RECORD_MERGER_IMPLS.key(), "org.apache.hudi.HoodieSparkRecordMerger").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), deleteWithRecordPositions).
      mode(Append).
      save(basePath)


    val secondUpdateData = Seq((14, "5", "E_3", false), (3, "3", "C_3", false))
    val secondUpdates = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option("hoodie.table.name", "test_table").
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
    val finalDf = df.select("ts", "key", "name")
    val finalColumns = Seq("ts", "key", "name")

    val finalExpectedData = Seq(
      (11, "1", "A_1"),
      (10, "2", "B"),
      (14, "5", "E_3"))

    val expectedDf = spark.createDataFrame(finalExpectedData).toDF(finalColumns: _*)

    assertEquals(0, finalDf.except(expectedDf).count())
    assertEquals(0, expectedDf.except(finalDf).count())

    //test filter pushdown
    //if the filter is pushed down, then record 2 will be filtered from the base file
    //but record 2 in the log file won't be. Since precombine is larger in the base file,
    //that record is the "winner" of merging, and therefore the record should not be
    //in the output
    sqlContext.clearCache()
    val finalFilterDf = finalDf.filter("name != 'B'")
    val finalExpectedFilterData = Seq(
      (11, "1", "A_1"),
      (14, "5", "E_3"))

    val expectedFilterDf = spark.createDataFrame(finalExpectedFilterData).toDF(finalColumns: _*)
    assertEquals(0, finalFilterDf.except(expectedFilterDf).count())
    assertEquals(0, expectedFilterDf.except(finalFilterDf).count())
  }
}

object TestPositionBasedMergingFallback {
  def testArgs: java.util.stream.Stream[Arguments] = {
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
}
