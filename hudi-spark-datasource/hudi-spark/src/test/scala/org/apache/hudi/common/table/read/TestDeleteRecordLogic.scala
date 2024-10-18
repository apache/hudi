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

package org.apache.hudi.common.table.read

import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.{DataSourceWriteOptions, DefaultSparkRecordMerger, HoodieSparkRecordMerger}
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.slf4j.LoggerFactory

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.jdk.CollectionConverters.asJavaIterableConverter

class TestDeleteRecordLogic extends SparkClientFunctionalTestHarness{
  val LOG = LoggerFactory.getLogger(classOf[TestDeleteRecordLogic])
  val expected = Seq(
    (11, "1", "rider-X", "driver-X", 19.1, 9),
    (14, "5", "rider-Z", "driver-Z", 17.85, 3),
    (-7, "4", "rider-DDD", "driver-DDD", 20.0, 1),
    (10, "3", "rider-C", "driver-C", 33.9, 10),
    (10, "2", "rider-B", "driver-B", 27.7, 1))

  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def showDeleteIsInconsistent(useFgReader: String, tableType: String, recordType: String): Unit = {
    LOG.info(
      "Testing: {} with file group reader {} and record type: {}",
      tableType, useFgReader, recordType)

    val merger = classOf[DefaultSparkRecordMerger].getName
    val columns = Seq("ts", "key", "rider", "driver", "fare", "number")
    val data = Seq(
      (10, "1", "rider-A", "driver-A", 19.10, 7),
      (10, "2", "rider-B", "driver-B", 27.70, 1),
      (10, "3", "rider-C", "driver-C", 33.90, 10),
      (-1, "4", "rider-D", "driver-D", 34.15, 6),
      (10, "5", "rider-E", "driver-E", 17.85, 10))

    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "test_table").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, merger).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      mode(SaveMode.Overwrite).
      save(basePath)

    val updateData = Seq(
      (11, "1", "rider-X", "driver-X", 19.10, 9),
      (9, "2", "rider-Y", "driver-Y", 27.70, 7))

    val updates = spark.createDataFrame(updateData).toDF(columns: _*)
    updates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, merger).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      mode(SaveMode.Append).
      save(basePath)

    val deletesData = Seq((-5, "4", "rider-D", "driver-D", 34.15, 6))

    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "delete").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, merger).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      mode(SaveMode.Append).
      save(basePath)

    val secondUpdateData = Seq(
      (14, "5", "rider-Z", "driver-Z", 17.85, 3),
      (-10, "4", "rider-DD", "driver-DD", 34.15, 5))
    val secondUpdates = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, merger).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      mode(SaveMode.Append).
      save(basePath)

    val thirdUpdateData = Seq((-8, "4", "rider-DDD", "driver-DDD", 20.00, 1))
    val thirdUpdates = spark.createDataFrame(thirdUpdateData).toDF(columns: _*)
    thirdUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, merger).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      mode(SaveMode.Append).
      save(basePath)

    val fourUpdateData = Seq((-7, "4", "rider-DDD", "driver-DDD", 20.00, 1))
    val fourUpdates = spark.createDataFrame(fourUpdateData).toDF(columns: _*)
    fourUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "upsert").
      option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet").
      option(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, merger).
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      mode(SaveMode.Append).
      save(basePath)

    val df = spark.read.format("hudi").
      option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true").
      option(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, merger).
      option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(), "false").load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "number").sort("ts")

    val expectedDf = spark.createDataFrame(expected).toDF(columns: _*).sort("ts")
    val expectedMinusActual = expectedDf.except(finalDf)
    val actualMinusExpected = finalDf.except(expectedDf)

    expectedMinusActual.show(false)
    actualMinusExpected.show(false)

    assertTrue(expectedMinusActual.isEmpty && actualMinusExpected.isEmpty)
  }
}

object TestDeleteRecordLogic {
  def provideParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("false", "COPY_ON_WRITE", "AVRO"),
      Arguments.of("false", "COPY_ON_WRITE", "SPARK"),
      Arguments.of("false", "MERGE_ON_READ", "AVRO"),
      Arguments.of("false", "MERGE_ON_READ", "SPARK"),
      Arguments.of("true", "COPY_ON_WRITE", "AVRO"),
      Arguments.of("true", "COPY_ON_WRITE", "SPARK"),
      Arguments.of("true", "MERGE_ON_READ", "AVRO"),
      Arguments.of("true", "MERGE_ON_READ", "SPARK"))
  }
}
