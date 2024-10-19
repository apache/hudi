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
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceWriteOptions, DefaultSparkRecordMerger}
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

class TestDeleteRecordLogic extends SparkClientFunctionalTestHarness{
  val expected = Seq(
    (14, "5", "rider-Z", "driver-Z", 17.85, 3),
    (-9, "4", "rider-DDDD", "driver-DDDD", 20.0, 1),
    (10, "3", "rider-C", "driver-C", 33.9, 10),
    (10, "2", "rider-B", "driver-B", 27.7, 1))

  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def testDeleteLogic(useFgReader: String, tableType: String, recordType: String): Unit = {
    val sparkOpts: Map[String, String] = Map(
      HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet",
      HoodieWriteConfig.RECORD_MERGER_IMPLS.key -> classOf[DefaultSparkRecordMerger].getName)
    val fgReaderOpts: Map[String, String] = Map(
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> useFgReader)

    val opts = if (recordType.equals("SPARK")) sparkOpts ++ fgReaderOpts else fgReaderOpts
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
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)

    val updateData = Seq(
      (11, "1", "rider-X", "driver-X", 19.10, 9),
      (9, "2", "rider-Y", "driver-Y", 27.70, 7))
    val updates = spark.createDataFrame(updateData).toDF(columns: _*)
    updates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "upsert").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    val deletesData = Seq((-5, "4", "rider-D", "driver-D", 34.15, 6))
    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "delete").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    val secondUpdateData = Seq(
      (14, "5", "rider-Z", "driver-Z", 17.85, 3),
      (-10, "4", "rider-DD", "driver-DD", 34.15, 5))
    val secondUpdates = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "upsert").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    val secondDeletesData = Seq(
      (10, "4", "rider-D", "driver-D", 34.15, 6),
      (0, "1", "rider-X", "driver-X", 19.10, 8))
    val secondDeletes = spark.createDataFrame(secondDeletesData).toDF(columns: _*)
    secondDeletes.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "delete").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    val thirdUpdateData = Seq((-8, "4", "rider-DDD", "driver-DDD", 20.00, 1))
    val thirdUpdates = spark.createDataFrame(thirdUpdateData).toDF(columns: _*)
    thirdUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "upsert").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    val thirdDeletesData = Seq(
      (10, "4", "rider-D4", "driver-D4", 34.15, 6),
      (0, "1", "rider-X", "driver-X", 19.10, 8))
    val thirdDeletes = spark.createDataFrame(thirdDeletesData).toDF(columns: _*)
    thirdDeletes.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(OPERATION.key(), "delete").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    val fourUpdateData = Seq((-9, "4", "rider-DDDD", "driver-DDDD", 20.00, 1))
    val fourUpdates = spark.createDataFrame(fourUpdateData).toDF(columns: _*)
    fourUpdates.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(HoodieCompactionConfig.INLINE_COMPACT.key(),
        if (tableType.equals("MERGE_ON_READ")) "true" else "false").
      option(OPERATION.key(), "upsert").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    // Read data to compare.
    val df = spark.read.format("hudi").options(opts).load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "number").sort("ts")
    finalDf.show(false)

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
