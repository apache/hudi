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

import org.apache.hudi.{DataSourceWriteOptions, DefaultSparkRecordMerger, OverwriteWithLatestSparkRecordMerger}
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig, RecordMergeMode}
import org.apache.hudi.common.model.{HoodieAvroRecordMerger, HoodieRecordMerger, OverwriteWithLatestMerger}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.{DELETE_KEY, DELETE_MARKER}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

class TestCustomDeleteRecords extends SparkClientFunctionalTestHarness {
  val expectedEventTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"),
    (10, "2", "rider-B", "driver-B", 27.7, "i"))
  val expectedCommitTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"))

  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def testCustomDelete(useFgReader: String,
                       tableType: String,
                       recordType: String,
                       positionUsed: String,
                       mergeMode: String): Unit = {
    val sparkMergeClasses = List(
      classOf[DefaultSparkRecordMerger].getName,
      classOf[OverwriteWithLatestSparkRecordMerger].getName).mkString(",")
    val avroMergerClasses = List(
      classOf[HoodieAvroRecordMerger].getName,
      classOf[OverwriteWithLatestMerger].getName).mkString(",")

    val mergeStrategy = if (mergeMode.equals(RecordMergeMode.EVENT_TIME_ORDERING.name)) {
      HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID
    } else {
      HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID
    }
    val mergeOpts: Map[String, String] = Map(
      HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet",
      HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key ->
        (if (recordType.equals("SPARK")) sparkMergeClasses else avroMergerClasses),
      HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key -> mergeStrategy)
    val fgReaderOpts: Map[String, String] = Map(
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> useFgReader,
      HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key -> positionUsed,
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> mergeMode
    )
    val deleteOpts: Map[String, String] = Map(
      DELETE_KEY -> "op", DELETE_MARKER -> "d")
    val opts = mergeOpts ++ fgReaderOpts ++ deleteOpts
    val columns = Seq("ts", "key", "rider", "driver", "fare", "op")

    val data = Seq(
      (10, "1", "rider-A", "driver-A", 19.10, "i"),
      (10, "2", "rider-B", "driver-B", 27.70, "i"),
      (10, "3", "rider-C", "driver-C", 33.90, "i"),
      (10, "4", "rider-D", "driver-D", 34.15, "i"),
      (10, "5", "rider-E", "driver-E", 17.85, "i"))
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)

    // Delete using delete markers.
    val updateData = Seq(
      (11, "1", "rider-X", "driver-X", 19.10, "d"),
      (9, "2", "rider-Y", "driver-Y", 27.70, "d"))
    val updates = spark.createDataFrame(updateData).toDF(columns: _*)
    updates.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    // Delete from operation.
    val deletesData = Seq((-5, "4", "rider-D", "driver-D", 34.15, 6))
    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(OPERATION.key(), "DELETE").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    // Validate in the end.
    val df = spark.read.format("hudi").options(opts).load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "op").sort("key")
    finalDf.show(false)
    val expected = if (mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING.name()) {
      expectedCommitTimeBased
    } else {
      expectedEventTimeBased
    }
    val expectedDf = spark.createDataFrame(expected).toDF(columns: _*).sort("key")
    TestCustomDeleteRecords.validate(expectedDf, finalDf)
  }
}

object TestCustomDeleteRecords {
  def provideParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("true", "MERGE_ON_READ", "AVRO", "false", "EVENT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "AVRO", "true", "EVENT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "AVRO", "false", "COMMIT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "AVRO", "true", "COMMIT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "SPARK", "false", "EVENT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "SPARK", "true", "EVENT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "SPARK", "false", "COMMIT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "SPARK", "true", "COMMIT_TIME_ORDERING"))
  }

  def validate(expectedDf: Dataset[Row], actualDf: Dataset[Row]): Unit = {
    val expectedMinusActual = expectedDf.except(actualDf)
    val actualMinusExpected = actualDf.except(expectedDf)
    expectedDf.show(false)
    actualDf.show(false)
    assertTrue(expectedMinusActual.isEmpty && actualMinusExpected.isEmpty)
  }
}
