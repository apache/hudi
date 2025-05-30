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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, EventTimeAvroPayload, FirstValueAvroPayload, HoodieAvroRecordMerger, HoodieRecordMerger, OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload, OverwriteWithLatestMerger, PartialUpdateAvroPayload}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.{HoodieClientTestBase, SparkClientFunctionalTestHarness}
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

class TestMergerForBuiltinPayload extends SparkClientFunctionalTestHarness {
  val expectedEventTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"),
    (10, "2", "rider-B", "driver-B", 27.7, "i"))
  val expectedCommitTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"))

  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def testCustomDelete(tableType: String,
                       payloadClazz: String): Unit = {
    val avroMergerClasses = List(
      classOf[HoodieAvroRecordMerger].getName,
      classOf[OverwriteWithLatestMerger].getName).mkString(",")
    val mergeStrategy = HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID
    val opts: Map[String, String] = Map(
      HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> avroMergerClasses,
      HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key -> mergeStrategy,
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClazz)
    val columns = Seq("ts", "key", "rider", "driver", "fare", "delete")

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

    // Validate in the end.
    val df = spark.read.format("hudi").options(opts).load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "delete").sort("key")
    finalDf.show(false)
  }
}

object TestCustomDeleteRecord {
  def provideParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteWithLatestAvroPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[PartialUpdateAvroPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[DefaultHoodieRecordPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[EventTimeAvroPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[FirstValueAvroPayload].getName))
  }
}
