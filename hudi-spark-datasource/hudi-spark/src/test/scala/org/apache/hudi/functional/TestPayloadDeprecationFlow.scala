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
import org.apache.hudi.common.model.{AWSDmsAvroPayload, EventTimeAvroPayload, OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload, PartialUpdateAvroPayload}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

class TestPayloadDeprecationFlow extends SparkClientFunctionalTestHarness {
  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def testMergerBuiltinPayload(tableType: String,
                               payloadClazz: String): Unit = {
    val opts: Map[String, String] = Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClazz,
      HoodieTableConfig.MERGE_CUSTOM_PROPERTY_PREFIX + ".hoodie.payload.delete.field" -> "Op",
      HoodieTableConfig.MERGE_CUSTOM_PROPERTY_PREFIX + ".hoodie.payload.delete.marker" -> "d")
    val columns = Seq("ts", "key", "rider", "driver", "fare", "Op")

    // 1. Add an insert.
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
    // 2. Add an update.
    val firstUpdateData = Seq(
      (11, "1", "rider-X", "driver-X", 19.10, "d"),
      (11, "2", "rider-Y", "driver-Y", 27.70, "u"))
    val firstUpdate = spark.createDataFrame(firstUpdateData).toDF(columns: _*)
    firstUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // 3. Add an update.
    val secondUpdateData = Seq(
      (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
      (9, "4", "rider-DD", "driver-DD", 34.15, "i"),
      (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
    val secondUpdate = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "true").
      option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // 4. Validate.
    val df = spark.read.format("hudi").options(opts).load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "Op").sort("key")

    val expectedData = if (!payloadClazz.equals(classOf[AWSDmsAvroPayload].getName)) {
      if (payloadClazz.equals(classOf[PartialUpdateAvroPayload].getName)
        || payloadClazz.equals(classOf[EventTimeAvroPayload].getName)) {
        Seq(
          (11, "1", "rider-X", "driver-X", 19.10, "d"),
          (11, "2", "rider-Y", "driver-Y", 27.70, "u"),
          (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
          (10, "4", "rider-D", "driver-D", 34.15, "i"),
          (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
      } else {
        Seq(
          (11, "1", "rider-X", "driver-X", 19.10, "d"),
          (11, "2", "rider-Y", "driver-Y", 27.70, "u"),
          (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
          (9, "4", "rider-DD", "driver-DD", 34.15, "i"),
          (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
      }
    } else {
      Seq(
        (11, "2", "rider-Y", "driver-Y", 27.70, "u"),
        (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
        (9, "4", "rider-DD", "driver-DD", 34.15, "i"),
        (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
    }
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData)).toDF(columns: _*).sort("key")
    assertTrue(
      expectedDf.except(finalDf).isEmpty && finalDf.except(expectedDf).isEmpty)
  }
}

// TODO: Add COPY_ON_WRITE table type tests when write path is updated accordingly.
object TestPayloadDeprecationFlow {
  def provideParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("MERGE_ON_READ", classOf[OverwriteWithLatestAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[PartialUpdateAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[EventTimeAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[AWSDmsAvroPayload].getName)
    )
  }
}
