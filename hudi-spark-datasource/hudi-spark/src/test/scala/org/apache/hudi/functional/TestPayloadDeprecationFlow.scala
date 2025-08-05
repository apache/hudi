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
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.{AWSDmsAvroPayload, EventTimeAvroPayload, OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload, PartialUpdateAvroPayload}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.jdk.CollectionConverters._

class TestPayloadDeprecationFlow extends SparkClientFunctionalTestHarness {
  /**
   * Test if the payload based read have the same behavior for different table versions.
   */
  @ParameterizedTest
  @MethodSource(Array("provideParamsForPayloadBehavior"))
  def testMergerBuiltinPayload(tableType: String,
                               payloadClazz: String): Unit = {
    val opts: Map[String, String] = Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClazz,
      HoodieMetadataConfig.ENABLE.key() -> "false"
    )
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
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)
    // Validate table version.
    var metaClient: HoodieTableMetaClient = HoodieTableMetaClient
      .builder().setConf(storageConf()).setBasePath(basePath()).build()
    assertEquals(8, metaClient.getTableConfig.getTableVersion.versionCode())
    assertEquals(payloadClazz, metaClient.getTableConfig.getPayloadClass)
    assertEquals("", metaClient.getTableConfig.getLegacyPayloadClass)
    assertTrue(metaClient.getActiveTimeline.firstInstant().isPresent)
    val firstInstantTime = metaClient.getActiveTimeline.firstInstant().get().requestedTime()

    // 2. Add an update.
    val firstUpdateData = Seq(
      (11, "1", "rider-X", "driver-X", 19.10, "d"),
      (11, "2", "rider-Y", "driver-Y", 27.70, "u"))
    val firstUpdate = spark.createDataFrame(firstUpdateData).toDF(columns: _*)
    firstUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(8, metaClient.getTableConfig.getTableVersion.versionCode())
    val firstUpdateInstantTime = metaClient.getActiveTimeline.getInstants.get(1).requestedTime()

    // 3. Add an update.
    val secondUpdateData = Seq(
      (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
      (9, "4", "rider-DD", "driver-DD", 34.15, "i"),
      (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
    val secondUpdate = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9").
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(9, metaClient.getTableConfig.getTableVersion.versionCode())
    assertEquals(payloadClazz, metaClient.getTableConfig.getLegacyPayloadClass)
    val compactionInstants = metaClient.getActiveTimeline.getCommitsAndCompactionTimeline.getInstants
    val foundCompaction = compactionInstants.stream().anyMatch(i => i.getAction.equals("commit"))
    assertTrue(foundCompaction)

    // 4. Validate.
    // Validate snapshot query.
    val df = spark.read.format("hudi").load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "Op").sort("key")
    val expectedData = getExpectedResultForSnapshotQuery(payloadClazz)
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData)).toDF(columns: _*).sort("key")
    assertTrue(
      expectedDf.except(finalDf).isEmpty && finalDf.except(expectedDf).isEmpty)

    // Validate time travel query.
    val timeTravelDf = spark.read.format("hudi")
      .option("as.of.instant", firstUpdateInstantTime).load(basePath)
      .select("ts", "key", "rider", "driver", "fare", "Op").sort("key")
    timeTravelDf.show(false)
    val expectedTimeTravelData = getExpectedResultForTimeTravelQuery(payloadClazz)
    val expectedTimeTravelDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedTimeTravelData)).toDF(columns: _*).sort("key")
    assertTrue(
      expectedTimeTravelDf.except(timeTravelDf).isEmpty
      && timeTravelDf.except(expectedTimeTravelDf).isEmpty)
  }

  def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath())
      .build()
  }

  def getExpectedResultForSnapshotQuery(payloadClazz: String): Seq[(Int, String, String, String, Double, String)] = {
    if (!payloadClazz.equals(classOf[AWSDmsAvroPayload].getName)) {
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
  }

  def getExpectedResultForTimeTravelQuery(payloadClazz: String):
  Seq[(Int, String, String, String, Double, String)] = {
    if (!payloadClazz.equals(classOf[AWSDmsAvroPayload].getName)) {
      Seq(
        (11, "1", "rider-X", "driver-X", 19.10, "d"),
        (11, "2", "rider-Y", "driver-Y", 27.70, "u"),
        (10, "3", "rider-C", "driver-C", 33.90, "i"),
        (10, "4", "rider-D", "driver-D", 34.15, "i"),
        (10, "5", "rider-E", "driver-E", 17.85, "i"))
    } else {
      Seq(
        (11, "2", "rider-Y", "driver-Y", 27.70, "u"),
        (10, "3", "rider-C", "driver-C", 33.90, "i"),
        (10, "4", "rider-D", "driver-D", 34.15, "i"),
        (10, "5", "rider-E", "driver-E", 17.85, "i"))
    }
  }
}

// TODO: Add COPY_ON_WRITE table type tests when write path is updated accordingly.s
object TestPayloadDeprecationFlow {
  def provideParamsForPayloadBehavior(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("MERGE_ON_READ", classOf[OverwriteWithLatestAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[PartialUpdateAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[EventTimeAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[AWSDmsAvroPayload].getName)
    )
  }
}
