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

import org.apache.hudi.{DataSourceWriteOptions, DefaultSparkRecordMerger, OverwriteWithLatestSparkRecordMerger}
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, TypedProperties}
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.io.FileGroupReaderBasedMergeHandle
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.jdk.CollectionConverters._

// TODO: After binary Avro HoodieRecord is supported, we enable this test.
@Disabled
class TestPayloadDeprecationFlow extends SparkClientFunctionalTestHarness {
  // Create a custom schema that includes the Op field for AWSDmsAvroPayload testing
  private val CUSTOM_SCHEMA_WITH_OP = StructType(Seq(
    StructField("timestamp", LongType, false),
    StructField("_row_key", StringType, false),
    StructField("rider", StringType, false),
    StructField("driver", StringType, false),
    StructField("fare", StructType(Seq(
      StructField("amount", DoubleType, false),
      StructField("currency", StringType, false)
    )), false),
    StructField("_hoodie_is_deleted", BooleanType, false),
    StructField("Op", StringType, false)
  ))

  /**
   * Test if the payload based read have the same behavior for different table versions.
   */
  @ParameterizedTest
  @MethodSource(Array("provideParamsForPayloadBehavior"))
  def testMergerBuiltinPayload(tableType: String,
                               payloadClazz: String,
                               tableVersion: String,
                               mergeMode: String): Unit = {
    val mergers = List(classOf[DefaultSparkRecordMerger].getName, classOf[OverwriteWithLatestSparkRecordMerger].getName)
    val mergerClasses = mergers.mkString(",")
    val compactionEnabled = if (tableType.equals(HoodieTableType.MERGE_ON_READ.name)) "true" else "false"
    val opts: Map[String, String] = Map(
      HoodieCompactionConfig.INLINE_COMPACT.key() -> compactionEnabled,
      HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> "parquet",
      HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key() -> classOf[FileGroupReaderBasedMergeHandle[_, _, _, _]].getName,
      HoodieMetadataConfig.ENABLE.key() -> "true",
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClazz,
      HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key() -> mergerClasses,
      KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key() -> "_row_key",
      HoodieTableConfig.RECORDKEY_FIELDS.key() -> "_row_key",
      HoodieTableConfig.RECORD_MERGE_MODE.key() -> mergeMode)

    var metaClient: HoodieTableMetaClient = getHoodieMetaClient(storageConf(), basePath())
    new UpgradeDowngrade(metaClient, getWriteConfig(opts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.SIX, null)

    // 1. Add initial inserts using Spark Rows
    val initialRecords = List(
      createTestRecord("1", "rider-A", "driver-A", 19.10, 10L, false),
      createTestRecord("2", "rider-B", "driver-B", 27.70, 10L, false),
      createTestRecord("3", "rider-C", "driver-C", 33.90, 10L, false),
      createTestRecord("4", "rider-D", "driver-D", 34.15, 10L, false),
      createTestRecord("5", "rider-E", "driver-E", 17.85, 10L, false)
    )
    val initialDf = createDataFrameFromRows(initialRecords)
    initialDf.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "_row_key").
      option(PRECOMBINE_FIELD.key(), "timestamp").
      option(TABLE_TYPE.key(), tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersion).
      option(HoodieTableConfig.INITIAL_VERSION.key(), tableVersion).
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(
      Integer.valueOf(tableVersion),
      metaClient.getTableConfig.getTableVersion.versionCode())
    // 2. Add first update using Spark Rows
    val firstUpdateRecords = List(
      createTestRecord("1", "rider-X", "driver-X", 19.10, 11L, true), // delete
      createTestRecord("2", "rider-Y", "driver-Y", 27.70, 11L, false) // update
    )
    val firstUpdateDf = createDataFrameFromRows(firstUpdateRecords)
    firstUpdateDf.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "_row_key").
      option(PRECOMBINE_FIELD.key(), "timestamp").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersion).
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(
      Integer.valueOf(tableVersion),
      metaClient.getTableConfig.getTableVersion.versionCode())

    val df1 = spark.read.format("hudi").options(opts).load(basePath)
    val finalDf1 = df1.select("timestamp", "_row_key", "rider", "driver", "fare").sort("_row_key")
    finalDf1.show(false)

    // 3. Add second update using Spark Rows
    val secondUpdateRecords = List(
      createTestRecord("3", "rider-CC", "driver-CC", 33.90, 12L, false),
      createTestRecord("4", "rider-DD", "driver-DD", 34.15, 9L, false),
      createTestRecord("5", "rider-EE", "driver-EE", 17.85, 12L, false)
    )

    val secondUpdateDf = createDataFrameFromRows(secondUpdateRecords)
    secondUpdateDf.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersion).
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(
      Integer.valueOf(tableVersion),
      metaClient.getTableConfig.getTableVersion.versionCode())

    // 4. Validate final results using Spark Rows
    val df = spark.read.format("hudi").options(opts).load(basePath)
    val finalDf = df.select("timestamp", "_row_key", "rider", "driver", "fare").sort("_row_key")

    val expectedRecords = if (!payloadClazz.equals(classOf[AWSDmsAvroPayload].getName)) {
      if (payloadClazz.equals(classOf[PartialUpdateAvroPayload].getName)
        || payloadClazz.equals(classOf[EventTimeAvroPayload].getName)) {
        List(
          createTestRecord("2", "rider-Y", "driver-Y", 27.70, 11L, false),
          createTestRecord("3", "rider-CC", "driver-CC", 33.90, 12L, false),
          createTestRecord("4", "rider-D", "driver-D", 34.15, 10L, false),
          createTestRecord("5", "rider-EE", "driver-EE", 17.85, 12L, false)
        )
      } else {
        List(
          createTestRecord("2", "rider-Y", "driver-Y", 27.70, 11L, false),
          createTestRecord("3", "rider-CC", "driver-CC", 33.90, 12L, false),
          createTestRecord("4", "rider-DD", "driver-DD", 34.15, 9L, false),
          createTestRecord("5", "rider-EE", "driver-EE", 17.85, 12L, false)
        )
      }
    } else {
      List(
        createTestRecord("2", "rider-Y", "driver-Y", 27.70, 11L, false),
        createTestRecord("3", "rider-CC", "driver-CC", 33.90, 12L, false),
        createTestRecord("4", "rider-DD", "driver-DD", 34.15, 9L, false),
        createTestRecord("5", "rider-EE", "driver-EE", 17.85, 12L, false)
      )
    }
    val expectedDf = createDataFrameFromRows(expectedRecords)
      .select("timestamp", "_row_key", "rider", "driver", "fare").sort("_row_key")

    expectedDf.show(false)
    finalDf.show(false)
    assertTrue(
      expectedDf.except(finalDf).isEmpty && finalDf.except(expectedDf).isEmpty)
  }

  /**
   * Helper method to create test Spark Row
   */
  private def createTestRecord(key: String,
                              rider: String,
                              driver: String,
                              fare: Double,
                              timestamp: Long,
                              isDelete: Boolean): Row = {
    // Create nested fare row
    val fareRow = Row(fare, "USD")
    // Set Op field for operation indication
    val opValue = if (isDelete) "D" else "I"
    // Create the main row
    Row(timestamp, key, rider, driver, fareRow, isDelete, opValue)
  }

  /**
   * Helper method to convert Spark Row list to Spark DataFrame
   */
  private def createDataFrameFromRows(records: List[Row]): org.apache.spark.sql.DataFrame = {
    spark.createDataFrame(records.asJava, CUSTOM_SCHEMA_WITH_OP)
  }

  def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath())
      .build()
  }
}

object TestPayloadDeprecationFlow {
  def provideParamsForPayloadBehavior(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      // For COW merge.
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteWithLatestAvroPayload].getName, "6", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName, "6", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[PartialUpdateAvroPayload].getName, "6", "EVENT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[EventTimeAvroPayload].getName, "6", "EVENT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[AWSDmsAvroPayload].getName, "6", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteWithLatestAvroPayload].getName, "8", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName, "8", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[PartialUpdateAvroPayload].getName, "8", "EVENT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[EventTimeAvroPayload].getName, "8", "EVENT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[AWSDmsAvroPayload].getName, "8", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteWithLatestAvroPayload].getName, "9", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName, "9", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[PartialUpdateAvroPayload].getName, "9", "EVENT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[EventTimeAvroPayload].getName, "9", "EVENT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", classOf[AWSDmsAvroPayload].getName, "9", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", "", "6", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", "", "8", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", "", "9", "EVENT_TIME_ORDERING"),
      // For compaction.
      Arguments.of("MERGE_ON_WRITE", classOf[OverwriteWithLatestAvroPayload].getName, "6", "COMMIT_TIME_ORDERING"),
      Arguments.of("MERGE_ON_WRITE", classOf[PartialUpdateAvroPayload].getName, "9", "EVENT_TIME_ORDERING"),
      Arguments.of("MERGE_ON_WRITE", classOf[EventTimeAvroPayload].getName, "9", "EVENT_TIME_ORDERING"),
      Arguments.of("MERGE_ON_WRITE", classOf[AWSDmsAvroPayload].getName, "9", "COMMIT_TIME_ORDERING")
    )
  }
}
