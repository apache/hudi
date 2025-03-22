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
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.functional.TestMergeModeDuringUpgradeDowngrade.{getReadOpts, getWriteOptsForTableVersionEight, getWriteOptsForTableVersionSix}
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.jdk.CollectionConverters.{mapAsJavaMapConverter, setAsJavaSetConverter}

class TestMergeModeDuringUpgradeDowngrade extends SparkClientFunctionalTestHarness {
  val expectedEventTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"),
    (20, "6", "rider-Z", "driver-Z", 27.7, "i"))
  val expectedCommitTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"),
    (20, "6", "rider-Z", "driver-Z", 27.7, "i"))
  val data: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "1", "rider-A", "driver-A", 19.10, "i"),
    (10, "2", "rider-B", "driver-B", 27.70, "i"),
    (10, "3", "rider-C", "driver-C", 33.90, "i"),
    (10, "4", "rider-D", "driver-D", 34.15, "i"),
    (10, "5", "rider-E", "driver-E", 17.85, "i"))
  val columns: Seq[String] = Seq("ts", "key", "rider", "driver", "fare", "op")

  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def testMergeModeDuringUpgradeDowngrade(tableType: String,
                                          fromTableVersion: String,
                                          toTableVersion: String,
                                          fromMergeMode: String,
                                          toMergeMode: String): Unit = {
    // Create the table with specific version and merge mode.
    createOriginalTable(tableType, fromTableVersion, fromMergeMode, columns)

    // Update table version.
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf).setBasePath(basePath).build
    updateTableVersion(fromTableVersion, toTableVersion, tableType, metaClient)

    // Validate final table merge mode and data.
    validateUpdatedTable(tableType, toTableVersion, toMergeMode, columns)
  }

  def createOriginalTable(tableType: String,
                          fromTableVersion: String,
                          fromMergeMode: String,
                          columns: Seq[String]): Unit = {
    val opts: Map[String, String] = if (fromTableVersion == "6") {
      getWriteOptsForTableVersionSix(tableType, fromMergeMode)
    } else {
      getWriteOptsForTableVersionEight(tableType, fromMergeMode)
    }
    val readOpts: Map[String, String] = getReadOpts

    // Insert some data.
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)
    // Delete from operation.
    val deletesData = Seq(
      (-5, "4", "rider-D", "driver-D", 34.15, "i"),
      (11, "1", "rider-X", "driver-X", 19.10, "d"),
      (9, "2", "rider-Y", "driver-Y", 27.70, "d"))
    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(OPERATION.key(), "DELETE").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // Add a record back to test ensure event time ordering work.
    val updateDataSecond = Seq(
      (20, "6", "rider-Z", "driver-Z", 27.70, "i"))
    val updatesSecond = spark.createDataFrame(updateDataSecond).toDF(columns: _*)
    updatesSecond.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // Validate in the end.
    val metaClient: HoodieTableMetaClient = {
      HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(basePath).build()
    }
    // Table version should match with from_table_version.
    assertEquals(
      Integer.valueOf(fromTableVersion),
      metaClient.getTableConfig.getTableVersion.versionCode())
    // Validate merge mode.
    if (fromTableVersion == "8") {
      assertEquals(fromMergeMode, metaClient.getTableConfig.getRecordMergeMode.name())
    } else {
      if (fromMergeMode == RecordMergeMode.EVENT_TIME_ORDERING.name()) {
        assertEquals(
          classOf[DefaultHoodieRecordPayload].getName,
          metaClient.getTableConfig.getPayloadClass)
      } else {
        assertEquals(
          classOf[OverwriteWithLatestAvroPayload].getName,
          metaClient.getTableConfig.getPayloadClass
        )
      }
    }
    // Validate data.
    val columnsToCompare = Set("ts", "key", "rider", "driver", "fare", "op")
    val df = spark.read.options(readOpts).format("hudi").load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "op")
      .sort("key")
    val expected = if (fromMergeMode == RecordMergeMode.EVENT_TIME_ORDERING.name()) {
      expectedEventTimeBased
    } else {
      expectedCommitTimeBased
    }
    val expectedDf = spark.createDataFrame(expected).toDF(columns: _*).sort("key")
    assertTrue(
      SparkClientFunctionalTestHarness.areDataframesEqual(
        expectedDf, finalDf, columnsToCompare.asJava))
  }

  def updateTableVersion(fromTableVersion: String,
                         toTableVersion: String,
                         tableType: String,
                         metaClient: HoodieTableMetaClient): Unit = {
    var newMetaClient: HoodieTableMetaClient = HoodieTableMetaClient.reload(metaClient)
    val fromOpts: Map[String, String] = if (fromTableVersion == "6") {
      getWriteOptsForTableVersionSix(tableType, fromTableVersion)
    } else {
      getWriteOptsForTableVersionEight(tableType, fromTableVersion)
    }
    val toOpts: Map[String, String] = if (toTableVersion == "6") {
      getWriteOptsForTableVersionSix(tableType, toTableVersion)
    } else {
      getWriteOptsForTableVersionEight(tableType, toTableVersion)
    }

    // Explicit update is needed for downgrade.
    if (true) {
      val toVersion: HoodieTableVersion =
        HoodieTableVersion.fromVersionCode(Integer.valueOf(toTableVersion))
      new UpgradeDowngrade(
        metaClient, getWriteConfig(fromOpts), context, SparkUpgradeDowngradeHelper.getInstance)
        .run(toVersion, null)

      // Check to table version.
      newMetaClient = HoodieTableMetaClient
        .builder().setConf(storageConf).setBasePath(basePath).build()
      assertEquals(
        Integer.valueOf(toTableVersion),
        newMetaClient.getTableConfig.getTableVersion.versionCode())
    }
  }

  def validateUpdatedTable(tableType: String,
                           tableVersion: String,
                           mergeMode: String,
                           columns: Seq[String]): Unit = {
    val readOpts: Map[String, String] = getReadOpts
    val opts: Map[String, String] = if (tableVersion == "6") {
      getWriteOptsForTableVersionSix(tableType, mergeMode)
    } else {
      getWriteOptsForTableVersionEight(tableType, mergeMode)
    }

    // Insert one row to trigger upgrade if applicable.
    val updateDataSecond = Seq(
      (100, "100", "rider-X", "driver-X", 100.00, "i"))
    val updatesSecond = spark.createDataFrame(updateDataSecond).toDF(columns: _*)
    updatesSecond.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)

    // Validate merge mode.
    val metaClient: HoodieTableMetaClient = {
      HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(basePath).build()
    }
    if (tableVersion == "8") {
      assertEquals(mergeMode, metaClient.getTableConfig.getRecordMergeMode.name())
    } else {
      if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING.name()) {
        assertEquals(
          classOf[DefaultHoodieRecordPayload].getName,
          metaClient.getTableConfig.getPayloadClass)
      } else {
        assertEquals(
          classOf[OverwriteWithLatestAvroPayload].getName,
          metaClient.getTableConfig.getPayloadClass
        )
      }
    }
    // Validate.
    val df = spark.read.options(readOpts).format("hudi").load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "op")
      .sort("key")
    val expected = if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING.name()) {
      expectedEventTimeBased :+ (100, "100", "rider-X", "driver-X", 100.00, "i")
    } else {
      expectedCommitTimeBased :+ (100, "100", "rider-X", "driver-X", 100.00, "i")
    }
    val expectedDf = spark.createDataFrame(expected).toDF(columns: _*).sort("key")

    assertTrue(
      SparkClientFunctionalTestHarness.areDataframesEqual(
        expectedDf, finalDf, columns.toSet.asJava))
  }

  protected def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }
}

object TestMergeModeDuringUpgradeDowngrade {
  def provideParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("MERGE_ON_READ", "6", "8", "EVENT_TIME_ORDERING", "EVENT_TIME_ORDERING"),
      Arguments.of("MERGE_ON_READ", "6", "8", "COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"),
      Arguments.of("MERGE_ON_READ", "8", "6", "EVENT_TIME_ORDERING", "EVENT_TIME_ORDERING"),
      Arguments.of("MERGE_ON_READ", "8", "6", "COMMIT_TIME_ORDERING", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", "6", "8", "EVENT_TIME_ORDERING", "EVENT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", "6", "8", "COMMIT_TIME_ORDERING", "COMMIT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", "8", "6", "EVENT_TIME_ORDERING", "EVENT_TIME_ORDERING"),
      Arguments.of("COPY_ON_WRITE", "8", "6", "COMMIT_TIME_ORDERING", "COMMIT_TIME_ORDERING"))
  }

  def getWriteOptsForTableVersionSix(tableType: String,
                                     mergeMode: String): Map[String, String] = {
    Map(
      RECORDKEY_FIELD.key -> "key",
      PRECOMBINE_FIELD.key -> "ts",
      DataSourceWriteOptions.TABLE_NAME.key -> "test_table",
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> (
        if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING.name)
          classOf[DefaultHoodieRecordPayload].getName
        else
          classOf[OverwriteWithLatestAvroPayload].getName),
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "6")
  }

  def getWriteOptsForTableVersionEight(tableType: String,
                                       mergeMode: String): Map[String, String] = {
    Map(
      RECORDKEY_FIELD.key -> "key",
      PRECOMBINE_FIELD.key -> "ts",
      DataSourceWriteOptions.TABLE_NAME.key -> "test_table",
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> mergeMode,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8")
  }

  def getReadOpts: Map[String, String] = {
    Map(
      RECORDKEY_FIELD.key -> "key",
      PRECOMBINE_FIELD.key -> "ts",
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "true",
      HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key -> "false")
  }

  def getTableVersion(metaClient: HoodieTableMetaClient): HoodieTableVersion = {
    metaClient.getTableConfig.getTableVersion
  }
}
