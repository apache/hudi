/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.config.HoodieCompactionConfig
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.jdk.CollectionConverters.{asScalaIteratorConverter, collectionAsScalaIterableConverter}

class TestSixToFiveDowngradeHandler extends RecordLevelIndexTestBase {

  private var partitionPaths: java.util.List[Path] = null

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testDowngradeWithMDTAndLogFiles(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key() -> "0")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getTableConfig.isMetadataTableAvailable)
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertTrue(getLogFilesCount(hudiOpts) > 0)
    }

    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.FIVE, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    // Ensure file slices have been compacted and the MDT table has been deleted
    assertFalse(metaClient.getTableConfig.isMetadataTableAvailable)
    assertEquals(HoodieTableVersion.FIVE, metaClient.getTableConfig.getTableVersion)
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertEquals(0, getLogFilesCount(hudiOpts))
    }
  }

  @Test
  def testDowngradeWithoutLogFiles(): Unit = {
    val hudiOpts = commonOpts + (
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key() -> "0")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(0, getLogFilesCount(hudiOpts))

    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.FIVE, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(0, getLogFilesCount(hudiOpts))
    assertEquals(HoodieTableVersion.FIVE, metaClient.getTableConfig.getTableVersion)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testDowngradeWithoutMDT(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieMetadataConfig.ENABLE.key() -> "false")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertFalse(metaClient.getTableConfig.isMetadataTableAvailable)

    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.FIVE, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertFalse(metaClient.getTableConfig.isMetadataTableAvailable)
    assertEquals(HoodieTableVersion.FIVE, metaClient.getTableConfig.getTableVersion)
  }

  private def getLogFilesCount(opts: Map[String, String]) = {
    var numFileSlicesWithLogFiles = 0L
    val fsView = getTableFileSystemView(opts)
    getAllPartititonPaths(fsView).asScala.flatMap { partitionPath =>
      val relativePath = FSUtils.getRelativePartitionPath(metaClient.getBasePathV2, partitionPath)
      fsView.getLatestMergedFileSlicesBeforeOrOn(relativePath, getLatestMetaClient(false)
        .getActiveTimeline.lastInstant().get().getTimestamp).iterator().asScala.toSeq
    }.foreach(
      slice => if (slice.getLogFiles.count() > 0) {
        numFileSlicesWithLogFiles += 1
      })
    numFileSlicesWithLogFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieTableFileSystemView = {
    if (metaClient.getTableConfig.isMetadataTableAvailable) {
      new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline, metadataWriter(getWriteConfig(opts)).getTableMetadata)
    } else {
      new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline)
    }
  }

  private def getAllPartititonPaths(fsView: HoodieTableFileSystemView): java.util.List[Path] = {
    if (partitionPaths == null) {
      fsView.loadAllPartitions()
      partitionPaths = fsView.getPartitionPaths
    }
    partitionPaths
  }
}
