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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.config.HoodieCompactionConfig
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Disabled, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.collection.JavaConverters._

class TestSixToFiveDowngradeHandler extends RecordLevelIndexTestBase {

  private var partitionPaths: java.util.List[StoragePath] = null

  @Disabled
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
    metaClient = getHoodieMetaClient(metaClient.getStorageConf, basePath)
    // Ensure file slices have been compacted and the MDT table has been deleted
    assertFalse(metaClient.getTableConfig.isMetadataTableAvailable)
    assertEquals(HoodieTableVersion.FIVE, metaClient.getTableConfig.getTableVersion)
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertEquals(0, getLogFilesCount(hudiOpts))
    }
  }

  @Disabled
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

  @Disabled
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
      val relativePath = FSUtils.getRelativePartitionPath(metaClient.getBasePath, partitionPath)
      val lastInstantOption = getLatestMetaClient(false).getActiveTimeline.lastInstant()
      if (lastInstantOption.isPresent) {
        fsView.getLatestMergedFileSlicesBeforeOrOn(relativePath, getLatestMetaClient(false)
          .getActiveTimeline.lastInstant().get().requestedTime).iterator().asScala.toSeq
      } else {
        Seq.empty
      }
    }.foreach(
      slice => if (slice.getLogFiles.count() > 0) {
        numFileSlicesWithLogFiles += 1
      })
    numFileSlicesWithLogFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieTableFileSystemView = {
    if (metaClient.getTableConfig.isMetadataTableAvailable) {
      new HoodieTableFileSystemView(metadataWriter(getWriteConfig(opts)).getTableMetadata, metaClient, metaClient.getActiveTimeline)
    } else {
      HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, metaClient.getActiveTimeline)
    }
  }

  private def getAllPartititonPaths(fsView: HoodieTableFileSystemView): java.util.List[StoragePath] = {
    if (partitionPaths == null) {
      fsView.loadAllPartitions()
      partitionPaths = fsView.getPartitionPaths
    }
    partitionPaths
  }
}
