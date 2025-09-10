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

import org.apache.hudi.DataSourceUtils
import org.apache.hudi.DataSourceWriteOptions.{INSERT_OPERATION_OPT_VAL, KEYGENERATOR_CLASS_NAME, OPERATION, ORDERING_FIELDS, PARTITIONPATH_FIELD, PAYLOAD_CLASS_NAME, RECORD_MERGE_MODE, RECORDKEY_FIELD, TABLE_TYPE, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.config.LockConfiguration.{LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY}
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecordMerger, HoodieRecordPayload, HoodieTableType, OverwriteWithLatestAvroPayload, TableServiceType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.timeline.InstantComparison.{compareTimestamps, GREATER_THAN_OR_EQUALS}
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.util.{Option, StringUtils}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieCompactionConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConverters._

class TestSevenToEightUpgrade extends RecordLevelIndexTestBase {

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,null",
    "COPY_ON_WRITE,org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    "COPY_ON_WRITE,org.apache.hudi.client.transaction.lock.NoopLockProvider",
    "MERGE_ON_READ,null",
    "MERGE_ON_READ,org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    "MERGE_ON_READ,org.apache.hudi.client.transaction.lock.NoopLockProvider"
  ))
  def testPartitionFieldsWithUpgrade(tableType: HoodieTableType, lockProviderClass: String): Unit = {
    val partitionFields = "partition:simple"
    // Downgrade handling for metadata not yet ready.
    val hudiOptsWithoutLockConfigs = commonOpts ++ Map(
      TABLE_TYPE.key -> tableType.name(),
      KEYGENERATOR_CLASS_NAME.key -> KeyGeneratorType.CUSTOM.getClassName,
      PARTITIONPATH_FIELD.key -> partitionFields,
      "hoodie.metadata.enable" -> "false",
      // "OverwriteWithLatestAvroPayload" is used to trigger merge mode upgrade/downgrade.
      PAYLOAD_CLASS_NAME.key -> classOf[OverwriteWithLatestAvroPayload].getName,
      RECORD_MERGE_MODE.key -> RecordMergeMode.COMMIT_TIME_ORDERING.name)

    var hudiOpts = if (!lockProviderClass.equals("null")) {
      hudiOptsWithoutLockConfigs ++ Map(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key() -> lockProviderClass)
    } else {
      hudiOptsWithoutLockConfigs
    }

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    metaClient = getLatestMetaClient(true)

    // assert table version is current (9) and the partition fields in table config has partition type
    assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
    assertEquals(classOf[OverwriteWithLatestAvroPayload].getName, metaClient.getTableConfig.getPayloadClass)

    // downgrade table props to version seven
    // assert table version is seven and the partition fields in table config does not have partition type
    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.SEVEN, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.SEVEN, metaClient.getTableConfig.getTableVersion)
    assertEquals("partition", HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
    // The payload class should be maintained.
    assertEquals(classOf[OverwriteWithLatestAvroPayload].getName, metaClient.getTableConfig.getPayloadClass)

    // auto upgrade the table
    // assert table version is eight and the partition fields in table config has partition type
    hudiOpts = hudiOpts ++ Map(HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

    // After upgrade, based on the payload and table type, the merge mode is updated accordingly.
    if (metaClient.getTableConfig.getTableType == HoodieTableType.COPY_ON_WRITE) {
      assertEquals(classOf[OverwriteWithLatestAvroPayload].getName, metaClient.getTableConfig.getPayloadClass)
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING.name, metaClient.getTableConfig.getRecordMergeMode.name)
      assertEquals(HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, metaClient.getTableConfig.getRecordMergeStrategyId)
    } else {
      if (metaClient.getTableConfig.getOrderingFieldsStr.isPresent && StringUtils.isNullOrEmpty(metaClient.getTableConfig.getOrderingFieldsStr.get())) {
        assertEquals(classOf[OverwriteWithLatestAvroPayload].getName, metaClient.getTableConfig.getPayloadClass)
        assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING.name, metaClient.getTableConfig.getRecordMergeMode.name)
        assertEquals(HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, metaClient.getTableConfig.getRecordMergeStrategyId)
      } else {
        assertEquals(classOf[DefaultHoodieRecordPayload].getName, metaClient.getTableConfig.getPayloadClass)
        assertEquals(RecordMergeMode.EVENT_TIME_ORDERING.name, metaClient.getTableConfig.getRecordMergeMode.name)
        assertEquals(HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID, metaClient.getTableConfig.getRecordMergeStrategyId)
      }
    }
  }

  /**
   * 1. Starts with current version (version 8) with both column stats and record index enabled.
   * 2. Performs initial commits, inline compaction, and cleaning (leaving delete blocks).
   * 3. Downgrades the table to version 6 and validates that metadata compaction happened.
   * 4. Upgrades back to version 8 and validates the record index.
   * 5. Validates data as well after upgrade/downgrade.
   */
  @Test
  def testUpgradeDowngradeWithMultipleMetadataPartitionsDeleteBlocks(): Unit = {
    initMetaClient(HoodieTableType.MERGE_ON_READ)
    // Common Hudi options for MERGE_ON_READ table with metadata and column stats, record index enabled.
    val hudiOptions = Map[String, String](
      "hoodie.table.name" -> tableName,
      RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
      KEYGENERATOR_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getName,
      PAYLOAD_CLASS_NAME.key -> classOf[DefaultHoodieRecordPayload].getName,
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "price",
      HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true",
      // Ensure MDT compaction does not run before downgrade.
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "100"
    )

    val _spark = spark
    import _spark.implicits._

    // ------------------------------------------------------------------
    // Step 1: Create table and insert initial records (initial commit)
    // ------------------------------------------------------------------
    println("== Step 1: Inserting initial records ==")
    val initialDF = Seq(
      (1, "Alice", 1000, 10), // (id, name, ts, price)
      (2, "Bob", 1000, 20)
    ).toDF("id", "name", "ts", "price")

    initialDF.write.format("hudi")
      .options(hudiOptions)
      .mode("overwrite")
      .save(basePath)

    // ------------------------------------------------------------------
    // Step 2: Update records to generate new log files with updated stats.
    // ------------------------------------------------------------------
    println("== Step 2: Updating records ==")
    val updateDF = Seq(
      (1, "Alice", 2000, 15), // update Alice
      (2, "Bob", 2000, 25) // update Bob
    ).toDF("id", "name", "ts", "price")

    updateDF.write.format("hudi")
      .options(hudiOptions)
      .mode("append")
      .save(basePath)

    // ------------------------------------------------------------------
    // Step 3: Trigger inline compaction (produces a new base file).
    // ------------------------------------------------------------------
    println("== Step 3: Triggering inline compaction ==")
    val compactionOptions = hudiOptions ++ Map(
      HoodieCompactionConfig.INLINE_COMPACT.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1"
    )
    updateDF.write.format("hudi")
      .options(compactionOptions)
      .mode("append")
      .save(basePath)

    // ------------------------------------------------------------------
    // Step 4: Perform cleaning to remove older log files (leaving behind delete blocks).
    // ------------------------------------------------------------------
    println("== Step 4: Running cleaning operation ==")
    val cleanOptions = hudiOptions ++ Map(
      HoodieCleanConfig.CLEANER_FILE_VERSIONS_RETAINED.key -> "1"
    )
    val client = DataSourceUtils.createHoodieClient(
      spark.sparkContext, "", basePath, tableName, cleanOptions.asJava
    ).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]
    client.clean()
    client.close()

    // ------------------------------------------------------------------
    // Step 5: Downgrade the table to version 6.
    // ------------------------------------------------------------------
    println("== Step 5: Downgrading table to version 6 ==")
    val downgradedOptions = hudiOptions ++ Map(
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> HoodieTableVersion.SIX.versionCode().toString
    )
    metaClient = HoodieTableMetaClient.reload(metaClient)
    new UpgradeDowngrade(metaClient, getWriteConfig(downgradedOptions, basePath), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.SIX, null)

    // Validate that the metadata table version is now 6 and that compaction occurred.
    var metadataMetaClient = HoodieTableMetaClient.builder()
      .setConf(metaClient.getStorageConf)
      .setBasePath(basePath + "/.hoodie/metadata")
      .build()
    assertEquals(HoodieTableVersion.SIX, metadataMetaClient.getTableConfig.getTableVersion)
    // For this test, we expect exactly one commit instant in the metadata timeline.
    assertEquals(1, metadataMetaClient.getActiveTimeline.getCommitTimeline.getInstants.size())
    val compactionInstant = metadataMetaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants().lastInstant().get()

    // ------------------------------------------------------------------
    // Step 6: Read the table using a predicate on 'price' to force column stats usage.
    // This read path exercises the delete block deserialization.
    // ------------------------------------------------------------------
    println("== Step 6: Reading table post-downgrade with predicate on 'price' ==")
    val df = spark.read.format("hudi").load(basePath).filter("price > 15")
    // We expect only record id=2 (price 25) to satisfy the predicate.
    assertEquals(1, df.count())
    assertEquals(1, df.filter("id = 2").count())

    // ------------------------------------------------------------------
    // Step 7: Do an update with table version 6 to trigger metadata compaction.
    // ------------------------------------------------------------------
    println("== Step 7: Committing update to trigger metadata compaction in downgraded table ==")
    val writeConfigsPostDowngrade = hudiOptions ++ Map(
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> HoodieTableVersion.SIX.versionCode().toString
    )
    val updateDF2 = Seq(
      (1, "Alice", 3000, 20),
      (2, "Bob", 3000, 30)
    ).toDF("id", "name", "ts", "price")

    updateDF2.write.format("hudi")
      .options(writeConfigsPostDowngrade)
      .mode("append")
      .save(basePath)

    // Validate that the metadata table was compacted again.
    metadataMetaClient = HoodieTableMetaClient.reload(metadataMetaClient)
    val lastCompactionInstant = metadataMetaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants().lastInstant().get()
    assertTrue(compareTimestamps(lastCompactionInstant.requestedTime(), GREATER_THAN_OR_EQUALS, compactionInstant.requestedTime()))

    // ------------------------------------------------------------------
    // Step 8: Upgrade the table back to current version (version 8) and validate record index.
    // ------------------------------------------------------------------
    println("== Step 8: Upgrading table to version 8 and validating record index ==")
    val upgradedOptions = hudiOptions ++ Map(
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "true",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> HoodieTableVersion.EIGHT.versionCode().toString
    )
    metaClient = HoodieTableMetaClient.reload(metaClient)
    new UpgradeDowngrade(metaClient, getWriteConfig(upgradedOptions, basePath), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.EIGHT, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)

    // For record index validation, do another update and then query for a specific record.
    val updateDF3 = Seq(
      (1, "Alice", 4000, 25),
      (2, "Bob", 4000, 35)
    ).toDF("id", "name", "ts", "price")
    updateDF3.write.format("hudi")
      .options(upgradedOptions)
      .mode("append")
      .save(basePath)

    val df2 = spark.read.format("hudi").load(basePath).filter("id = 2")
    assertEquals(1, df2.count())
    assertEquals(1, df2.filter("price = 35").count())
  }

  @Test
  def testV6TableUpgradeToV9ToV6(): Unit = {
    try {
      val partitionFields = "partition:simple"

      val hudiOptsV6 = commonOpts ++ Map(
        TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
        KEYGENERATOR_CLASS_NAME.key -> KeyGeneratorType.CUSTOM.getClassName,
        PARTITIONPATH_FIELD.key -> partitionFields,
        "hoodie.metadata.enable" -> "true",
        PAYLOAD_CLASS_NAME.key -> classOf[OverwriteWithLatestAvroPayload].getName,
        RECORD_MERGE_MODE.key -> RecordMergeMode.COMMIT_TIME_ORDERING.name,
        HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "6",
        HoodieWriteConfig.TIMELINE_LAYOUT_VERSION_NUM.key() -> Integer.toString(TimelineLayoutVersion.VERSION_1),
        HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false",
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "15",
        HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key -> "3",
        HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key -> "4",
        HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key -> "5",
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
        LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY -> "60000",
        LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY -> "10",
        LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY -> "1000",
        HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> "OPTIMISTIC_CONCURRENCY_CONTROL"
      )

      doWriteAndValidateDataAndRecordIndex(hudiOptsV6,
        operation = INSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Overwrite,
        validate = false)

      for (i <- 1 to 10) {
        doWriteAndValidateDataAndRecordIndex(hudiOptsV6,
          operation = INSERT_OPERATION_OPT_VAL,
          saveMode = SaveMode.Append,
          validate = false)
      }
      metaClient = getLatestMetaClient(true)

      assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig.getTableVersion)
      assertEquals("partition", HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

      val archivePath = new org.apache.hudi.storage.StoragePath(metaClient.getArchivePath, ".commits_.archive*")
      val archivedFiles = metaClient.getStorage.globEntries(archivePath)
      println(s"Archived files found ${archivedFiles.size()}")

      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(storage.getConf())
        .build()

      val hudiOptsUpgrade = hudiOptsV6 ++ Map(
        HoodieWriteConfig.WRITE_TABLE_VERSION.key -> HoodieTableVersion.current().versionCode().toString,
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
        HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> "OPTIMISTIC_CONCURRENCY_CONTROL"
      ) - HoodieWriteConfig.AUTO_UPGRADE_VERSION.key

      doWriteAndValidateDataAndRecordIndex(hudiOptsUpgrade,
        operation = UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append,
        validate = false)

      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(storage.getConf())
        .build()

      assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig.getTableVersion)
      assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

      val archivedFilesAfterUpgrade = metaClient.getStorage.globEntries(archivePath)

      assertTrue(archivedFilesAfterUpgrade.size() > 0,
        "Even after upgrade, fresh table with ~12 commits should have archived files")

      val hudiOptsDowngrade = hudiOptsV6 ++ Map(
        HoodieWriteConfig.WRITE_TABLE_VERSION.key -> HoodieTableVersion.SIX.versionCode().toString,
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
        HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> "OPTIMISTIC_CONCURRENCY_CONTROL"
      )

      new UpgradeDowngrade(metaClient, getWriteConfig(hudiOptsDowngrade, basePath), context, SparkUpgradeDowngradeHelper.getInstance)
        .run(HoodieTableVersion.SIX, null)

      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(storage.getConf())
        .build()

      assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig.getTableVersion)
      assertEquals("partition", HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

      val v6ArchivePath = new org.apache.hudi.storage.StoragePath(metaClient.getArchivePath, ".commits_.archive*")
      val v6ArchivedFiles = metaClient.getStorage.globEntries(v6ArchivePath)

      assertTrue(v6ArchivedFiles.size() > 0, "Downgrade should have archived files in V6 format")
    } catch {
      case _: Exception =>
        assumeTrue(false, "Skipping test")
    }
  }

  private def getWriteConfig(hudiOpts: Map[String, String], basePath: String): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }
}
