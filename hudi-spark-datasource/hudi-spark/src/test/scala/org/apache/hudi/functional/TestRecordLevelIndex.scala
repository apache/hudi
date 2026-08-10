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

import org.apache.hudi.{DataSourceWriteOptions, SparkDatasetMixin}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordGlobalLocation, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, InProcessTimeGenerator}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.core.index.record.HoodieRecordIndex
import org.apache.hudi.exception.{HoodieException, HoodieMetadataException}
import org.apache.hudi.functional.TestRecordLevelIndex.TestPartitionedRecordLevelIndexTestCase
import org.apache.hudi.index.HoodieIndex.IndexType.RECORD_LEVEL_INDEX
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertFalse, assertThrows, assertTrue, fail}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource, ValueSource}

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.stream.Collectors

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

@Tag("functional")
class TestRecordLevelIndex extends RecordLevelIndexTestBase with SparkDatasetMixin {
  private class testRecordLevelIndexHolder {
    var bulkRecordKeys: java.util.List[String] = null
    var options: Map[String, String] = null
    var recordKeys: java.util.List[String] = null
    var newRecordKeys: java.util.List[String] = null
  }

  @Test
  def testRecordIndexRebootstrapWhenHoodiePartitionMetadataIsMissingScala(): Unit = {
    val partitionToCorrupt = HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH
    val insertedRecords = 30
    val localDataGen = new HoodieTestDataGenerator()
    val inserts = localDataGen.generateInserts("001", insertedRecords)
    val insertDf = toDataset(spark, inserts)
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name())
    insertDf.write.format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(insertedRecords, spark.read.format("hudi").load(basePath).count())

    metaClient.reloadActiveTimeline()
    val latestTableSchema = new TableSchemaResolver(metaClient).getTableSchemaFromLatestCommit(false).get().toString
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(
      options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestTableSchema)).asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
    val metadataBeforeRebootstrap = metadataWriter(writeConfig).getTableMetadata
    // Verify that the metadata table contains all the data partitions.
    val partitionPaths = metadataBeforeRebootstrap.getAllPartitionPaths()
    assertEquals(localDataGen.getPartitionPaths().size, partitionPaths.size,
      "Metadata table should contain all the data partitions")
    assertTrue(partitionPaths.contains(partitionToCorrupt),
      "Metadata table should contain the partition to corrupt")

    val filesInAllPartitionsBeforeRebootstrap = getFilesInAllPartitions(metadataBeforeRebootstrap)
    val recordKeys = getRecordKeys()
    assertEquals(recordKeys.size(), getRecordIndexEntries(metadataBeforeRebootstrap, recordKeys, localDataGen.getPartitionPaths.toSeq).size,
      "Record index entries should match inserted records after first batch")

    assertTrue(storage.exists(new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(basePath))),
      "Metadata table should exist before deletion")

    // Remove _hoodie_partition_metadata from one data partition.
    removeOnePartitionMetadataFile(partitionToCorrupt)

    // Delete metadata table and force a full metadata rebootstrap.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    HoodieTableMetadataUtil.deleteMetadataTable(metaClient, context, false)
    assertFalse(storage.exists(new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(basePath))),
      "Metadata table should be removed before rebootstrap")

    // Rebootstrap should succeed even when one partition metadata file is missing.
    assertDoesNotThrow(() => metadataWriter(writeConfig).getTableMetadata,
      "Metadata rebootstrap with record index enabled should succeed")
    val metadataAfterRebootstrap = metadataWriter(writeConfig).getTableMetadata.asInstanceOf[HoodieBackedTableMetadata]

    // Verify the record_index partition is created after rebootstrap.
    val recordIndexPath = new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(basePath), MetadataPartitionType.RECORD_INDEX.getPartitionPath)
    assertTrue(storage.exists(recordIndexPath),
      "Record index partition should exist after metadata rebootstrap")

    // Verify that the metadata table does not contain the partition that was missing metadata.
    val partitionPathsAfterBootstrap = metadataAfterRebootstrap.getAllPartitionPaths()
    assertFalse(partitionPathsAfterBootstrap.contains(partitionToCorrupt),
      "Metadata table should not contain the partition that was missing metadata")
    assertEquals(localDataGen.getPartitionPaths().size - 1, partitionPathsAfterBootstrap.size,
      "Metadata table should contain all the data partitions except the one that was missing metadata")

    // Missing partition metadata should lead to fewer indexed records than initially inserted.
    val recordIndexEntriesCount = getRecordIndexEntries(metadataAfterRebootstrap, recordKeys, localDataGen.getPartitionPaths.toSeq).size
    assertTrue(insertedRecords > recordIndexEntriesCount,
      "Record index entries should not match inserted records after metadata rebootstrap")

    // Files metadata should also undercount when that partition is skipped during bootstrap.
    val filesInAllPartitionsAfterRebootstrap = getFilesInAllPartitions(metadataAfterRebootstrap)
    assertTrue(filesInAllPartitionsBeforeRebootstrap.size > filesInAllPartitionsAfterRebootstrap.size,
      "Metadata files partition count should be lower than data table file count after rebootstrap")
  }

  def testRecordLevelIndex(tableType: HoodieTableType, streamingWriteEnabled: Boolean, holder: testRecordLevelIndexHolder,
                           rliInitDeferred: Boolean = false): Unit = {
    val dataGen = new HoodieTestDataGenerator();
    val inserts = dataGen.generateInserts("001", 5)
    val latestBatchDf = toDataset(spark, inserts)
    val insertDf = latestBatchDf.withColumn("data_partition_path", lit("partition1")).union(latestBatchDf.withColumn("data_partition_path", lit("partition2")))
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> streamingWriteEnabled.toString,
      HoodieMetadataConfig.DEFER_RLI_INIT_FOR_FRESH_TABLE.key() -> rliInitDeferred.toString,
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name())
    holder.options = options
    insertDf.write.format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    if (rliInitDeferred) {
      // With defer enabled, the first commit should NOT have initialized the RLI partition.
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assertFalse(metaClient.getTableConfig.getMetadataPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath),
        "RLI partition should not be initialized after the first commit when defer is enabled")
    }
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(options).asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
    // Constructing the metadata writer here will initialize RLI (lazily, on this second metadata-writer entry)
    // when defer is enabled, since there is now 1 completed commit on the data table.
    var metadata = metadataWriter(writeConfig).getTableMetadata
    val recordKeys = inserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    holder.recordKeys = recordKeys
    var partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    var partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    var df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newDeletes =  dataGen.generateUpdates("004", 1)
    val updates =  dataGen.generateUniqueUpdates("002", 3)
    val lowerOrderingValue = 1L
    updates.addAll(dataGen.generateUniqueDeleteRecords("002", 2, lowerOrderingValue))
    val nextBatchDf = toDataset(spark, updates)
    val updateDf = nextBatchDf.withColumn("data_partition_path", lit("partition1"))

    updateDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newInserts =  dataGen.generateInserts("003", 3)
    val newInsertBatchDf = toDataset(spark, newInserts)
    val newInsertDf = newInsertBatchDf.withColumn("data_partition_path", lit("partition2")).union(newInsertBatchDf.withColumn("data_partition_path", lit("partition3")))
    newInsertDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(16, spark.read.format("hudi").load(basePath).count())
    metadata = metadataWriter(writeConfig).getTableMetadata
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    val newRecordKeys = newInserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    holder.newRecordKeys = newRecordKeys
    partition1Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    var partition3Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition3Locations, "partition3")

    val newDeletesBatchDf = toDataset(spark, newDeletes)
    val newDeletesDf = newDeletesBatchDf.withColumn("data_partition_path", lit("partition1"))
    newDeletesDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(15, spark.read.format("hudi").load(basePath).count())
    metadata = metadataWriter(writeConfig).getTableMetadata
    partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(4, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    assertFalse(partition1Locations.contains(newDeletes.get(0).getRecordKey))
    assertTrue(partition2Locations.contains(newDeletes.get(0).getRecordKey))
    partition1Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    partition3Locations = readRecordIndex(metadata, newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    validateDFWithLocations(df, partition3Locations, "partition3")

    val bulkInserts = dataGen.generateInserts("005", 5)
    val bulkInsertDf = toDataset(spark, bulkInserts)
    val bulkInsertPartitionedDf = bulkInsertDf.withColumn("data_partition_path", lit("partition0"))

    // Use bulk_insert operation explicitly
    bulkInsertPartitionedDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val bulkRecordKeys = bulkInserts.asScala.map(_.getRecordKey).asJava
    holder.bulkRecordKeys = bulkRecordKeys
    metadata = metadataWriter(writeConfig).getTableMetadata
    val partition0Locations = readRecordIndex(metadata, bulkRecordKeys, HOption.of("partition0"))
    assertEquals(5, partition0Locations.size)
    df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition0Locations, "partition0")

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(HoodieRecordIndex.isPartitioned(metaClient.getIndexMetadata.get().getIndexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)))
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexRollback(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    val holder = new testRecordLevelIndexHolder
    testRecordLevelIndex(testCase.tableType, testCase.streamingWriteEnabled, holder)
    val writeConfig = getWriteConfig(holder.options)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    writeClient.rollback(metaClient.getActiveTimeline.lastInstant().get().requestedTime())
    writeClient.close()
    val metadata = metadataWriter(writeConfig).getTableMetadata
    try {
      val partition0Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition0"))
      fail("rollback happened, so partition should be deleted")
    } catch {
      case t: Throwable => assertTrue(t.isInstanceOf[ArithmeticException])
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionedRecordLevelIndexDefer(streamingWriteEnabled: Boolean): Unit = {
    val holder = new testRecordLevelIndexHolder
    testRecordLevelIndex(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled, holder, true)
    assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options)
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleCompaction(HOption.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.compact(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("compaction", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    writeClient.close()
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionedRecordLevelIndexDeferWithBulkInsert(streamingWriteEnabled: Boolean): Unit = {
    val tableType = HoodieTableType.MERGE_ON_READ
    val dataGen = new HoodieTestDataGenerator()
    val inserts1 = dataGen.generateInserts("001", 5)
    val batch1Df = toDataset(spark, inserts1)
    val insertDf1 = batch1Df.withColumn("data_partition_path", lit("partition1"))
      .union(batch1Df.withColumn("data_partition_path", lit("partition2")))

    val options = Map(
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> streamingWriteEnabled.toString,
      HoodieMetadataConfig.DEFER_RLI_INIT_FOR_FRESH_TABLE.key() -> "true",
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name())

    // Commit #1: bulk_insert on a fresh table with defer enabled.
    insertDf1.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())

    // Defer should have kicked in: RLI partition is not initialized after the first bulk_insert.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertFalse(metaClient.getTableConfig.getMetadataPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath),
      "RLI partition should not be initialized after the first bulk_insert when defer is enabled")

    // Commit #2: another bulk_insert into a new partition. New rows must use distinct record keys.
    val inserts2 = dataGen.generateInserts("002", 5)
    val batch2Df = toDataset(spark, inserts2)
    val insertDf2 = batch2Df.withColumn("data_partition_path", lit("partition3"))

    insertDf2.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(15, spark.read.format("hudi").load(basePath).count())

    // Build metadata writer/reader; this entry will initialize RLI now that there is a completed commit.
    val writeConfig = getWriteConfig(options)
    val metadata = metadataWriter(writeConfig).getTableMetadata

    // RLI partition should now be present in the metadata table.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath),
      "RLI partition should be initialized once a completed commit exists on the data table")
    assertTrue(HoodieRecordIndex.isPartitioned(
      metaClient.getIndexMetadata.get().getIndexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)),
      "RLI should be initialized as partitioned RLI")

    // Validate record key -> location mapping for both batches against the data.
    val tableRows = spark.read.format("hudi").load(basePath).collect()

    val batch1Keys = inserts1.asScala.map(_.getRecordKey).asJava.stream().collect(Collectors.toList())
    val partition1Locations = readRecordIndex(metadata, batch1Keys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    validateDFWithLocations(tableRows, partition1Locations, "partition1")
    val partition2Locations = readRecordIndex(metadata, batch1Keys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    validateDFWithLocations(tableRows, partition2Locations, "partition2")

    val batch2Keys = inserts2.asScala.map(_.getRecordKey).asJava.stream().collect(Collectors.toList())
    val partition3Locations = readRecordIndex(metadata, batch2Keys, HOption.of("partition3"))
    assertEquals(5, partition3Locations.size)
    validateDFWithLocations(tableRows, partition3Locations, "partition3")

    // Cross-partition lookups for batch1 keys against partition3 (and vice versa) should be empty.
    assertEquals(0, readRecordIndex(metadata, batch1Keys, HOption.of("partition3")).size)
    assertEquals(0, readRecordIndex(metadata, batch2Keys, HOption.of("partition1")).size)
    assertEquals(0, readRecordIndex(metadata, batch2Keys, HOption.of("partition2")).size)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionedRecordLevelIndexCompact(streamingWriteEnabled: Boolean): Unit = {
    val holder = new testRecordLevelIndexHolder
    testRecordLevelIndex(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled, holder)
    assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options)
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleCompaction(HOption.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.compact(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("compaction", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    writeClient.close()
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexCluster(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    val holder = new testRecordLevelIndexHolder
    testRecordLevelIndex(testCase.tableType, testCase.streamingWriteEnabled, holder)
    assertEquals(if (testCase.tableType.equals(HoodieTableType.MERGE_ON_READ)) "deltacommit" else "commit",
      metaClient.getActiveTimeline.lastInstant().get().getAction)
    val writeConfig = getWriteConfig(holder.options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> HoodieTestDataGenerator.AVRO_SCHEMA.toString))
    var metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    val timeOpt = writeClient.scheduleClustering(HOption.empty())
    assertTrue(timeOpt.isPresent)
    writeClient.cluster(timeOpt.get())
    metaClient.reloadActiveTimeline()
    assertEquals("replacecommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
    metadata = metadataWriter(writeConfig).getTableMetadata
    doAllAssertions(holder, metadata)
    writeClient.close()
  }

  private def validateDFWithLocations(df: Array[Row], locations: Map[String, HoodieRecordGlobalLocation],
                                      partition: String): Unit = {
    var count: Int = 0
    for (row <- df) {
      val recordKey = row.getString(2)
      locations.get(recordKey).foreach { loc =>
        if (partition == row.getString(3)) {
          count += 1
          assertEquals(row.getString(3), loc.getPartitionPath)
          assertEquals(FSUtils.getFileId(row.getString(4)), loc.getFileId)
        }
      }
    }
    assertEquals(locations.size, count)
  }

  private def doAllAssertions(holder: testRecordLevelIndexHolder, metadata: HoodieBackedTableMetadata): Unit = {
    val df = spark.read.format("hudi").load(basePath).collect()
    var partition0Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition0"))
    assertEquals(0, partition0Locations.size)
    var partition1Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition1"))
    assertEquals(4, partition1Locations.size)
    validateDFWithLocations(df, partition1Locations, "partition1")
    var partition2Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    var partition3Locations = readRecordIndex(metadata, holder.recordKeys, HOption.of("partition3"))
    assertEquals(0, partition3Locations.size)

    partition0Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition0"))
    assertEquals(0, partition0Locations.size)
    partition1Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition2"))
    assertEquals(3, partition2Locations.size)
    validateDFWithLocations(df, partition2Locations, "partition2")
    partition3Locations = readRecordIndex(metadata, holder.newRecordKeys, HOption.of("partition3"))
    assertEquals(3, partition3Locations.size)
    validateDFWithLocations(df, partition3Locations, "partition3")

    partition0Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition0"))
    assertEquals(5, partition0Locations.size)
    validateDFWithLocations(df, partition0Locations, "partition0")
    partition1Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition1"))
    assertEquals(0, partition1Locations.size)
    partition2Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition2"))
    assertEquals(0, partition2Locations.size)
    partition3Locations = readRecordIndex(metadata, holder.bulkRecordKeys, HOption.of("partition3"))
    assertEquals(0, partition3Locations.size)
  }

  @Test
  def testPartitionedRecordLevelIndexLookupUsesFullKey(): Unit = {
    initMetaClient(HoodieTableType.COPY_ON_WRITE)
    val dataGen = new HoodieTestDataGenerator()
    val inserts = dataGen.generateInserts("001", 3)
    val insertDf = toDataset(spark, inserts).withColumn("data_partition_path", lit("partition1"))
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name())
    insertDf.write.format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val writeConfig = getWriteConfig(options)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val recordKeys = inserts.asScala.map(i => i.getRecordKey).asJava
    assertEquals(3, readRecordIndex(metadata, recordKeys, HOption.of("partition1")).size)

    // Partitioned RLI lookup is still a full record-key lookup within the data partition.
    // A prefix-only key would incorrectly match real records if the lookup used prefix matching.
    val recordKeySet = recordKeys.asScala.toSet
    val prefixOnlyKey = recordKeys.asScala
      .flatMap(key => (1 until key.length).map(key.substring(0, _)))
      .find(prefix => !recordKeySet.contains(prefix))
      .get
    assertTrue(readRecordIndex(metadata, util.Collections.singletonList(prefixOnlyKey), HOption.of("partition1")).isEmpty)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationBasic(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = false, compact = false, cluster = false)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationRollback(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = true, compact = false, cluster = false)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionedRecordLevelIndexInitializationCompact(streamingWriteEnabled: Boolean): Unit = {
    testPartitionedRecordLevelIndexInitialization(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled, failAndDoRollback = false, compact = true, cluster = false)
  }

  @ParameterizedTest
  @MethodSource(Array("testArgsForPartitionedRecordLevelIndex"))
  def testPartitionedRecordLevelIndexInitializationCluster(testCase: TestPartitionedRecordLevelIndexTestCase): Unit = {
    testPartitionedRecordLevelIndexInitialization(testCase.tableType, testCase.streamingWriteEnabled, failAndDoRollback = false, compact = false, cluster = true)
  }

  def testPartitionedRecordLevelIndexInitialization(tableType: HoodieTableType,
                                                    streamingWriteEnabled: Boolean,
                                                    failAndDoRollback: Boolean,
                                                    compact: Boolean,
                                                    cluster: Boolean): Unit = {
    initMetaClient(tableType)
    val dataGen = new HoodieTestDataGenerator()
    val inserts = dataGen.generateInserts("001", 5)
    val latestBatchDf = toDataset(spark, inserts)
    val insertDf = latestBatchDf.withColumn("data_partition_path", lit("partition1")).union(latestBatchDf.withColumn("data_partition_path", lit("partition2")))
    val options = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "data_partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key()-> "false",
      HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> streamingWriteEnabled.toString,
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name())
    insertDf.write.format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertEquals(10, spark.read.format("hudi").load(basePath).count())

    val updates =  dataGen.generateUniqueUpdates("002", 3)
    val nextBatchDf = toDataset(spark, updates)
    val updateDf = nextBatchDf.withColumn("data_partition_path", lit("partition1"))
    updateDf.write.format("hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(10, spark.read.format("hudi").load(basePath).count())
    metaClient.reloadActiveTimeline()
    val tableSchemaResolver = new TableSchemaResolver(metaClient)
    val latestTableSchemaFromCommitMetadata = tableSchemaResolver.getTableSchemaFromLatestCommit(false)

    if (failAndDoRollback) {
      val updatesToFail =  dataGen.generateUniqueUpdates("003", 3)
      val batchToFailDf = toDataset(spark, updatesToFail)
      val failDf = batchToFailDf.withColumn("data_partition_path", lit("partition1")).union(batchToFailDf.withColumn("data_partition_path", lit("partition3")))
      failDf.write.format("hudi")
        .options(options)
        .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
      assertEquals(13, spark.read.format("hudi").load(basePath).count())

      metaClient.reloadActiveTimeline()
      val lastInstant = metaClient.getActiveTimeline.lastInstant().get()
      assertTrue(storage.deleteFile(new StoragePath(metaClient.getTimelinePath, metaClient.getInstantFileNameGenerator.getFileName(lastInstant))))
      assertEquals(10, spark.read.format("hudi").load(basePath).count())

      // rollback
      val writeConfig = HoodieWriteConfig.newBuilder()
        .withProps(TypedProperties.fromMap(JavaConverters
          .mapAsJavaMapConverter(options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestTableSchemaFromCommitMetadata.get().toString)).asJava))
        .withPath(basePath)
        .build()
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      writeClient.rollback(lastInstant.requestedTime())
      writeClient.close()
    }

    if (compact) {
      assertEquals("deltacommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
      val writeConfig = getWriteConfig(options ++
        Map(HoodieCompactionConfig.COMPACTION_STRATEGY.key() -> classOf[UnBoundedCompactionStrategy].getName,
          HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"))
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      val timeOpt = writeClient.scheduleCompaction(HOption.empty())
      assertTrue(timeOpt.isPresent)
      writeClient.compact(timeOpt.get())
      metaClient.reloadActiveTimeline()
      assertEquals("compaction", metaClient.getActiveTimeline.lastInstant().get().getAction)
      writeClient.close()
    }

    if (cluster) {
      assertEquals(if (tableType.equals(HoodieTableType.MERGE_ON_READ)) "deltacommit" else "commit",
        metaClient.getActiveTimeline.lastInstant().get().getAction)
      val writeConfig = getWriteConfig(options ++ Map(HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> HoodieTestDataGenerator.AVRO_SCHEMA.toString))
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      val timeOpt = writeClient.scheduleClustering(HOption.empty())
      assertTrue(timeOpt.isPresent)
      writeClient.cluster(timeOpt.get())
      metaClient.reloadActiveTimeline()
      assertEquals("replacecommit", metaClient.getActiveTimeline.lastInstant().get().getAction)
      writeClient.close()
    }

    //init mdt
    val updateOptions = options ++ Map(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestTableSchemaFromCommitMetadata.get().toString)
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(updateOptions).asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val recordKeys = inserts.asScala.map(i => i.getRecordKey).asJava.stream().collect(Collectors.toList())
    val partition1Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition1"))
    assertEquals(5, partition1Locations.size)
    val partition2Locations = readRecordIndex(metadata, recordKeys, HOption.of("partition2"))
    assertEquals(5, partition2Locations.size)
    val df = spark.read.format("hudi").load(basePath).collect()
    validateDFWithLocations(df, partition1Locations, "partition1")
    validateDFWithLocations(df, partition2Locations, "partition2")

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(HoodieRecordIndex.isPartitioned(metaClient.getIndexMetadata.get().getIndexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)))
  }

  def readRecordIndex(metadata: HoodieBackedTableMetadata, recordKeys: java.util.List[String], dataTablePartition: HOption[String]): Map[String, HoodieRecordGlobalLocation] = {
    metadata.readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys), dataTablePartition)
      .collectAsList().asScala.map(p => p.getKey -> p.getValue).toMap
  }

  private def getRecordKeys(): java.util.List[String] = {
    spark.read.format("hudi").load(basePath)
      .select("_hoodie_record_key")
      .collectAsList()
      .asScala
      .map(row => row.getAs[String]("_hoodie_record_key"))
      .asJava
  }

  private def getRecordIndexEntries(metadata: HoodieBackedTableMetadata,
                                    recordKeys: java.util.List[String],
                                    partitionPaths: Seq[String]): Map[String, HoodieRecordGlobalLocation] = {
    partitionPaths
      .flatMap(partition => {
        try {
          readRecordIndex(metadata, recordKeys, HOption.of(partition))
        } catch {
          // Partitioned RLI can throw when a partition is skipped from metadata bootstrap.
          case _: ArithmeticException => Map.empty[String, HoodieRecordGlobalLocation]
        }
      })
      .toMap
  }

  private def removeOnePartitionMetadataFile(partition: String): Unit = {
    val partitionPath = new StoragePath(basePath, partition)
    val entries = storage.listDirectEntries(partitionPath).asScala
    val partitionMetadataFile = entries
      .map(_.getPath)
      .find(path => path.getName.startsWith(".hoodie_partition_metadata"))
      .getOrElse(throw new IllegalStateException(s"No partition metadata file found under $partitionPath"))
    assertTrue(storage.deleteFile(partitionMetadataFile),
      s"Failed to delete partition metadata file $partitionMetadataFile")
  }

  private def getFilesInAllPartitions(metadata: HoodieBackedTableMetadata): Seq[StoragePathInfo] = {
    val partitionPaths = metadata.getAllPartitionPaths.asScala
      .map(partitionPath => FSUtils.getAbsolutePartitionPath(new StoragePath(basePath), partitionPath).toString)
      .asJava
    metadata.getAllFilesInPartitions(partitionPaths).values().asScala.flatMap(_.asScala).toSeq
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testRLIForDeletesWithHoodieIsDeletedColumn(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name()) +
      (HoodieIndexConfig.INDEX_TYPE.key -> "RECORD_INDEX") +
      (HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "false")
    val insertDf = doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    insertDf.cache()

    val instantTime = InProcessTimeGenerator.createNewInstantTime()
    // Issue two deletes, where one has an older ordering value that should be ignored
    val deletedRecords = dataGen.generateUniqueDeleteRecords(instantTime, 1)
    val inputRecords = new util.ArrayList[HoodieRecord[_]](deletedRecords)
    val lowerOrderingValue = 1L
    inputRecords.addAll(dataGen.generateUniqueDeleteRecords(instantTime, 1, lowerOrderingValue))
    val deleteBatch = recordsToStrings(inputRecords).asScala
    val deleteDf = spark.read.json(spark.sparkContext.parallelize(deleteBatch.toSeq, 1))
    deleteDf.cache()
    val recordKeyToDelete = deleteDf.collectAsList().get(0).getAs("_row_key").asInstanceOf[String]
    deleteDf.write.format("hudi")
      .options(hudiOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val prevDf = mergedDfList.last
    mergedDfList = mergedDfList :+ prevDf.filter(row => row.getAs("_row_key").asInstanceOf[String] != recordKeyToDelete)
    validateDataAndRecordIndices(hudiOpts, spark.read.json(spark.sparkContext.parallelize(recordsToStrings(deletedRecords).asScala.toSeq, 1)))
    deleteDf.unpersist()
  }

  @Test
  def testRecordIndexRebootstrapWithZeroByteBaseFile(): Unit = {
    val insertedRecords = 30
    val localDataGen = new HoodieTestDataGenerator()
    val inserts = localDataGen.generateInserts("001", insertedRecords)
    val insertDf = toDataset(spark, inserts)
    val optionsWithoutRecordIndex = Map(HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false")

    // Create first commit with record_index disabled.
    insertDf.write.format("hudi")
      .options(optionsWithoutRecordIndex)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(insertedRecords, spark.read.format("hudi").load(basePath).count())

    // Corrupt one base parquet file by replacing it with an empty file.
    val corruptedBaseFileName = replaceOneBaseFileWithEmpty(localDataGen.getPartitionPaths.toSeq)

    // Delete metadata table to force rebootstrap.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    HoodieTableMetadataUtil.deleteMetadataTable(metaClient, context, false)
    assertFalse(storage.exists(new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(basePath))),
      "Metadata table should be removed before rebootstrap")

    // Rebootstrap metadata with record_index enabled should still succeed.
    metaClient.reloadActiveTimeline()
    val latestSchema = new TableSchemaResolver(metaClient).getTableSchemaFromLatestCommit(false).get().toString
    val optionsWithRecordIndex = optionsWithoutRecordIndex ++ Map(
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieIndexConfig.INDEX_TYPE.key() -> RECORD_LEVEL_INDEX.name(),
      HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestSchema)
    val writeConfig = getWriteConfig(optionsWithRecordIndex)
    try {
      metadataWriter(writeConfig).getTableMetadata
    } catch {
      case e: HoodieMetadataException =>
        val stackTraceWriter = new StringWriter()
        e.printStackTrace(new PrintWriter(stackTraceWriter))
        val stackTraceText = stackTraceWriter.toString
        assertTrue(stackTraceText.contains(corruptedBaseFileName),
          s"Expected HoodieMetadataException stack trace to contain corrupted file name: $corruptedBaseFileName")
      case t: Throwable =>
        fail(s"Expected HoodieMetadataException but got ${t.getClass.getName}: ${t.getMessage}")
    }
  }

  /**
   * Tests that when a zero-size base file is skipped during MDT bootstrap, a subsequent upsert
   * still succeeds and produces consistent data. Because the skipped file group is absent from MDT,
   * the upsert treats those records as new inserts and writes them to a new file group. The test
   * verifies the final record count and that every RLI entry points to a real, readable file.
   */
  @Test
  def testUpsertAfterSkippingZeroSizeFileOnInitialize(): Unit = {
    // Use a single-partition data generator so all inserts land in one parquet file.
    val singlePartitionDataGen = HoodieTestDataGenerator.createTestGeneratorFirstPartition()
    val singlePartition = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH
    val insertedRecords = 10
    val inserts = singlePartitionDataGen.generateInserts("001", insertedRecords)
    val insertDf = toDataset(spark, inserts)

    val baseOptions = Map(
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "false",
      HoodieCompactionConfig.INLINE_COMPACT.key() -> "false")

    insertDf.write.format("hudi")
      .options(baseOptions)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(insertedRecords, spark.read.format("hudi").load(basePath).count())

    // Confirm there is exactly one parquet base file in the single partition.
    val partitionPath = new StoragePath(basePath, singlePartition)
    val baseFilesBeforeCorruption = storage.listDirectEntries(partitionPath).asScala
      .filter(_.getPath.getName.endsWith(".parquet"))
      .toSeq
    assertEquals(1, baseFilesBeforeCorruption.size, "Expected exactly one parquet file in the single partition")
    val zeroSizeFileId = FSUtils.getFileId(baseFilesBeforeCorruption.head.getPath.getName)

    // Replace the only base file with an empty (zero-size) file.
    replaceOneBaseFileWithEmpty(Seq(singlePartition))

    // Delete MDT to force a full rebootstrap.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    HoodieTableMetadataUtil.deleteMetadataTable(metaClient, context, false)
    assertFalse(storage.exists(new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(basePath))),
      "Metadata table should be absent before rebootstrap")

    // The upsert triggers MDT rebootstrap (since MDT was deleted). Skip config is set so
    // the zero-size file is skipped during bootstrap. Because RLI has no entries for these
    // records (they were in the skipped file), the upsert treats them as new inserts.
    // Use global RLI so that the MDT bootstraps at least minFileGroupCount (10) file groups
    // for record_index even when all data files were skipped as zero-size.
    metaClient.reloadActiveTimeline()
    val latestSchema = new TableSchemaResolver(metaClient).getTableSchemaFromLatestCommit(false).get().toString
    val optionsWithSkip = baseOptions ++ Map(
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
      HoodieIndexConfig.INDEX_TYPE.key() -> "GLOBAL_RECORD_LEVEL_INDEX",
      HoodieMetadataConfig.SKIP_ZERO_SIZE_FILES_ON_INITIALIZE.key() -> "true",
      HoodieWriteConfig.AVRO_SCHEMA_STRING.key() -> latestSchema)

    // Reuse the same 10 records for the upsert. Since RLI has no entries for these record keys
    // (the original file was zero-size and skipped during bootstrap), the upsert treats them as
    // new inserts and writes them to a brand-new file group — NOT the zero-size one.
    val upsertDf = toDataset(spark, inserts)
    // Upsert should succeed — any exception here fails the test.
    upsertDf.write.format("hudi")
      .options(optionsWithSkip)
      .option(DataSourceWriteOptions.OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // Data consistency check: all 10 records must be readable in the new file group.
    // (The zero-size base file contributes 0 readable records; the new file group has 10.)
    val readDf = spark.read.format("hudi").load(basePath)
    assertEquals(insertedRecords, readDf.count(),
      "All records should be readable after upsert into a new file group")

    // RLI consistency check: every live record must be indexed at the location where it actually lives.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val writeConfig = getWriteConfig(optionsWithSkip)
    val postUpsertMetadata = metadataWriter(writeConfig).getTableMetadata.asInstanceOf[HoodieBackedTableMetadata]

    // MDT files partition must not track the zero-size file group.
    val allMdtFiles = getFilesInAllPartitions(postUpsertMetadata)
    assertFalse(allMdtFiles.exists(_.getPath.getName.contains(zeroSizeFileId)),
      s"MDT should not contain the zero-size file group $zeroSizeFileId after bootstrap with skip enabled")

    // Global RLI: look up all record keys without a partition hint.
    val recordKeys = inserts.asScala.map(_.getRecordKey).asJava.stream().collect(Collectors.toList())
    val postUpsertLocations = readRecordIndex(postUpsertMetadata, recordKeys, HOption.empty())
    assertEquals(insertedRecords, postUpsertLocations.size,
      "All upserted records should have an RLI entry after upsert")
    val df = readDf.collect()
    validateDFWithLocations(df, postUpsertLocations, singlePartition)

    // The zero-size file group must not have been selected as the write target — its file ID
    // should not appear in any of the post-upsert RLI locations.
    assertFalse(postUpsertLocations.values.exists(_.getFileId == zeroSizeFileId),
      "The zero-size file group should not have been picked as a write target during upsert")
  }

  private def replaceOneBaseFileWithEmpty(partitionPaths: Seq[String]): String = {
    val candidateBaseFile = partitionPaths.view.flatMap { partition =>
      storage.listDirectEntries(new StoragePath(basePath, partition)).asScala
        .map(_.getPath)
        .find(path => path.getName.endsWith(".parquet"))
    }.headOption.getOrElse(throw new IllegalStateException("No base file found to replace with empty file"))
    assertTrue(storage.deleteFile(candidateBaseFile),
      s"Failed to delete base file $candidateBaseFile")
    assertTrue(storage.createNewFile(candidateBaseFile),
      s"Failed to create empty replacement file $candidateBaseFile")
    candidateBaseFile.getName
  }
}

object TestRecordLevelIndex {

  case class TestPartitionedRecordLevelIndexTestCase(tableType: HoodieTableType, streamingWriteEnabled: Boolean)

  def testArgsForPartitionedRecordLevelIndex: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.COPY_ON_WRITE, streamingWriteEnabled = true)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.COPY_ON_WRITE, streamingWriteEnabled = false)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled = true)),
      Arguments.arguments(TestPartitionedRecordLevelIndexTestCase(HoodieTableType.MERGE_ON_READ, streamingWriteEnabled = false))
    )
  }
}
