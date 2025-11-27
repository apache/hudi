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

import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport, DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, PartitionStatsIndexSupport}
import org.apache.hudi.DataSourceWriteOptions.{BULK_INSERT_OPERATION_OPT_VAL, MOR_TABLE_TYPE_OPT_VAL, PARTITIONPATH_FIELD, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.avro.model.HoodieCleanMetadata
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieFailedWritesCleaningPolicy, HoodieTableType, WriteConcurrencyMode, WriteOperationType}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeAvroMetadataLegacy
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.{HoodieCleanConfig, HoodieClusteringConfig, HoodieCompactionConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieWriteConflictException
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, MetadataPartitionType}
import org.apache.hudi.util.{JavaConversions, JFunction}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BitwiseOr, EqualNullSafe, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, IsNull, LessThanOrEqual, Literal, Not, Or}
import org.apache.spark.sql.hudi.DataSkippingUtils
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, EnumSource, MethodSource}

import java.util.concurrent.Executors
import java.util.stream.Stream

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
 * Test cases on partition stats index with Spark datasource.
 */
@Tag("functional-b")
class TestPartitionStatsIndex extends PartitionStatsIndexTestBase {

  val sqlTempTable = "hudi_tbl"

  /**
   * Test case to validate partition stats for a logical type column
   */
  @Test
  def testPartitionStatsWithLogicalType(): Unit = {
    //val basePath = "/tmp/test/test_table"
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "current_date"
    )

    val records = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    inputDF.write.partitionBy("partition").format("hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key, "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot0 = spark.read.format("org.apache.hudi").options(hudiOpts).load(basePath)
    assertEquals(100, snapshot0.count())

    val updateRecords = recordsToStrings(dataGen.generateUniqueUpdates("002", 50)).asScala.toList
    val updateDF = spark.read.json(spark.sparkContext.parallelize(updateRecords, 2))
    updateDF.write.format("hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val readOpts = hudiOpts ++ Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true"
    )
    val snapshot1 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    val dataFilter = EqualTo(attribute("current_date"), Literal(snapshot1.limit(1).collect().head.getAs("current_date")))
    verifyFilePruning(readOpts, dataFilter, shouldSkipFiles = false)
  }

  /**
   * Test case to do a write (no updates) and validate the partition stats index initialization.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testIndexInitialization(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
  }

  /**
   * Test case to do a write with updates and validate the partition stats index.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testIndexWithUpsert(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndPartitionStats(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  /**
   * Test case to write with updates for non-partitioned table and validate the partition stats index is not created.
   */
  @Test
  def testIndexWithUpsertNonPartitioned(): Unit = {
    val hudiOpts = commonOpts - PARTITIONPATH_FIELD.key + (DataSourceWriteOptions.TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL)
    doWriteAndValidateDataAndPartitionStats(hudiOpts, operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, saveMode = SaveMode.Overwrite, validate = false)
    doWriteAndValidateDataAndPartitionStats(hudiOpts, operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, saveMode = SaveMode.Append, validate = false)
    // there should not be any partition stats
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertFalse(metaClient.getTableConfig.getMetadataPartitions.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath))
  }

  /**
   * Test case to do a write with updates and rollback the last instant and validate the partition stats index.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testIndexUpsertAndRollback(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    // Insert Operation
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    // Upsert Operation
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    // Another Upsert
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    // Rollback
    rollbackLastInstant(hudiOpts)
    // Validate
    validateDataAndPartitionStats()
  }

  /**
   * Test case to do a write with updates and then validate file pruning using partition stats.
   */
  @Test
  def testPartitionStatsIndexFilePruning(): Unit = {
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts + (
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)

    createTempTable(hudiOpts)
    verifyQueryPredicate(hudiOpts)
  }

  /**
   * Test case to do a write with updates and then validate partition stats with multi-writer.
   */
  @ParameterizedTest
  @MethodSource(Array("supplyTestArguments"))
  def testPartitionStatsWithMultiWriter(tableType: HoodieTableType, useUpsert: Boolean): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name,
      HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key -> HoodieFailedWritesCleaningPolicy.LAZY.name,
      HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key -> classOf[InProcessLockProvider].getName,
      HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key -> classOf[SimpleConcurrentFileWritesConflictResolutionStrategy].getName
    )

    val insertRecords: mutable.Buffer[String] =
      recordsToStrings(dataGen.generateInserts(getInstantTime, 20)).asScala
    doWriteAndValidateDataAndPartitionStats(
      insertRecords,
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)

    val write1Records: mutable.Buffer[String] = insertRecords
    val write2Records: mutable.Buffer[String] =
      if (useUpsert) insertRecords else recordsToStrings(dataGen.generateInserts(getInstantTime, 20)).asScala

    val executor = Executors.newFixedThreadPool(2)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutor(executor)
    val function = new (mutable.Buffer[String] => Boolean) {
      def apply(records: mutable.Buffer[String]): Boolean = {
        try {
          doWriteAndValidateDataAndPartitionStats(
            records,
            hudiOpts,
            operation = if (useUpsert) UPSERT_OPERATION_OPT_VAL else BULK_INSERT_OPERATION_OPT_VAL,
            saveMode = SaveMode.Append,
            validate = false)
          true
        } catch {
          case _: HoodieWriteConflictException => false
          case e => throw new Exception("Multi write failed", e)
        }
      }
    }
    val f1 = Future[Boolean] {
      function.apply(write1Records)
    }
    val f2 = Future[Boolean] {
      function.apply(write2Records)
    }

    Await.result(f1, Duration("5 minutes"))
    Await.result(f2, Duration("5 minutes"))
    assertTrue(f1.value.get.get || f2.value.get.get)
    executor.shutdownNow()

    if (useUpsert) {
      pollForTimeline(basePath, storageConf, 2)
      assertTrue(hasPendingCommitsOrRollbacks())
    } else {
      pollForTimeline(basePath, storageConf, 3)
      assertTrue(checkIfCommitsAreConcurrent())
    }
    validateDataAndPartitionStats()
  }

  /**
   * Test case to do a write with updates using partitionBy and validation partition filters pushed down to physical plan.
   */
  @Test
  def testPartitionStatsWithPartitionBy(): Unit = {
    val hudiOpts = commonOpts.-(PARTITIONPATH_FIELD.key)
    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    inputDF.write.partitionBy("partition").format("hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key, "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot0 = spark.read.format("org.apache.hudi").options(hudiOpts).load(basePath).where("partition > '2015/03/16'")
    snapshot0.cache()
    assertTrue(checkPartitionFilters(snapshot0.queryExecution.executedPlan.toString, "partition.* > 2015/03/16"))
    assertEquals(67, snapshot0.count())
  }

  /**
   * Test case to do updates and then validate partition stats with cleaning.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionStatsWithCompactionAndCleaning(tableType: HoodieTableType): Unit = {
    var hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1")
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      hudiOpts = hudiOpts ++ Map(
        HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "2",
        HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key() -> "0"
      )
    }
    // insert followed by two upserts (trigger a compaction so that prev version can be cleaned)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    // Clean Operation
    val lastCleanInstant = getLatestMetaClient(false).getActiveTimeline.getCleanerTimeline.lastInstant()
    assertTrue(lastCleanInstant.isPresent)
    // validation that the compaction commit is present in case of MOR table
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      val lastCompactionInstant = getLatestMetaClient(false).getActiveTimeline.getCommitTimeline.filterCompletedInstants().lastInstant()
      assertTrue(lastCompactionInstant.isPresent)
    }

    // do another upsert and validate the partition stats including file pruning
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    validateDataAndPartitionStats()
    createTempTable(hudiOpts)
    verifyQueryPredicate(hudiOpts)
  }

  /**
   * Test case to do updates and then validate partition stats with clustering.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionStatsWithClustering(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
      HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "2",
      KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key() -> "true")

    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    // validate clustering instant
    val lastClusteringInstant = getLatestClusteringInstant
    assertTrue(getLatestClusteringInstant.isPresent)
    // do two more rounds of upsert to trigger another clustering
    doWriteAndValidateDataAndPartitionStats(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndPartitionStats(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    assertTrue(getLatestClusteringInstant.get().requestedTime.compareTo(lastClusteringInstant.get().requestedTime) > 0)
    assertEquals(getLatestClusteringInstant, metaClient.getActiveTimeline.lastInstant())
    // We are validating rollback of a DT clustering instant here
    rollbackLastInstant(hudiOpts)

    validateDataAndPartitionStats()
    createTempTable(hudiOpts)
    verifyQueryPredicate(hudiOpts)
  }

  /**
   * 1. Enable column_stats, partition_stats and record_index (files/RLI already enabled by default).
   * 2. Do two inserts and validate index initialization.
   * 3. Do a savepoint on the second commits.
   * 4. Add three more commits to trigger clean, which cleans the files from the first commit.
   * 5. Restore, and validate that partition_stats is deleted, but column_stats partition exists.
   * 6. Validate that column_stats does not contain records with file names from first commit.
   */
  @Test
  def testPartitionStatsWithRestore(): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true")

    // First ingest.
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    val firstCompletedInstant = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant()
    // Second ingest.
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val secondCompletedInstant = metaClient.getActiveTimeline
      .getCommitsTimeline.filterCompletedInstants().getInstants().get(1)
    // Validate index partitions are present.
    val initialMetadataPartitions = metaClient.getTableConfig.getMetadataPartitions
    assertTrue(initialMetadataPartitions.contains(MetadataPartitionType.FILES.getPartitionPath))
    assertTrue(initialMetadataPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    assertTrue(initialMetadataPartitions.contains(MetadataPartitionType.COLUMN_STATS.getPartitionPath))
    assertTrue(initialMetadataPartitions.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath))
    // Do a savepoint on the second commit.
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), getWriteConfig(hudiOpts))
    writeClient.savepoint(secondCompletedInstant.requestedTime, "testUser", "savepoint to second commit")
    writeClient.close()
    val savepointTimestamp = metaClient.reloadActiveTimeline()
      .getSavePointTimeline.filterCompletedInstants().lastInstant().get().requestedTime
    assertEquals(secondCompletedInstant.requestedTime, savepointTimestamp)

    // Add more ingests and trigger a clean to remove files from first ingestion.
    val writeOpt = hudiOpts ++ Map(
      HoodieCleanConfig.AUTO_CLEAN.key -> "true",
      HoodieCleanConfig.CLEAN_MAX_COMMITS.key -> "1",
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key -> "2")
    // Do three more ingestion to trigger clean operation.
    for (_ <- 0 until 3) {
      doWriteAndValidateDataAndPartitionStats(
        writeOpt,
        operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append)
    }
    metaClient = HoodieTableMetaClient.reload(metaClient)
    // Clean commit should be triggered.
    val cleanInstantOpt = metaClient.getActiveTimeline.getCleanerTimeline.lastInstant()
    assertTrue(cleanInstantOpt.isPresent)
    val cleanMetadataBytes = metaClient.getActiveTimeline.getInstantDetails(cleanInstantOpt.get)
    val cleanMetadata = deserializeAvroMetadataLegacy(cleanMetadataBytes.get(), classOf[HoodieCleanMetadata])
    // This clean operation deletes 6 files created by the first commit.
    assertTrue(cleanMetadata.getTotalFilesDeleted > 0)
    // Restore to savepoint
    writeClient.restoreToSavepoint(savepointTimestamp)
    // Verify restore completed
    assertTrue(metaClient.reloadActiveTimeline().getRestoreTimeline.lastInstant().isPresent)
    // Verify partition stats is delete, but other index are not.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val metadataPartitions = metaClient.getTableConfig.getMetadataPartitions
    assertTrue(metadataPartitions.contains(MetadataPartitionType.FILES.getPartitionPath))
    assertTrue(metadataPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    assertFalse(metadataPartitions.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath))
    assertTrue(metadataPartitions.contains(MetadataPartitionType.COLUMN_STATS.getPartitionPath))
    // Do another upsert and validate the partition stats
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)
    val latestDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    val partitionStatsIndex = new PartitionStatsIndexSupport(
      spark,
      latestDf.schema,
      HoodieSchema.fromAvroSchema(AvroConversionUtils.convertStructTypeToAvroSchema(latestDf.schema, "record", "")),
      HoodieMetadataConfig.newBuilder()
        .enable(true)
        .build(),
      metaClient)
    val partitionStats = partitionStatsIndex.loadColumnStatsIndexRecords(
      targetColumnsToIndex,
      shouldReadInMemory = true)
      .collectAsList()
    assertTrue(partitionStats.size() > 0)
    // Assert column stats after restore.
    val hoodieSchema = HoodieSchema.fromAvroSchema(AvroConversionUtils.convertStructTypeToAvroSchema(latestDf.schema, "record", ""))
    val columnStatsIndex = new ColumnStatsIndexSupport(
      spark, latestDf.schema,
      hoodieSchema,
      HoodieMetadataConfig.newBuilder()
        .enable(true)
        .build(),
      metaClient)
    val columnStats = columnStatsIndex
      .loadColumnStatsIndexRecords(targetColumnsToIndex, shouldReadInMemory = true)
      .collectAsList()
    // All files from first commit have been removed.
    for (stats <- columnStats.asScala) {
      assertFalse(stats.getFileName.contains(firstCompletedInstant.get().requestedTime()))
    }
  }

  /**
   * Test case to do updates and then validate partition stats with MDT compaction.
   * Any one table type is enough to test this as we are validating the metadata table.
   */
  @Test
  def testPartitionStatsWithMDTCompaction(): Unit = {
    val hudiOpts = commonOpts ++ Map(
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key() -> "2"
    )
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
    // validate MDT compaction instant
    val metadataTableFSView = getHoodieTable(metaClient, getWriteConfig(hudiOpts)).getMetadataTable
      .asInstanceOf[HoodieBackedTableMetadata].getMetadataFileSystemView
    try {
      val compactionTimeline = metadataTableFSView.getVisibleCommitsAndCompactionTimeline.filterCompletedAndCompactionInstants()
      val lastCompactionInstant = compactionTimeline
        .filter(JavaConversions.getPredicate((instant: HoodieInstant) =>
          compactionTimeline.readCommitMetadata(instant)
            .getOperationType == WriteOperationType.COMPACT))
        .lastInstant()
      val compactionBaseFile = metadataTableFSView.getAllBaseFiles(MetadataPartitionType.PARTITION_STATS.getPartitionPath)
        .filter(JavaConversions.getPredicate((f: HoodieBaseFile) => f.getCommitTime.equals(lastCompactionInstant.get().requestedTime)))
        .findAny()
      assertTrue(compactionBaseFile.isPresent)
    } finally {
      metadataTableFSView.close()
    }
  }

  /**
   * 1. Create a table and enable column_stats, partition_stats.
   * 2. Do an insert and validate the partition stats index initialization.
   * 3. Form a filter expression containing isNull on an indexed column.
   * 4. Validate that the partition stats lookup is skipped for the filter expression.
   */
  @Test
  def testPartitionStatsLookupSkippedForCertainFilters(): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieMetadataConfig.ENABLE.key() -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key() -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    // Filter expression with isNull on an indexed column
    val dataFilter = IsNull(attribute("_row_key"))
    val indexedCols = Seq("_row_key")
    // Because there is a null filter, the partition stats lookup should be skipped.
    validateContainsNullAndValueFilters(dataFilter, indexedCols, expectedValue = true)
    verifyFilePruning(hudiOpts, dataFilter, shouldSkipFiles = false)
  }

  @Test
  def testTranslateIntoColumnStatsIndexFilterExpr(): Unit = {
    var dataFilter: Expression = EqualTo(attribute("c1"), literal("619sdc"))
    validateContainsNullAndValueFilters(dataFilter, Seq("c1"), expectedValue = false)

    // c1 = 619sdc and c2 = 100, where both c1 and c2 are indexed.
    val dataFilter1 = And(dataFilter, EqualTo(attribute("c2"), literal("100")))
    validateContainsNullAndValueFilters(dataFilter1, Seq("c1","c2"), expectedValue = false)

    // add contains null
    val dataFilter2 = And(dataFilter, IsNull(attribute("c3")))
    validateContainsNullAndValueFilters(dataFilter2, Seq("c1","c2","c3"), expectedValue = true)

    // checks for not null
    val dataFilter3 = And(dataFilter, IsNotNull(attribute("c4")))
    validateContainsNullAndValueFilters(dataFilter3, Seq("c1","c2","c3","c4"), expectedValue = true)

    // nested And and Or case
    val dataFilter4 = And(
      Or(
        EqualTo(attribute("c1"), literal("619sdc")),
        And(EqualTo(attribute("c2"), literal("100")), EqualTo(attribute("c3"), literal("200")))
      ),
      EqualTo(attribute("c4"), literal("300"))
    )
    validateContainsNullAndValueFilters(dataFilter4, Seq("c1","c2","c3","c4"), expectedValue = false)

    // embed a null filter and validate
    val dataFilter5 = Or(dataFilter4, And(
      Or(
        EqualTo(attribute("c1"), literal("619sdc")),
        And(EqualTo(attribute("c2"), literal("100")), EqualTo(attribute("c3"), literal("200")))
      ),
      IsNotNull(attribute("c4"))
    ))
    validateContainsNullAndValueFilters(dataFilter5, Seq("c1","c2","c3","c4"), expectedValue = true)

    // unsupported filter type
    val dataFilter6 = BitwiseOr(
      EqualTo(attribute("c1"), literal("619sdc")),
      EqualTo(attribute("c2"), literal("100"))
    )
    validateContainsNullAndValueFilters(dataFilter6, Seq("c1","c2","c3","c4"), expectedValue = false)

    // too many filters, out of which only half are indexed.
    val largeFilter = (1 to 100).map(i => EqualTo(attribute(s"c$i"), literal("value"))).reduce(And)
    val indexedColumns = (1 to 50).map(i => s"c$i")
    validateContainsNullAndValueFilters(largeFilter, indexedColumns, expectedValue = false)

    // add just 1 null check
    val largeFilter1 = And(largeFilter, IsNull(attribute("c10")))
    validateContainsNullAndValueFilters(largeFilter1, indexedColumns, expectedValue = true)

    // Not(IsNull(...)) → IsNotNull(...)
    dataFilter = Not(IsNull(attribute("c1")))
    // Because pushDownNot should translate Not(IsNull(...)) -> IsNotNull(...)
    // The result should be true if c1 is in indexedCols
    validateContainsNullAndValueFilters(dataFilter, Seq("c1"), expectedValue = true)
    // If c1 is not indexed, result should be false
    validateContainsNullAndValueFilters(dataFilter, Seq("c2"), expectedValue = false)

    // Not(IsNotNull(...)) → IsNull(...)
    dataFilter = Not(IsNotNull(attribute("c2")))
    // pushDownNot translates Not(IsNotNull(...)) -> IsNull(...)
    validateContainsNullAndValueFilters(dataFilter, Seq("c2"), expectedValue = true)

    // Not(EqualNullSafe(attr, Literal(null, ...)))
    dataFilter = Not(EqualNullSafe(attribute("c3"), Literal("value")))
    // This doesn’t have a direct pushDownNot pattern unless you explicitly handle it,
    // but verifying that it doesn’t break your logic is useful.
    validateContainsNullAndValueFilters(dataFilter, Seq("c3"), expectedValue = false)

    // Double-Negation: Not(Not(IsNull(...)))
    dataFilter = Not(Not(IsNull(attribute("c1"))))
    validateContainsNullAndValueFilters(dataFilter, Seq("c1"), expectedValue = true)

    // Complex Nested: Not(And(IsNull(c1), Or(IsNotNull(c2), EqualNullSafe(c3, null))))
    dataFilter = Not(
      And(
        IsNull(attribute("c1")),
        Or(
          IsNotNull(attribute("c2")),
          EqualNullSafe(attribute("c3"), literal("value"))
        )
      )
    )

    // If columns c1, c2, c3 are all indexed:
    //   - We have IsNull/IsNotNull/EqualNullSafe references => returns true
    validateContainsNullAndValueFilters(dataFilter, Seq("c1", "c2", "c3"), expectedValue = true)

    // If none are indexed: returns false
    validateContainsNullAndValueFilters(dataFilter, Seq.empty, expectedValue = false)
  }

  def validateContainsNullAndValueFilters(dataFilter: Expression, indexedCols: Seq[String],
                                          expectedValue: Boolean): Unit = {
    assertEquals(expectedValue, DataSkippingUtils.containsNullOrValueCountBasedFilters(dataFilter, indexedCols))
  }

  private def literal(value: String): Literal = {
    Literal.create(value)
  }

  def generateColStatsExprForGreaterthanOrEquals(colName: String, colValue: String): Expression = {
    val expectedExpr: Expression = GreaterThanOrEqual(UnresolvedAttribute(colName + "_maxValue"), literal(colValue))
    And(LessThanOrEqual(UnresolvedAttribute(colName + "_minValue"), literal(colValue)), expectedExpr)
  }

  def verifyQueryPredicate(hudiOpts: Map[String, String]): Unit = {
    val candidateRow = mergedDfList.last.groupBy("_row_key").count().limit(1).collect().head
    val rowKey = candidateRow.getAs[String]("_row_key")
    val count = candidateRow.getLong(1)
    val dataFilter = EqualTo(attribute("_row_key"), Literal(rowKey))
    assertEquals(count, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    verifyFilePruning(hudiOpts, dataFilter)

    // validate that if filter contains null filters, there is no data skipping
    val dataFilter1 = IsNotNull(attribute("_row_key"))
    verifyFilePruning(hudiOpts, dataFilter1, shouldSkipFiles = false)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, nullable = true)()
  }

  private def createTempTable(hudiOpts: Map[String, String]): Unit = {
    val readDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    readDf.createOrReplaceTempView(sqlTempTable)
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression, shouldSkipFiles: Boolean = true): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    if (shouldSkipFiles) {
      assertTrue(filteredFilesCount <= getLatestDataFilesCount(opts))
    } else {
      assertTrue(filteredFilesCount == getLatestDataFilesCount(opts))
    }

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filteredFilesCount <= filesCountWithNoSkipping)
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    getTableFileSystemView(opts).getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().requestedTime)
      .values()
      .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
        (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
          slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
            + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    totalLatestDataFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieTableFileSystemView = {
    new HoodieTableFileSystemView(metadataWriter(getWriteConfig(opts)).getTableMetadata, metaClient, metaClient.getActiveTimeline)
  }
}

object TestPartitionStatsIndex {
  def supplyTestArguments(): Stream[Arguments] = {
    List(
      Arguments.of(HoodieTableType.MERGE_ON_READ, java.lang.Boolean.TRUE),
      Arguments.of(HoodieTableType.MERGE_ON_READ, java.lang.Boolean.FALSE),
      Arguments.of(HoodieTableType.COPY_ON_WRITE, java.lang.Boolean.TRUE),
      Arguments.of(HoodieTableType.COPY_ON_WRITE, java.lang.Boolean.FALSE)
    ).asJava.stream()
  }
}
