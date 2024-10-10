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

import org.apache.hudi.DataSourceWriteOptions.PARTITIONPATH_FIELD
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieCommitMetadata, HoodieFailedWritesCleaningPolicy, HoodieTableType, WriteConcurrencyMode, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieCleanConfig, HoodieClusteringConfig, HoodieCompactionConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieWriteConflictException
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieMetadataFileSystemView, MetadataPartitionType}
import org.apache.hudi.util.{JFunction, JavaConversions}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Test cases on partition stats index with Spark datasource.
 */
@Tag("functional")
class TestPartitionStatsIndex extends PartitionStatsIndexTestBase {

  val sqlTempTable = "hudi_tbl"

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
   * Test case to do a write with updates for non-partitioned table and validate the partition stats index.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testIndexWithUpsertNonPartitioned(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts - PARTITIONPATH_FIELD.key + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name())
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)
    doWriteAndValidateDataAndPartitionStats(
      hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
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
  @EnumSource(classOf[HoodieTableType])
  def testPartitionStatsWithMultiWriter(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key() -> WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name,
      HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() -> HoodieFailedWritesCleaningPolicy.LAZY.name,
      HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key() -> classOf[InProcessLockProvider].getName,
      HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key() -> classOf[SimpleConcurrentFileWritesConflictResolutionStrategy].getName
    )

    doWriteAndValidateDataAndPartitionStats(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)

    val executor = Executors.newFixedThreadPool(2)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutor(executor)
    val function = new Function0[Boolean] {
      override def apply(): Boolean = {
        try {
          doWriteAndValidateDataAndPartitionStats(hudiOpts,
            operation = DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
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
      function.apply()
    }
    val f2 = Future[Boolean] {
      function.apply()
    }

    Await.result(f1, Duration("5 minutes"))
    Await.result(f2, Duration("5 minutes"))

    assertTrue(f1.value.get.get || f2.value.get.get)
    executor.shutdownNow()
    Thread.sleep(2000L)
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
    assertTrue(getLatestClusteringInstant.get().getTimestamp.compareTo(lastClusteringInstant.get().getTimestamp) > 0)
    assertEquals(getLatestClusteringInstant, metaClient.getActiveTimeline.lastInstant())
    // We are validating rollback of a DT clustering instant here
    rollbackLastInstant(hudiOpts)

    validateDataAndPartitionStats()
    createTempTable(hudiOpts)
    verifyQueryPredicate(hudiOpts)
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
          HoodieCommitMetadata.fromBytes(compactionTimeline.getInstantDetails(instant).get, classOf[HoodieCommitMetadata])
            .getOperationType == WriteOperationType.COMPACT))
        .lastInstant()
      val compactionBaseFile = metadataTableFSView.getAllBaseFiles(MetadataPartitionType.PARTITION_STATS.getPartitionPath)
        .filter(JavaConversions.getPredicate((f: HoodieBaseFile) => f.getCommitTime.equals(lastCompactionInstant.get().getTimestamp)))
        .findAny()
      assertTrue(compactionBaseFile.isPresent)
    } finally {
      metadataTableFSView.close()
    }
  }

  def verifyQueryPredicate(hudiOpts: Map[String, String]): Unit = {
    val reckey = mergedDfList.last.limit(1).collect().map(row => row.getAs("_row_key").toString)
    val dataFilter = EqualTo(attribute("_row_key"), Literal(reckey(0)))
    assertEquals(2, spark.sql("select * from " + sqlTempTable + " where " + dataFilter.sql).count())
    verifyFilePruning(hudiOpts, dataFilter)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, true)()
  }

  private def createTempTable(hudiOpts: Map[String, String]): Unit = {
    val readDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    readDf.createOrReplaceTempView(sqlTempTable)
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    assertTrue(filteredFilesCount <= getLatestDataFilesCount(opts))

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filteredFilesCount <= filesCountWithNoSkipping)
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    getTableFileSystemView(opts).getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
      .values()
      .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
        (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
          slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
            + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    totalLatestDataFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieMetadataFileSystemView = {
    new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline, metadataWriter(getWriteConfig(opts)).getTableMetadata)
  }
}
