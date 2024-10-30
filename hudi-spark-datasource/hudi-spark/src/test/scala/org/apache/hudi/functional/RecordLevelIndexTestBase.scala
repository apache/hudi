/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieInstantTimeGenerator, MetadataConversionUtils}
import org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_FACTORY
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JavaConversions

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, not}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api._

import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}
import scala.util.Using

class RecordLevelIndexTestBase extends HoodieSparkClientTestBase {
  var spark: SparkSession = _
  var instantTime: AtomicInteger = _
  val metadataOpts = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    RECORDKEY_FIELD.key -> "_row_key",
    PARTITIONPATH_FIELD.key -> "partition",
    PRECOMBINE_FIELD.key -> "timestamp",
    HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
  ) ++ metadataOpts

  val secondaryIndexOpts = Map(
    HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key -> "true"
  )

  val commonOptsWithSecondaryIndex = commonOpts ++ secondaryIndexOpts ++ metadataOpts

  val commonOptsNewTableSITest = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "trips_table",
    RECORDKEY_FIELD.key -> "uuid",
    SECONDARYKEY_COLUMN_NAME.key -> "city",
    PARTITIONPATH_FIELD.key -> "state",
    PRECOMBINE_FIELD.key -> "ts",
    HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
  ) ++ metadataOpts

  val commonOptsWithSecondaryIndexSITest = commonOptsNewTableSITest ++ secondaryIndexOpts
  var mergedDfList: List[DataFrame] = List.empty
  var metaClientReloaded = false

  @BeforeEach
  override def setUp() {
    initPath()
    initQueryIndexConf()
    initSparkContexts()
    initHoodieStorage()
    initTestDataGenerator()

    setTableName("hoodie_test")
    initMetaClient()
    metaClientReloaded = false

    instantTime = new AtomicInteger(1)

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

  protected def getLatestMetaClient(enforce: Boolean): HoodieTableMetaClient = {
    val lastInsant = HoodieInstantTimeGenerator.getLastInstantTime
    if (enforce || metaClient.getActiveTimeline.lastInstant().get().getRequestTime.compareTo(lastInsant) < 0) {
      println("Reloaded timeline")
      metaClient.reloadActiveTimeline()
      metaClient
    }
    metaClient
  }

  protected def rollbackLastInstant(hudiOpts: Map[String, String]): HoodieInstant = {
    val lastInstant = getLatestMetaClient(false).getActiveTimeline
      .filter(JavaConversions.getPredicate(instant => instant.getAction != ActionType.rollback.name()))
      .lastInstant().get()
    if (getLatestCompactionInstant() != getLatestMetaClient(false).getActiveTimeline.lastInstant()
      && lastInstant.getAction != ActionType.replacecommit.name()
      && lastInstant.getAction != ActionType.clean.name()) {
      mergedDfList = mergedDfList.take(mergedDfList.size - 1)
    }
    val writeConfig = getWriteConfig(hudiOpts)
    new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
      .rollback(lastInstant.getRequestTime)

    if (lastInstant.getAction != ActionType.clean.name()) {
      assertEquals(ActionType.rollback.name(), getLatestMetaClient(true).getActiveTimeline.lastInstant().get().getAction)
    }
    lastInstant
  }

  protected def executeFunctionNTimes[T](function0: Function0[T], n: Int): Unit = {
    for (i <- 1 to n) {
      function0.apply()
    }
  }

  protected def deleteLastCompletedCommitFromDataAndMetadataTimeline(hudiOpts: Map[String, String]): Unit = {
    val writeConfig = getWriteConfig(hudiOpts)
    val lastInstant = getHoodieTable(getLatestMetaClient(false), writeConfig).getCompletedCommitsTimeline.lastInstant().get()
    val metadataTableMetaClient = getHoodieTable(metaClient, writeConfig).getMetadataTable.asInstanceOf[HoodieBackedTableMetadata].getMetadataMetaClient
    val metadataTableLastInstant = metadataTableMetaClient.getCommitsTimeline.lastInstant().get()
    assertTrue(storage.deleteFile(new StoragePath(metaClient.getMetaPath, INSTANT_FILE_NAME_FACTORY.getFileName(lastInstant))))
    assertTrue(storage.deleteFile(new StoragePath(
      metadataTableMetaClient.getMetaPath, INSTANT_FILE_NAME_FACTORY.getFileName(metadataTableLastInstant))))
    mergedDfList = mergedDfList.take(mergedDfList.size - 1)
  }

  protected def deleteLastCompletedCommitFromTimeline(hudiOpts: Map[String, String]): Unit = {
    val writeConfig = getWriteConfig(hudiOpts)
    val lastInstant = getHoodieTable(getLatestMetaClient(false), writeConfig).getCompletedCommitsTimeline.lastInstant().get()
    assertTrue(storage.deleteFile(new StoragePath(metaClient.getMetaPath, INSTANT_FILE_NAME_FACTORY.getFileName(lastInstant))))
    mergedDfList = mergedDfList.take(mergedDfList.size - 1)
  }

  protected def getMetadataMetaClient(hudiOpts: Map[String, String]): HoodieTableMetaClient = {
    getHoodieTable(metaClient, getWriteConfig(hudiOpts)).getMetadataTable.asInstanceOf[HoodieBackedTableMetadata].getMetadataMetaClient
  }

  protected def getLatestCompactionInstant(): org.apache.hudi.common.util.Option[HoodieInstant] = {
    getLatestMetaClient(false).getActiveTimeline
      .filter(JavaConversions.getPredicate(s => Option(
        try {
          val commitMetadata = MetadataConversionUtils.getHoodieCommitMetadata(metaClient, s)
            .orElse(new HoodieCommitMetadata())
          commitMetadata
        } catch {
          case _: Exception => new HoodieCommitMetadata()
        })
        .map(c => c.getOperationType == WriteOperationType.COMPACT)
        .get))
      .lastInstant()
  }

  protected def getLatestClusteringInstant(): org.apache.hudi.common.util.Option[HoodieInstant] = {
    getLatestMetaClient(false).getActiveTimeline.getCompletedReplaceTimeline.lastInstant()
  }

  protected def doWriteAndValidateDataAndRecordIndex(hudiOpts: Map[String, String],
                                                   operation: String,
                                                   saveMode: SaveMode,
                                                   validate: Boolean = true,
                                                   numUpdates: Int = 1,
                                                   onlyUpdates: Boolean = false): DataFrame = {
    var latestBatch: mutable.Buffer[String] = null
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      val instantTime = getInstantTime()
      val records = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, numUpdates))
      if (!onlyUpdates) {
        records.addAll(recordsToStrings(dataGen.generateInserts(instantTime, 1)))
      }
      latestBatch = records.asScala
    } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
      latestBatch = recordsToStrings(dataGen.generateInsertsForPartition(
        getInstantTime(), 5, dataGen.getPartitionPaths.last)).asScala
    } else {
      latestBatch = recordsToStrings(dataGen.generateInserts(getInstantTime(), 5)).asScala
    }
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch.toSeq, 2))
    latestBatchDf.cache()
    latestBatchDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)
    val deletedDf = calculateMergedDf(latestBatchDf, operation)
    deletedDf.cache()
    if (!metaClientReloaded) {
      // initialization of meta client is required again after writing data so that
      // latest table configs are picked up
      metaClient = HoodieTableMetaClient.reload(metaClient)
      metaClientReloaded = true
    }

    if (validate) {
      validateDataAndRecordIndices(hudiOpts, deletedDf)
    }

    deletedDf.unpersist()
    latestBatchDf
  }

  protected def calculateMergedDf(latestBatchDf: DataFrame, operation: String): DataFrame = {
    calculateMergedDf(latestBatchDf, operation, false)
  }

  /**
   * @return [[DataFrame]] that should not exist as of the latest instant; used for non-existence validation.
   */
  protected def calculateMergedDf(latestBatchDf: DataFrame, operation: String, globalIndexEnableUpdatePartitions: Boolean): DataFrame = {
    val prevDfOpt = mergedDfList.lastOption
    if (prevDfOpt.isEmpty) {
      mergedDfList = mergedDfList :+ latestBatchDf
      sparkSession.emptyDataFrame
    } else {
      if (operation == INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL) {
        mergedDfList = mergedDfList :+ latestBatchDf
        // after insert_overwrite_table, all previous snapshot's records should be deleted from RLI
        prevDfOpt.get
      } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
        val overwrittenPartitions = latestBatchDf.select("partition")
          .collectAsList().stream().map[String](JavaConversions.getFunction[Row, String](r => r.getString(0))).collect(Collectors.toList[String])
        val prevDf = prevDfOpt.get
        val latestSnapshot = prevDf
          .filter(not(col("partition").isInCollection(overwrittenPartitions)))
          .union(latestBatchDf)
        mergedDfList = mergedDfList :+ latestSnapshot

        // after insert_overwrite (partition), all records in the overwritten partitions should be deleted from RLI
        prevDf.filter(col("partition").isInCollection(overwrittenPartitions))
      } else {
        val prevDf = prevDfOpt.get
        if (globalIndexEnableUpdatePartitions) {
          val prevDfOld = prevDf.join(latestBatchDf, prevDf("_row_key") === latestBatchDf("_row_key"), "leftanti")
          val latestSnapshot = prevDfOld.union(latestBatchDf)
          mergedDfList = mergedDfList :+ latestSnapshot
        } else {
          val prevDfOld = prevDf.join(latestBatchDf, prevDf("_row_key") === latestBatchDf("_row_key")
            && prevDf("partition") === latestBatchDf("partition"), "leftanti")
          val latestSnapshot = prevDfOld.union(latestBatchDf)
          mergedDfList = mergedDfList :+ latestSnapshot
        }
        sparkSession.emptyDataFrame
      }
    }
  }

  private def getInstantTime(): String = {
    String.format("%03d", new Integer(instantTime.incrementAndGet()))
  }

  protected def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  protected def getFileGroupCountForRecordIndex(writeConfig: HoodieWriteConfig): Long = {
    Using(getHoodieTable(metaClient, writeConfig).getMetadataTable.asInstanceOf[HoodieBackedTableMetadata]) { metadataTable =>
      metadataTable.getMetadataFileSystemView.getAllFileGroups(MetadataPartitionType.RECORD_INDEX.getPartitionPath).count
    }.get
  }

  protected def validateDataAndRecordIndices(hudiOpts: Map[String, String],
                                           deletedDf: DataFrame = sparkSession.emptyDataFrame): Unit = {
    val writeConfig = getWriteConfig(hudiOpts)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val readDf = spark.read.format("hudi").load(basePath)
    val rowArr = readDf.collect()
    val recordIndexMap = metadata.readRecordIndex(
      JavaConverters.seqAsJavaListConverter(rowArr.map(row => row.getAs("_hoodie_record_key").toString).toList).asJava)

    assertTrue(rowArr.length > 0)
    for (row <- rowArr) {
      val recordKey: String = row.getAs("_hoodie_record_key")
      val partitionPath: String = row.getAs("_hoodie_partition_path")
      val fileName: String = row.getAs("_hoodie_file_name")
      val recordLocations = recordIndexMap.get(recordKey)
      assertFalse(recordLocations.isEmpty)
      // assuming no duplicate keys for now
      val recordLocation = recordLocations.get(0)
      assertEquals(partitionPath, recordLocation.getPartitionPath)
      assertTrue(fileName.startsWith(recordLocation.getFileId), fileName + " should start with " + recordLocation.getFileId)
    }

    val deletedRows = deletedDf.collect()
    val recordIndexMapForDeletedRows = metadata.readRecordIndex(
      JavaConverters.seqAsJavaListConverter(deletedRows.map(row => row.getAs("_row_key").toString).toList).asJava)
    assertEquals(0, recordIndexMapForDeletedRows.size(), "deleted records should not present in RLI")

    assertEquals(rowArr.length, recordIndexMap.keySet.size)
    val estimatedFileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(MetadataPartitionType.RECORD_INDEX, rowArr.length, 48,
      writeConfig.getRecordIndexMinFileGroupCount, writeConfig.getRecordIndexMaxFileGroupCount,
      writeConfig.getRecordIndexGrowthFactor, writeConfig.getRecordIndexMaxFileGroupSizeBytes)
    assertEquals(estimatedFileGroupCount, getFileGroupCountForRecordIndex(writeConfig))
    val prevDf = mergedDfList.last.drop("tip_history")
    val nonMatchingRecords = readDf.drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
      "_hoodie_partition_path", "_hoodie_file_name", "tip_history")
      .join(prevDf, prevDf.columns, "leftanti")
    assertEquals(0, nonMatchingRecords.count())
    assertEquals(readDf.count(), prevDf.count())
  }
}
