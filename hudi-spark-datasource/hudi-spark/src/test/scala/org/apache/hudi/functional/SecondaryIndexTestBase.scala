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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.{ActionType, FileSlice, HoodieCommitMetadata, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieInstantTimeGenerator, MetadataConversionUtils}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.{JFunction, JavaConversions}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.functions.{col, not}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}

class SecondaryIndexTestBase extends HoodieSparkClientTestBase {

  var spark: SparkSession = _
  var instantTime: AtomicInteger = _
  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    RECORDKEY_FIELD.key -> "record_key_col",
    PARTITIONPATH_FIELD.key -> "partition_key_col",
    HIVE_STYLE_PARTITIONING.key -> "true",
    PRECOMBINE_FIELD.key -> "ts"
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty
  var metaClientReloaded = false

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initQueryIndexConf()
    initSparkContexts()
    initHoodieStorage()
    initTestDataGenerator()
    setTableName("hoodie_test")
    spark = sqlContext.sparkSession
    instantTime = new AtomicInteger(1)
    metaClientReloaded = false
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupResources()
  }

  def verifyQueryPredicate(hudiOpts: Map[String, String], columnName: String): Unit = {
    mergedDfList = spark.read.format("hudi").options(hudiOpts).load(basePath).repartition(1).cache() :: mergedDfList
    val secondaryKey = mergedDfList.last.limit(1).collect().map(row => row.getAs(columnName).toString)
    val dataFilter = EqualTo(attribute(columnName), Literal(secondaryKey(0)))
    verifyFilePruning(hudiOpts, dataFilter)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, nullable = true)()
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    val latestDataFilesCount = getLatestDataFilesCount(opts)
    assertTrue(filteredFilesCount > 0 && filteredFilesCount < latestDataFilesCount)

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filesCountWithNoSkipping == latestDataFilesCount)
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    val fsView: HoodieMetadataFileSystemView = getTableFileSystemView(opts)
    try {
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
        .values()
        .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
          (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
            slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
              + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    } finally {
      fsView.close()
    }
    totalLatestDataFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieMetadataFileSystemView = {
    new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline, metadataWriter(getWriteConfig(opts)).getTableMetadata)
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  protected def doWriteAndValidateSecondaryIndex(hudiOpts: Map[String, String],
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
      // TODO: pass the partition name as an argument
      validateDataAndSecondaryIndex(hudiOpts, deletedDf, "secondary_index_idx_not_record_key_col")
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

  protected def validateDataAndSecondaryIndex(hudiOpts: Map[String, String],
                                              deletedDf: DataFrame = sparkSession.emptyDataFrame,
                                              partitionName: String): Unit = {
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
    val recordIndexMapForDeletedRows = metadata.readSecondaryIndex(
      JavaConverters.seqAsJavaListConverter(deletedRows.map(row => row.getAs("_row_key").toString).toList).asJava, partitionName)
    assertEquals(0, recordIndexMapForDeletedRows.size(), "deleted records should not present in RLI")
    assertEquals(rowArr.length, recordIndexMap.keySet.size)
    val prevDf = mergedDfList.last.drop("tip_history")
    val nonMatchingRecords = readDf.drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
        "_hoodie_partition_path", "_hoodie_file_name", "tip_history")
      .join(prevDf, prevDf.columns, "leftanti")
    assertEquals(0, nonMatchingRecords.count())
    assertEquals(readDf.count(), prevDf.count())
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
      .rollback(lastInstant.getTimestamp)

    if (lastInstant.getAction != ActionType.clean.name()) {
      assertEquals(ActionType.rollback.name(), getLatestMetaClient(true).getActiveTimeline.lastInstant().get().getAction)
    }
    lastInstant
  }

  protected def getLatestMetaClient(enforce: Boolean): HoodieTableMetaClient = {
    val lastInsant = HoodieInstantTimeGenerator.getLastInstantTime
    if (enforce || metaClient.getActiveTimeline.lastInstant().get().getTimestamp.compareTo(lastInsant) < 0) {
      println("Reloaded timeline")
      metaClient.reloadActiveTimeline()
      metaClient
    }
    metaClient
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

  private def getInstantTime(): String = {
    String.format("%03d", new Integer(instantTime.incrementAndGet()))
  }

}
