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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.avro.HoodieAvroWrapperUtils.unwrapAvroValueWrapper
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieColumnRangeMetadata
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.view.FileSystemViewManager
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.util.JavaScalaConverters.convertJavaListToScalaSeq

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, explode}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util
import java.util.Collections
import java.util.stream.Collectors

import scala.collection.JavaConverters._

@Tag("functional")
class TestMetadataTableWithSparkDataSource extends SparkClientFunctionalTestHarness {

  val hudi = "org.apache.hudi"
  var nonPartitionedCommonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  var partitionedCommonOpts =  nonPartitionedCommonOpts ++ Map(
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition")

  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @CsvSource(Array("1,true", "1,false")) // TODO: fix for higher compactNumDeltaCommits - HUDI-6340
  def testReadability(compactNumDeltaCommits: Int, testPartitioned: Boolean): Unit = {
    val dataGen = new HoodieTestDataGenerator()

    val metadataOpts: Map[String, String] = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = if (testPartitioned) {
      partitionedCommonOpts
    } else {
      nonPartitionedCommonOpts
    }

    val combinedOpts: Map[String, String] = commonOpts ++ metadataOpts ++
      Map(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> compactNumDeltaCommits.toString)

    // Insert records
    val newRecords = dataGen.generateInserts("001", 100)
    val newRecordsDF = parseRecords(recordsToStrings(newRecords).asScala.toSeq)

    newRecordsDF.write.format(hudi)
      .options(combinedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // Update records
    val updatedRecords = dataGen.generateUpdates("002", newRecords)
    val updatedRecordsDF = parseRecords(recordsToStrings(updatedRecords).asScala.toSeq)

    updatedRecordsDF.write.format(hudi)
      .options(combinedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    if (testPartitioned) {
      validatePartitionedTable(basePath)
    } else {
      validateUnPartitionedTable(basePath)
    }
  }

  private def validatePartitionedTable(basePath: String) : Unit = {
    // Files partition of MT
    val filesPartitionDF = spark.read.format(hudi).load(s"$basePath/.hoodie/metadata/files")
    // Smoke test
    filesPartitionDF.show()

    // Query w/ 0 requested columns should be working fine
    assertEquals(4, filesPartitionDF.count())

    val expectedKeys = Seq("2015/03/16", "2015/03/17", "2016/03/15", HoodieTableMetadata.RECORDKEY_PARTITION_LIST)
    val keys = filesPartitionDF.select("key")
      .collect()
      .map(_.getString(0))
      .toSeq
      .sorted

    assertEquals(expectedKeys, keys)

    // Column Stats Index partition of MT
    val colStatsDF = spark.read.format(hudi).load(s"$basePath/.hoodie/metadata/column_stats")
    // Smoke test
    colStatsDF.show()

    // lets pick one data file and validate col stats
    val partitionPathToTest = "2015/03/16"
    val engineContext = new HoodieSparkEngineContext(jsc())
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(true).build();
    val baseTableMetada: HoodieTableMetadata = new HoodieBackedTableMetadata(
      engineContext, hoodieStorage(), metadataConfig, s"$basePath", false)

    val fileStatuses = baseTableMetada.getAllFilesInPartition(new StoragePath(s"$basePath/" + partitionPathToTest))
    val fileName = fileStatuses.get(0).getPath.getName

    val partitionFileNamePair: java.util.List[org.apache.hudi.common.util.collection.Pair[String, String]] = new util.ArrayList
    partitionFileNamePair.add(org.apache.hudi.common.util.collection.Pair.of(partitionPathToTest, fileName))

    val colStatsRecords = baseTableMetada.getColumnStats(partitionFileNamePair, "begin_lat")
    assertEquals(colStatsRecords.size(), 1)
    val metadataColStats = colStatsRecords.get(partitionFileNamePair.get(0))

    // read parquet file and verify stats
    val colRangeMetadataList: java.util.List[HoodieColumnRangeMetadata[Comparable[_]]] = new ParquetUtils()
      .readColumnStatsFromMetadata(
        new HoodieHadoopStorage(fileStatuses.get(0).getPath, HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration())),
        fileStatuses.get(0).getPath, Collections.singletonList("begin_lat"))
    val columnRangeMetadata = colRangeMetadataList.get(0)

    assertEquals(metadataColStats.getValueCount, columnRangeMetadata.getValueCount)
    assertEquals(metadataColStats.getTotalSize, columnRangeMetadata.getTotalSize)
    assertEquals(unwrapAvroValueWrapper(metadataColStats.getMaxValue), columnRangeMetadata.getMaxValue)
    assertEquals(unwrapAvroValueWrapper(metadataColStats.getMinValue), columnRangeMetadata.getMinValue)
    assertEquals(metadataColStats.getFileName, fileName)
  }

  private def validateUnPartitionedTable(basePath: String) : Unit = {
    // Files partition of MT
    val filesPartitionDF = spark.read.format(hudi).load(s"$basePath/.hoodie/metadata/files")
    // Smoke test
    filesPartitionDF.show()

    // Query w/ 0 requested columns should be working fine
    assertEquals(2, filesPartitionDF.count())

    val expectedKeys = Seq(HoodieTableMetadata.NON_PARTITIONED_NAME, HoodieTableMetadata.RECORDKEY_PARTITION_LIST)
    val keys = filesPartitionDF.select("key")
      .collect()
      .map(_.getString(0))
      .toSeq
      .sorted

    assertEquals(expectedKeys, keys)

    // Column Stats Index partition of MT
    val colStatsDF = spark.read.format(hudi).load(s"$basePath/.hoodie/metadata/column_stats")
    // Smoke test
    colStatsDF.show()

    // lets pick one data file and validate col stats
    val partitionPathToTest = ""
    val engineContext = new HoodieSparkEngineContext(jsc())
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(true).build();
    val baseTableMetada: HoodieTableMetadata = new HoodieBackedTableMetadata(
      engineContext, hoodieStorage(), metadataConfig, s"$basePath", false)

    val allPartitionPaths = baseTableMetada.getAllPartitionPaths
    assertEquals(allPartitionPaths.size(), 1)
    assertEquals(allPartitionPaths.get(0), HoodieTableMetadata.EMPTY_PARTITION_NAME)

    val fileStatuses = baseTableMetada.getAllFilesInPartition(new StoragePath(s"$basePath/"))
    val fileName = fileStatuses.get(0).getPath.getName

    val partitionFileNamePair: java.util.List[org.apache.hudi.common.util.collection.Pair[String, String]] = new util.ArrayList
    partitionFileNamePair.add(org.apache.hudi.common.util.collection.Pair.of(partitionPathToTest, fileName))

    val colStatsRecords = baseTableMetada.getColumnStats(partitionFileNamePair, "begin_lat")
    assertEquals(colStatsRecords.size(), 1)
    val metadataColStats = colStatsRecords.get(partitionFileNamePair.get(0))

    // read parquet file and verify stats
    val colRangeMetadataList: java.util.List[HoodieColumnRangeMetadata[Comparable[_]]] = new ParquetUtils()
      .readColumnStatsFromMetadata(
        new HoodieHadoopStorage(fileStatuses.get(0).getPath, HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration())),
        fileStatuses.get(0).getPath, Collections.singletonList("begin_lat"))
    val columnRangeMetadata = colRangeMetadataList.get(0)

    assertEquals(metadataColStats.getValueCount, columnRangeMetadata.getValueCount)
    assertEquals(metadataColStats.getTotalSize, columnRangeMetadata.getTotalSize)
    assertEquals(unwrapAvroValueWrapper(metadataColStats.getMaxValue), columnRangeMetadata.getMaxValue)
    assertEquals(unwrapAvroValueWrapper(metadataColStats.getMinValue), columnRangeMetadata.getMinValue)
    assertEquals(metadataColStats.getFileName, fileName)
  }

  private def parseRecords(records: Seq[String]) = {
    spark.read.json(spark.sparkContext.parallelize(records, 2))
  }

  @Test
  def testTimeTravelQuery(): Unit = {
    val dataGen = new HoodieTestDataGenerator()
    val metadataOpts: Map[String, String] = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      DataSourceWriteOptions.TABLE_TYPE.key -> "MERGE_ON_READ",
      HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "5"
    )
    val combinedOpts: Map[String, String] = partitionedCommonOpts ++ metadataOpts

    // Insert T0
    val newRecords = dataGen.generateInserts("000", 100)
    val newRecordsDF = parseRecords(recordsToStrings(newRecords).asScala.toSeq)
    newRecordsDF.write.format(hudi)
      .options(combinedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    //Validate T0
    val metaClient = HoodieTableMetaClient.builder
      .setConf(storageConf())
      .setBasePath(s"$basePath/.hoodie/metadata")
      .build
    val timelineT0 = metaClient.getActiveTimeline
    assertEquals(4, timelineT0.countInstants())
    assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, timelineT0.lastInstant().get().getAction)
    val t0 = timelineT0.lastInstant().get().requestedTime

    val filesT0 = getFiles(basePath)
    assertEquals(3, filesT0.size)

    val baseMetaClient = HoodieTableMetaClient.builder
      .setConf(storageConf())
      .setBasePath(basePath)
      .build
    val filesT0FS = getFilesFromFs(baseMetaClient)
    assertEquals(3, filesT0FS.size)
    assertEquals(3, filesT0.intersect(filesT0FS).size)

    // Update T1
    val updatedRecords = dataGen.generateUpdates("001", newRecords)
    val updatedRecordsDF = parseRecords(recordsToStrings(updatedRecords).asScala.toSeq)
    updatedRecordsDF.write.format(hudi)
      .options(combinedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    //Validate T1
    val timelineT1 = metaClient.reloadActiveTimeline()
    assertEquals(5, timelineT1.countInstants())
    assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, timelineT1.lastInstant().get().getAction)
    val t1 =  timelineT1.lastInstant().get().requestedTime

    val filesT1 = getFiles(basePath)
    assertEquals(6, filesT1.size)
    assertEquals(3, filesT1.diff(filesT0).size)

    val filesT1FS = getFilesFromFs(baseMetaClient)
    assertEquals(6, filesT1FS.size)
    assertEquals(6, filesT1.intersect(filesT1FS).size)

    val filesT1travelT0 = getFilesAsOf(basePath, t0)
    assertEquals(3, filesT1travelT0.size)
    assertEquals(3, filesT1travelT0.intersect(filesT0).size)

    //Update T2
    val updatedRecords2 = dataGen.generateUpdates("002", updatedRecords)
    val updatedRecords2DF = parseRecords(recordsToStrings(updatedRecords2).asScala.toSeq)
    updatedRecords2DF.write.format(hudi)
      .options(combinedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    //Validate T2
    val timelineT2 = metaClient.reloadActiveTimeline()
    assertEquals(7, timelineT2.countInstants())
    // one dc and compaction commit
    assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, timelineT2.getInstants.get(5).getAction)
    assertEquals(HoodieTimeline.COMMIT_ACTION, timelineT2.lastInstant().get().getAction)
    val t2 =  timelineT2.lastInstant().get().requestedTime

    val filesT2 = getFiles(basePath)
    assertEquals(9, filesT2.size)
    assertEquals(3, filesT2.diff(filesT1).size)

    val filesT2FS = getFilesFromFs(baseMetaClient)
    assertEquals(9, filesT2FS.size)
    assertEquals(9, filesT2.intersect(filesT2FS).size)

    val filesT2travelT1 = getFilesAsOf(basePath, t1)
    assertEquals(6, filesT2travelT1.size)
    assertEquals(6, filesT2travelT1.intersect(filesT1).size)

    val filesT2travelT0 = getFilesAsOf(basePath, t0)
    assertEquals(3, filesT2travelT0.size)
    assertEquals(3, filesT2travelT0.intersect(filesT0).size)

    //Update T3
    val updatedRecords3 = dataGen.generateUpdates("003", updatedRecords2)
    val updatedRecords3DF = parseRecords(recordsToStrings(updatedRecords3).asScala.toSeq)
    updatedRecords3DF.write.format(hudi)
      .options(combinedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    //Validate T3
    val timelineT3 = metaClient.reloadActiveTimeline()
    assertEquals(8, timelineT3.countInstants())
    assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, timelineT3.lastInstant().get().getAction)

    val filesT3 = getFiles(basePath)
    assertEquals(12, filesT3.size)
    assertEquals(3, filesT3.diff(filesT2).size)

    val filesT3FS = getFilesFromFs(baseMetaClient)
    assertEquals(12, filesT3FS.size)
    assertEquals(12, filesT3.intersect(filesT3FS).size)

    val filesT3travelT2 = getFilesAsOf(basePath, t2)
    assertEquals(9, filesT3travelT2.size)
    assertEquals(9, filesT3travelT2.intersect(filesT2).size)

    val filesT3travelT1 = getFilesAsOf(basePath, t1)
    assertEquals(6, filesT3travelT1.size)
    assertEquals(6, filesT3travelT1.intersect(filesT1).size)

    val filesT3travelT0 = getFilesAsOf(basePath, t0)
    assertEquals(3, filesT3travelT0.size)
    assertEquals(3, filesT3travelT0.intersect(filesT0).size)
  }

  private def getFilesAsOf(basePath: String, timestamp: String): scala.collection.GenSet[Any] = {
    getFiles(basePath, Map(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key() -> timestamp))
  }

  private def getFiles(basePath: String): scala.collection.GenSet[Any] = {
    getFiles(basePath, Map.empty)
  }

  private def getFiles(basePath: String, opts: Map[String, String]): scala.collection.GenSet[Any] = {
    spark.read.format(hudi).options(opts).load(s"$basePath/.hoodie/metadata").where("type = 2").select(explode(col("filesystemMetadata"))).drop("value").rdd.map(r => r(0)).collect().toSet
  }

  private def getFilesFromFs(metaClient: HoodieTableMetaClient): scala.collection.GenSet[Any] = {
    val engineContext = new HoodieSparkEngineContext(jsc())
    val files = new util.ArrayList[String]()
    val fsview = FileSystemViewManager.createInMemoryFileSystemView(engineContext,
      metaClient, HoodieMetadataConfig.newBuilder.enable(false).build())
    fsview.loadAllPartitions()
    convertJavaListToScalaSeq(fsview.getAllFileGroups.collect(Collectors.toList())).foreach(fg => {
      convertJavaListToScalaSeq(fg.getAllFileSlices.collect(Collectors.toList())).foreach(fileSlice => {
        if (fileSlice.getBaseFile.isPresent) {
          files.add(fileSlice.getBaseFile.get().getFileName)
        }
        convertJavaListToScalaSeq(fileSlice.getLogFiles.collect(Collectors.toList())).foreach(logFile => files.add(logFile.getFileName))
      })
    })
    files.toArray.toSet
  }
}
