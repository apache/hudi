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
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieColumnRangeMetadata
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util
import java.util.Collections

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
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
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
    val baseTableMetada: HoodieTableMetadata = new HoodieBackedTableMetadata(engineContext, metadataConfig, s"$basePath", false)

    val fileStatuses = baseTableMetada.getAllFilesInPartition(new StoragePath(s"$basePath/" + partitionPathToTest))
    val fileName = fileStatuses.get(0).getPath.getName

    val partitionFileNamePair: java.util.List[org.apache.hudi.common.util.collection.Pair[String, String]] = new util.ArrayList
    partitionFileNamePair.add(org.apache.hudi.common.util.collection.Pair.of(partitionPathToTest, fileName))

    val colStatsRecords = baseTableMetada.getColumnStats(partitionFileNamePair, "begin_lat")
    assertEquals(colStatsRecords.size(), 1)
    val metadataColStats = colStatsRecords.get(partitionFileNamePair.get(0))

    // read parquet file and verify stats
    val colRangeMetadataList: java.util.List[HoodieColumnRangeMetadata[Comparable[_]]] = new ParquetUtils()
      .readColumnStatsFromMetadata(HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration()),
        fileStatuses.get(0).getPath, Collections.singletonList("begin_lat"))
    val columnRangeMetadata = colRangeMetadataList.get(0)

    assertEquals(metadataColStats.getValueCount, columnRangeMetadata.getValueCount)
    assertEquals(metadataColStats.getTotalSize, columnRangeMetadata.getTotalSize)
    assertEquals(HoodieAvroUtils.unwrapAvroValueWrapper(metadataColStats.getMaxValue), columnRangeMetadata.getMaxValue)
    assertEquals(HoodieAvroUtils.unwrapAvroValueWrapper(metadataColStats.getMinValue), columnRangeMetadata.getMinValue)
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
    val baseTableMetada: HoodieTableMetadata = new HoodieBackedTableMetadata(engineContext, metadataConfig, s"$basePath", false)

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
      .readColumnStatsFromMetadata(HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration()),
        fileStatuses.get(0).getPath, Collections.singletonList("begin_lat"))
    val columnRangeMetadata = colRangeMetadataList.get(0)

    assertEquals(metadataColStats.getValueCount, columnRangeMetadata.getValueCount)
    assertEquals(metadataColStats.getTotalSize, columnRangeMetadata.getTotalSize)
    assertEquals(HoodieAvroUtils.unwrapAvroValueWrapper(metadataColStats.getMaxValue), columnRangeMetadata.getMaxValue)
    assertEquals(HoodieAvroUtils.unwrapAvroValueWrapper(metadataColStats.getMinValue), columnRangeMetadata.getMinValue)
    assertEquals(metadataColStats.getFileName, fileName)
  }

  private def parseRecords(records: Seq[String]) = {
    spark.read.json(spark.sparkContext.parallelize(records, 2))
  }
}
