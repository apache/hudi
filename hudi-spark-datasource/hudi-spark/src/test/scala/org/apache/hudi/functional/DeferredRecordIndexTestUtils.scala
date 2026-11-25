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

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.function.SerializableFunctionUnchecked
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.core.index.record.HoodieRecordIndex
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.storage.{StorageConfiguration, StoragePath}

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.util.Properties

import scala.collection.JavaConverters._

/**
 * Assertions shared by the end to end tests of deferred record index initialization, so every writer is held to the
 * same checks: what the metadata table holds before and after the build, whether the index agrees with the table, and
 * how the index was sized.
 */
object DeferredRecordIndexTestUtils {

  private val RecordIndexPath: String = MetadataPartitionType.RECORD_INDEX.getPartitionPath

  /**
   * Asserts the record index left nothing behind: the table config does not list it, it has no index definition, the
   * metadata table holds no files under its partition, and no completed metadata table commit wrote to it.
   */
  def assertRecordIndexNotBuilt(basePath: String, storageConf: StorageConfiguration[_]): Unit = {
    val metaClient = buildMetaClient(basePath, storageConf)
    assertFalse(metaClient.getTableConfig.getMetadataPartitions.contains(RecordIndexPath),
      "The table config should not list the record index")
    assertTrue(metaClient.getIndexForMetadataPartition(RecordIndexPath).isEmpty,
      "The record index should have no index definition")

    val metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)
    val partitionPath = new StoragePath(metadataBasePath, RecordIndexPath)
    val storage = metaClient.getStorage
    if (storage.exists(partitionPath)) {
      assertEquals(Seq.empty, storage.listFiles(partitionPath).asScala.map(_.getPath.toString).toSeq,
        "The metadata table should hold no record index files")
    }

    val metadataTimeline = buildMetaClient(metadataBasePath, storageConf).getActiveTimeline
    val commitsWritingRecordIndex = metadataTimeline.getCommitsTimeline.filterCompletedInstants().getInstants.asScala
      .filter(instant => metadataTimeline.readCommitMetadata(instant).getPartitionToWriteStats.containsKey(RecordIndexPath))
      .map(_.requestedTime()).toSeq
    assertEquals(Seq.empty, commitsWritingRecordIndex, "No metadata table commit should write to the record index")
  }

  /**
   * Asserts the record index is listed in the table config and has an index definition that says whether it is
   * partitioned, which is what readers go by.
   */
  def assertRecordIndexBuilt(basePath: String, storageConf: StorageConfiguration[_], partitioned: Boolean): Unit = {
    val metaClient = buildMetaClient(basePath, storageConf)
    assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(RecordIndexPath),
      "The table config should list the record index")
    val definition = metaClient.getIndexForMetadataPartition(RecordIndexPath)
    assertTrue(definition.isPresent, "The record index should have an index definition")
    assertEquals(partitioned, HoodieRecordIndex.isPartitioned(definition.get),
      "The record index definition should say whether the index is partitioned")
  }

  /**
   * Asserts the record index holds exactly one entry per row of the table, pointing at the partition and the file group
   * the row is in. A key indexed twice, a key missing from the index, or an entry left for a deleted row fails here.
   */
  def assertRecordIndexMatchesTable(spark: SparkSession, basePath: String, storageConf: StorageConfiguration[_]): Unit = {
    val metaClient = buildMetaClient(basePath, storageConf)
    val engineContext = new HoodieSparkEngineContext(JavaSparkContext.fromSparkContext(spark.sparkContext))
    val metadata = new HoodieBackedTableMetadata(engineContext, metaClient.getStorage,
      HoodieMetadataConfig.newBuilder().enable(true).build(), basePath)
    val allFileSlices: SerializableFunctionUnchecked[java.util.List[FileSlice], java.util.List[FileSlice]] = fileSlices => fileSlices
    val indexed = try {
      metadata.readRecordIndexLocations(allFileSlices).collectAsList().asScala
        .map(pair => (pair.getKey, pair.getValue.getPartitionPath, pair.getValue.getFileId)).toSeq
    } finally {
      metadata.close()
    }
    val stored = spark.read.format("hudi").load(basePath)
      .select("_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name").collect()
      .map(row => (row.getString(0), row.getString(1), row.getString(2))).toSeq

    assertEquals(stored.map(row => (row._1, row._2)).sorted, indexed.map(entry => (entry._1, entry._2)).sorted,
      "The record index should hold one entry per row, in the row's partition")
    val indexedFileIds = indexed.map(entry => entry._1 -> entry._3).toMap
    val rowsInAnotherFileGroup = stored.filterNot(row => row._3.contains(indexedFileIds(row._1))).map(_._1)
    assertEquals(Seq.empty, rowsInAnotherFileGroup, "The record index should point every row at its file group")
  }

  /**
   * Asserts the record index was sized from the given rows, per partition for the partitioned index and over the whole
   * table for the global one, with the formula and the sizing configs the writer used.
   */
  def assertRecordIndexSizedFrom(basePath: String,
                                 storageConf: StorageConfiguration[_],
                                 writeProps: Properties,
                                 partitioned: Boolean,
                                 rowsPerPartition: java.util.Map[String, java.lang.Long]): Unit = {
    val metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(writeProps).build()
    val expectedFileGroupCount = (records: Long) => HoodieTableMetadataUtil.estimateFileGroupCount(
      MetadataPartitionType.RECORD_INDEX, () => records, HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE,
      if (partitioned) metadataConfig.getRecordLevelIndexMinFileGroupCount else metadataConfig.getGlobalRecordLevelIndexMinFileGroupCount,
      if (partitioned) metadataConfig.getRecordLevelIndexMaxFileGroupCount else metadataConfig.getGlobalRecordLevelIndexMaxFileGroupCount,
      metadataConfig.getRecordIndexGrowthFactor, metadataConfig.getRecordIndexMaxFileGroupSizeBytes)

    val metadataMetaClient = buildMetaClient(HoodieTableMetadata.getMetadataTableBasePath(basePath), storageConf)
    val fileGroupIds = HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, HOption.empty(), RecordIndexPath)
      .asScala.map(_.getFileId).toSeq
    if (partitioned) {
      val expected = rowsPerPartition.asScala.map { case (partition, rows) => partition -> expectedFileGroupCount(rows) }.toMap
      val actual = fileGroupIds.groupBy(HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName).mapValues(_.size).toMap
      assertEquals(expected, actual, "Each partition's record index should be sized from that partition's rows")
    } else {
      assertEquals(expectedFileGroupCount(rowsPerPartition.asScala.values.map(_.longValue).sum), fileGroupIds.size,
        "The record index should be sized from the table's rows")
    }
  }

  /** Builds the write properties for {@link assertRecordIndexSizedFrom} from writer options. */
  def toProperties(options: Map[String, String]): Properties = {
    val props = new TypedProperties()
    options.foreach { case (key, value) => props.setProperty(key, value) }
    props
  }

  /** Counts the rows of the table in each of its partitions. */
  def rowsPerPartition(spark: SparkSession, basePath: String): java.util.Map[String, java.lang.Long] =
    spark.read.format("hudi").load(basePath).groupBy("_hoodie_partition_path").count().collect()
      .map(row => row.getString(0) -> java.lang.Long.valueOf(row.getLong(1))).toMap.asJava

  private def buildMetaClient(basePath: String, storageConf: StorageConfiguration[_]): HoodieTableMetaClient =
    HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf.newInstance()).build()
}
