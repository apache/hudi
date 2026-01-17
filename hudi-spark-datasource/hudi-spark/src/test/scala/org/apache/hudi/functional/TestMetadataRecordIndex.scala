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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.common.util.HoodieDataUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql._
import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

@Tag("functional")
class TestMetadataRecordIndex extends HoodieSparkClientTestBase {
  var spark: SparkSession = _
  var instantTime: AtomicInteger = _
  val metadataOpts = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    RECORDKEY_FIELD.key -> "_row_key",
    PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    initHoodieStorage()
    initTestDataGenerator()

    setTableName("hoodie_test")
    initMetaClient()

    instantTime = new AtomicInteger(1)

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

  protected def withRDDPersistenceValidation(f: => Unit): Unit = {
    org.apache.hudi.testutils.SparkRDDValidationUtils.withRDDPersistenceValidation(spark, new org.apache.hudi.testutils.SparkRDDValidationUtils.ThrowingRunnable {
      override def run(): Unit = f
    })
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testClusteringWithRecordIndex(tableType: HoodieTableType): Unit = {
    withRDDPersistenceValidation {
      val hudiOpts = commonOpts ++ Map(
        TABLE_TYPE.key -> tableType.name(),
        HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
        HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "2"
      )

      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = INSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Overwrite)
      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append)

      val lastClusteringInstant = getLatestClusteringInstant()
      assertTrue(lastClusteringInstant.isPresent)

      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append)
      doWriteAndValidateDataAndRecordIndex(hudiOpts,
        operation = UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append)

      assertTrue(getLatestClusteringInstant().get().requestedTime.compareTo(lastClusteringInstant.get().requestedTime) > 0)
      validateDataAndRecordIndices(hudiOpts)
    }
  }

  private def getLatestClusteringInstant(): Option[HoodieInstant] = {
    metaClient.getActiveTimeline.getCompletedReplaceTimeline.lastInstant()
  }

  private def doWriteAndValidateDataAndRecordIndex(hudiOpts: Map[String, String],
                                                   operation: String,
                                                   saveMode: SaveMode,
                                                   validate: Boolean = true): DataFrame = {
    var records1: mutable.Buffer[String] = null
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      val instantTime = getInstantTime()
      val records = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, 20))
      records.addAll(recordsToStrings(dataGen.generateInserts(instantTime, 20)))
      records1 = records.asScala
    } else {
      records1 = recordsToStrings(dataGen.generateInserts(getInstantTime(), 100)).asScala
    }
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1.toSeq, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)
    calculateMergedDf(inputDF1)
    if (validate) {
      validateDataAndRecordIndices(hudiOpts)
    }
    inputDF1
  }

  def calculateMergedDf(inputDF1: DataFrame): Unit = {
    val prevDfOpt = mergedDfList.lastOption
    if (prevDfOpt.isEmpty) {
      mergedDfList = mergedDfList :+ inputDF1
    } else {
      val prevDf = prevDfOpt.get
      val prevDfOld = prevDf.join(inputDF1, prevDf("_row_key") === inputDF1("_row_key")
        && prevDf("partition") === inputDF1("partition"), "leftanti")
      val unionDf = prevDfOld.union(inputDF1)
      mergedDfList = mergedDfList :+ unionDf
    }
  }

  private def getInstantTime(): String = {
    String.format("%03d", new Integer(instantTime.getAndIncrement()))
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  def getFileGroupCountForRecordIndex(writeConfig: HoodieWriteConfig): Long = {
    val tableMetadata = getHoodieTable(metaClient, writeConfig).getTableMetadata
    tableMetadata.asInstanceOf[HoodieBackedTableMetadata].getNumFileGroupsForPartition(MetadataPartitionType.RECORD_INDEX)
  }

  private def validateDataAndRecordIndices(hudiOpts: Map[String, String]): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val writeConfig = getWriteConfig(hudiOpts)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val readDf = spark.read.format("hudi").load(basePath)
    val rowArr = readDf.collect()
    val res = metadata.readRecordIndexLocationsWithKeys(
      HoodieListData.eager(rowArr.map(row => row.getAs("_hoodie_record_key").toString).toList.asJava));
    val recordIndexMap = HoodieDataUtils.dedupeAndCollectAsMap(res);
    res.unpersistWithDependencies()

    assertTrue(rowArr.length > 0)
    for (row <- rowArr) {
      val recordKey: String = row.getAs("_hoodie_record_key")
      val partitionPath: String = row.getAs("_hoodie_partition_path")
      val fileName: String = row.getAs("_hoodie_file_name")
      val recordLocation = recordIndexMap.get(recordKey)
      assertEquals(partitionPath, recordLocation.getPartitionPath)
      if (!writeConfig.inlineClusteringEnabled && !writeConfig.isAsyncClusteringEnabled) {
        // The file id changes after clustering, so only assert it for usual upsert and compaction operations
        assertTrue(fileName.contains(recordLocation.getFileId), fileName + " does not contain " + recordLocation.getFileId)
      }
    }

    assertEquals(rowArr.length, recordIndexMap.keySet.size)
    val estimatedFileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(
      MetadataPartitionType.RECORD_INDEX, () => rowArr.length, 48,
      writeConfig.getGlobalRecordLevelIndexMinFileGroupCount, writeConfig.getGlobalRecordLevelIndexMaxFileGroupCount,
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
