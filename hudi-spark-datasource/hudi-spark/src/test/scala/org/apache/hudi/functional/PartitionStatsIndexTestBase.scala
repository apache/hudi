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
import org.apache.hudi.{AvroConversionUtils, PartitionStatsIndexSupport}
import org.apache.hudi.TestHoodieSparkUtils.dropMetaFields
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.functional.PartitionStatsIndexTestBase.checkIfOverlapped
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions.assertEquals

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Common test setup and validation methods for partition stats index testing.
 */
class PartitionStatsIndexTestBase extends HoodieStatsIndexTestBase {
  val targetColumnsToIndex: Seq[String] = Seq("rider", "driver")
  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
    HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> targetColumnsToIndex.mkString(",")
  )
  val commonOpts: Map[String, String] = Map(
    PARTITIONPATH_FIELD.key -> "partition,trip_type",
    HIVE_STYLE_PARTITIONING.key -> "true"
  ) ++ baseOpts ++ metadataOpts

  override def createJoinCondition(prevDf: DataFrame, latestBatchDf: DataFrame): Column = {
    prevDf("_row_key") === latestBatchDf("_row_key") &&
      prevDf("partition") === latestBatchDf("partition") &&
      prevDf("trip_type") === latestBatchDf("trip_type")
  }

  protected override def getLastInstant(): String = {
    String.format("%03d", new Integer(instantTime.incrementAndGet()))
  }

  protected def doWriteAndValidateDataAndPartitionStats(hudiOpts: Map[String, String],
                                                        operation: String,
                                                        saveMode: SaveMode,
                                                        validate: Boolean = true): DataFrame = {
    var latestBatch: mutable.Buffer[String] = null
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      val instantTime = getInstantTime
      val records = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, 20))
      records.addAll(recordsToStrings(dataGen.generateInserts(instantTime, 20)))
      latestBatch = records.asScala
    } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
      latestBatch = recordsToStrings(dataGen.generateInsertsForPartition(
        getInstantTime, 20, dataGen.getPartitionPaths.last)).asScala
    } else {
      latestBatch = recordsToStrings(dataGen.generateInserts(getInstantTime, 20)).asScala
    }
    doWriteAndValidateDataAndPartitionStats(latestBatch, hudiOpts, operation, saveMode, validate)
  }

  protected def doWriteAndValidateDataAndPartitionStats(records: mutable.Buffer[String],
                                                        hudiOpts: Map[String, String],
                                                        operation: String,
                                                        saveMode: SaveMode,
                                                        validate: Boolean): DataFrame = {
    val latestBatch: mutable.Buffer[String] = records
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch.toSeq, 2))
    latestBatchDf.cache()
    latestBatchDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)
    latestBatchDf.show(false)
    val deletedDf = calculateMergedDf(latestBatchDf, operation)
    deletedDf.cache()
    if (validate) {
      validateDataAndPartitionStats(deletedDf)
    }
    deletedDf.unpersist()
    latestBatchDf
  }

  protected def calculateMergedDf(latestBatchDf: DataFrame, operation: String): DataFrame = synchronized {
    calculateMergedDf(latestBatchDf, operation, globalIndexEnableUpdatePartitions = false)
  }

  protected def validateDataAndPartitionStats(inputDf: DataFrame = sparkSession.emptyDataFrame): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val readDf = spark.read.format("hudi").load(basePath)
    val partitionStatsIndex = new PartitionStatsIndexSupport(
      spark,
      inputDf.schema,
      AvroConversionUtils.convertStructTypeToAvroSchema(inputDf.schema, "record", ""),
      HoodieMetadataConfig.newBuilder().enable(true).build(),
      metaClient)
    val partitionStats = partitionStatsIndex.loadColumnStatsIndexRecords(List("partition", "trip_type"), shouldReadInMemory = true).collectAsList()
    assertEquals(0, partitionStats.size())
    val prevDf = mergedDfList.last.drop("tip_history")
    val nonMatchingRecords = dropMetaFields(readDf).drop("tip_history")
      .join(prevDf, prevDf.columns, "leftanti")
    assertEquals(0, nonMatchingRecords.count())
  }

  def checkPartitionFilters(sparkPlan: String, partitionFilter: String): Boolean = {
    val partitionFilterPattern: Regex = """PartitionFilters: \[(.*?)\]""".r
    val tsPattern: Regex = (partitionFilter).r

    val partitionFilterMatch = partitionFilterPattern.findFirstMatchIn(sparkPlan)

    partitionFilterMatch match {
      case Some(m) =>
        val filters = m.group(1)
        tsPattern.findFirstIn(filters).isDefined
      case None =>
        false
    }
  }

  // Check if the last instant has overlapped with other instants.
  def checkIfCommitsAreConcurrent(): Boolean = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val timeline = metaClient.getActiveTimeline.filterCompletedInstants()
    val instants = timeline.getInstants.asScala
    val lastInstant = instants.last
    val instantsWithoutLastOne = instants.dropRight(1).toList
    findConcurrentInstants(lastInstant, instantsWithoutLastOne).nonEmpty
  }

  def findConcurrentInstants(givenInstant: HoodieInstant, instants: List[HoodieInstant]): List[HoodieInstant] = {
    instants.filter(i => checkIfOverlapped(i, givenInstant))
  }
}

object PartitionStatsIndexTestBase {
  // Check if two completed instants are overlapped in time.
  def checkIfOverlapped(a: HoodieInstant, b: HoodieInstant): Boolean = {
    !(a.getCompletionTime.compareTo(b.requestedTime) < 0 || a.requestedTime.compareTo(b.getCompletionTime) > 0)
  }
}
