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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.{HoodieCLIUtils, SparkAdapterSupport}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline, TimelineLayout}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Collections
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowCleansProcedure(includePartitionMetadata: Boolean) extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(2, "showArchived", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("start_clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_deleted", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("earliest_commit_to_retain", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("last_completed_commit_timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("version", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  private val PARTITION_METADATA_OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("start_clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("policy", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("delete_path_patterns", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("success_delete_files", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("failed_delete_files", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("is_partition_deleted", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_deleted", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = if (includePartitionMetadata) PARTITION_METADATA_OUTPUT_TYPE else OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    val metaClient = createMetaClient(jsc, basePath)

    val activeResults = if (includePartitionMetadata) {
      getCleansWithPartitionMetadata(metaClient.getActiveTimeline, limit)
    } else {
      getCleans(metaClient.getActiveTimeline, limit)
    }
    val finalResults = if (showArchived) {
      val archivedResults = if (includePartitionMetadata) {
        getCleansWithPartitionMetadata(metaClient.getArchivedTimeline, limit)
      } else {
        getCleans(metaClient.getArchivedTimeline, limit)
      }
      (activeResults ++ archivedResults)
        .sortWith((a, b) => a.getString(0) > b.getString(0))
        .take(limit)
    } else {
      activeResults
    }
    finalResults
  }

  override def build: Procedure = new ShowCleansProcedure(includePartitionMetadata)

  private def getCleansWithPartitionMetadata(timeline: HoodieTimeline,
                                             limit: Int): Seq[Row] = {
    import scala.collection.JavaConverters._

    val (rows: util.ArrayList[Row], cleanInstants: util.ArrayList[HoodieInstant]) = getSortedCleans(timeline)

    var rowCount = 0

    cleanInstants.asScala.takeWhile(_ => rowCount < limit).foreach { cleanInstant =>
      val cleanMetadata = timeline.readCleanMetadata(cleanInstant)

      cleanMetadata.getPartitionMetadata.entrySet.asScala.takeWhile(_ => rowCount < limit).foreach { partitionMetadataEntry =>
        val partitionPath = partitionMetadataEntry.getKey
        val partitionMetadata = partitionMetadataEntry.getValue

        rows.add(Row(
          cleanInstant.requestedTime(),
          cleanInstant.getCompletionTime,
          cleanInstant.getAction,
          cleanMetadata.getStartCleanTime,
          partitionPath,
          partitionMetadata.getPolicy,
          partitionMetadata.getDeletePathPatterns.size(),
          partitionMetadata.getSuccessDeleteFiles.size(),
          partitionMetadata.getFailedDeleteFiles.size(),
          partitionMetadata.getIsPartitionDeleted,
          cleanMetadata.getTimeTakenInMillis,
          cleanMetadata.getTotalFilesDeleted
        ))
        rowCount += 1
      }
    }

    rows.asScala.toList
  }

  private def getSortedCleans(timeline: HoodieTimeline): (util.ArrayList[Row], util.ArrayList[HoodieInstant]) = {
    val rows = new util.ArrayList[Row]
    val cleanInstants: util.List[HoodieInstant] = timeline.getCleanerTimeline.filterCompletedInstants
      .getInstants.toArray().map(instant => instant.asInstanceOf[HoodieInstant]).toList.asJava
    val sortedCleanInstants = new util.ArrayList[HoodieInstant](cleanInstants)
    val layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion)
    Collections.sort(sortedCleanInstants, layout.getInstantComparator.requestedTimeOrderedComparator.reversed)
    (rows, sortedCleanInstants)
  }

  private def getCleans(timeline: HoodieTimeline,
                        limit: Int): Seq[Row] = {
    val (rows: util.ArrayList[Row], cleanInstants: util.ArrayList[HoodieInstant]) = getSortedCleans(timeline)

    for (i <- 0 until Math.min(cleanInstants.size, limit)) {
      val cleanInstant = cleanInstants.get(i)
      val cleanMetadata = timeline.readCleanMetadata(cleanInstant)

      rows.add(Row(
        cleanInstant.requestedTime(),
        cleanInstant.getCompletionTime,
        cleanInstant.getAction,
        cleanMetadata.getStartCleanTime,
        cleanMetadata.getTimeTakenInMillis,
        cleanMetadata.getTotalFilesDeleted,
        cleanMetadata.getEarliestCommitToRetain,
        cleanMetadata.getLastCompletedCommitTimestamp,
        cleanMetadata.getVersion
      ))
    }

    rows.asScala.toList
  }
}

object ShowCleansProcedure {
  val NAME = "show_cleans"

  def builder: Supplier[ProcedureBuilder] = () => new ShowCleansProcedure(false)
}

object ShowCleansPartitionMetadataProcedure {
  val NAME = "show_cleans_metadata"

  def builder: Supplier[ProcedureBuilder] = () => new ShowCleansProcedure(true)
}
