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

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.timeline.{HoodieDefaultTimeline, HoodieInstant}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Collections
import java.util.function.Supplier
import scala.collection.JavaConverters._

class ShowCommitsProcedure(includeExtraMetadata: Boolean) extends BaseProcedure with ProcedureBuilder {
  var sortByFieldParameter: ProcedureParameter = _

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("commit_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("total_bytes_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_added", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_updated", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_partitions_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_records_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_update_records_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_errors", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  private val METADATA_OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("commit_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("previous_commit", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("num_writes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_inserts", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_deletes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_update_writes", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_errors", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_log_blocks", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_corrupt_log_blocks", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_rollback_blocks", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_log_records", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_updated_records_compacted", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_bytes_written", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = if (includeExtraMetadata) METADATA_OUTPUT_TYPE else OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    val metaClient = createMetaClient(jsc, basePath)

    val activeTimeline = metaClient.getActiveTimeline
    if (includeExtraMetadata) {
      getCommitsWithMetadata(activeTimeline, limit)
    } else {
      getCommits(activeTimeline, limit)
    }
  }

  override def build: Procedure = new ShowCommitsProcedure(includeExtraMetadata)

  private def getCommitsWithMetadata(timeline: HoodieDefaultTimeline,
                                     limit: Int): Seq[Row] = {
    import scala.collection.JavaConverters._

    val (rows: util.ArrayList[Row], newCommits: util.ArrayList[HoodieInstant]) = getSortCommits(timeline)

    for (i <- 0 until newCommits.size) {
      val commit = newCommits.get(i)
      val commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(commit).get, classOf[HoodieCommitMetadata])
      for (partitionWriteStat <- commitMetadata.getPartitionToWriteStats.entrySet.asScala) {
        for (hoodieWriteStat <- partitionWriteStat.getValue.asScala) {
          rows.add(Row(
            commit.getTimestamp, commit.getStateTransitionTime, commit.getAction, hoodieWriteStat.getPartitionPath,
            hoodieWriteStat.getFileId, hoodieWriteStat.getPrevCommit, hoodieWriteStat.getNumWrites,
            hoodieWriteStat.getNumInserts, hoodieWriteStat.getNumDeletes, hoodieWriteStat.getNumUpdateWrites,
            hoodieWriteStat.getTotalWriteErrors, hoodieWriteStat.getTotalLogBlocks, hoodieWriteStat.getTotalCorruptLogBlock,
            hoodieWriteStat.getTotalRollbackBlocks, hoodieWriteStat.getTotalLogRecords,
            hoodieWriteStat.getTotalUpdatedRecordsCompacted, hoodieWriteStat.getTotalWriteBytes))
        }
      }
    }

    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  private def getSortCommits(timeline: HoodieDefaultTimeline): (util.ArrayList[Row], util.ArrayList[HoodieInstant]) = {
    val rows = new util.ArrayList[Row]
    // timeline can be read from multiple files. So sort is needed instead of reversing the collection
    val commits: util.List[HoodieInstant] = timeline.getCommitsTimeline.filterCompletedInstants
      .getInstants.toArray().map(instant => instant.asInstanceOf[HoodieInstant]).toList.asJava
    val newCommits = new util.ArrayList[HoodieInstant](commits)
    Collections.sort(newCommits, HoodieInstant.COMPARATOR.reversed)
    (rows, newCommits)
  }

  def getCommits(timeline: HoodieDefaultTimeline,
                 limit: Int): Seq[Row] = {
    val (rows: util.ArrayList[Row], newCommits: util.ArrayList[HoodieInstant]) = getSortCommits(timeline)

    for (i <- 0 until newCommits.size) {
      val commit = newCommits.get(i)
      val commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(commit).get, classOf[HoodieCommitMetadata])
      rows.add(Row(commit.getTimestamp, commit.getStateTransitionTime, commit.getAction, commitMetadata.fetchTotalBytesWritten, commitMetadata.fetchTotalFilesInsert,
        commitMetadata.fetchTotalFilesUpdated, commitMetadata.fetchTotalPartitionsWritten,
        commitMetadata.fetchTotalRecordsWritten, commitMetadata.fetchTotalUpdateRecordsWritten,
        commitMetadata.fetchTotalWriteErrors))
    }

    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }
}

object ShowCommitsProcedure {
  val NAME = "show_commits"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowCommitsProcedure(false)
  }
}

object ShowCommitsMetadataProcedure {
  val NAME = "show_commits_metadata"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowCommitsProcedure(true)
  }
}


