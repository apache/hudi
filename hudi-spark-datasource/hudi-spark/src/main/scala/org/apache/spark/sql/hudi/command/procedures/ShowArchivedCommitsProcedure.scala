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
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline, TimelineLayout, TimelineUtils}
import org.apache.hudi.common.util.StringUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.time.ZonedDateTime
import java.util
import java.util.{Collections, Date}
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowArchivedCommitsProcedure(includeExtraMetadata: Boolean) extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(3, "start_ts", DataTypes.StringType, ""),
    ProcedureParameter.optional(4, "end_ts", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "filter", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("commit_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state_transition_time", DataTypes.StringType, nullable = true, Metadata.empty),
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

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    var startTs = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    var endTs = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val filter = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, OUTPUT_TYPE, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    // start time for commits, default: now - 10 days
    // end time for commits, default: now - 1 day
    if (StringUtils.isNullOrEmpty(startTs)) startTs = getTimeDaysAgo(10)
    if (StringUtils.isNullOrEmpty(endTs)) endTs = getTimeDaysAgo(1)

    val archivedTimeline = metaClient.getArchivedTimeline
    val results = try {
      archivedTimeline.loadInstantDetailsInMemory(startTs, endTs)
      val timelineRange = archivedTimeline.findInstantsInRange(startTs, endTs).asInstanceOf[HoodieTimeline]
      if (includeExtraMetadata) {
        getCommitsWithMetadata(timelineRange, limit)
      } else {
        getCommits(timelineRange, limit)
      }
    } finally {
      // clear the instant details from memory after printing to reduce usage
      archivedTimeline.clearInstantDetailsFromMemory(startTs, endTs)
    }
    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(results, filter, OUTPUT_TYPE, sparkSession)
    } else {
      results
    }
  }

  override def build: Procedure = new ShowArchivedCommitsProcedure(includeExtraMetadata)

  private def getCommitsWithMetadata(timeline: HoodieTimeline,
                                     limit: Int): Seq[Row] = {
    import scala.collection.JavaConverters._

    val (rows: util.ArrayList[Row], newCommits: util.ArrayList[HoodieInstant]) = getSortCommits(timeline)
    val layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion)
    for (i <- 0 until newCommits.size) {
      val commit = newCommits.get(i)
      val commitMetadata = timeline.readCommitMetadata(commit)
      for (partitionWriteStat <- commitMetadata.getPartitionToWriteStats.entrySet.asScala) {
        for (hoodieWriteStat <- partitionWriteStat.getValue.asScala) {
          rows.add(Row(
            commit.requestedTime, commit.getCompletionTime, commit.getAction, hoodieWriteStat.getPartitionPath,
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

  private def getSortCommits(timeline: HoodieTimeline): (util.ArrayList[Row], util.ArrayList[HoodieInstant]) = {
    val rows = new util.ArrayList[Row]
    // timeline can be read from multiple files. So sort is needed instead of reversing the collection
    val commits: util.List[HoodieInstant] = timeline.getCommitsTimeline.filterCompletedInstants
      .getInstants.toArray().map(instant => instant.asInstanceOf[HoodieInstant]).toList.asJava
    val newCommits = new util.ArrayList[HoodieInstant](commits)
    val layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion)
    Collections.sort(newCommits, layout.getInstantComparator.requestedTimeOrderedComparator.reversed)
    (rows, newCommits)
  }

  def getCommits(timeline: HoodieTimeline,
                 limit: Int): Seq[Row] = {
    val (rows: util.ArrayList[Row], newCommits: util.ArrayList[HoodieInstant]) = getSortCommits(timeline)
    val layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion)
    for (i <- 0 until newCommits.size) {
      val commit = newCommits.get(i)
      val commitMetadata = timeline.readCommitMetadata(commit)
      rows.add(Row(commit.requestedTime, commit.getCompletionTime, commitMetadata.fetchTotalBytesWritten, commitMetadata.fetchTotalFilesInsert,
        commitMetadata.fetchTotalFilesUpdated, commitMetadata.fetchTotalPartitionsWritten,
        commitMetadata.fetchTotalRecordsWritten, commitMetadata.fetchTotalUpdateRecordsWritten,
        commitMetadata.fetchTotalWriteErrors))
    }

    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  def getTimeDaysAgo(numberOfDays: Int): String = {
    val date = Date.from(ZonedDateTime.now.minusDays(numberOfDays).toInstant)
    TimelineUtils.formatDate(date)
  }
}

object ShowArchivedCommitsProcedure {
  val NAME = "show_archived_commits"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowArchivedCommitsProcedure(false)
  }
}

object ShowArchivedCommitsMetadataProcedure {
  val NAME = "show_archived_commits_metadata"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowArchivedCommitsProcedure(true)
  }
}
