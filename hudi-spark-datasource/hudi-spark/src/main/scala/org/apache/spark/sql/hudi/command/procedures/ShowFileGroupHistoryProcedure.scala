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

import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowFileGroupHistoryProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.required(2, "file_group_id", DataTypes.StringType),
    ProcedureParameter.optional(3, "partition", DataTypes.StringType),
    ProcedureParameter.optional(4, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(5, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(6, "filter", DataTypes.StringType, ""),
    ProcedureParameter.optional(7, "startTime", DataTypes.StringType, ""),
    ProcedureParameter.optional(8, "endTime", DataTypes.StringType, "")
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = ShowFileHistoryProcedureUtils.OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val fileGroupId = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val partition = getArgValueOrDefault(args, PARAMETERS(3)).asInstanceOf[Option[String]]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Boolean]
    val limit = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[Int]
    val filter = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val startTime = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[String]
    val endTime = getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[String]

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, outputType, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val fileGroupHistory = collectFileGroupHistory(metaClient, fileGroupId, partition, showArchived, limit, startTime, endTime)

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(fileGroupHistory, filter, outputType, sparkSession)
    } else {
      fileGroupHistory
    }
  }

  private def collectFileGroupHistory(
                                       metaClient: HoodieTableMetaClient,
                                       fileGroupId: String,
                                       partition: Option[String],
                                       showArchived: Boolean,
                                       limit: Int,
                                       startTime: String,
                                       endTime: String): Seq[Row] = {

    import ShowFileHistoryProcedureUtils._

    val activeEntries = new util.ArrayList[HistoryEntry]()
    val activeTimeline = metaClient.getActiveTimeline
    ShowFileHistoryProcedureUtils.processTimeline(activeTimeline, fileGroupId, partition, "ACTIVE", activeEntries, limit, startTime, endTime)

    val archivedEntries = new util.ArrayList[HistoryEntry]()
    if (showArchived) {
      try {
        val archivedTimeline = metaClient.getArchivedTimeline.reload()
        archivedTimeline.loadCompletedInstantDetailsInMemory()
        ShowFileHistoryProcedureUtils.processTimeline(archivedTimeline, fileGroupId, partition, "ARCHIVED", archivedEntries, limit, startTime, endTime)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process archived timeline: ${e.getMessage}")
      }
    }

    val allEntries = (activeEntries.asScala ++ archivedEntries.asScala).toList
    val sortedEntries = allEntries
      .sortBy(_.instantTime)(Ordering[String].reverse)

    val finalEntries = if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
      sortedEntries
    } else {
      sortedEntries.take(limit)
    }

    val (deletionInfo, replacementInfo) = ShowFileHistoryProcedureUtils.checkForDeletionsAndReplacements(metaClient, fileGroupId, partition, showArchived)

    val rows = finalEntries.map { entry =>
      val deletion = deletionInfo.get(entry.fileName)
      val replacement = replacementInfo.get(entry.fileName).orElse {
        val fileId = entry.fileName.split("_").headOption.getOrElse("")
        replacementInfo.get(fileId)
      }
      Row(
        entry.instantTime,
        entry.completionTime,
        entry.action,
        entry.timelineType,
        entry.state,
        entry.partitionPath,
        entry.fileName,
        entry.operationType,
        entry.numWrites,
        entry.numInserts,
        entry.numUpdates,
        entry.numDeletes,
        entry.fileSizeBytes,
        entry.totalWriteBytes,
        entry.prevCommit,
        deletion.isDefined,
        deletion.map(_.action).orNull,
        deletion.map(_.instant).orNull,
        replacement.isDefined,
        replacement.map(_.action).orNull,
        replacement.map(_.instant).orNull,
        entry.totalWriteErrors,
        entry.totalScanTimeMs,
        entry.totalUpsertTimeMs,
        entry.totalCreateTimeMs,
        entry.prevBaseFile.orNull,
        entry.columnStatsAvailable
      )
    }.toSeq

    if (rows.nonEmpty) {
      val firstRow = rows.head
      val firstRowValues = firstRow.toSeq.toArray
      val markedFirstRow = Row.fromSeq(("*" + firstRowValues(0)) +: firstRowValues.tail)
      markedFirstRow +: rows.tail
    } else {
      rows
    }
  }


  override def build: Procedure = new ShowFileGroupHistoryProcedure()
}

object ShowFileGroupHistoryProcedure {
  val NAME = "show_file_group_history"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowFileGroupHistoryProcedure()
  }
}
