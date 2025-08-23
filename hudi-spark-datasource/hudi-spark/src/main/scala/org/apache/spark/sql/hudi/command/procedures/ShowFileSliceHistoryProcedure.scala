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

class ShowFileSliceHistoryProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "file_name", DataTypes.StringType),
    ProcedureParameter.optional(2, "partition", DataTypes.StringType),
    ProcedureParameter.optional(3, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(5, "filter", DataTypes.StringType, "")
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = ShowFileHistoryProcedureUtils.OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val fileName = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val partition = getArgValueOrDefault(args, PARAMETERS(2)).asInstanceOf[Option[String]]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val limit = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Int]
    val filter = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, outputType, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }

    val basePath = getBasePath(Option(tableName), Option.empty)
    val metaClient = createMetaClient(jsc, basePath)

    val fileSliceHistory = collectFileSliceHistory(metaClient, fileName, partition, showArchived, limit)

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(fileSliceHistory, filter, outputType, sparkSession)
    } else {
      fileSliceHistory
    }
  }

  private def collectFileSliceHistory(
                                       metaClient: HoodieTableMetaClient,
                                       fileName: String,
                                       partition: Option[String],
                                       showArchived: Boolean,
                                       limit: Int): Seq[Row] = {

    import ShowFileHistoryProcedureUtils._
    import FileHistoryType._

    val activeEntries = new util.ArrayList[HistoryEntry]()
    val activeTimeline = metaClient.getActiveTimeline
    ShowFileHistoryProcedureUtils.processTimeline(activeTimeline, fileName, partition, "ACTIVE", activeEntries, limit, FILE_SLICE)
    ShowFileHistoryProcedureUtils.processCleanAndRollbackOperations(activeTimeline, fileName, partition, "ACTIVE", activeEntries, limit, FILE_SLICE)

    val archivedEntries = new util.ArrayList[HistoryEntry]()
    if (showArchived) {
      try {
        val archivedTimeline = metaClient.getArchivedTimeline.reload()
        archivedTimeline.loadCompletedInstantDetailsInMemory()
        ShowFileHistoryProcedureUtils.processTimeline(archivedTimeline, fileName, partition, "ARCHIVED", archivedEntries, limit, FILE_SLICE)
        ShowFileHistoryProcedureUtils.processCleanAndRollbackOperations(archivedTimeline, fileName, partition, "ARCHIVED", archivedEntries, limit, FILE_SLICE)
      } catch {
        case e: Exception =>
          log.warn(s"Failed to process archived timeline: ${e.getMessage}")
      }
    }

    val allEntries = (activeEntries.asScala ++ archivedEntries.asScala).toList
    val sortedEntries = allEntries
      .sortBy(_.instantTime)(Ordering[String].reverse)
      .take(limit)

    val deletionInfo = ShowFileHistoryProcedureUtils.checkForDeletions(metaClient, fileName, partition, showArchived, FILE_SLICE)

    sortedEntries.map { entry =>
      val deletion = deletionInfo.get(entry.fileName)
      Row(
        entry.instantTime,
        entry.completionTime,
        entry.action,
        entry.timelineType,
        entry.state,
        entry.partitionPath,
        entry.fileGroupId,
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
        entry.totalWriteErrors,
        entry.totalScanTimeMs,
        entry.totalUpsertTimeMs,
        entry.totalCreateTimeMs,
        entry.prevBaseFile.orNull,
        entry.columnStatsAvailable
      )
    }.toSeq
  }

  override def build: Procedure = new ShowFileSliceHistoryProcedure()
}

object ShowFileSliceHistoryProcedure {
  val NAME = "show_file_slice_history"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowFileSliceHistoryProcedure()
  }
}
