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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL procedure to show timeline information for a Hudi table.
 *
 * This procedure displays comprehensive information about all timeline instants including commits,
 * compactions, clustering, cleaning, and other table operations. It supports both active and
 * archived timelines with time-based filtering and generic SQL filters.
 *
 * == Parameters ==
 * - `table`: Optional. The name of the Hudi table to query (mutually exclusive with `path`)
 * - `path`: Optional. The base path of the Hudi table (mutually exclusive with `table`)
 * - `limit`: Optional. Maximum number of timeline entries to return (default: 20)
 * - `showArchived`: Optional. Whether to include archived timeline data (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 * - `startTime`: Optional. Start timestamp for filtering results (format: yyyyMMddHHmmss)
 * - `endTime`: Optional. End timestamp for filtering results (format: yyyyMMddHHmmss)
 *
 * == Output Schema ==
 * - `instant_time`: Timestamp when the operation was performed
 * - `action`: The action type (commit, deltacommit, compaction, clustering, clean, rollback, etc.)
 * - `state`: Current state of the operation (REQUESTED, INFLIGHT, COMPLETED)
 * - `requested_time`: Formatted date when the operation was requested (MM-dd HH:mm:ss format)
 * - `inflight_time`: Formatted date when the operation became inflight (MM-dd HH:mm:ss format)
 * - `completed_time`: Formatted date when the operation completed (MM-dd HH:mm format, "-" if not completed)
 * - `timeline_type`: Whether the data is from ACTIVE or ARCHIVED timeline
 * - `rollback_info`: Information about rollback operations (what was rolled back or what rolled back this operation)
 *
 * == Error Handling ==
 * - Throws `IllegalArgumentException` for invalid filter expressions
 * - Throws `HoodieException` for table access issues
 * - Returns empty result set if no timeline entries match the criteria
 * - Gracefully handles archived timeline access failures with warning logs
 *
 * == Filter Support ==
 * The `filter` parameter supports SQL expressions for filtering results on any output column.
 * The filter uses Spark SQL syntax and supports various data types and operations.
 *
 * == Usage Examples ==
 * {{{
 * -- Basic usage: Show timeline entries
 * CALL show_timeline(table => 'hudi_table')
 *
 * -- Show timeline with custom limit
 * CALL show_timeline(table => 'hudi_table', limit => 50)
 *
 * -- Include archived timeline data
 * CALL show_timeline(table => 'hudi_table', showArchived => true)
 *
 * -- Filter for specific actions
 * CALL show_timeline(
 *   table => 'hudi_table',
 *   filter => "action = 'commit'"
 * )
 *
 * -- Show timeline with time range and filtering
 * CALL show_timeline(
 *   table => 'hudi_table',
 *   startTime => '20251201000000',
 *   endTime => '20251231235959',
 *   filter => "action = 'commit' AND state = 'COMPLETED'"
 * )
 * }}}
 *
 * @see [[HoodieProcedureFilterUtils]] for detailed filter expression syntax
 */
class ShowTimelineProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(3, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "filter", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "startTime", DataTypes.StringType, ""),
    ProcedureParameter.optional(6, "endTime", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("requested_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("inflight_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("completed_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("timeline_type", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("rollback_info", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val filter = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val startTime = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val endTime = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, outputType, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val timelineEntries = getTimelineEntries(metaClient, limit, showArchived, startTime, endTime)

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(timelineEntries, filter, outputType, sparkSession)
    } else {
      timelineEntries
    }
  }

  override def build: Procedure = new ShowTimelineProcedure()

  private def getTimelineEntries(metaClient: HoodieTableMetaClient,
                                 limit: Int,
                                 showArchived: Boolean,
                                 startTime: String,
                                 endTime: String): Seq[Row] = {

    val instantInfoMap = buildInstantInfoFromTimeline(metaClient)

    val activeRollbackInfoMap = getRolledBackInstantInfo(metaClient.getActiveTimeline, metaClient)
    val archivedRollbackInfoMap = if (showArchived) {
      getRolledBackInstantInfo(metaClient.getArchivedTimeline, metaClient)
    } else {
      Map.empty[String, List[String]]
    }

    val activeEntries = getTimelineEntriesFromTimeline(
      metaClient.getActiveTimeline, "ACTIVE", metaClient, instantInfoMap, activeRollbackInfoMap, limit, startTime, endTime
    )

    val finalEntries = if (showArchived) {
      val archivedEntries = getTimelineEntriesFromTimeline(
        metaClient.getArchivedTimeline, "ARCHIVED", metaClient, instantInfoMap, archivedRollbackInfoMap, limit, startTime, endTime
      )
      val combinedEntries = (activeEntries ++ archivedEntries)
        .sortWith((a, b) => a.getString(0) > b.getString(0))

      if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
        combinedEntries
      } else {
        combinedEntries.take(limit)
      }
    } else {
      if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
        activeEntries
      } else {
        activeEntries.take(limit)
      }
    }

    finalEntries
  }

  private def getTimelineEntriesFromTimeline(timeline: HoodieTimeline,
                                             timelineType: String,
                                             metaClient: HoodieTableMetaClient,
                                             instantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]],
                                             rollbackInfoMap: Map[String, List[String]],
                                             limit: Int,
                                             startTime: String,
                                             endTime: String): Seq[Row] = {

    val instants = timeline.getInstants.iterator().asScala.toSeq

    val filteredInstants = if (startTime.trim.nonEmpty || endTime.trim.nonEmpty) {
      instants.filter { instant =>
        val instantTime = instant.requestedTime()
        val withinStartTime = if (startTime.trim.nonEmpty) instantTime >= startTime else true
        val withinEndTime = if (endTime.trim.nonEmpty) instantTime <= endTime else true
        withinStartTime && withinEndTime
      }
    } else {
      instants
    }

    val sortedInstants = filteredInstants.sortWith((a, b) => a.requestedTime() > b.requestedTime())

    val limitedInstants = if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
      sortedInstants
    } else {
      sortedInstants.take(limit)
    }

    limitedInstants.map { instant =>
      createTimelineEntry(instant, timelineType, metaClient, instantInfoMap, rollbackInfoMap)
    }
  }

  private def getRolledBackInstantInfo(timeline: HoodieTimeline, metaClient: HoodieTableMetaClient): Map[String, List[String]] = {
    val rollbackInfoMap = scala.collection.mutable.Map[String, scala.collection.mutable.ListBuffer[String]]()

    val rollbackInstants = timeline.filter(instant =>
      HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(instant.getAction)).getInstants

    rollbackInstants.asScala.foreach { rollbackInstant =>
      try {
        if (rollbackInstant.isInflight) {
          val instantToUse = metaClient.createNewInstant(
            HoodieInstant.State.REQUESTED, rollbackInstant.getAction, rollbackInstant.requestedTime())
          val metadata = timeline.readRollbackPlan(instantToUse)
          val rolledBackInstant = metadata.getInstantToRollback.getCommitTime
          rollbackInfoMap.getOrElseUpdate(rolledBackInstant, scala.collection.mutable.ListBuffer[String]())
            .append(rollbackInstant.requestedTime())
        } else {
          val metadata = timeline.readRollbackMetadata(rollbackInstant)
          metadata.getCommitsRollback.asScala.foreach { instant =>
            rollbackInfoMap.getOrElseUpdate(instant, scala.collection.mutable.ListBuffer[String]())
              .append(rollbackInstant.requestedTime())
          }
        }
      } catch {
        case _: Exception => // Skip invalid rollback instants
      }
    }
    rollbackInfoMap.map { case (k, v) => k -> v.toList }.toMap
  }

  private def getRollbackInfo(instant: HoodieInstant, timeline: HoodieTimeline, rollbackInfoMap: Map[String, List[String]], metaClient: HoodieTableMetaClient): String = {
    try {
      if (HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(instant.getAction)) {
        if (instant.isInflight) {
          val instantToUse = metaClient.createNewInstant(
            HoodieInstant.State.REQUESTED, instant.getAction, instant.requestedTime())
          val metadata = timeline.readRollbackPlan(instantToUse)
          s"Rolls back ${metadata.getInstantToRollback.getCommitTime}"
        } else {
          val metadata = timeline.readRollbackMetadata(instant)
          val rolledBackInstants = metadata.getCommitsRollback.asScala.mkString(", ")
          s"Rolled back: $rolledBackInstants"
        }
      } else {
        val instantTimestamp = instant.requestedTime()
        if (rollbackInfoMap.contains(instantTimestamp)) {
          s"Rolled back by: ${rollbackInfoMap(instantTimestamp).mkString(", ")}"
        } else {
          null
        }
      }
    } catch {
      case _: Exception => null
    }
  }

  private case class HoodieInstantWithModTime(state: HoodieInstant.State,
                                              action: String,
                                              requestedTime: String,
                                              completionTime: String,
                                              modificationTimeMs: Long
                                             ) {
    def getModificationTime: Long = modificationTimeMs
  }

  private def buildInstantInfoFromTimeline(metaClient: HoodieTableMetaClient): Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]] = {
    try {
      val storage = metaClient.getStorage
      val timelinePath = metaClient.getTimelinePath
      val instantFileNameParser = metaClient.getInstantFileNameParser
      val instantGenerator = metaClient.getInstantGenerator

      val instantMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[HoodieInstant.State, HoodieInstantWithModTime]]()

      val fileStream = HoodieTableMetaClient.scanFiles(storage, timelinePath, path => {
        val extension = instantFileNameParser.getTimelineFileExtension(path.getName)
        metaClient.getActiveTimeline.getValidExtensionsInActiveTimeline.contains(extension)
      })

      fileStream.forEach { storagePathInfo =>
        try {
          val instant = instantGenerator.createNewInstant(storagePathInfo)
          val instantWithModTime = HoodieInstantWithModTime(
            instant.getState,
            instant.getAction,
            instant.requestedTime(),
            instant.getCompletionTime,
            storagePathInfo.getModificationTime
          )

          instantMap.getOrElseUpdate(instant.requestedTime(), scala.collection.mutable.Map[HoodieInstant.State, HoodieInstantWithModTime]())
            .put(instant.getState, instantWithModTime)
        } catch {
          case _: Exception => // Skip invalid files
        }
      }
      instantMap.map { case (timestamp, stateMap) =>
        timestamp -> stateMap.toMap
      }.toMap
    } catch {
      case _: Exception => Map.empty[String, Map[HoodieInstant.State, HoodieInstantWithModTime]]
    }
  }

  private def getFormattedDateForState(instantTimestamp: String, state: HoodieInstant.State,
                                       instantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]]): String = {
    val stateMap = instantInfoMap.get(instantTimestamp)
    if (stateMap.isDefined) {
      val stateInfo = stateMap.get.get(state)
      if (stateInfo.isDefined) {
        val modificationTime = stateInfo.get.getModificationTime
        if (modificationTime > 0) {
          val date = new java.util.Date(modificationTime)
          val formatter = new java.text.SimpleDateFormat("MM-dd HH:mm:ss")
          formatter.format(date)
        } else {
          instantTimestamp
        }
      } else {
        null
      }
    } else {
      null
    }
  }

  private def createTimelineEntry(instant: HoodieInstant, timelineType: String, metaClient: HoodieTableMetaClient,
                                  instantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]],
                                  rollbackInfoMap: Map[String, List[String]]): Row = {

    val rollbackInfo = getRollbackInfo(instant,
      if (timelineType.equals("ARCHIVED")) metaClient.getArchivedTimeline else metaClient.getActiveTimeline,
      rollbackInfoMap, metaClient)

    val instantTimestamp = instant.requestedTime()

    val requestedTime = getFormattedDateForState(instantTimestamp, HoodieInstant.State.REQUESTED, instantInfoMap)
    val inFlightTime = getFormattedDateForState(instantTimestamp, HoodieInstant.State.INFLIGHT, instantInfoMap)
    val completedTime = getFormattedDateForState(instantTimestamp, HoodieInstant.State.COMPLETED, instantInfoMap)

    Row(
      instant.requestedTime(),
      instant.getAction,
      instant.getState.name(),
      requestedTime,
      inFlightTime,
      completedTime,
      timelineType,
      rollbackInfo
    )
  }
}

object ShowTimelineProcedure {
  val NAME = "show_timeline"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowTimelineProcedure()
  }
}
