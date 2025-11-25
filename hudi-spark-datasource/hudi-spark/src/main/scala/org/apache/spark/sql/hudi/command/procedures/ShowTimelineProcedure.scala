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
import org.apache.hudi.common.table.timeline.TimelineLayout

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

    validateFilter(filter, outputType)

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val timelineEntries = getTimelineEntries(metaClient, limit, showArchived, startTime, endTime)

    applyFilter(timelineEntries, filter, outputType)
  }

  override def build: Procedure = new ShowTimelineProcedure()

  /**
   * Retrieves timeline entries from both active and archived timelines based on the provided parameters.
   *
   * This is the main orchestration method that coordinates the retrieval of timeline entries. It:
   * 1. Builds instant information map from the active timeline (for modification time lookups)
   * 2. Loads rollback information for both active and archived timelines
   * 3. Optionally loads archived timeline details if `showArchived` is true
   * 4. Retrieves entries from active timeline
   * 5. Optionally retrieves entries from archived timeline and combines them
   * 6. Sorts combined entries by timestamp (descending) and state priority
   * 7. Applies limit or time range filtering as appropriate
   *
   * @param metaClient The Hudi table metadata client for accessing timeline data
   * @param limit Maximum number of entries to return (ignored if time range is specified)
   * @param showArchived Whether to include archived timeline entries in the result
   * @param startTime Optional start timestamp for filtering (format: yyyyMMddHHmmss). If empty, no start filtering is applied
   * @param endTime Optional end timestamp for filtering (format: yyyyMMddHHmmss). If empty, no end filtering is applied
   * @return Sequence of Row objects representing timeline entries, sorted by timestamp (descending) and state priority
   *         (COMPLETED > INFLIGHT > REQUESTED)
   *
   * @note When `showArchived` is true, this method will:
   *       - Reload the archived timeline to ensure it's up-to-date
   *       - Load completed instant details and compaction details into memory for the specified time range or limit
   *       - Combine active and archived entries, with archived entries marked with "ARCHIVED" timeline type
   *
   * @note The sorting logic prioritizes:
   *       1. Timestamp (newer first)
   *       2. State (COMPLETED > INFLIGHT > REQUESTED)
   *
   * @see [[getTimelineEntriesFromTimeline]] for extracting entries from a specific timeline
   * @see [[buildInstantInfoFromTimeline]] for building the instant information map
   */
  private def getTimelineEntries(metaClient: HoodieTableMetaClient,
                                 limit: Int,
                                 showArchived: Boolean,
                                 startTime: String,
                                 endTime: String): Seq[Row] = {

    val instantInfoMap = buildInstantInfoFromTimeline(metaClient)

    val activeRollbackInfoMap = getRolledBackInstantInfo(metaClient.getActiveTimeline, metaClient)

    // Create archived timeline starting from the maximum instant time in active timeline
    // This way, if all archived instants are older than the active timeline's max instant,
    // the archived timeline will be empty and won't load anything, avoiding unnecessary loading.
    // Instead of getArchivedTimeline() which loads with LoadMode.ACTION, we use the startTs
    // constructor which loads with LoadMode.METADATA, and then load specific details (PLAN for compactions).
    val (archivedTimeline, archivedRollbackInfoMap) = if (showArchived) {
      // Get the maximum instant time from active timeline
      val maxActiveInstantTime = {
        val lastInstantOpt = metaClient.getActiveTimeline
          .filterCompletedInstants()
          .firstInstant()
        if (lastInstantOpt.isPresent) {
          lastInstantOpt.get().requestedTime()
        } else {
          HoodieTimeline.INIT_INSTANT_TS
        }
      }
      // Create archived timeline starting from max active instant time
      // This will be empty as all archived instants are older than active timeline
      val timeline = if (maxActiveInstantTime.nonEmpty) {
        metaClient.getTableFormat().getTimelineFactory()
          .createArchivedTimeline(metaClient, maxActiveInstantTime)
      } else {
        metaClient.getArchivedTimeline()
      }
      // Load the required details with appropriate LoadMode (METADATA for commits, PLAN for compactions)
      // Note: loadCompletedInstantDetailsInMemory may have already loaded METADATA via constructor,
      // but we call it again to ensure we have the data for the specified time range or limit.
      if (startTime.nonEmpty && endTime.nonEmpty) {
        timeline.loadCompletedInstantDetailsInMemory(startTime, endTime)
        timeline.loadCompactionDetailsInMemory(startTime, endTime)
      } else {
        timeline.loadCompletedInstantDetailsInMemory(limit)
        timeline.loadCompactionDetailsInMemory(limit)
      }
      val rollbackInfoMap = getRolledBackInstantInfo(timeline, metaClient)
      (timeline, rollbackInfoMap)
    } else {
      (null, Map.empty[String, List[String]])
    }

    val finalEntries = if (showArchived) {
      // Collect instants from both timelines, sort using RequestedTimeBasedComparator, then convert to rows
      val activeInstants = getTimelineEntriesFromTimeline(
        metaClient.getActiveTimeline, "ACTIVE", metaClient, instantInfoMap, activeRollbackInfoMap, limit, startTime, endTime, returnInstants = true
      )
      val archivedInstants = getTimelineEntriesFromTimeline(
        archivedTimeline, "ARCHIVED", metaClient, instantInfoMap, archivedRollbackInfoMap, limit, startTime, endTime, returnInstants = true
      )
      val layout = TimelineLayout.fromVersion(metaClient.getActiveTimeline.getTimelineLayoutVersion)
      val comparator = layout.getInstantComparator.requestedTimeOrderedComparator.reversed()
      val sortedInstants = (activeInstants ++ archivedInstants)
        .asInstanceOf[Seq[(HoodieInstant, String)]]
        .sortWith((a, b) => comparator.compare(a._1, b._1) < 0)

      sortedInstants.map { case (instant, timelineType) =>
        createTimelineEntry(instant, timelineType, metaClient, instantInfoMap,
          if (timelineType == "ACTIVE") activeRollbackInfoMap else archivedRollbackInfoMap)
      }
    } else {
      getTimelineEntriesFromTimeline(
        metaClient.getActiveTimeline, "ACTIVE", metaClient, instantInfoMap, activeRollbackInfoMap, limit, startTime, endTime, returnInstants = false
      ).asInstanceOf[Seq[Row]]
    }

    // Apply limit if time range is not fully specified
    if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
      finalEntries
    } else {
      finalEntries.take(limit)
    }
  }


  /**
   * Extracts timeline entries from a specific timeline (active or archived) and converts them to Row objects.
   *
   * This method processes instants from the given timeline by:
   * 1. Filtering instants by time range using timeline's built-in filtering methods if `startTime` or `endTime` are provided
   * 2. Retrieving filtered instants from the timeline
   * 3. Sorting filtered instants by timestamp in descending order (newest first)
   * 4. Applying limit if time range is not specified (when time range is specified, all matching entries are returned)
   * 5. Converting each instant to a Row using `createTimelineEntry`
   *
   * @param timeline The timeline to extract entries from (can be active or archived timeline)
   * @param timelineType The type of timeline: "ACTIVE" or "ARCHIVED". This is used to mark entries in the output
   * @param metaClient The Hudi table metadata client for accessing timeline metadata
   * @param instantInfoMap Map of instant timestamps to their state information and modification times.
   *                      Used for formatting dates in the output. Only contains active timeline entries.
   * @param rollbackInfoMap Map of instant timestamps to list of rollback instants that rolled them back.
   *                       Used for populating rollback information in the output
   * @param limit Maximum number of entries to return. Only applied if time range is not specified
   * @param startTime Optional start timestamp for filtering (format: yyyyMMddHHmmss).
   *                 If non-empty, only instants with timestamp >= startTime are included
   * @param endTime Optional end timestamp for filtering (format: yyyyMMddHHmmss).
   *               If non-empty, only instants with timestamp <= endTime are included
   * @return Sequence of Row objects representing timeline entries from the specified timeline,
   *         sorted by timestamp (descending), limited if applicable
   *
   * @note Time range filtering is inclusive on both ends (startTime <= instantTime <= endTime)
   * @note If both `startTime` and `endTime` are provided, uses `findInstantsInClosedRange`
   * @note If only `startTime` is provided, uses `findInstantsAfterOrEquals`
   * @note If only `endTime` is provided, uses `findInstantsBeforeOrEquals`
   * @note If both `startTime` and `endTime` are provided, the limit is ignored and all matching entries are returned
   * @note If neither `startTime` nor `endTime` are provided, the limit is applied to the sorted results
   *
   * @see [[createTimelineEntry]] for the conversion of HoodieInstant to Row
   * @see [[getTimelineEntries]] for the main method that orchestrates timeline entry retrieval
   * @see [[HoodieTimeline.findInstantsInClosedRange]] for range filtering implementation
   */
  private def getTimelineEntriesFromTimeline(timeline: HoodieTimeline,
                                             timelineType: String,
                                             metaClient: HoodieTableMetaClient,
                                             instantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]],
                                             rollbackInfoMap: Map[String, List[String]],
                                             limit: Int,
                                             startTime: String,
                                             endTime: String,
                                             returnInstants: Boolean = false): Seq[Any] = {
    // Use timeline's built-in filtering methods for better performance and consistency
    val filteredTimeline = {
      val startTimeTrimmed = startTime.trim
      val endTimeTrimmed = endTime.trim
      if (startTimeTrimmed.nonEmpty && endTimeTrimmed.nonEmpty) {
        // Both start and end time provided: use closed range [startTime, endTime]
        timeline.findInstantsInClosedRange(startTimeTrimmed, endTimeTrimmed)
      } else if (startTimeTrimmed.nonEmpty) {
        // Only start time provided: get instants >= startTime
        timeline.findInstantsAfterOrEquals(startTimeTrimmed)
      } else if (endTimeTrimmed.nonEmpty) {
        // Only end time provided: get instants <= endTime
        timeline.findInstantsBeforeOrEquals(endTimeTrimmed)
      } else {
        // No time filtering: use original timeline
        timeline
      }
    }

    val instants = filteredTimeline.getInstants.iterator().asScala.toSeq

    // Sort by timestamp in descending order (newest first)
    val sortedInstants = instants.sortWith((a, b) => a.requestedTime() > b.requestedTime())

    // Apply limit only if time range is not fully specified
    val limitedInstants = if (startTime.trim.nonEmpty && endTime.trim.nonEmpty) {
      sortedInstants
    } else {
      sortedInstants.take(limit)
    }

    if (returnInstants) {
      // Return instants with timeline type for sorting across multiple timelines
      limitedInstants.map((_, timelineType))
    } else {
      // Convert to rows immediately
      limitedInstants.map { instant =>
        createTimelineEntry(instant, timelineType, metaClient, instantInfoMap, rollbackInfoMap)
      }
    }
  }

  /**
   * Builds a map of rollback information by scanning rollback instants in the given timeline.
   *
   * This method processes all rollback instants in the timeline and extracts information about
   * which instants were rolled back by each rollback operation. For each rollback instant:
   * - If the rollback is INFLIGHT or REQUESTED: reads the rollback plan to get the instant that will be rolled back
   * - If the rollback is COMPLETED: reads the rollback metadata to get all instants that were rolled back
   *
   * The resulting map has:
   * - Key: The timestamp of the instant that was rolled back
   * - Value: List of rollback instant timestamps that rolled back this instant
   *
   * @param timeline The timeline to scan for rollback instants (can be active or archived timeline)
   * @param metaClient The Hudi table metadata client for creating instant objects
   * @return Map from rolled-back instant timestamp to list of rollback instant timestamps.
   *         Returns empty map if no rollback instants are found or if scanning fails.
   *
   * @note Invalid or corrupted rollback instants are silently skipped
   * @note This method handles both inflight/requested rollbacks (which have plans) and
   *       completed rollbacks (which have metadata)
   */
  private def getRolledBackInstantInfo(timeline: HoodieTimeline, metaClient: HoodieTableMetaClient): Map[String, List[String]] = {
    val rollbackInfoMap = scala.collection.mutable.Map[String, scala.collection.mutable.ListBuffer[String]]()

    val rollbackInstants = timeline.filter(instant =>
      HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(instant.getAction)).getInstants

    rollbackInstants.asScala.foreach { rollbackInstant =>
      try {
        if (!rollbackInstant.isCompleted) {
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

  /**
   * Gets rollback information string for a given instant.
   *
   * This method determines the rollback information to display for an instant:
   * - If the instant is a rollback action:
   *   - For INFLIGHT or REQUESTED rollbacks: returns "Rolls back {instantTime}" by reading the rollback plan
   *   - For COMPLETED rollbacks: returns "Rolled back: {list of rolled back instants}" by reading rollback metadata
   * - If the instant was rolled back by another rollback operation:
   *   - Returns "Rolled back by: {list of rollback instant timestamps}" from the rollbackInfoMap
   * - Otherwise: returns null (no rollback information)
   *
   * @param instant The instant to get rollback information for
   * @param timeline The timeline containing the instant (used to read rollback plans/metadata)
   * @param rollbackInfoMap Map of instant timestamps to list of rollback instant timestamps that rolled them back.
   *                       This is built by [[getRolledBackInstantInfo]]
   * @param metaClient The Hudi table metadata client for creating instant objects
   * @return Rollback information string, or null if there is no rollback information for this instant
   *
   * @note Invalid or corrupted rollback metadata is handled gracefully by returning null
   * @note This method handles both inflight/requested rollbacks (which have plans) and
   *       completed rollbacks (which have metadata)
   */
  private def getRollbackInfo(instant: HoodieInstant, timeline: HoodieTimeline, rollbackInfoMap: Map[String, List[String]], metaClient: HoodieTableMetaClient): String = {
    try {
      if (HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(instant.getAction)) {
        if (!instant.isCompleted) {
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

  /**
   * Builds a map of instant information (including modification times) by scanning the active timeline path.
   *
   * This method scans the active timeline directory and extracts instant information including:
   * - Instant state (REQUESTED, INFLIGHT, COMPLETED)
   * - Action type (commit, deltacommit, compaction, etc.)
   * - Requested time
   * - Completion time (if available)
   * - File modification time (used for formatting dates in the output)
   *
   * The resulting map is structured as:
   * - Key: Instant timestamp (requestedTime)
   * - Value: Map of State -> HoodieInstantWithModTime
   *
   * This allows looking up modification times for active timeline entries to format dates accurately.
   * For archived timeline entries, which are not included in this map, the `getFormattedDateForState`
   * method falls back to using completion time or requested time directly from the instant object.
   *
   * @param metaClient The Hudi table metadata client for accessing storage and timeline paths
   * @return Map from instant timestamp to a map of state -> instant information with modification time.
   *         Returns empty map if scanning fails or no instants are found.
   *
   * @note This method only scans the active timeline path (not the archive path), so:
   *       - Only active timeline entries will have modification times in the returned map
   *       - Archived entries won't be present in this map
   *       - For archived entries, date formatting uses completion time or requested time from the instant object
   *
   * @note Invalid files encountered during scanning are silently skipped
   *
   * @note The modification time is obtained from the file system metadata of the instant file,
   *       which represents when the file was last modified (i.e., when the instant transitioned to that state)
   *
   * @see [[HoodieInstantWithModTime]] for the structure containing instant information
   * @see [[getFormattedDateForState]] for how this map is used in date formatting
   * @see [[getTimelineEntries]] for where this method is called
   */
  private def buildInstantInfoFromTimeline(metaClient: HoodieTableMetaClient): Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]] = {
    try {
      val storage = metaClient.getStorage
      val timelinePath = metaClient.getTimelinePath  // Only scans active timeline path, not archive path
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
                                       instantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]],
                                       instant: Option[HoodieInstant] = None): String = {
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
      if (instant.isDefined) {
        val timeToFormat = state match {
          case HoodieInstant.State.REQUESTED =>
            instant.get.requestedTime()
          case HoodieInstant.State.INFLIGHT =>
            instant.get.requestedTime()
          case HoodieInstant.State.COMPLETED =>
            val completionTime = instant.get.getCompletionTime
            if (completionTime != null && completionTime.nonEmpty) {
              completionTime
            } else {
              instant.get.requestedTime()
            }
          case _ =>
            instant.get.getCompletionTime()
        }

        try {
          val formatter = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
          val date = formatter.parse(timeToFormat)
          val outputFormatter = new java.text.SimpleDateFormat("MM-dd HH:mm:ss")
          outputFormatter.format(date)
        } catch {
          case _: Exception => null
        }
      } else {
        null
      }
    }
  }

  private def createTimelineEntry(instant: HoodieInstant, timelineType: String, metaClient: HoodieTableMetaClient,
                                  instantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]],
                                  rollbackInfoMap: Map[String, List[String]]): Row = {

    val rollbackInfo = getRollbackInfo(instant,
      if (timelineType.equals("ARCHIVED")) metaClient.getArchivedTimeline else metaClient.getActiveTimeline,
      rollbackInfoMap, metaClient)

    val instantTimestamp = instant.requestedTime()

    val requestedTime = getFormattedDateForState(instantTimestamp, HoodieInstant.State.REQUESTED, instantInfoMap, Option(instant))
    val inFlightTime = getFormattedDateForState(instantTimestamp, HoodieInstant.State.INFLIGHT, instantInfoMap, Option(instant))
    val completedTime = getFormattedDateForState(instantTimestamp, HoodieInstant.State.COMPLETED, instantInfoMap, Option(instant))

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
