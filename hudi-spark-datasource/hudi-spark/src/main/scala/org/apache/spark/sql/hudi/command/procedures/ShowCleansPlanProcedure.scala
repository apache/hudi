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
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, TimelineLayout}
import org.apache.hudi.exception.HoodieException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Spark SQL procedure to show clean plans for a Hudi table.
 *
 * This procedure displays information about clean operations that have been planned but not yet executed.
 * Clean plans contain metadata about which files are scheduled for deletion during the next clean operation.
 *
 * == Parameters ==
 * - `table`: Required. The name of the Hudi table to query
 * - `limit`: Optional. Maximum number of clean plans to return (default: 10)
 * - `showArchived`: Optional. Whether to include archived clean plans (default: false)
 * - `filter`: Optional. SQL expression to filter results (default: empty string)
 *
 * == Output Schema ==
 * - `plan_time`: Timestamp when the clean plan was created
 * - `state`: Current state of the clean plan (REQUESTED, INFLIGHT, COMPLETED)
 * - `action`: The action type (always 'clean')
 * - `earliest_instant_to_retain`: The earliest commit that will be retained after cleaning
 * - `last_completed_commit_timestamp`: The last completed commit at the time of planning
 * - `policy`: The clean policy used (e.g., KEEP_LATEST_COMMITS, KEEP_LATEST_FILE_VERSIONS)
 * - `version`: Version of the clean plan metadata format
 * - `total_partitions_to_clean`: Number of partitions that have files to be cleaned
 * - `total_partitions_to_delete`: Number of partitions that will have files deleted
 * - `extra_metadata`: Additional metadata associated with the clean plan
 *
 * == Error Handling ==
 * - Throws `IllegalArgumentException` for invalid filter expressions
 * - Throws `HoodieException` for table access issues
 * - Returns empty result set if no clean plans match the criteria
 *
 * == Filter Support ==
 * The `filter` parameter supports SQL expressions that can be applied to any output column.
 * The filter uses Spark SQL syntax and supports various data types and operations.
 *
 * === Filter Examples ===
 * {{{
 * -- Show clean plans created after a specific timestamp
 * CALL show_clean_plans(
 *   table => 'my_table',
 *   filter => "plan_time > '20241201000000'"
 * )
 *
 * -- Show clean plans that will clean many partitions
 * CALL show_clean_plans(
 *   table => 'my_table', 
 *   filter => "total_partitions_to_clean > 10"
 * )
 *
 * -- Show recent clean plans with complex conditions
 * CALL show_clean_plans(
 *   table => 'my_table',
 *   filter => "plan_time > '20241201000000' AND total_partitions_to_delete BETWEEN 1 AND 100"
 * )
 *
 * -- Show clean plans using string functions and policy filters
 * CALL show_clean_plans(
 *   table => 'my_table',
 *   filter => "LENGTH(earliest_instant_to_retain) > 10 AND policy = 'KEEP_LATEST_COMMITS'"
 * )
 *
 * -- Show clean plans with null checks and state filtering
 * CALL show_clean_plans(
 *   table => 'my_table',
 *   filter => "last_completed_commit_timestamp IS NOT NULL AND state = 'COMPLETED'"
 * )
 *
 * -- Show clean plans using IN operator for states
 * CALL show_clean_plans(
 *   table => 'my_table',
 *   filter => "state IN ('REQUESTED', 'INFLIGHT') AND total_partitions_to_clean > 0"
 * )
 * }}}
 *
 * == Usage Examples ==
 * {{{
 * -- Basic usage: Show last 10 clean plans
 * CALL show_clean_plans(table => 'hudi_table_2')
 *
 * -- Show more results with custom limit
 * CALL show_clean_plans(table => 'hudi_table_2', limit => 50)
 *
 * -- Include archived clean plans
 * CALL show_clean_plans(table => 'hudi_table_2', showArchived => true)
 *
 * -- Filter for recent clean plans
 * CALL show_clean_plans(
 *   table => 'hudi_table_2',
 *   filter => "plan_time > '20241201000000'"
 * )
 *
 * -- Show clean plans that will clean many partitions
 * CALL show_clean_plans(
 *   table => 'hudi_table_2',
 *   filter => "total_partitions_to_clean > 5",
 *   limit => 20
 * )
 * }}}
 *
 * @see [[ShowCleansProcedure]] for information about completed clean operations
 * @see [[HoodieProcedureFilterUtils]] for detailed filter expression syntax
 */
class ShowCleansPlanProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {

  import ShowCleansPlanProcedure._

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]
    val filter = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]

    validateInputs(tableName, limit)

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, outputType, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }

    val rows = Try {
      val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, tableName)
      val metaClient = createMetaClient(jsc, hoodieCatalogTable.tableLocation)
      getCleanerPlans(metaClient, limit, showArchived)
    } match {
      case Success(result) => result
      case Failure(exception) =>
        val errorMsg = s"Failed to retrieve clean plan information for table '$tableName'"
        logError(errorMsg, exception)
        throw new HoodieException(s"$errorMsg: ${exception.getMessage}", exception)
    }

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(rows, filter, outputType, sparkSession)
    } else {
      rows
    }
  }

  override def build: Procedure = new ShowCleansPlanProcedure()

  private def validateInputs(tableName: String, limit: Int): Unit = {
    require(tableName.nonEmpty, "Table name cannot be empty")
    require(limit > 0, s"Limit must be positive, got: $limit")
  }

  private def getCleanerPlans(metaClient: HoodieTableMetaClient, limit: Int, showArchived: Boolean): Seq[Row] = {
    val activeCleanInstants = getSortedCleanInstants(metaClient.getActiveTimeline)
      .take(limit)

    val cleanInstants = if (showArchived) {
      val archivedCleanInstants = getSortedCleanInstants(metaClient.getArchivedTimeline)
        .take(limit)
      (activeCleanInstants ++ archivedCleanInstants)
        .sortWith((a, b) => a.requestedTime() > b.requestedTime())
        .take(limit)
    } else {
      activeCleanInstants
    }

    cleanInstants.map { cleanInstant =>
      processCleanPlan(metaClient, metaClient.getActiveTimeline, cleanInstant)
    }
  }

  private def getSortedCleanInstants(timeline: org.apache.hudi.common.table.timeline.HoodieTimeline): Seq[HoodieInstant] = {
    // Get both inflight and completed clean instants
    val cleanInstants = timeline.getCleanerTimeline.getInstants.asScala.toSeq
    val layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion)
    val comparator = layout.getInstantComparator.requestedTimeOrderedComparator.reversed()

    cleanInstants.sortWith((a, b) => comparator.compare(a, b) < 0)
  }

  private def processCleanPlan(metaClient: HoodieTableMetaClient,
                               timeline: org.apache.hudi.common.table.timeline.HoodieTimeline,
                               cleanInstant: HoodieInstant): Row = {
    Try {
      val requestedCleanInstant = metaClient.getInstantGenerator.createNewInstant(
        HoodieInstant.State.REQUESTED,
        cleanInstant.getAction,
        cleanInstant.requestedTime()
      )
      val cleanerPlan = timeline.readCleanerPlan(requestedCleanInstant)

      val planStats = extractCleanPlanStats(cleanerPlan)

      Row(
        cleanInstant.requestedTime(),
        cleanInstant.getState.toString,
        cleanInstant.getAction,
        planStats.earliestInstantToRetain,
        cleanerPlan.getLastCompletedCommitTimestamp,
        cleanerPlan.getPolicy,
        cleanerPlan.getVersion,
        planStats.totalPartitionsToClean,
        planStats.totalPartitionsToDelete,
        planStats.extraMetadata
      )
    } match {
      case Success(row) => row
      case Failure(exception) =>
        logWarning(s"Failed to read cleaner plan for instant ${cleanInstant.requestedTime()}", exception)
        createErrorRow(cleanInstant)
    }
  }

  private def extractCleanPlanStats(cleanerPlan: org.apache.hudi.avro.model.HoodieCleanerPlan): CleanPlanStatistics = {
    val earliestInstantToRetain = Option(cleanerPlan.getEarliestInstantToRetain)
      .map(_.getTimestamp)
      .orNull

    val totalPartitionsToClean = Option(cleanerPlan.getFilePathsToBeDeletedPerPartition)
      .map(_.size())
      .getOrElse(0)

    val totalPartitionsToDelete = Option(cleanerPlan.getPartitionsToBeDeleted)
      .map(_.size())
      .getOrElse(0)

    val extraMetadata = Option(cleanerPlan.getExtraMetadata)
      .filter(!_.isEmpty)
      .map(_.asScala.map { case (k, v) => s"$k=$v" }.mkString(", "))
      .orNull

    CleanPlanStatistics(
      earliestInstantToRetain = earliestInstantToRetain,
      totalPartitionsToClean = totalPartitionsToClean,
      totalPartitionsToDelete = totalPartitionsToDelete,
      extraMetadata = extraMetadata
    )
  }

  private def createErrorRow(cleanInstant: HoodieInstant): Row = {
    Row(
      cleanInstant.requestedTime(),
      cleanInstant.getState.toString,
      cleanInstant.getAction,
      null, // earliest_instant_to_retain
      null, // last_completed_commit_timestamp
      null, // policy
      null, // version
      null, // total_partitions_to_clean
      null, // total_partitions_to_delete
      null // extra_metadata
    )
  }

  private case class CleanPlanStatistics(
                                          earliestInstantToRetain: String,
                                          totalPartitionsToClean: Int,
                                          totalPartitionsToDelete: Int,
                                          extraMetadata: String
                                        )
}

object ShowCleansPlanProcedure {
  val NAME = "show_clean_plans"

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(2, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(3, "filter", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("plan_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("earliest_instant_to_retain", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("last_completed_commit_timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("policy", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("version", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("total_partitions_to_clean", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("total_partitions_to_delete", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("extra_metadata", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def builder: Supplier[ProcedureBuilder] = () => new ShowCleansPlanProcedure()
}
