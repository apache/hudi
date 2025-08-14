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

class ShowCleansPlanProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {

  import ShowCleansPlanProcedure._

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]

    validateInputs(tableName, limit)

    Try {
      val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, tableName)
      val metaClient = createMetaClient(jsc, hoodieCatalogTable.tableLocation)
      getCleanerPlans(metaClient, limit)
    } match {
      case Success(result) => result
      case Failure(exception) =>
        logError(s"Failed to retrieve clean plan information for table '$tableName'", exception)
        throw new HoodieException(s"Error retrieving clean plans for table '$tableName': ${exception.getMessage}", exception)
    }
  }

  override def build: Procedure = new ShowCleansPlanProcedure()

  private def validateInputs(tableName: String, limit: Int): Unit = {
    require(tableName.nonEmpty, "Table name cannot be empty")
    require(limit > 0, s"Limit must be positive, got: $limit")
  }

  private def getCleanerPlans(metaClient: HoodieTableMetaClient, limit: Int): Seq[Row] = {
    val activeTimeline = metaClient.getActiveTimeline
    val sortedCleanInstants = getSortedCleanInstants(activeTimeline)

    sortedCleanInstants.take(limit).map { cleanInstant =>
      processCleanPlan(metaClient, activeTimeline, cleanInstant)
    }
  }

  private def getSortedCleanInstants(timeline: org.apache.hudi.common.table.timeline.HoodieTimeline): Seq[HoodieInstant] = {
    val cleanInstants = timeline.getCleanerTimeline.getInstants.asScala
    val layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion)
    val comparator = layout.getInstantComparator.requestedTimeOrderedComparator.reversed()

    cleanInstants.sortWith((a, b) => comparator.compare(a, b) < 0)
  }

  private def processCleanPlan(metaClient: HoodieTableMetaClient,
                               activeTimeline: org.apache.hudi.common.table.timeline.HoodieTimeline,
                               cleanInstant: HoodieInstant): Row = {
    Try {
      val requestedCleanInstant = metaClient.getInstantGenerator.createNewInstant(
        HoodieInstant.State.REQUESTED,
        cleanInstant.getAction,
        cleanInstant.requestedTime()
      )
      val cleanerPlan = activeTimeline.readCleanerPlan(requestedCleanInstant)

      val planStats = extractCleanPlanStats(cleanerPlan)

      Row(
        cleanInstant.requestedTime(),
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
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("plan_time", DataTypes.StringType, nullable = true, Metadata.empty),
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
