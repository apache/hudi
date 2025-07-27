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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Collections
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowCleanPlansProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {

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

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    val metaClient = createMetaClient(jsc, basePath)

    getCleanerPlans(metaClient, limit)
  }

  override def build: Procedure = new ShowCleanPlansProcedure()

  private def getCleanerPlans(metaClient: HoodieTableMetaClient, limit: Int): Seq[Row] = {
    val activeTimeline = metaClient.getActiveTimeline
    val rows = new util.ArrayList[Row]
    val cleanerInstants: util.List[HoodieInstant] = activeTimeline.getCleanerTimeline.getInstants
    val sortedCleanInstants = new util.ArrayList[HoodieInstant](cleanerInstants)
    val layout = TimelineLayout.fromVersion(activeTimeline.getTimelineLayoutVersion)
    Collections.sort(sortedCleanInstants, layout.getInstantComparator.requestedTimeOrderedComparator.reversed)

    for (i <- 0 until sortedCleanInstants.size) {
      val cleanInstant = sortedCleanInstants.get(i)

      try {
        val requestedCleanInstant = metaClient.createNewInstant(HoodieInstant.State.REQUESTED,
          cleanInstant.getAction, cleanInstant.requestedTime())
        val cleanerPlan = activeTimeline.readCleanerPlan(requestedCleanInstant)

        val earliestInstantToRetain = if (cleanerPlan.getEarliestInstantToRetain != null) {
          cleanerPlan.getEarliestInstantToRetain.getTimestamp
        } else null

        val totalPartitionsToClean = if (cleanerPlan.getFilePathsToBeDeletedPerPartition != null) {
          cleanerPlan.getFilePathsToBeDeletedPerPartition.size()
        } else 0

        val totalPartitionsToDelete = if (cleanerPlan.getPartitionsToBeDeleted != null) {
          cleanerPlan.getPartitionsToBeDeleted.size()
        } else 0

        val extraMetadataStr = if (cleanerPlan.getExtraMetadata != null) {
          cleanerPlan.getExtraMetadata.asScala.map { case (k, v) => s"$k=$v" }.mkString(", ")
        } else null

        rows.add(Row(
          cleanInstant.requestedTime,
          cleanInstant.getAction,
          earliestInstantToRetain,
          cleanerPlan.getLastCompletedCommitTimestamp,
          cleanerPlan.getPolicy,
          cleanerPlan.getVersion,
          totalPartitionsToClean,
          totalPartitionsToDelete,
          extraMetadataStr
        ))
      } catch {
        case e: Exception =>
          logWarning(s"Failed to read cleaner plan for instant ${cleanInstant.requestedTime}", e)
      }
    }

    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }
}

object ShowCleanPlansProcedure {
  val NAME = "show_clean_plans"

  def builder: Supplier[ProcedureBuilder] = () => new ShowCleanPlansProcedure()
}
