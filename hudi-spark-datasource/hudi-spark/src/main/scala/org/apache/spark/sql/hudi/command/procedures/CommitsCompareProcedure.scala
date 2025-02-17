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
import org.apache.hudi.common.table.timeline.InstantComparison
import org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

class CommitsCompareProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "path", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("compare_detail", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val path = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    val source = createMetaClient(jsc, basePath)
    val target = createMetaClient(jsc, path)
    val sourceTimeline = source.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val targetTimeline = target.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val targetLatestCommit =
      if (targetTimeline.getInstants.iterator().hasNext) targetTimeline.lastInstant.get.requestedTime else "0"
    val sourceLatestCommit =
      if (sourceTimeline.getInstants.iterator().hasNext) sourceTimeline.lastInstant.get.requestedTime else "0"

    if (sourceLatestCommit != null && compareTimestamps(targetLatestCommit, InstantComparison.GREATER_THAN, sourceLatestCommit)) { // source is behind the target
      val commitsToCatchup = targetTimeline.findInstantsAfter(sourceLatestCommit, Integer.MAX_VALUE).getInstants.iterator().asScala.map(instant => instant.requestedTime).toList.asJava
      Seq(Row("Source " + source.getTableConfig.getTableName + " is behind by " + commitsToCatchup.size + " commits. Commits to catch up - " + commitsToCatchup))
    } else {
      val commitsToCatchup = sourceTimeline.findInstantsAfter(targetLatestCommit, Integer.MAX_VALUE).getInstants.iterator().asScala.map(instant => instant.requestedTime).toList.asJava
      Seq(Row("Source " + source.getTableConfig.getTableName + " is ahead by " + commitsToCatchup.size + " commits. Commits to catch up - " + commitsToCatchup))
    }
  }

  override def build: Procedure = new CommitsCompareProcedure()
}

object CommitsCompareProcedure {
  val NAME = "commits_compare"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new CommitsCompareProcedure()
  }
}
