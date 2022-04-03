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
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.util.Option
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class RollbackToInstantTimeProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.required(1, "instant_time", DataTypes.StringType, None))

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("rollback_result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val instantTime = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, new TableIdentifier(table))
    val basePath = hoodieCatalogTable.tableLocation
    val client = createHoodieClient(jsc, basePath)
    val config = getWriteConfig(basePath)
    val metaClient = HoodieTableMetaClient.builder
      .setConf(jsc.hadoopConfiguration)
      .setBasePath(config.getBasePath)
      .setLoadActiveTimelineOnLoad(false)
      .setConsistencyGuardConfig(config.getConsistencyGuardConfig)
      .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion)))
      .build

    val activeTimeline = metaClient.getActiveTimeline
    val completedTimeline: HoodieTimeline = activeTimeline.getCommitsTimeline.filterCompletedInstants
    val filteredTimeline = completedTimeline.containsInstant(instantTime)
    if (!filteredTimeline) {
      throw new HoodieException(s"Commit $instantTime not found in Commits $completedTimeline")
    }

    val result = if (client.rollback(instantTime)) true else false
    val outputRow = Row(result)

    Seq(outputRow)
  }

  override def build: Procedure = new RollbackToInstantTimeProcedure()
}

object RollbackToInstantTimeProcedure {
  val NAME: String = "rollback_to_instant"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): RollbackToInstantTimeProcedure = new RollbackToInstantTimeProcedure()
  }
}
