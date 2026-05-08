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
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.time.Duration
import java.util.Date
import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL stored procedure to list all pending inflight and requested instants on a Hudi table.
 *
 * Unlike cleanup_stale_inflight_commits (which targets the write timeline only), this procedure
 * queries the full active timeline and therefore includes inflight rollback, clean, restore, and
 * indexing instants in addition to write-action instants. A stale rollback or clean inflight
 * visible here cannot be cleaned by cleanup_stale_inflight_commits.
 *
 * Parameters:
 *   - table:           Required. Catalog name of the Hudi table.
 *   - min_age_minutes: Optional (default 0). When > 0, only instants older than this many minutes
 *                      are returned. When 0 (default), all pending instants are returned.
 *
 * Output columns (one row per pending instant):
 *   - instant_time: The instant's requested timestamp.
 *   - action:       The action type (commit, delta_commit, compaction, replace, rollback, clean, etc.).
 *   - state:        The instant's state (REQUESTED or INFLIGHT).
 *
 * Example usage:
 * {{{
 *   -- Show all pending inflights
 *   CALL show_inflight_commits(table => 'my_table');
 *
 *   -- Show only inflights older than 2 hours
 *   CALL show_inflight_commits(table => 'my_table', min_age_minutes => 120);
 * }}}
 */
class ShowInflightCommitsProcedure extends BaseProcedure with ProcedureBuilder {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "min_age_minutes", DataTypes.IntegerType, 0)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action",       DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("state",        DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def build: Procedure = new ShowInflightCommitsProcedure()

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName     = getArgValueOrDefault(args, PARAMETERS(0))
    val minAgeMinutes = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]

    val basePath = getBasePath(tableName)
    val metaClient = HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration))
      .setBasePath(basePath)
      .build

    val baseTimeline = metaClient.reloadActiveTimeline().filterInflightsAndRequested()

    val timeline = if (minAgeMinutes > 0) {
      val goBackMs = Duration.ofMinutes(minAgeMinutes).getSeconds * 1000L
      val cutoff = HoodieInstantTimeGenerator.formatDate(new Date(System.currentTimeMillis() - goBackMs))
      baseTimeline.findInstantsBefore(cutoff)
    } else {
      baseTimeline
    }

    timeline.getInstants.asScala.map { instant =>
      Row(instant.requestedTime, instant.getAction, instant.getState.name())
    }.toSeq
  }
}

object ShowInflightCommitsProcedure {
  val NAME = "show_inflight_commits"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowInflightCommitsProcedure()
  }
}
