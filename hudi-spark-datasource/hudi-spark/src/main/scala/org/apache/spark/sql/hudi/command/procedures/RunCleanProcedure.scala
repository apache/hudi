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

import java.util.function.Supplier
import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.util.JsonUtils
import org.apache.hudi.config.HoodieCleanConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

class RunCleanProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "skipLocking", DataTypes.BooleanType, false),
    ProcedureParameter.optional(2, "scheduleInLine", DataTypes.BooleanType, true),
    ProcedureParameter.optional(3, "cleanPolicy", DataTypes.StringType, None),
    ProcedureParameter.optional(4, "retainCommits", DataTypes.IntegerType, 10)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("start_clean_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_files_deleted", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("earliest_commit_to_retain", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("bootstrap_part_metadata", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("version", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  override def build: Procedure = new RunCleanProcedure

  /**
   * Returns the input parameters of this procedure.
   */
  override def parameters: Array[ProcedureParameter] = PARAMETERS

  /**
   * Returns the type of rows produced by this procedure.
   */
  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val skipLocking = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Boolean]
    val scheduleInLine = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]
    val cleanPolicy = getArgValueOrDefault(args, PARAMETERS(3))
    val retainCommits = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Integer]
    val basePath = getBasePath(tableName, Option.empty)
    val cleanInstantTime = HoodieActiveTimeline.createNewInstantTime()
    var props: Map[String, String] = Map(
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> String.valueOf(retainCommits)
    )
    if (cleanPolicy.isDefined) {
      props += (HoodieCleanConfig.CLEANER_POLICY.key() -> String.valueOf(cleanPolicy.get))
    }
    val client = HoodieCLIUtils.createHoodieClientFromPath(sparkSession, basePath, props)
    val hoodieCleanMeta = client.clean(cleanInstantTime, scheduleInLine, skipLocking)

    if (hoodieCleanMeta == null) Seq(Row.empty)
    else Seq(Row(hoodieCleanMeta.getStartCleanTime,
      hoodieCleanMeta.getTimeTakenInMillis,
      hoodieCleanMeta.getTotalFilesDeleted,
      hoodieCleanMeta.getEarliestCommitToRetain,
      JsonUtils.getObjectMapper.writeValueAsString(hoodieCleanMeta.getBootstrapPartitionMetadata),
      hoodieCleanMeta.getVersion))
  }
}

object RunCleanProcedure {
  val NAME = "run_clean"

  def builder : Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RunCleanProcedure
  }
}
