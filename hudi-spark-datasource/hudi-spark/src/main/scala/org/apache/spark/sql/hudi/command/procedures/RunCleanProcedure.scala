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
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.util.JsonUtils
import org.apache.hudi.config.HoodieCleanConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class RunCleanProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "schedule_in_line", DataTypes.BooleanType, true),
    ProcedureParameter.optional(2, "clean_policy", DataTypes.StringType),
    ProcedureParameter.optional(3, "retain_commits", DataTypes.IntegerType),
    ProcedureParameter.optional(4, "hours_retained", DataTypes.IntegerType),
    ProcedureParameter.optional(5, "file_versions_retained", DataTypes.IntegerType),
    ProcedureParameter.optional(6, "trigger_strategy", DataTypes.StringType),
    ProcedureParameter.optional(7, "trigger_max_commits", DataTypes.IntegerType),
    ProcedureParameter.optional(8, "options", DataTypes.StringType)
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
    val scheduleInLine = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Boolean]
    var confs: Map[String, String] = Map.empty
    if (getArgValueOrDefault(args, PARAMETERS(2)).isDefined) {
      confs += HoodieCleanConfig.CLEANER_POLICY.key() -> getArgValueOrDefault(args, PARAMETERS(2)).get.toString
    }
    if (getArgValueOrDefault(args, PARAMETERS(3)).isDefined) {
      confs += HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> getArgValueOrDefault(args, PARAMETERS(3)).get.toString
    }
    if (getArgValueOrDefault(args, PARAMETERS(4)).isDefined) {
      confs += HoodieCleanConfig.CLEANER_HOURS_RETAINED.key() -> getArgValueOrDefault(args, PARAMETERS(4)).get.toString
    }
    if (getArgValueOrDefault(args, PARAMETERS(5)).isDefined) {
      confs += HoodieCleanConfig.CLEANER_FILE_VERSIONS_RETAINED.key() -> getArgValueOrDefault(args, PARAMETERS(5)).get.toString
    }
    if (getArgValueOrDefault(args, PARAMETERS(6)).isDefined) {
      confs += HoodieCleanConfig.CLEAN_TRIGGER_STRATEGY.key() -> getArgValueOrDefault(args, PARAMETERS(6)).get.toString
    }
    if (getArgValueOrDefault(args, PARAMETERS(7)).isDefined) {
      confs += HoodieCleanConfig.CLEAN_MAX_COMMITS.key() -> getArgValueOrDefault(args, PARAMETERS(7)).get.toString
    }
    if (getArgValueOrDefault(args, PARAMETERS(8)).isDefined) {
      confs ++= HoodieCLIUtils.extractOptions(getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[String])
    }

    val basePath = getBasePath(tableName, Option.empty)

    var client: SparkRDDWriteClient[_] = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, confs,
        tableName.asInstanceOf[Option[String]])
      val hoodieCleanMeta = client.clean(scheduleInLine)

      if (hoodieCleanMeta == null) Seq.empty
      else Seq(Row(hoodieCleanMeta.getStartCleanTime,
        hoodieCleanMeta.getTimeTakenInMillis,
        hoodieCleanMeta.getTotalFilesDeleted,
        hoodieCleanMeta.getEarliestCommitToRetain,
        JsonUtils.getObjectMapper.writeValueAsString(hoodieCleanMeta.getBootstrapPartitionMetadata),
        hoodieCleanMeta.getVersion))
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }
}

object RunCleanProcedure {
  val NAME = "run_clean"

  def builder : Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RunCleanProcedure
  }
}
