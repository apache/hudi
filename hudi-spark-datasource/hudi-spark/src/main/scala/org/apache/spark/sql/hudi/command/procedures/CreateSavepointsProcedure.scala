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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.exception.{HoodieException, HoodieSavepointException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class CreateSavepointsProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.required(1, "commit_Time", DataTypes.StringType, None),
    ProcedureParameter.optional(2, "user", DataTypes.StringType, ""),
    ProcedureParameter.optional(3, "comments", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("create_savepoint_result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val commitTime = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val user = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val comments = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]

    val basePath: String = getBasePath(tableName)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build

    val activeTimeline: HoodieActiveTimeline = metaClient.getActiveTimeline
    if (!activeTimeline.getCommitsTimeline.filterCompletedInstants.containsInstant(commitTime)) {
      throw new HoodieException("Commit " + commitTime + " not found in Commits " + activeTimeline)
    }

    val client = createHoodieClient(jsc, basePath)
    var result = false

    try {
      client.savepoint(commitTime, user, comments)
      logInfo(s"The commit $commitTime has been savepointed.")
      result = true
    } catch {
      case _: HoodieSavepointException =>
        logWarning(s"Failed: Could not create savepoint $commitTime.")
    }

    Seq(Row(result))
  }

  override def build: Procedure = new CreateSavepointsProcedure()
}

object CreateSavepointsProcedure {
  val NAME: String = "create_savepoints"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): CreateSavepointsProcedure = new CreateSavepointsProcedure()
  }
}



