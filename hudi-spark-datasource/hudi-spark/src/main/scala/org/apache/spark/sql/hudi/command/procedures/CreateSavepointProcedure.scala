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
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.{HoodieException, HoodieSavepointException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class CreateSavepointProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "commit_time", DataTypes.StringType, ""),
    ProcedureParameter.optional(2, "user", DataTypes.StringType, ""),
    ProcedureParameter.optional(3, "comments", DataTypes.StringType, ""),
    ProcedureParameter.optional(4, "path", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("create_savepoint_result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(4))
    var commitTime = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val user = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val comments = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val completedTimeline: HoodieTimeline = metaClient.getCommitsTimeline.filterCompletedInstants
    if (StringUtils.isNullOrEmpty(commitTime)) {
      commitTime = completedTimeline.lastInstant.get.getTimestamp
    } else if (!completedTimeline.containsInstant(commitTime)) {
      throw new HoodieException("Commit " + commitTime + " not found in Commits " + completedTimeline)
    }

    val client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty,
      tableName.asInstanceOf[Option[String]])
    var result = false

    try {
      client.savepoint(commitTime, user, comments)
      logInfo(s"The commit $commitTime has been savepointed.")
      result = true
    } catch {
      case _: HoodieSavepointException =>
        logWarning(s"Failed: Could not create savepoint $commitTime.")
    } finally {
      client.close()
    }

    Seq(Row(result))
  }

  override def build: Procedure = new CreateSavepointProcedure()
}

object CreateSavepointProcedure {
  val NAME: String = "create_savepoint"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): CreateSavepointProcedure = new CreateSavepointProcedure()
  }
}
