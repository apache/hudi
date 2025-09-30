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
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.{HoodieException, HoodieSavepointException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class RollbackToSavepointProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "instant_time", DataTypes.StringType, ""),
    ProcedureParameter.optional(2, "path", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("rollback_savepoint_result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(2))
    var instantTime = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)
    val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
    val completedInstants = metaClient.getActiveTimeline.getSavePointTimeline.filterCompletedInstants
    if (completedInstants.empty) throw new HoodieException("There are no completed savepoint to run delete")
    if (StringUtils.isNullOrEmpty(instantTime)) {
      instantTime = completedInstants.lastInstant.get.requestedTime
    }
    val savePoint = instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, instantTime)

    if (!completedInstants.containsInstant(savePoint)) {
      throw new HoodieException("Commit " + instantTime + " not found in Commits " + completedInstants)
    }

    val client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty,
      tableName.asInstanceOf[Option[String]])
    var result = false

    try {
      client.restoreToSavepoint(instantTime)
      if (tableName.isDefined) {
        spark.catalog.refreshTable(tableName.get.asInstanceOf[String])
      }
      logInfo(s"The commit $instantTime rolled back.")
      result = true
    } catch {
      case _: HoodieSavepointException =>
        logError(s"Failed to roll back commit at instant time: $instantTime")
    } finally {
      client.close()
    }

    Seq(Row(result))
  }

  override def build: Procedure = new RollbackToSavepointProcedure()
}

object RollbackToSavepointProcedure {
  val NAME: String = "rollback_to_savepoint"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): RollbackToSavepointProcedure = new RollbackToSavepointProcedure()
  }
}
