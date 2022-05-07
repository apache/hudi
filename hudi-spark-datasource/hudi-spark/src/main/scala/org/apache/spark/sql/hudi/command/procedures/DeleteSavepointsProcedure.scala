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
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.exception.{HoodieException, HoodieSavepointException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class DeleteSavepointsProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.required(1, "instant_time", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("delete_savepoint_result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val instantTime = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]

    val basePath: String = getBasePath(tableName)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build

    val completedInstants = metaClient.getActiveTimeline.getSavePointTimeline.filterCompletedInstants
    if (completedInstants.empty) throw new HoodieException("There are no completed savepoint to run delete")
    val savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, instantTime)

    if (!completedInstants.containsInstant(savePoint)) {
      throw new HoodieException("Commit " + instantTime + " not found in Commits " + completedInstants)
    }

    val client = createHoodieClient(jsc, basePath)
    var result = false

    try {
      client.deleteSavepoint(instantTime)
      logInfo(s"The commit $instantTime has been deleted savepoint.")
      result = true
    } catch {
      case _: HoodieSavepointException =>
        logWarning(s"Failed: Could not delete savepoint $instantTime.")
    }

    Seq(Row(result))
  }

  override def build: Procedure = new DeleteSavepointsProcedure()
}

object DeleteSavepointsProcedure {
  val NAME: String = "delete_savepoints"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): DeleteSavepointsProcedure = new DeleteSavepointsProcedure()
  }
}




