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
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.table.marker.WriteMarkersFactory

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.util.{Failure, Success, Try}

class DeleteMarkerProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "instant_time", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("delete_marker_result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val instantTime = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val basePath = getBasePath(tableName)
    val instantTimes = instantTime.split(",")
    var currentInstant = ""

    var client: SparkRDDWriteClient[_] = null
    val result = Try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty,
        tableName.asInstanceOf[Option[String]])
      val config = client.getConfig
      val context = client.getEngineContext
      val table = HoodieSparkTable.createForReads(config, context)
      for (it <- instantTimes) {
        currentInstant = it
        WriteMarkersFactory.get(config.getMarkersType, table, it)
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism)
      }
    } match {
      case Success(_) =>
        logInfo(s"Marker $instantTime deleted.")
        true
      case Failure(e) =>
        logWarning(s"Failed: Could not clean marker instantTime: $currentInstant.", e)
        false
    }

    if (client != null) {
      client.close()
    }

    Seq(Row(result))
  }

  override def build: Procedure = new DeleteMarkerProcedure()
}

object DeleteMarkerProcedure {
  val NAME: String = "delete_marker"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): DeleteMarkerProcedure = new DeleteMarkerProcedure()
  }
}
