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

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.table.marker.WriteMarkersFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.util.{Failure, Success, Try}

class DeleteMarkerProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.required(1, "instant_time", DataTypes.StringType, None)
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

    val result = Try {
      val client = createHoodieClient(jsc, basePath)
      val config = client.getConfig
      val context = client.getEngineContext
      val table = HoodieSparkTable.create(config, context)
      WriteMarkersFactory.get(config.getMarkersType, table, instantTime)
        .quietDeleteMarkerDir(context, config.getInt(HoodieWriteConfig.MARKERS_DELETE_PARALLELISM_VALUE))
    } match {
      case Success(_) =>
        logInfo(s"Marker $instantTime deleted.")
        true
      case Failure(e) =>
        logWarning(s"Failed: Could not clean marker instantTime: $instantTime.", e)
        false
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
