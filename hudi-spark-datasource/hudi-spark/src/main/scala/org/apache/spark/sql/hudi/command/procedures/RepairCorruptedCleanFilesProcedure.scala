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

import org.apache.avro.AvroRuntimeException
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant}
import org.apache.hudi.common.util.CleanerUtils
import org.apache.hudi.exception.HoodieIOException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.io.IOException
import java.util.function.Supplier
import scala.collection.JavaConverters.asScalaIteratorConverter

class RepairCorruptedCleanFilesProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getBasePath(tableName)

    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(tablePath).build

    val cleanerTimeline = metaClient.getActiveTimeline.getCleanerTimeline
    logInfo("Inspecting pending clean metadata in timeline for corrupted files")
    var result = true
    cleanerTimeline.filterInflightsAndRequested.getInstants.iterator().asScala.foreach((instant: HoodieInstant) => {
      try {
        CleanerUtils.getCleanerPlan(metaClient, instant)
      } catch {
        case e: AvroRuntimeException =>
          logWarning("Corruption found. Trying to remove corrupted clean instant file: " + instant)
          HoodieActiveTimeline.deleteInstantFile(metaClient.getFs, metaClient.getMetaPath, instant)
        case ioe: IOException =>
          if (ioe.getMessage.contains("Not an Avro data file")) {
            logWarning("Corruption found. Trying to remove corrupted clean instant file: " + instant)
            HoodieActiveTimeline.deleteInstantFile(metaClient.getFs, metaClient.getMetaPath, instant)
          } else {
            result = false
            throw new HoodieIOException(ioe.getMessage, ioe)
          }
      }
    })
    Seq(Row(result))
  }

  override def build: Procedure = new RepairCorruptedCleanFilesProcedure()
}

object RepairCorruptedCleanFilesProcedure {
  val NAME = "repair_corrupted_clean_files"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RepairCorruptedCleanFilesProcedure()
  }
}
