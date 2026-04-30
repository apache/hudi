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
import org.apache.hudi.client.timeline.versioning.v2.LSMTimelineWriter
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.table.HoodieSparkTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.concurrent.TimeUnit
import java.util.function.Supplier

class RunTimelineCompactionProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("start_compaction_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val basePath = getBasePath(tableName, tablePath)

    var client: SparkRDDWriteClient[_] = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty,
        tableName.asInstanceOf[Option[String]])
      val config = client.getConfig
      val context = client.getEngineContext
      val table = HoodieSparkTable.create(config, context)
      val writer = LSMTimelineWriter.getInstance(config, table)
      val txnManager = client.getTransactionManager

      // Acquire the same state-change lock that TimelineArchiverV2 holds while it
      // calls compactAndClean, so this procedure cannot race with a concurrent
      // archival/compaction over the LSM timeline manifest.
      val startInstant = HoodieInstantTimeGenerator.getCurrentInstantTimeStr
      val startNanos = System.nanoTime()
      txnManager.beginStateChange(HOption.empty(), HOption.empty())
      try {
        writer.compactAndClean(context)
      } finally {
        txnManager.endStateChange(HOption.empty())
      }
      val durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)

      Seq(Row(startInstant, durationMillis))
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  override def build: Procedure = new RunTimelineCompactionProcedure()
}

object RunTimelineCompactionProcedure {
  val NAME: String = "run_timeline_compaction"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): RunTimelineCompactionProcedure = new RunTimelineCompactionProcedure()
  }
}
