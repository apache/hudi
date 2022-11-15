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

import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.text.DecimalFormat
import java.util.function.Supplier
import scala.collection.JavaConverters.asScalaIteratorConverter

class StatsWriteAmplificationProcedure extends BaseProcedure with ProcedureBuilder {
  override def parameters: Array[ProcedureParameter] = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10)
  )

  override def outputType: StructType = StructType(Array[StructField](
    StructField("commit_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("total_upserted", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("write_amplification_factor", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    checkArgs(parameters, args)
    val table = getArgValueOrDefault(args, parameters(0))
    val limit: Int = getArgValueOrDefault(args, parameters(1)).get.asInstanceOf[Int]
    val basePath = getBasePath(table)
    val client = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val activeTimeline = client.getActiveTimeline
    val timeline = activeTimeline.getCommitTimeline.filterCompletedInstants()

    val rows = new java.util.ArrayList[Row]
    val df = new DecimalFormat("#.00")
    var totalRecordsUpserted = 0L
    var totalRecordsWritten = 0L
    timeline.getInstants.iterator.asScala.foreach(
      instantTime => {
        var waf = "0"
        val commit = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(instantTime).get(), classOf[HoodieCommitMetadata])
        if (commit.fetchTotalUpdateRecordsWritten() > 0) {
          waf = df.format(commit.fetchTotalRecordsWritten().toFloat / commit.fetchTotalUpdateRecordsWritten())
        }
        rows.add(Row(instantTime.getTimestamp, commit.fetchTotalUpdateRecordsWritten, commit.fetchTotalRecordsWritten, waf))
        totalRecordsUpserted = totalRecordsUpserted + commit.fetchTotalUpdateRecordsWritten()
        totalRecordsWritten = totalRecordsWritten + commit.fetchTotalRecordsWritten()
      }
    )
    var waf = "0"
    if (totalRecordsUpserted > 0) {
      waf = df.format(totalRecordsWritten.toFloat / totalRecordsUpserted)
    }
    rows.add(Row("Total", totalRecordsUpserted, totalRecordsWritten, waf))
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new StatsWriteAmplificationProcedure
}

object StatsWriteAmplificationProcedure {
  val NAME = "stats_wa"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new StatsWriteAmplificationProcedure()
  }
}
