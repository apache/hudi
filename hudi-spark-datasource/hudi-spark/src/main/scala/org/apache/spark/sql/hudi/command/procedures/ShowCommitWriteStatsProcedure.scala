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
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieReplaceCommitMetadata}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier
import scala.collection.JavaConversions._

class ShowCommitWriteStatsProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.required(2, "instant_time", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("total_bytes_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_records_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("avg_record_size", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]
    val instantTime = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val activeTimeline = metaClient.getActiveTimeline
    val timeline = activeTimeline.getCommitsTimeline.filterCompletedInstants
    val hoodieInstantOption = getCommitForInstant(timeline, instantTime)
    val commitMetadataOptional = getHoodieCommitMetadata(timeline, hoodieInstantOption)

    if (commitMetadataOptional.isEmpty) {
      throw new HoodieException(s"Commit $instantTime not found in Commits $timeline.")
    }

    val meta = commitMetadataOptional.get

    val action: String = hoodieInstantOption.get.getAction
    val recordsWritten = meta.fetchTotalRecordsWritten
    val bytesWritten = meta.fetchTotalBytesWritten
    val avgRecSize = Math.ceil((1.0 * bytesWritten) / recordsWritten).toLong
    val rows = new util.ArrayList[Row]
    rows.add(Row(action, bytesWritten, recordsWritten, avgRecSize))

    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new ShowCommitWriteStatsProcedure()

  private def getCommitForInstant(timeline: HoodieTimeline, instantTime: String): Option[HoodieInstant] = {
    val instants: util.List[HoodieInstant] = util.Arrays.asList(
      new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, instantTime),
      new HoodieInstant(false, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime),
      new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime))

    val hoodieInstant: Option[HoodieInstant] = instants.find((i: HoodieInstant) => timeline.containsInstant(i))
    hoodieInstant
  }

  private def getHoodieCommitMetadata(timeline: HoodieTimeline, hoodieInstant: Option[HoodieInstant]): Option[HoodieCommitMetadata] = {
    if (hoodieInstant.isDefined) {
      if (hoodieInstant.get.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION) {
        Option(HoodieReplaceCommitMetadata.fromBytes(timeline.getInstantDetails(hoodieInstant.get).get,
          classOf[HoodieReplaceCommitMetadata]))
      } else {
        Option(HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(hoodieInstant.get).get,
          classOf[HoodieCommitMetadata]))
      }
    } else {
      Option.empty
    }
  }
}

object ShowCommitWriteStatsProcedure {
  val NAME = "show_commit_write_stats"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowCommitWriteStatsProcedure()
  }
}
