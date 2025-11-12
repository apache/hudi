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
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.io.IOException
import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowRollbacksProcedure(showDetails: Boolean) extends BaseProcedure with ProcedureBuilder {
  private val ROLLBACKS_PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10)
  )

  private val ROLLBACK_PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.required(2, "instant_time", DataTypes.StringType)
  )

  private val ROLLBACKS_OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("rollback_instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("total_files_deleted", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_partitions", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  private val ROLLBACK_OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("rollback_instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("deleted_file", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("succeeded", DataTypes.BooleanType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = if (showDetails) ROLLBACK_PARAMETERS else ROLLBACKS_PARAMETERS

  def outputType: StructType = if (showDetails) ROLLBACK_OUTPUT_TYPE else ROLLBACKS_OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(parameters, args)

    val tableName = getArgValueOrDefault(args, parameters(0))
    val limit = getArgValueOrDefault(args, parameters(1)).get.asInstanceOf[Int]

    val basePath = getBasePath(tableName)
    val metaClient = createMetaClient(jsc, basePath)
    val activeTimeline = metaClient.getActiveTimeline
    if (showDetails) {
      val instantTime = getArgValueOrDefault(args, parameters(2)).get.asInstanceOf[String]
      getRollbackDetail(metaClient, activeTimeline, instantTime, limit)
    } else {
      getRollbacks(activeTimeline, limit)
    }
  }

  override def build: Procedure = new ShowRollbacksProcedure(showDetails)

  def getRollbackDetail(metaClient: HoodieTableMetaClient,
                        activeTimeline: HoodieActiveTimeline,
                        instantTime: String,
                        limit: Int): Seq[Row] = {
    val rows = new util.ArrayList[Row]
    val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
    val metadata = activeTimeline.readRollbackMetadata(
      instantGenerator.createNewInstant(State.COMPLETED, ROLLBACK_ACTION, instantTime))

    metadata.getPartitionMetadata.asScala.toMap.iterator.foreach(entry => Stream
      .concat(entry._2.getSuccessDeleteFiles.asScala.map(f => (f, true)),
        entry._2.getFailedDeleteFiles.asScala.map(f => (f, false)))
      .iterator.foreach(fileWithDeleteStatus => {
        rows.add(Row(metadata.getStartRollbackTime, metadata.getCommitsRollback.toString,
          entry._1, fileWithDeleteStatus._1, fileWithDeleteStatus._2))
      }))
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  def getRollbacks(activeTimeline: HoodieActiveTimeline,
                   limit: Int): Seq[Row] = {
    val rows = new util.ArrayList[Row]
    val rollback = activeTimeline.getRollbackTimeline.filterCompletedInstants

    rollback.getInstants.iterator().asScala.foreach(instant => {
      try {
        val metadata = activeTimeline.readRollbackMetadata(instant)

        metadata.getCommitsRollback.iterator().asScala.foreach(c => {
          rows.add(Row(metadata.getStartRollbackTime, c,
            metadata.getTotalFilesDeleted, metadata.getTimeTakenInMillis,
            if (metadata.getPartitionMetadata != null) metadata.getPartitionMetadata.size else 0))
        })
      } catch {
        case e: IOException =>
          throw new HoodieException(s"Failed to get rollback's info from instant ${instant.requestedTime}.")
      }
    })
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }
}

object ShowRollbacksProcedure {
  val NAME = "show_rollbacks"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowRollbacksProcedure(false)
  }
}

object ShowRollbackDetailProcedure {
  val NAME = "show_rollback_detail"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowRollbacksProcedure(true)
  }
}
