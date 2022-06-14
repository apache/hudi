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

import java.io.IOException
import java.util
import java.util.function.Supplier

import org.apache.hudi.avro.model.HoodieRollbackMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant, HoodieTimeline, TimelineMetadataUtils}
import org.apache.hudi.common.util.CollectionUtils
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters._

class ShowRollbacksProcedure(showDetails: Boolean) extends BaseProcedure with ProcedureBuilder {
  private val ROLLBACKS_PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10)
  )

  private val ROLLBACK_PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.required(2, "instant_time", DataTypes.StringType, None)
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
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val activeTimeline = new RollbackTimeline(metaClient)
    if (showDetails) {
      val instantTime = getArgValueOrDefault(args, parameters(2)).get.asInstanceOf[String]
      getRollbackDetail(activeTimeline, instantTime, limit)
    } else {
      getRollbacks(activeTimeline, limit)
    }
  }

  override def build: Procedure = new ShowRollbacksProcedure(showDetails)

  class RollbackTimeline(metaClient: HoodieTableMetaClient) extends HoodieActiveTimeline(metaClient,
    CollectionUtils.createImmutableSet(HoodieTimeline.ROLLBACK_EXTENSION)) {
  }

  def getRollbackDetail(activeTimeline: RollbackTimeline,
                  instantTime: String,
                  limit: Int): Seq[Row] = {
    val rows = new util.ArrayList[Row]
    val metadata = TimelineMetadataUtils.deserializeAvroMetadata(activeTimeline.getInstantDetails(
      new HoodieInstant(State.COMPLETED, ROLLBACK_ACTION, instantTime)).get, classOf[HoodieRollbackMetadata])

    metadata.getPartitionMetadata.asScala.toMap.iterator.foreach(entry => Stream
      .concat(entry._2.getSuccessDeleteFiles.map(f => (f, true)),
        entry._2.getFailedDeleteFiles.map(f => (f, false)))
      .iterator.foreach(fileWithDeleteStatus => {
        rows.add(Row(metadata.getStartRollbackTime, metadata.getCommitsRollback.toString,
          entry._1, fileWithDeleteStatus._1, fileWithDeleteStatus._2))
      }))
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  def getRollbacks(activeTimeline: RollbackTimeline,
                   limit: Int): Seq[Row] = {
    val rows = new util.ArrayList[Row]
    val rollback = activeTimeline.getRollbackTimeline.filterCompletedInstants

    rollback.getInstants.iterator().asScala.foreach(instant => {
      try {
        val metadata = TimelineMetadataUtils.deserializeAvroMetadata(activeTimeline.getInstantDetails(instant).get,
          classOf[HoodieRollbackMetadata])

        metadata.getCommitsRollback.iterator().asScala.foreach(c => {
          rows.add(Row(metadata.getStartRollbackTime, c,
            metadata.getTotalFilesDeleted, metadata.getTimeTakenInMillis,
            if (metadata.getPartitionMetadata != null) metadata.getPartitionMetadata.size else 0))
        })
      } catch {
        case e: IOException =>
          throw new HoodieException(s"Failed to get rollback's info from instant ${instant.getTimestamp}.")
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
