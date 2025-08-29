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
import org.apache.hudi.common.HoodiePendingRollbackInfo
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.{ClusteringUtils, Option}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.table.HoodieSparkTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

/**
 * When calling this procedure, one of parameters table and path must be specified at least.
 * If both parameters are given, table will take effect.
 * pending_instant represents the instant information of the current operation, and pending_instant can be either
 * a clustering instant or a compaction instant.
 * delete_request_instant_file is used to mark whether to delete the request instant file.
 */
class RunRollbackInflightTableServiceProcedure extends BaseProcedure
  with ProcedureBuilder
  with PredicateHelper
  with Logging {
  override def build: Procedure = new RunRollbackInflightTableServiceProcedure

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "path", DataTypes.StringType, None),
    ProcedureParameter.required(2, "pending_instant", DataTypes.StringType),
    ProcedureParameter.optional(3, "delete_request_instant_file", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_cost", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val pendingInstant = getArgValueOrDefault(args, PARAMETERS(2)).get.toString
    val deleteRequestInstantFile = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration)).setBasePath(basePath).build
    val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
    // determine whether the current instant exists and whether it is clustering or compaction
    var isClustering: Boolean = true
    var instant: HoodieInstant = null
    val pendingCompactionInstant = instantGenerator.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, pendingInstant)
    val pendingClusteringInstant = ClusteringUtils.getInflightClusteringInstant(pendingInstant,
      metaClient.getActiveTimeline, metaClient.getTimelineLayout.getInstantGenerator)
    val timeline = metaClient.getActiveTimeline.getWriteTimeline
    if (!timeline.containsInstant(pendingCompactionInstant) && !pendingClusteringInstant.isPresent) {
      throw new RuntimeException(s"there is no pending instant : [$pendingClusteringInstant | $pendingCompactionInstant]")
    } else if (timeline.containsInstant(pendingCompactionInstant)) {
      isClustering = false
      instant = pendingCompactionInstant
      logInfo(s"compaction instant to rollback : ${instant}")
    } else {
      isClustering = true
      instant = pendingClusteringInstant.get()
      logInfo(s"clustering instant to rollback : ${instant}")
    }

    var client: SparkRDDWriteClient[_] = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty,
        tableName.asInstanceOf[scala.Option[String]])

      val startTs = System.currentTimeMillis()
      doRollbackOnInflightInstant(client, instant, isClustering)
      if (deleteRequestInstantFile) {
        val requestInstant = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, instant.getAction, instant.requestedTime)
        metaClient.getActiveTimeline.deleteInstantFileIfExists(requestInstant)
      }
      val timeCost = System.currentTimeMillis() - startTs
      logInfo(s"Finish rollback pending instant: $pendingInstant," +
        s" time cost: $timeCost ms.")

      Seq(Row(instant.requestedTime, timeCost.toString))
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  private def doRollbackOnInflightInstant(client: SparkRDDWriteClient[_], inflightInstant: HoodieInstant, isClustering: Boolean): Unit = {
    val tsClient = client.getTableServiceClient
    val table = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
    // FIXME-vc: this is hacky
    table.setTxnManager(client.getTransactionManager)

    val getPendingRollbackInstantFunc: java.util.function.Function[String, Option[HoodiePendingRollbackInfo]] =
      new java.util.function.Function[String, Option[HoodiePendingRollbackInfo]] {
        override def apply(commitToRollback: String): Option[HoodiePendingRollbackInfo] = {
          tsClient.getPendingRollbackInfo(table.getMetaClient, commitToRollback, false)
        }
      }

    if (isClustering) {
      table.rollbackInflightClustering(inflightInstant, getPendingRollbackInstantFunc, client.getTransactionManager)
    } else {
      table.rollbackInflightCompaction(inflightInstant, getPendingRollbackInstantFunc, client.getTransactionManager)
    }
  }
}

object RunRollbackInflightTableServiceProcedure {
  val NAME = "run_rollback_inflight_tableservice"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new RunRollbackInflightTableServiceProcedure
  }
}
