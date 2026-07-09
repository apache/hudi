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
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.table.HoodieSparkTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

class RollbackToInstantTimeProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "instant_time", DataTypes.StringType))

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("rollback_result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val instantTime = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    var client: SparkRDDWriteClient[_] = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty, scala.Option(table))
      val config = getWriteConfig(basePath)
      val metaClient = HoodieTableMetaClient.builder
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration))
        .setBasePath(config.getBasePath)
        .setLoadActiveTimelineOnLoad(false)
        .setConsistencyGuardConfig(config.getConsistencyGuardConfig)
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion)))
        .build

      val activeTimeline = metaClient.getActiveTimeline.getCommitsAndCompactionTimeline
      val filteredTimeline = activeTimeline.containsInstant(instantTime)
      if (!filteredTimeline) {
        throw new HoodieException(s"Commit $instantTime not found in Commits $activeTimeline")
      }

      val res = new util.ArrayList[Row]()
      val instantsToRollback = activeTimeline.findInstantsAfterOrEquals(instantTime, Integer.MAX_VALUE)
        .getInstants.asScala.reverse
      breakable {
        instantsToRollback.foreach { currentInstant =>
          var finished = false
          if (HoodieTimeline.COMPACTION_ACTION.equalsIgnoreCase(currentInstant.getAction)) {
            if (currentInstant.isInflight) {
              logInfo("Start to rollback inflight compaction instant " + currentInstant.requestedTime)
              config.setValue(ROLLBACK_USING_MARKERS_ENABLE, "false")
              val hoodieTable = HoodieSparkTable.create(config, client.getEngineContext, metaClient)
              hoodieTable.rollbackInflightCompaction(currentInstant, client.getTransactionManager)
              val requestedInstant = metaClient.createNewInstant(
                State.REQUESTED, currentInstant.getAction, currentInstant.requestedTime)
              metaClient.getActiveTimeline.deleteInstantFileIfExists(requestedInstant)
              finished = true
            } else if (currentInstant.isRequested) {
              logInfo("Start to rollback requested compaction instant " + currentInstant.requestedTime)
              finished = rollbackRequestedCompaction(
                currentInstant, metaClient, client.getEngineContext, client.getConfig)
            }
          } else {
            logInfo("Start to rollback instant " + currentInstant.requestedTime)
            finished = client.rollback(currentInstant.requestedTime)
          }
          res.add(Row(finished))
          if (!finished) {
            break()
          }
        }
      }
      spark.catalog.refreshTable(table)
      res.asScala.toSeq
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  private def rollbackRequestedCompaction(currentInstant: HoodieInstant, metaClient: HoodieTableMetaClient,
                                          context: HoodieEngineContext, config: HoodieWriteConfig): Boolean = {
    val hoodieTable = HoodieSparkTable.create(config, context, metaClient)
    val rollbackInstantTime = metaClient.createNewInstantTime(false)
    val rollbackPlan = hoodieTable.scheduleRollback(context, rollbackInstantTime, currentInstant, false, false, false)
    if (rollbackPlan.isPresent) {
      hoodieTable.rollback(context, rollbackInstantTime, currentInstant, true, false)
      true
    } else {
      false
    }
  }

  override def build: Procedure = new RollbackToInstantTimeProcedure()
}

object RollbackToInstantTimeProcedure {
  val NAME: String = "rollback_to_instant"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): RollbackToInstantTimeProcedure = new RollbackToInstantTimeProcedure()
  }
}
