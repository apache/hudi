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
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.{CompactionUtils, HoodieTimer, Option => HOption}
import org.apache.hudi.config.HoodieLockConfig
import org.apache.hudi.exception.HoodieException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier

import scala.collection.JavaConverters._

class RunCompactionProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {

  /**
   * operation = (RUN | SCHEDULE) COMPACTION  ON tableIdentifier (AT instantTimestamp = INTEGER_VALUE)?
   * operation = (RUN | SCHEDULE) COMPACTION  ON path = STRING   (AT instantTimestamp = INTEGER_VALUE)?
   */
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "op", DataTypes.StringType),
    ProcedureParameter.optional(1, "table", DataTypes.StringType),
    ProcedureParameter.optional(2, "path", DataTypes.StringType),
    ProcedureParameter.optional(3, "timestamp", DataTypes.LongType),
    ProcedureParameter.optional(4, "options", DataTypes.StringType),
    ProcedureParameter.optional(5, "instants", DataTypes.StringType),
    ProcedureParameter.optional(6, "limit", DataTypes.IntegerType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("operation_size", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    var op = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String].toLowerCase
    val tableName = getArgValueOrDefault(args, PARAMETERS(1))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(2))
    val instantTimestamp = getArgValueOrDefault(args, PARAMETERS(3))
    var confs: Map[String, String] = Map.empty
    if (getArgValueOrDefault(args, PARAMETERS(4)).isDefined) {
      confs = confs ++ HoodieCLIUtils.extractOptions(getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String])
    }
    var specificInstants = getArgValueOrDefault(args, PARAMETERS(5))
    val limit = getArgValueOrDefault(args, PARAMETERS(6))

    // For old version compatibility
    if (op.equals("run")) {
      op = "scheduleandexecute"
    }
    if (instantTimestamp.isDefined && specificInstants.isEmpty) {
      specificInstants = Option(instantTimestamp.get.toString)
      op = "execute"
    }

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val pendingCompactionInstants = metaClient.getActiveTimeline.getWriteTimeline.getInstants.iterator().asScala
      .filter(p => p.getAction == HoodieTimeline.COMPACTION_ACTION)
      .map(_.requestedTime)
      .toSeq.sortBy(f => f)

    var (filteredPendingCompactionInstants, operation) = HoodieProcedureUtils.filterPendingInstantsAndGetOperation(
      pendingCompactionInstants, specificInstants.asInstanceOf[Option[String]], Option(op), limit.asInstanceOf[Option[Int]])

    var client: SparkRDDWriteClient[_] = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, confs,
        tableName.asInstanceOf[Option[String]])

      if (metaClient.getTableConfig.isMetadataTableAvailable) {
        if (!confs.contains(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key)) {
          confs = confs ++ HoodieCLIUtils.getLockOptions(basePath, metaClient.getBasePath.toUri.getScheme, client.getConfig.getCommonConfig.getProps())
        }
      }

      if (operation.isSchedule) {
        val instantTime = client.createNewInstantTime()
        if (client.scheduleCompactionAtInstant(instantTime, HOption.empty[java.util.Map[String, String]])) {
          filteredPendingCompactionInstants = Seq(instantTime)
        }
      }

      logInfo(s"Compaction instants to run: ${filteredPendingCompactionInstants.mkString(",")}.")

      if (operation.isExecute) {
        val timer = HoodieTimer.start
        filteredPendingCompactionInstants.foreach { compactionInstant =>
          val writeResponse = client.compact(compactionInstant)
          client.commitCompaction(compactionInstant, writeResponse, HOption.empty())
          handleResponse(writeResponse.getCommitMetadata.get())
        }
        logInfo(s"Finish Run compaction at instants: [${filteredPendingCompactionInstants.mkString(",")}]," +
          s" spend: ${timer.endTimer()}ms")
      }

      val compactionInstants = metaClient.reloadActiveTimeline().getInstantsAsStream.iterator().asScala
        .filter(instant => filteredPendingCompactionInstants.contains(instant.requestedTime))
        .toSeq
        .sortBy(p => p.requestedTime)
        .reverse

      compactionInstants.map(instant =>
        (instant, CompactionUtils.getCompactionPlan(metaClient, instant.requestedTime))
      ).map { case (instant, plan) =>
        Row(instant.requestedTime, plan.getOperations.size(), instant.getState.name())
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  private def handleResponse(metadata: HoodieCommitMetadata): Unit = {
    // Handle error
    val writeStatsHasErrors = metadata.getPartitionToWriteStats.entrySet()
      .asScala
      .flatMap(e => e.getValue.asScala)
      .filter(_.getTotalWriteErrors > 0)
    if (writeStatsHasErrors.nonEmpty) {
      val errorsCount = writeStatsHasErrors.map(_.getTotalWriteErrors).sum
      log.error(s"Found $errorsCount when writing record.\n Printing out the top 100 file path with errors.")
      writeStatsHasErrors.take(100).foreach(state =>
        log.error(s"Error occurred while writing the file: ${state.getPath}."))
      throw new HoodieException(s"Found $errorsCount when writing record")
    }
  }

  override def build: Procedure = new RunCompactionProcedure()

}

object RunCompactionProcedure {
  val NAME = "run_compaction"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RunCompactionProcedure
  }
}
