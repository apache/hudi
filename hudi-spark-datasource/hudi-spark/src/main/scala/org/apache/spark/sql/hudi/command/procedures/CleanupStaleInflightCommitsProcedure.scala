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

import org.apache.hudi.{HoodieCLIUtils, HoodieTimelineCleanupUtil}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.HoodiePendingRollbackInfo
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.table.HoodieSparkTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL stored procedure to roll back stale inflight write commits older than a configurable
 * age threshold.
 *
 * SAFETY WARNING: Unlike rollback_to_instant_time which targets a single named instant, this
 * procedure rolls back a SET of instants matched by age. A misconfigured threshold can erase
 * recent in-progress writes when include_ingestion_commits=true. Operators should call
 * show_inflight_commits first to preview the pending instants, and use dry_run => true (see
 * below) when in doubt about which instants will be processed.
 *
 * This procedure targets the write timeline (COMMIT, DELTA_COMMIT, COMPACTION, LOG_COMPACTION,
 * REPLACE_COMMIT, CLUSTERING actions). Stale rollback, clean, or restore inflights visible in
 * show_inflight_commits are NOT covered by this procedure.
 *
 * Compaction, log-compaction, and clustering inflights are handled via the targeted table
 * methods (table.rollbackInflightCompaction / rollbackInflightLogCompaction /
 * rollbackInflightClustering) — the supporting state (HoodieSparkTable, table-service client,
 * pending-rollback lookup) is constructed lazily on the first such instant and reused.
 *
 * Parameters:
 *   - table:                             Required. Catalog name of the Hudi table.
 *   - allowed_inflight_interval_minutes: Optional (default 180). Instants older than this many
 *                                        minutes are considered stale and eligible for rollback.
 *   - include_ingestion_commits:         Optional (default false). DANGEROUS when true: enabling
 *                                        this allows the procedure to roll back COMMIT_ACTION and
 *                                        DELTA_COMMIT_ACTION inflights, which means it can drop
 *                                        in-progress ingestion data. The default false is the
 *                                        safe choice; true is for operators recovering from a
 *                                        known-stuck ingestion job.
 *   - dry_run:                           Optional (default false). When true, the matched-instant
 *                                        set is resolved exactly as in normal mode but no rollback
 *                                        calls are issued. Each returned row carries
 *                                        rollback_status = NULL meaning "matched but not acted
 *                                        upon". Re-run with dry_run => false to act.
 *
 * Output columns (one row per processed instant):
 *   - instant_time:    The instant's requested timestamp.
 *   - action:          The action type.
 *   - rollback_status: true if the rollback succeeded, false if the rollback failed,
 *                      NULL if dry_run was true (matched but not actioned).
 *
 * Example usage:
 * {{{
 *   -- Clean stale table-service inflights (default 180-min threshold)
 *   CALL cleanup_stale_inflight_commits(table => 'my_table');
 *
 *   -- Preview what would be processed without acting
 *   CALL cleanup_stale_inflight_commits(table => 'my_table', dry_run => true);
 *
 *   -- Clean stale inflights older than 1 hour, including ingestion commits (DANGEROUS)
 *   CALL cleanup_stale_inflight_commits(
 *     table => 'my_table',
 *     allowed_inflight_interval_minutes => 60,
 *     include_ingestion_commits => true
 *   );
 * }}}
 *
 * For inflight commit types not covered by this procedure (clean, restore, rollback inflights),
 * use hudi-cli's `repair rollback` command.
 */
class CleanupStaleInflightCommitsProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "allowed_inflight_interval_minutes", DataTypes.IntegerType, 180),
    ProcedureParameter.optional(2, "include_ingestion_commits", DataTypes.BooleanType, false),
    ProcedureParameter.optional(3, "dry_run", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time",    DataTypes.StringType,  nullable = true, Metadata.empty),
    StructField("action",          DataTypes.StringType,  nullable = true, Metadata.empty),
    StructField("rollback_status", DataTypes.BooleanType, nullable = true, Metadata.empty)
  ))

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def build: Procedure = new CleanupStaleInflightCommitsProcedure()

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName               = getArgValueOrDefault(args, PARAMETERS(0))
    val allowedMinutes          = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]
    val includeIngestionCommits = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]
    val dryRun                  = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]

    val basePath = getBasePath(tableName)
    val metaClient = HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration))
      .setBasePath(basePath)
      .build

    val staleInflights = HoodieTimelineCleanupUtil
      .inflightWriteCommitsOlderThan(metaClient, allowedMinutes.toLong, includeIngestionCommits)

    if (staleInflights.isEmpty) {
      Seq.empty[Row]
    } else if (dryRun) {
      // Dry-run: do not open the table for write — emit preview rows with NULL rollback_status
      // to mean "matched but not actioned". Re-run with dry_run => false to act.
      staleInflights.asScala.map { instant =>
        Row(instant.requestedTime, instant.getAction, null)
      }.toSeq
    } else {
      // Pass ROLLBACK_USING_MARKERS_ENABLE=false via the createHoodieWriteClient confs Map.
      // Inflight commits may not have marker files, so timeline-based rollback is required.
      // The user-specified confs win over defaults / table config / session conf — see
      // HoodieCLIUtils.scala "Priority: defaults < catalog props < table config < sparkSession conf < specified conf".
      val confs = Map(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key() -> "false")
      var client: SparkRDDWriteClient[_] = null
      try {
        client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, confs,
          tableName.asInstanceOf[scala.Option[String]])

        // Lazy state for compaction / log-compaction / clustering branches.
        // For matched sets that contain no such instants, none of these are constructed.
        lazy val tsClient = client.getTableServiceClient
        lazy val table = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
        lazy val getPendingRollbackInstantFunc: java.util.function.Function[String, HOption[HoodiePendingRollbackInfo]] =
          new java.util.function.Function[String, HOption[HoodiePendingRollbackInfo]] {
            override def apply(commitToRollback: String): HOption[HoodiePendingRollbackInfo] = {
              tsClient.getPendingRollbackInfo(table.getMetaClient, commitToRollback, false)
            }
          }

        val rows = staleInflights.asScala.map { instant =>
          val status: java.lang.Boolean = try {
            val result: Boolean = instant.getAction match {
              case HoodieTimeline.COMPACTION_ACTION =>
                table.rollbackInflightCompaction(instant, getPendingRollbackInstantFunc, client.getTransactionManager)
                true
              case HoodieTimeline.LOG_COMPACTION_ACTION =>
                table.rollbackInflightLogCompaction(instant, getPendingRollbackInstantFunc, client.getTransactionManager)
                true
              case HoodieTimeline.CLUSTERING_ACTION =>
                table.rollbackInflightClustering(instant, getPendingRollbackInstantFunc, client.getTransactionManager)
                true
              case _ =>
                client.rollback(instant.requestedTime)
            }
            java.lang.Boolean.valueOf(result)
          } catch {
            case e: Exception =>
              logError(s"Failed to rollback inflight instant ${instant.requestedTime}", e)
              java.lang.Boolean.FALSE
          }
          Row(instant.requestedTime, instant.getAction, status)
        }.toSeq

        // Refresh catalog after all rollbacks, inside try — consistent with RollbackToInstantTimeProcedure.
        // Not placed in finally to avoid refreshing on client-creation failures.
        if (tableName.isDefined) {
          spark.catalog.refreshTable(tableName.get.asInstanceOf[String])
        }
        rows
      } finally {
        if (client != null) client.close()
      }
    }
  }
}

object CleanupStaleInflightCommitsProcedure {
  val NAME = "cleanup_stale_inflight_commits"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new CleanupStaleInflightCommitsProcedure()
  }
}
