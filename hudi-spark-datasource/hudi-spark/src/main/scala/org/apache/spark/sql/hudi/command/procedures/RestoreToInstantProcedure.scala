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
import org.apache.hudi.avro.model.HoodieRestoreMetadata
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.ConsistencyGuardConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.StoragePath

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.command.procedures.RestoreToInstantProcedure._
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Stored procedure to perform a full point-in-time table restore to a given instant.
 *
 * Unlike [[RollbackToSavepointProcedure]] (which requires a savepoint at the target instant),
 * this procedure calls restoreToInstant() directly and works on any arbitrary instant on the
 * active timeline.
 *
 * Parameters:
 *   - table / path: identifies the Hudi table (one must be provided)
 *   - instant_time: target commit to restore to (required when audit_only=false; must be omitted
 *                   when audit_only=true)
 *   - start_restore_time: the restore operation's own timeline timestamp (the start_restore_time
 *                   value returned by a prior restore_to_instant call). Required when
 *                   audit_only=true; must be omitted otherwise.
 *   - enable_metadata: whether the metadata table is enabled (default: true)
 *   - rollback_parallelism: Spark parallelism for rollback and audit operations (default: 4)
 *   - enable_consistency_guard: enable consistency guard for file existence checks (default: false)
 *   - audit_post_restore: after restoring, verify that all successfully-deleted files are absent (default: false)
 *   - audit_only: skip the restore and only audit a previously completed restore instant (default: false)
 *
 * Output columns:
 *   - restore_result: true if restore succeeded; null if audit_only=true
 *   - start_restore_time: the restore operation's own timeline timestamp; null if audit_only=true.
 *                         Pass this value as start_restore_time to re-run the audit later.
 *   - time_taken_in_millis: restore duration; null if audit_only=true
 *   - instants_rolled_back: number of commits rolled back; null if audit_only=true
 *   - audit_result: one of "PASSED" / "FAILED" / "INCONCLUSIVE" when an audit ran; null otherwise.
 *                   INCONCLUSIVE means at least one file existence check threw an IOException
 *                   (e.g. transient cloud-storage timeout) — re-run audit_only=true to retry.
 */
class RestoreToInstantProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "instant_time", DataTypes.StringType),
    ProcedureParameter.optional(2, "enable_metadata", DataTypes.BooleanType, true),
    ProcedureParameter.optional(3, "rollback_parallelism", DataTypes.IntegerType, 4),
    ProcedureParameter.optional(4, "enable_consistency_guard", DataTypes.BooleanType, false),
    ProcedureParameter.optional(5, "audit_post_restore", DataTypes.BooleanType, false),
    ProcedureParameter.optional(6, "audit_only", DataTypes.BooleanType, false),
    ProcedureParameter.optional(7, "path", DataTypes.StringType),
    ProcedureParameter.optional(8, "start_restore_time", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("restore_result", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("start_restore_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_taken_in_millis", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("instants_rolled_back", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("audit_result", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val instantTime = getArgValueOrDefault(args, PARAMETERS(1))
    val enableMetadata = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]
    val rollbackParallelism = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Int]
    val enableConsistencyGuard = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Boolean]
    val shouldAuditPostRestore = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[Boolean]
    val auditOnly = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[Boolean]
    val tablePath = getArgValueOrDefault(args, PARAMETERS(7))
    val startRestoreTimeArg = getArgValueOrDefault(args, PARAMETERS(8))

    // Cross-validation: each of (instant_time, start_restore_time) has one unambiguous meaning.
    if (!auditOnly && instantTime.isEmpty) {
      throw new HoodieException("instant_time is required when audit_only=false.")
    }
    if (auditOnly && startRestoreTimeArg.isEmpty) {
      throw new HoodieException(
        "start_restore_time is required when audit_only=true. " +
          "Pass the start_restore_time value from a prior restore_to_instant call.")
    }
    if (!auditOnly && startRestoreTimeArg.isDefined) {
      throw new HoodieException("start_restore_time may only be specified when audit_only=true.")
    }
    if (auditOnly && instantTime.isDefined) {
      throw new HoodieException(
        "instant_time may only be specified when audit_only=false. " +
          "Use start_restore_time to identify a previously executed restore.")
    }
    if (auditOnly && shouldAuditPostRestore) {
      logWarning("Both audit_only and audit_post_restore are set. Only audit_only will be honored.")
    }

    val basePath = getBasePath(tableName, tablePath)

    val confs = Map(
      HoodieMetadataConfig.ENABLE.key() -> enableMetadata.toString,
      HoodieWriteConfig.ROLLBACK_PARALLELISM_VALUE.key() -> rollbackParallelism.toString,
      HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key() -> "false"
    ) ++ (if (enableConsistencyGuard) Map(ConsistencyGuardConfig.ENABLE.key() -> "true") else Map.empty)

    val metaClient = createMetaClient(jsc, basePath)

    // Nullable boxed types so Row can hold null for audit_only runs
    var restoreResult: java.lang.Boolean = null
    var startRestoreTime: String = null
    var timeTakenInMillis: java.lang.Long = null
    var instantsRolledBack: java.lang.Long = null

    if (!auditOnly) {
      val targetInstant = instantTime.get.asInstanceOf[String]
      var client: SparkRDDWriteClient[_] = null
      try {
        client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, confs,
          tableName.asInstanceOf[Option[String]])
        // Pre-check: if the target instant is at or before the penultimate/oldest MDT compaction,
        // pre-emptively delete the MDT so restoreToInstant does not leave it inconsistent.
        // deleteMetadataTableIfNecessaryBeforeRestore returns false when the MDT was deleted
        // (caller must not re-initialize it), true otherwise.
        val shouldInitMdt = if (enableMetadata) {
          client.deleteMetadataTableIfNecessaryBeforeRestore(targetInstant)
        } else true
        val restoreMetadata = client.restoreToInstant(targetInstant, shouldInitMdt && enableMetadata)
        restoreResult = true
        startRestoreTime = restoreMetadata.getStartRestoreTime
        timeTakenInMillis = restoreMetadata.getTimeTakenInMillis
        instantsRolledBack = restoreMetadata.getInstantsToRollback.size().toLong
      } finally {
        if (client != null) {
          client.close()
        }
      }
      if (tableName.isDefined) {
        spark.catalog.refreshTable(tableName.get.asInstanceOf[String])
      }
      // getActiveTimeline is lazily cached; reload so the new .restore instant is visible to the audit.
      if (shouldAuditPostRestore) {
        metaClient.reloadActiveTimeline()
      }
    }

    var auditResult: String = null
    if (auditOnly || shouldAuditPostRestore) {
      val restoreInstant: HoodieInstant = if (auditOnly) {
        val ts = startRestoreTimeArg.get.asInstanceOf[String]
        val instants = metaClient.getActiveTimeline.getRestoreTimeline.filterCompletedInstants
          .getInstants.asScala
        instants.find(_.requestedTime().equals(ts)).getOrElse(
          throw new HoodieException(s"No completed restore instant found for $ts. " +
            "Pass the start_restore_time from a prior restore_to_instant call as start_restore_time.")
        )
      } else {
        // Use startRestoreTime captured from the restore metadata — more deterministic than
        // lastInstant() since another concurrent restore could otherwise land in between.
        val ts = startRestoreTime
        val instants = metaClient.getActiveTimeline.getRestoreTimeline.filterCompletedInstants
          .getInstants.asScala
        instants.find(_.requestedTime().equals(ts)).getOrElse(
          throw new HoodieException(s"No completed restore instant found for $ts after restore.")
        )
      }
      auditResult = auditPostRestore(metaClient, basePath, restoreInstant, rollbackParallelism)
    }

    Seq(Row(restoreResult, startRestoreTime, timeTakenInMillis, instantsRolledBack, auditResult))
  }

  /**
   * Verifies that all files expected to have been deleted by a restore operation are actually
   * absent from storage. Returns one of "PASSED", "FAILED", or "INCONCLUSIVE":
   *   - PASSED: every file expected to be absent is in fact absent.
   *   - FAILED: at least one file is still present after restore.
   *   - INCONCLUSIVE: no file was confirmed present, but at least one existence check threw
   *                   an IOException (e.g. transient cloud-storage timeout). Re-run with
   *                   audit_only=true to retry.
   */
  private def auditPostRestore(
      metaClient: HoodieTableMetaClient,
      basePath: String,
      restoreInstant: HoodieInstant,
      rollbackParallelism: Int): String = {
    try {
      val restoreMetadata: HoodieRestoreMetadata =
        metaClient.getActiveTimeline.readRestoreMetadata(restoreInstant)

      val filesToCheck = new java.util.ArrayList[String]()
      restoreMetadata.getHoodieRestoreMetadata.values.asScala.foreach { rollbackList =>
        rollbackList.asScala.foreach { rollback =>
          rollback.getPartitionMetadata.asScala.foreach { case (partition, pm) =>
            val partitionPathStr = basePath + Path.SEPARATOR + partition
            val partitionStoragePath = new StoragePath(partitionPathStr)
            if (!metaClient.getStorage.exists(partitionStoragePath)) {
              logInfo(s"Partition path $partitionStoragePath does not exist. Skipping audit for its files.")
            } else {
              // Use .toString() to preserve the full absolute path. Using .getName() would strip
              // the path to just the filename, making the subsequent FS.exists() check vacuously pass.
              // Only check getSuccessDeleteFiles: these are the files the restore intended to delete
              // and whose absence we can meaningfully verify. Files in getFailedDeleteFiles already
              // failed to be deleted and are expected to still be present.
              pm.getSuccessDeleteFiles.asScala.foreach(f =>
                filesToCheck.add(new Path(partitionPathStr, f).toString))
            }
          }
        }
      }

      if (filesToCheck.isEmpty) {
        logInfo(s"No files to audit for restore instant ${restoreInstant.requestedTime()}")
        "PASSED"
      } else {
        // HadoopFSUtils.getStorageConfWithCopy returns a Serializable StorageConfiguration; the
        // closure captures only storageConf, so Spark's ClosureCleaner can serialize it cleanly.
        // HadoopFSUtils.getFs internally calls prepareHadoopConf, preserving HOODIE_ENV_* (e.g.
        // S3A) injection at cloud DCs.
        val storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())
        val outcomes = jsc.parallelize(filesToCheck, Math.max(1, rollbackParallelism))
          .map { pathStr =>
            val p = new Path(pathStr)
            try {
              val exists = HadoopFSUtils.getFs(p, storageConf.unwrap()).exists(p)
              val status: AuditFileStatus = if (exists) Present else Absent
              (pathStr, status)
            } catch {
              case e: Exception => (pathStr, IndeterminateError(e.getMessage))
            }
          }
          .collect()
          .asScala
          .toArray

        val present = outcomes.collect { case (p, Present) => p }
        val indeterminate = outcomes.collect { case (p, IndeterminateError(msg)) => (p, msg) }
        if (present.nonEmpty) {
          logError(s"Restore audit FAILED for instant ${restoreInstant.requestedTime()}: " +
            s"${present.length} file(s) still present, e.g. ${present.take(5).mkString(", ")}")
          "FAILED"
        } else if (indeterminate.nonEmpty) {
          logWarning(s"Restore audit INCONCLUSIVE for instant ${restoreInstant.requestedTime()}: " +
            s"${indeterminate.length} file(s) had IO errors, e.g. ${indeterminate.take(5).mkString(", ")}")
          "INCONCLUSIVE"
        } else {
          logInfo(s"Restore audit PASSED for instant ${restoreInstant.requestedTime()}: " +
            s"${outcomes.length} file(s) confirmed absent.")
          "PASSED"
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Exception during restore audit for instant ${restoreInstant.requestedTime()}", e)
        "INCONCLUSIVE"
    }
  }

  override def build: Procedure = new RestoreToInstantProcedure()
}

object RestoreToInstantProcedure {
  val NAME: String = "restore_to_instant"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): RestoreToInstantProcedure = new RestoreToInstantProcedure()
  }

  // ADT for per-file audit outcomes. Defined at object level (NOT inside auditPostRestore) so the
  // generated bytecode does not embed a synthetic outer-class reference; otherwise Spark's
  // ClosureCleaner cannot serialize the closure that returns these values from executors.
  private sealed trait AuditFileStatus extends Serializable
  private case object Absent extends AuditFileStatus
  private case object Present extends AuditFileStatus
  private case class IndeterminateError(message: String) extends AuditFileStatus
}
