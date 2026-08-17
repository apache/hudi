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

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.HoodieStorageUtils
import org.apache.hudi.metadata.{FileSystemBackedTableMetadata, HoodieBackedTableMetadata}
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}
import org.apache.hudi.table.repair.RepairUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Spark SQL stored procedure that finds and optionally removes orphan data files — files
 * that exist on the filesystem but are not referenced by any commit (active or archived).
 *
 * Handles COW (base files) and MOR (base + log files), and all commit action types
 * (COMMIT, DELTA_COMMIT, REPLACE_COMMIT). The detection reuses
 * [[org.apache.hudi.table.repair.RepairUtils]], the same logic that backs the
 * `HoodieRepairTool` spark-submit utility, so results are consistent with that tool.
 *
 * Usage:
 * {{{
 * -- View mode (default): list orphan files without touching them
 * CALL repair_orphan_files(table => 'my_table')
 *
 * -- Scoped to one partition
 * CALL repair_orphan_files(table => 'my_table', partition => '2024/01/15')
 *
 * -- Cleanup: move orphan files to a backup location
 * CALL repair_orphan_files(
 *   table       => 'my_table',
 *   dry_run     => false,
 *   backup_path => '/user/hudi/orphan_files_backup'
 * )
 * }}}
 *
 * For very large tables, scope to one partition at a time using `partition =>` to avoid
 * collecting all orphan paths to the driver at once. The `max_orphans` parameter (default
 * 100,000) acts as a safety cap: if the detected count exceeds it the procedure fails with
 * a clear error instead of silently causing a driver OOM.
 */
class RepairOrphanFilesProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table",              DataTypes.StringType,   null),
    ProcedureParameter.optional(1, "path",               DataTypes.StringType,   null),
    ProcedureParameter.optional(2, "partition",          DataTypes.StringType,   ""),
    ProcedureParameter.optional(3, "dry_run",            DataTypes.BooleanType,  true),
    ProcedureParameter.optional(4, "backup_path",        DataTypes.StringType,   ""),
    ProcedureParameter.optional(5, "archived_start_ts",  DataTypes.StringType,   ""),
    ProcedureParameter.optional(6, "archived_end_ts",    DataTypes.StringType,   ""),
    ProcedureParameter.optional(7, "max_orphans",        DataTypes.IntegerType,  100000)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition",    DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_name",    DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("backup_path",  DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("status",       DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName       = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePathOpt    = getArgValueOrDefault(args, PARAMETERS(1))
    val partition       = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val dryRun          = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val backupPath      = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val archivedStartTs = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val archivedEndTs   = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val maxOrphans      = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[Int]

    if (!dryRun && backupPath.isEmpty) {
      throw new IllegalArgumentException("backup_path is required when dry_run is false")
    }

    // Phase 1: Partition listing (driver)
    val basePath   = getBasePath(tableName, tablePathOpt)
    val metaClient = createMetaClient(jsc, basePath)

    val partitions: java.util.List[String] =
      if (partition.nonEmpty) {
        java.util.Collections.singletonList(partition)
      } else {
        // Use FileSystemBackedTableMetadata (filesystem listing, no MDT) for partition discovery.
        // This avoids any reliance on the metadata table being present/consistent — the same
        // approach HoodieRepairTool uses.
        new FileSystemBackedTableMetadata(
          new HoodieLocalEngineContext(metaClient.getStorageConf),
          metaClient.getTableConfig, metaClient.getStorage, basePath).getAllPartitionPaths
      }

    if (partitions.isEmpty) {
      Seq.empty
    } else {
      doRepairOrphanFiles(basePath, metaClient, partitions, dryRun, backupPath,
        archivedStartTs, archivedEndTs, maxOrphans)
    }
  }

  private def doRepairOrphanFiles(
      basePath: String,
      metaClient: HoodieTableMetaClient,
      partitions: java.util.List[String],
      dryRun: Boolean,
      backupPath: String,
      archivedStartTs: String,
      archivedEndTs: String,
      maxOrphans: Int): Seq[Row] = {
    // Build the active and archived timelines once on the driver, loading completed-instant
    // details into memory so executors can read commit metadata without further I/O. This is
    // the same pattern as HoodieRepairTool: the loaded timelines are serializable and captured
    // by the RDD closure below. The HoodieTableMetaClient itself is not captured (not needed
    // on executors once details are loaded).
    val activeTimeline = metaClient.getActiveTimeline
    val archivedTimeline =
      if (archivedStartTs.nonEmpty) metaClient.getArchivedTimeline(archivedStartTs)
      else metaClient.getArchivedTimeline()
    archivedTimeline.loadCompletedInstantDetailsInMemory()

    // StorageConfiguration is Serializable and is the only stateful value captured into the
    // closure; storage handles are rebuilt per task from it.
    val storageConf = metaClient.getStorageConf
    val basePathStr = basePath
    val archStartTs = archivedStartTs
    val archEndTs   = archivedEndTs

    // Phase 2: Parallel orphan file detection (Spark RDD, one task per partition). Each task
    // lists its own partition and runs detection locally, so only the (small) set of orphan
    // candidates is collected back to the driver rather than the full file listing.
    val orphanRelPaths: List[String] = jsc.parallelize(partitions, partitions.size())
      .rdd
      .flatMap { partitionStr =>
        val storage     = HoodieStorageUtils.getStorage(basePathStr, storageConf)
        val partPath    = FSUtils.getAbsolutePartitionPath(new StoragePath(basePathStr), partitionStr)
        // getAllDataFilesInPartition handles FileNotFoundException (partition deleted between
        // listing and task execution) by returning an empty list rather than throwing.
        val allStatuses = FSUtils.getAllDataFilesInPartition(storage, partPath)
        val allPaths    = allStatuses.asScala.map((info: StoragePathInfo) => info.getPath).asJava

        val instantToFilesMap = RepairUtils.tagInstantsOfBaseAndLogFiles(basePathStr, allPaths)

        if (instantToFilesMap.isEmpty) {
          Iterator.empty
        } else {
          // Optionally scope detection to instants within [archived_start_ts, archived_end_ts].
          // Instants outside the range are left untouched (not reported as orphans).
          val instants = instantToFilesMap.keySet.asScala.toSeq.sorted.filter { instant =>
            (archStartTs.isEmpty || instant >= archStartTs) &&
              (archEndTs.isEmpty || instant <= archEndTs)
          }

          instants.flatMap { instant =>
            RepairUtils.findInstantFilesToRemove(
              instant,
              instantToFilesMap.get(instant),
              activeTimeline,
              archivedTimeline
            ).asScala
          }.iterator
        }
      }
      .collect()
      .toList

    if (orphanRelPaths.size > maxOrphans) {
      throw new IllegalStateException(
        s"Found ${orphanRelPaths.size} orphan candidates, which exceeds max_orphans=$maxOrphans. " +
        s"Re-run with partition => '<partition>' to scope to one partition at a time, " +
        s"or raise max_orphans if you are sure the driver has enough memory.")
    }

    // Phase 3: Metadata table (MDT) safety check (driver).
    // Files still visible in the MDT are not true orphans — surface them as
    // SKIPPED_PRESENT_IN_MDT (per-file exclusion) so the operator sees which candidates the
    // safety check held back, rather than silently dropping them.
    val mdtConfig = HoodieMetadataConfig.newBuilder
      .enable(true).ignoreSpuriousDeletes(true).build
    val mdtReader = new HoodieBackedTableMetadata(
      new HoodieLocalEngineContext(metaClient.getStorageConf), metaClient.getStorage, mdtConfig, basePath)

    val mdtUnsafePaths: Set[String] =
      if (mdtReader.enabled) {
        val byPartition = orphanRelPaths.groupBy(partitionOf)
        val unsafe = byPartition.flatMap { case (partRel, paths) =>
          val mdtPartPath = FSUtils.getAbsolutePartitionPath(new StoragePath(basePath), partRel)
          val mdtNames = mdtReader.getAllFilesInPartition(mdtPartPath).asScala
            .map(_.getPath.getName).toSet
          paths.filter(p => mdtNames.contains(new StoragePath(p).getName))
        }.toSet
        if (unsafe.nonEmpty) {
          logWarning(s"Found ${unsafe.size} orphan candidate(s) still visible in MDT — " +
            s"emitting as SKIPPED_PRESENT_IN_MDT (per-file exclusion): $unsafe")
        }
        unsafe
      } else {
        logWarning("Metadata table not enabled; skipping MDT safety cross-check")
        Set.empty[String]
      }

    val safeOrphanPaths = orphanRelPaths.filterNot(mdtUnsafePaths.contains)

    // Phase 4: Build result rows.
    val skippedRows: Seq[Row] = mdtUnsafePaths.toSeq.map { relPath =>
      val fileName    = new StoragePath(relPath).getName
      val partRel     = partitionOf(relPath)
      val instantTime = FSUtils.getCommitTime(fileName)
      Row(partRel, fileName, instantTime, "", "SKIPPED_PRESENT_IN_MDT")
    }

    val safeRows: Seq[Row] = if (dryRun) {
      safeOrphanPaths.map { relPath =>
        val fileName    = new StoragePath(relPath).getName
        val partRel     = partitionOf(relPath)
        val instantTime = FSUtils.getCommitTime(fileName)
        Row(partRel, fileName, instantTime, "", "IDENTIFIED")
      }
    } else {
      val storage = metaClient.getStorage
      val tableNm = metaClient.getTableConfig.getTableName  // always from metaClient — the 'table'
                                                            // proc arg is null when invoked via path =>
      safeOrphanPaths.map { relPath =>
        val fileName    = new StoragePath(relPath).getName
        val partRel     = partitionOf(relPath)
        val instantTime = FSUtils.getCommitTime(fileName)
        val srcPath     = new StoragePath(basePath, relPath)
        // Non-partitioned tables have partRel="" — back up directly under <backup>/<table>.
        val destDir     =
          if (partRel.isEmpty) new StoragePath(s"$backupPath/$tableNm")
          else new StoragePath(s"$backupPath/$tableNm/$partRel")
        val destPath    = new StoragePath(destDir, fileName)

        // Each storage op is wrapped to capture exception class+message — a backup can fail for
        // permissions, missing parent, RPC, or concurrent-delete reasons, and a status of
        // BACKUP_FAILED with no log line leaves the operator with nothing to diagnose. Causes
        // are accumulated and logged once at the end iff the final outcome is a failure.
        val causes = scala.collection.mutable.ArrayBuffer.empty[String]

        val dirCreated: Boolean =
          try {
            val ok = storage.createDirectory(destDir)
            if (!ok) causes += s"createDirectory($destDir)=false (likely permissions or destDir exists as a file)"
            ok
          } catch {
            case t: Throwable =>
              causes += s"createDirectory($destDir) threw ${t.getClass.getSimpleName}: ${t.getMessage}"
              false
          }

        val moved: Boolean =
          if (!dirCreated) {
            false
          } else {
            try {
              val ok = storage.rename(srcPath, destPath)
              if (!ok) causes += s"rename returned false (srcPath missing, destPath exists, or cross-volume rename)"
              ok
            } catch {
              case t: Throwable =>
                causes += s"rename threw ${t.getClass.getSimpleName}: ${t.getMessage}"
                false
            }
          }

        // A concurrent cleaner may have already deleted srcPath — that is success, not failure.
        val srcStillExists: Boolean =
          try {
            storage.exists(srcPath)
          } catch {
            case t: Throwable =>
              // Cannot confirm the concurrent-delete recovery path; treat as failure.
              causes += s"exists($srcPath) threw ${t.getClass.getSimpleName}: ${t.getMessage}"
              true
          }
        val succeeded = moved || !srcStillExists

        if (!succeeded) {
          logWarning(s"BACKUP_FAILED for $srcPath -> $destPath; causes: ${causes.mkString("; ")}")
        }
        val status    = if (succeeded) "BACKED_UP" else "BACKUP_FAILED"
        val backupOut = if (succeeded) destPath.toString else ""
        Row(partRel, fileName, instantTime, backupOut, status)
      }
    }

    safeRows ++ skippedRows
  }

  // Extract the relative partition path from a relative file path using string ops. This avoids
  // any path-normalization surprises and works uniformly for partitioned and non-partitioned
  // tables (root files yield "").
  private def partitionOf(relPath: String): String = {
    val s = relPath.lastIndexOf('/')
    if (s < 0) "" else relPath.substring(0, s)
  }

  override def build: Procedure = new RepairOrphanFilesProcedure()
}

object RepairOrphanFilesProcedure {
  val NAME = "repair_orphan_files"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new RepairOrphanFilesProcedure()
  }
}
