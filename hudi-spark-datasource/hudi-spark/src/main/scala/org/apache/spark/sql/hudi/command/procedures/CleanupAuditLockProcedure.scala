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

import org.apache.hudi.client.transaction.lock.audit.StorageLockProviderAuditService
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.collection.JavaConverters._

/**
 * Spark SQL procedure for cleaning up old audit lock files for Hudi tables.
 *
 * This procedure removes audit lock files that are older than a specified number
 * of days. It supports dry-run mode to preview which files would be deleted
 * without actually removing them.
 *
 * Usage:
 * {{{
 * CALL cleanup_audit_lock(table => 'my_table')
 * CALL cleanup_audit_lock(path => '/path/to/table', dry_run => true, age_days => 30)
 * }}}
 *
 * The procedure operates on audit files in:
 * `{table_path}/.hoodie/.locks/audit/`
 *
 * @author Apache Hudi
 * @since 1.0.0
 */
class CleanupAuditLockProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "dry_run", DataTypes.BooleanType, false),
    ProcedureParameter.optional(3, "age_days", DataTypes.IntegerType, 7)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("table", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("files_cleaned", DataTypes.IntegerType, nullable = false, Metadata.empty),
    StructField("dry_run", DataTypes.BooleanType, nullable = false, Metadata.empty),
    StructField("age_days", DataTypes.IntegerType, nullable = false, Metadata.empty),
    StructField("message", DataTypes.StringType, nullable = false, Metadata.empty)
  ))

  /**
   * Returns the procedure parameters definition.
   *
   * @return Array of parameters: table (optional String), path (optional String),
   *         dry_run (optional Boolean, default false), and age_days (optional Integer, default 7)
   */
  def parameters: Array[ProcedureParameter] = PARAMETERS

  /**
   * Returns the output schema for the procedure result.
   *
   * @return StructType containing table, files_cleaned, dry_run, age_days, and message columns
   */
  def outputType: StructType = OUTPUT_TYPE

  /**
   * Executes the audit lock cleanup procedure.
   *
   * @param args Procedure arguments containing table name or path, dry_run flag, and age threshold
   * @return Sequence containing a single Row with cleanup results
   * @throws IllegalArgumentException if neither table nor path is provided, or both are provided
   */
  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val dryRun = getArgValueOrDefault(args, PARAMETERS(2)).getOrElse(false).asInstanceOf[Boolean]
    val ageDays = getArgValueOrDefault(args, PARAMETERS(3)).getOrElse(7).asInstanceOf[Int]

    // Validate age_days is positive
    if (ageDays <= 0) {
      throw new IllegalArgumentException("age_days must be a positive integer")
    }

    // Get the base path using BaseProcedure helper (handles table/path validation)
    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    // Use table name if provided, otherwise extract from path
    val displayName = tableName.map(_.asInstanceOf[String]).getOrElse(tablePath.get.asInstanceOf[String])

    try {
      val auditFolderPath = new StoragePath(StorageLockProviderAuditService.getAuditFolderPath(basePath))
      val storage = metaClient.getStorage

      // Check if audit folder exists
      if (!storage.exists(auditFolderPath)) {
        val message = "No audit folder found - nothing to cleanup"
        Seq(Row(displayName, 0, dryRun, ageDays, message))
      } else {

        // Calculate cutoff timestamp (ageDays ago)
        val cutoffTime = System.currentTimeMillis() - (ageDays * 24 * 60 * 60 * 1000L)

        // List all files in audit folder and filter by modification time
        val allFiles = storage.listDirectEntries(auditFolderPath).asScala
        val auditFiles = allFiles.filter(pathInfo => pathInfo.isFile && pathInfo.getPath.getName.endsWith(".jsonl"))
        val oldFiles = auditFiles.filter(_.getModificationTime < cutoffTime)

        if (oldFiles.isEmpty) {
          val message = s"No audit files older than $ageDays days found"
          Seq(Row(displayName, 0, dryRun, ageDays, message))
        } else {

          val fileCount = oldFiles.size

          if (dryRun) {
            val message = s"Dry run: Would delete $fileCount audit files older than $ageDays days"
            Seq(Row(displayName, fileCount, dryRun, ageDays, message))
          } else {
            // Actually delete the files
            var deletedCount = 0
            var failedCount = 0

            oldFiles.foreach { pathInfo =>
              try {
                storage.deleteFile(pathInfo.getPath)
                deletedCount += 1
              } catch {
                case e: Exception =>
                  failedCount += 1
                  // Log the error but continue with other files
              }
            }

            val message = if (failedCount == 0) {
              s"Successfully deleted $deletedCount audit files older than $ageDays days"
            } else {
              s"Deleted $deletedCount audit files, failed to delete $failedCount files"
            }

            Seq(Row(displayName, deletedCount, dryRun, ageDays, message))
          }
        }
      }
    } catch {
      case e: Exception =>
        val errorMessage = s"Cleanup failed: ${e.getMessage}"
        Seq(Row(displayName, 0, dryRun, ageDays, errorMessage))
    }
  }

  /**
   * Builds a new instance of the CleanupAuditLockProcedure.
   *
   * @return New CleanupAuditLockProcedure instance
   */
  override def build: Procedure = new CleanupAuditLockProcedure()
}


/**
 * Companion object for CleanupAuditLockProcedure containing constants and factory methods.
 */
object CleanupAuditLockProcedure {
  /** The name used to register and invoke this procedure */
  val NAME = "cleanup_audit_lock"

  /**
   * Factory method to create procedure builder instances.
   *
   * @return Supplier that creates new CleanupAuditLockProcedure instances
   */
  def builder: Supplier[ProcedureBuilder] = () => new CleanupAuditLockProcedure()
}