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
import org.apache.hudi.io.util.FileIOUtils
import org.apache.hudi.storage.StoragePath

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.util.{Failure, Success, Try}

/**
 * Spark SQL procedure for showing the current audit logging status for Hudi tables.
 *
 * This procedure allows users to check whether audit logging is currently enabled
 * or disabled for storage lock operations. It reads the audit configuration file
 * and reports the current status along with relevant paths.
 *
 * Usage:
 * {{{
 * CALL show_audit_lock_status(table => 'my_table')
 * CALL show_audit_lock_status(path => '/path/to/table')
 * }}}
 *
 * The procedure reads the audit configuration file from:
 * `{table_path}/.hoodie/.locks/audit_enabled.json`
 *
 * @author Apache Hudi
 * @since 1.0.0
 */
class ShowAuditLockStatusProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("table", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("audit_enabled", DataTypes.BooleanType, nullable = false, Metadata.empty),
    StructField("config_path", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("audit_folder_path", DataTypes.StringType, nullable = false, Metadata.empty)
  ))

  private val OBJECT_MAPPER = new ObjectMapper()

  /**
   * Returns the procedure parameters definition.
   *
   * @return Array of parameters: table (optional String) and path (optional String)
   */
  def parameters: Array[ProcedureParameter] = PARAMETERS

  /**
   * Returns the output schema for the procedure result.
   *
   * @return StructType containing table, audit_enabled, config_path, and audit_folder_path columns
   */
  def outputType: StructType = OUTPUT_TYPE

  /**
   * Executes the show audit lock status procedure.
   *
   * @param args Procedure arguments containing table name or path
   * @return Sequence containing a single Row with status information
   * @throws IllegalArgumentException if neither table nor path is provided, or both are provided
   */
  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))

    // Get the base path using BaseProcedure helper (handles table/path validation)
    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    // Use table name if provided, otherwise extract from path
    val displayName = tableName.map(_.asInstanceOf[String]).getOrElse(tablePath.get.asInstanceOf[String])

    try {
      val auditStatus = checkAuditStatus(metaClient, basePath)
      val configPath = StorageLockProviderAuditService.getAuditConfigPath(basePath)
      val auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(basePath)

      Seq(Row(displayName, auditStatus, configPath, auditFolderPath))
    } catch {
      case e: Exception =>
        // Return false for audit status if we can't read the config
        val configPath = StorageLockProviderAuditService.getAuditConfigPath(basePath)
        val auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(basePath)
        Seq(Row(displayName, false, configPath, auditFolderPath))
    }
  }

  /**
   * Checks the current audit status by reading the audit configuration file.
   *
   * @param metaClient Hudi table meta client for storage operations
   * @param basePath Base path of the Hudi table
   * @return true if audit logging is enabled, false otherwise
   */
  private def checkAuditStatus(metaClient: org.apache.hudi.common.table.HoodieTableMetaClient, basePath: String): Boolean = {
    val storage = metaClient.getStorage
    val auditConfigPath = new StoragePath(StorageLockProviderAuditService.getAuditConfigPath(basePath))

    if (!storage.exists(auditConfigPath)) {
      false
    } else {

      Try {
        val inputStream = storage.open(auditConfigPath)
        try {
          val configContent = new String(FileIOUtils.readAsByteArray(inputStream))
          val rootNode: JsonNode = OBJECT_MAPPER.readTree(configContent)
          val enabledNode = rootNode.get(StorageLockProviderAuditService.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD)
          Option(enabledNode).exists(_.asBoolean(false))
        } finally {
          inputStream.close()
        }
      } match {
        case Success(enabled) => enabled
        case Failure(_) => false
      }
    }
  }

  /**
   * Builds a new instance of the ShowAuditLockStatusProcedure.
   *
   * @return New ShowAuditLockStatusProcedure instance
   */
  override def build: Procedure = new ShowAuditLockStatusProcedure()
}

/**
 * Companion object for ShowAuditLockStatusProcedure containing constants and factory methods.
 */
object ShowAuditLockStatusProcedure {
  /** The name used to register and invoke this procedure */
  val NAME = "show_audit_lock_status"

  /**
   * Factory method to create procedure builder instances.
   *
   * @return Supplier that creates new ShowAuditLockStatusProcedure instances
   */
  def builder: Supplier[ProcedureBuilder] = () => new ShowAuditLockStatusProcedure()
}
