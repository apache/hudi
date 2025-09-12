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

import org.apache.hudi.client.transaction.lock.StorageLockClient
import org.apache.hudi.client.transaction.lock.audit.StorageLockProviderAuditService
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.util.{Failure, Success, Try}


/**
 * Spark SQL procedure for enabling or disabling lock audit logging for Hudi tables.
 *
 * This procedure allows users to control audit logging for storage lock operations through
 * Spark SQL commands. When enabled, lock operations will generate audit logs in JSONL format
 * that track lock lifecycle events.
 *
 * Usage:
 * {{{
 * CALL set_audit_lock(table => 'my_table', state => 'enabled')
 * CALL set_audit_lock(path => '/path/to/table', state => 'disabled')
 * }}}
 *
 * The procedure creates or updates an audit configuration file at:
 * `{table_path}/.hoodie/.locks/audit_enabled.json`
 *
 * @author Apache Hudi
 * @since 1.0.0
 */
class SetAuditLockProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.required(2, "state", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("table", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("audit_state", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("message", DataTypes.StringType, nullable = false, Metadata.empty)
  ))

  private val OBJECT_MAPPER = new ObjectMapper()

  /**
   * Returns the procedure parameters definition.
   *
   * @return Array of parameters: table (optional String), path (optional String), and state (required String)
   */
  def parameters: Array[ProcedureParameter] = PARAMETERS

  /**
   * Returns the output schema for the procedure result.
   *
   * @return StructType containing table, audit_state, and message columns
   */
  def outputType: StructType = OUTPUT_TYPE

  /**
   * Executes the audit lock set procedure.
   *
   * @param args Procedure arguments containing table name or path and desired state
   * @return Sequence containing a single Row with execution results
   * @throws IllegalArgumentException if state parameter is not 'enabled' or 'disabled'
   */
  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val state = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String].toLowerCase

    // Validate state parameter
    if (state != "enabled" && state != "disabled") {
      throw new IllegalArgumentException("State parameter must be 'enabled' or 'disabled'")
    }

    // Get the base path using BaseProcedure helper (handles table/path validation)
    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    // Use table name if provided, otherwise extract from path
    val displayName = tableName.map(_.asInstanceOf[String]).getOrElse(tablePath.get.asInstanceOf[String])

    try {
      val auditEnabled = state == "enabled"
      setAuditState(metaClient, basePath, auditEnabled)

      val resultState = if (auditEnabled) "enabled" else "disabled"
      val message = s"Lock audit logging successfully $resultState"

      Seq(Row(displayName, resultState, message))
    } catch {
      case e: Exception =>
        val errorMessage = s"Failed to set audit state: ${e.getMessage}"
        Seq(Row(displayName, "error", errorMessage))
    }
  }

  /**
   * Sets the audit state by creating or updating the audit configuration file.
   *
   * @param metaClient Hudi table meta client for storage operations
   * @param basePath Base path of the Hudi table
   * @param enabled Whether audit logging should be enabled
   * @throws RuntimeException if unable to write the audit configuration
   */
  private def setAuditState(metaClient: HoodieTableMetaClient, basePath: String, enabled: Boolean): Unit = {
    val storage = metaClient.getStorage
    val lockFolderPath = StorageLockClient.getLockFolderPath(basePath)
    val auditConfigPath = new StoragePath(StorageLockProviderAuditService.getAuditConfigPath(basePath))

    // Ensure the locks folder exists
    if (!storage.exists(new StoragePath(lockFolderPath))) {
      storage.createDirectory(new StoragePath(lockFolderPath))
    }

    // Create or update the audit configuration file
    val jsonContent = createAuditConfig(enabled)

    Try {
      val outputStream = storage.create(auditConfigPath, true) // overwrite if exists
      try {
        outputStream.write(jsonContent.getBytes("UTF-8"))
      } finally {
        outputStream.close()
      }
    } match {
      case Success(_) =>
        // Configuration written successfully
      case Failure(exception) =>
        throw new RuntimeException(s"Failed to write audit configuration to ${auditConfigPath.toString}", exception)
    }
  }

  /**
   * Creates the JSON configuration content for audit settings.
   *
   * @param enabled Whether audit logging should be enabled
   * @return JSON string representation of the audit configuration
   */
  private def createAuditConfig(enabled: Boolean): String = {
    val rootNode: ObjectNode = OBJECT_MAPPER.createObjectNode()
    rootNode.put(StorageLockProviderAuditService.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD, enabled)
    OBJECT_MAPPER.writeValueAsString(rootNode)
  }

  override def build: Procedure = new SetAuditLockProcedure()
}

/**
 * Companion object for SetAuditLockProcedure containing constants and factory methods.
 */
object SetAuditLockProcedure {
  val NAME = "set_audit_lock"

  /**
   * Factory method to create procedure builder instances.
   *
   * @return Supplier that creates new SetAuditLockProcedure instances
   */
  def builder: Supplier[ProcedureBuilder] = () => new SetAuditLockProcedure()
}
