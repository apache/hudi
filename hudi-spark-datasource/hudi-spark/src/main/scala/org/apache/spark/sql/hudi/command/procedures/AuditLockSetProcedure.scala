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

import org.apache.hudi.common.config.HoodieLockAuditConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import java.util.function.Supplier

import scala.util.{Failure, Success, Try}

class AuditLockSetProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "state", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("table", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("audit_state", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("message", DataTypes.StringType, nullable = false, Metadata.empty)
  ))

  private val OBJECT_MAPPER = new ObjectMapper()

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val state = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String].toLowerCase

    // Validate state parameter
    if (state != "enabled" && state != "disabled") {
      throw new IllegalArgumentException("State parameter must be 'enabled' or 'disabled'")
    }

    val basePath: String = getBasePath(getArgValueOrDefault(args, PARAMETERS(0)), Option.empty)
    val metaClient = createMetaClient(jsc, basePath)
    
    try {
      val auditEnabled = state == "enabled"
      setAuditState(metaClient, basePath, auditEnabled)
      
      val resultState = if (auditEnabled) "enabled" else "disabled"
      val message = s"Lock audit logging successfully $resultState"
      
      Seq(Row(tableName, resultState, message))
    } catch {
      case e: Exception =>
        val errorMessage = s"Failed to set audit state: ${e.getMessage}"
        Seq(Row(tableName, "error", errorMessage))
    }
  }

  private def setAuditState(metaClient: HoodieTableMetaClient, basePath: String, enabled: Boolean): Unit = {
    val storage = metaClient.getStorage
    val lockFolderPath = getLockFolderPath(basePath)
    val auditConfigPath = new StoragePath(lockFolderPath, HoodieLockAuditConfig.AUDIT_CONFIG_FILE_NAME)

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

  private def createAuditConfig(enabled: Boolean): String = {
    val rootNode: ObjectNode = OBJECT_MAPPER.createObjectNode()
    rootNode.put(HoodieLockAuditConfig.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD, enabled)
    OBJECT_MAPPER.writeValueAsString(rootNode)
  }

  private def getLockFolderPath(basePath: String): String = {
    s"$basePath${StoragePath.SEPARATOR}${HoodieTableMetaClient.LOCKS_FOLDER_NAME}"
  }

  override def build: Procedure = new AuditLockSetProcedure()
}

object AuditLockSetProcedure {
  val NAME = "audit_lock_set"

  def builder: Supplier[ProcedureBuilder] = () => new AuditLockSetProcedure()
}