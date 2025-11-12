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
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncConfigHolder, HiveSyncTool}
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class HiveSyncProcedure extends BaseProcedure with ProcedureBuilder
  with ProvidesHoodieConfig with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "metastore_uri", DataTypes.StringType, ""),
    ProcedureParameter.optional(2, "username", DataTypes.StringType, ""),
    ProcedureParameter.optional(3, "password", DataTypes.StringType, ""),
    ProcedureParameter.optional(4, "use_jdbc", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "mode", DataTypes.StringType, ""),
    ProcedureParameter.optional(6, "partition_fields", DataTypes.StringType, ""),
    ProcedureParameter.optional(7, "partition_extractor_class", DataTypes.StringType, ""),
    ProcedureParameter.optional(8, "strategy", DataTypes.StringType, ""),
    ProcedureParameter.optional(9, "sync_incremental", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def build: Procedure = new HiveSyncProcedure

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val metastoreUri = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val username = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val password = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    val useJdbc = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val mode = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val partitionFields = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val partitionExtractorClass = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[String]
    val strategy = getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[String]
    val syncIncremental = getArgValueOrDefault(args, PARAMETERS(9)).get.asInstanceOf[String]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, tableName)
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val hiveConf = new HiveConf
    val sqlConf = sparkSession.sessionState.conf

    if (metastoreUri.nonEmpty) hadoopConf.set(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUri)
    if (username.nonEmpty) sqlConf.setConfString(HiveSyncConfig.HIVE_USER.key, username)
    if (password.nonEmpty) sqlConf.setConfString(HiveSyncConfig.HIVE_PASS.key, password)
    if (useJdbc.nonEmpty) sqlConf.setConfString(HiveSyncConfig.HIVE_USE_JDBC.key, useJdbc)
    if (mode.nonEmpty) sqlConf.setConfString(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, mode)
    if (partitionFields.nonEmpty) sqlConf.setConfString(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key, partitionFields)
    if (partitionExtractorClass.nonEmpty) sqlConf.setConfString(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key, partitionExtractorClass)
    if (strategy.nonEmpty) sqlConf.setConfString(HiveSyncConfigHolder.HIVE_SYNC_TABLE_STRATEGY.key, strategy)
    if (syncIncremental.nonEmpty) sqlConf.setConfString(HoodieSyncConfig.META_SYNC_INCREMENTAL.key, syncIncremental)

    hiveConf.addResource(hadoopConf)

    val tableConfig = hoodieCatalogTable.tableConfig
    val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)

    var hiveSyncTool: HiveSyncTool = null
    try {
      hiveSyncTool = new HiveSyncTool(hiveSyncConfig.getProps, hiveConf)
      hiveSyncTool.syncHoodieTable()
    } catch {
      case e: RuntimeException => throw new HoodieException("hive sync failed", e)
    } finally {
      if (hiveSyncTool != null) hiveSyncTool.close()
    }

    Seq(Row("hive sync success."))
  }
}

object HiveSyncProcedure {
  val NAME = "hive_sync"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new HiveSyncProcedure
  }
}
