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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieSyncTableStrategy
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncTool}
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand.buildHiveSyncConfig
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.Properties
import java.util.function.Supplier
import scala.collection.JavaConverters._

class TruncateTableProducer extends BaseProcedure
  with ProcedureBuilder
  with Logging {
  override def build: Procedure = new TruncateTableProducer

  override def parameters: Array[ProcedureParameter] = Array[ProcedureParameter] (
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "partitions", DataTypes.StringType)
  )

  override def outputType: StructType = StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_cost", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(parameters, args)

    val tableName = getArgValueOrDefault(args, parameters(0))
    val syncHive = getArgValueOrDefault(args, parameters(1)).get.asInstanceOf[Boolean]
    val tableNameStr = tableName.get.asInstanceOf[String]
    val basePath: String = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, tableNameStr).tableLocation

    val context = sparkSession.sparkContext
    val conf = context.hadoopConfiguration

    val targetPath = new StoragePath(basePath)
    val engineContext = new HoodieSparkEngineContext(context)

    val startTime = System.currentTimeMillis()
    try {
      val storage = HoodieStorageUtils.getStorage(
        basePath, HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf))
      FSUtils.deleteDir(engineContext, storage, targetPath, context.defaultParallelism)

      val metaClient = HoodieTableMetaClient
        .builder()
        .setBasePath(basePath)
        .setConf(HadoopFSUtils.getStorageConfWithCopy(conf))
        .build()

      val tableConfig = metaClient.getTableConfig

      val properties: Properties = generateProperties(tableConfig)

      // ReInit hoodie.properties
      HoodieTableMetaClient.newTableBuilder()
        .fromProperties(properties)
        .initTable(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf), basePath)

      // repair hms table partition
      repairTablePartition(syncHive, tableConfig, tableNameStr)
      logInfo(s"Success to execute truncate table producer for ${tableNameStr}")
      Seq(Row("SUCCESS", System.currentTimeMillis() - startTime))
    } catch {
      case e: Exception =>
        logError(s"Fail to execute truncate table producer for ${tableNameStr}", e)
        Seq(Row("FAILED", System.currentTimeMillis() - startTime))
    }
  }

  private def generateProperties(config: HoodieTableConfig): Properties = {
    val properties = new Properties()
    config.getProps.entrySet().asScala.foreach { entry =>
      properties.setProperty(entry.getKey.toString, entry.getValue.toString)
    }
    properties
  }

  private def repairTablePartition(syncHive: Boolean, tableConfig: HoodieTableConfig, tableName: String): Unit = {
    sparkSession.catalog.refreshTable(tableName)
    if (!syncHive) {
      logInfo("do not perform Hive partition synchronization operation, return directly.")
      return
    }

    val tableStrategyOpt = getTableSyncStrategyFromHMS(tableName)
    if (tableStrategyOpt.isEmpty) {
      logInfo("Non HMS table, no need for synchronization")
      return
    }

    val (db, table) = getDbAndTableName(tableName)
    val extraMap: Map[String, String] = Map(
      HiveSyncConfig.HIVE_ONLY_SYNC_PARTITION_ENABLED.key() -> true.toString,
      HiveSyncConfig.HIVE_SYNC_TABLE_STRATEGY.key() -> tableStrategyOpt.get
    )

    val hiveSyncConfig = buildHiveSyncConfig(sparkSession,
      HoodieCatalogTable(sparkSession = spark, tableIdentifier = TableIdentifier(table, Some(db))), tableConfig, extraMap)

    var hiveSyncTool: HiveSyncTool = null
    try {
      hiveSyncTool = new HiveSyncTool(hiveSyncConfig.getProps, new HiveConf)
      hiveSyncTool.syncHoodieTable()
    } catch {
      case e: RuntimeException =>
        throw new HoodieException("hive sync failed", e)
    } finally {
      if (hiveSyncTool != null)
        hiveSyncTool.close()
    }
  }

  def getTableSyncStrategyFromHMS(tableNameStr: String): Option[String] = {
    if (!"hive".equalsIgnoreCase(spark.conf.get("spark.sql.catalogImplementation"))) {
      return Option.empty[String]
    }
    val (dbName, tableName) = getDbAndTableName(tableNameStr)
    if (!spark.catalog.databaseExists(dbName) || !spark.catalog.tableExists(dbName, tableName)) {
      return Option.empty[String]
    }
    val tableProperties = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier(tableName, Some(dbName))).properties
    Some(tableProperties.getOrElse("hive_sync.table.strategy", HoodieSyncTableStrategy.ORI.name.toUpperCase))
  }
}

object TruncateTableProducer {
  val NAME = "truncate_table"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new TruncateTableProducer
  }
}
