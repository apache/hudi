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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.util.ConfigUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}

/**
 * Physical plan node for dropping a table.
 */
case class DropHoodieTableCommand(
                                   tableIdentifier: TableIdentifier,
    ifExists: Boolean,
    isView: Boolean,
    purge: Boolean) extends HoodieLeafRunnableCommand {

  private val MOR_SNAPSHOT_TABLE_SUFFIX = "_rt"
  private val MOR_READ_OPTIMIZED_TABLE_SUFFIX = "_ro"

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"Start executing 'DROP TABLE' on ${tableIdentifier.unquotedString}" +
      s" (ifExists=${ifExists}, purge=${purge}).")
    if (!sparkSession.catalog.tableExists(tableIdentifier.unquotedString)) {
      sparkSession.catalog.refreshTable(tableIdentifier.unquotedString)
    }
    val qualifiedTableName = QualifiedTableName(
      tableIdentifier.database.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase),
      tableIdentifier.table)
    sparkSession.sessionState.catalog.invalidateCachedTable(qualifiedTableName)

    dropTableInCatalog(sparkSession, tableIdentifier, ifExists, purge)

    logInfo(s"Finished executing 'DROP TABLE' on ${tableIdentifier.unquotedString}.")
    Seq.empty[Row]
  }

  /**
   * Drops table in Spark catalog. Note that RO & RT table could coexist with a MOR table.
   * If `purge` enabled, RO & RT table and corresponding data directory on filesystem will
   * all be removed.
   */
  private def dropTableInCatalog(sparkSession: SparkSession,
          tableIdentifier: TableIdentifier,
          ifExists: Boolean,
          purge: Boolean): Unit = {
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)
    val table = hoodieCatalogTable.table
    assert(table.tableType != CatalogTableType.VIEW)

    val basePath = hoodieCatalogTable.tableLocation
    val catalog = sparkSession.sessionState.catalog

    // Drop table in the catalog
    if (hoodieCatalogTable.hoodieTableExists &&
        HoodieTableType.MERGE_ON_READ == hoodieCatalogTable.tableType && purge) {
      val (rtTableOpt, roTableOpt) = getTableRTAndRO(catalog, hoodieCatalogTable)
      rtTableOpt.foreach(table => catalog.dropTable(table.identifier, true, false))
      roTableOpt.foreach(table => catalog.dropTable(table.identifier, true, false))
      catalog.dropTable(table.identifier.copy(table = hoodieCatalogTable.tableName), ifExists, purge)
    } else {
      catalog.dropTable(table.identifier, ifExists, purge)
    }

    // Recursively delete table directories
    if (purge) {
      logInfo("Clean up " + basePath)
      val targetPath = new StoragePath(basePath)
      val engineContext = new HoodieSparkEngineContext(sparkSession.sparkContext)
      val storage = HoodieStorageUtils.getStorage(basePath,
        HadoopFSUtils.getStorageConf(sparkSession.sparkContext.hadoopConfiguration))
      FSUtils.deleteDir(engineContext, storage, targetPath, sparkSession.sparkContext.defaultParallelism)
    }
  }

  private def getTableRTAndRO(catalog: SessionCatalog,
      hoodieTable: HoodieCatalogTable): (Option[CatalogTable], Option[CatalogTable]) = {
    val rtIdt = hoodieTable.table.identifier.copy(
      table = s"${hoodieTable.tableName}${MOR_SNAPSHOT_TABLE_SUFFIX}")
    val roIdt = hoodieTable.table.identifier.copy(
      table = s"${hoodieTable.tableName}${MOR_READ_OPTIMIZED_TABLE_SUFFIX}")

    var rtTableOpt: Option[CatalogTable] = None
    var roTableOpt: Option[CatalogTable] = None
    if (catalog.tableExists(rtIdt)) {
      val rtTable = catalog.getTableMetadata(rtIdt)
      if (rtTable.storage.locationUri.equals(hoodieTable.table.storage.locationUri)) {
        rtTable.storage.properties.get(ConfigUtils.IS_QUERY_AS_RO_TABLE) match {
          case Some(v) if v.equalsIgnoreCase("false") => rtTableOpt = Some(rtTable)
          case _ => // do-nothing
        }
      }
    }
    if (catalog.tableExists(roIdt)) {
      val roTable = catalog.getTableMetadata(roIdt)
      if (roTable.storage.locationUri.equals(hoodieTable.table.storage.locationUri)) {
        roTable.storage.properties.get(ConfigUtils.IS_QUERY_AS_RO_TABLE) match {
          case Some(v) if v.equalsIgnoreCase("true") => roTableOpt = Some(roTable)
          case _ => // do-nothing
        }
      }
    }
    (rtTableOpt, roTableOpt)
  }
}
