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

import org.apache.hadoop.fs.Path
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.sync.common.util.ConfigUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._

import scala.util.control.NonFatal

case class DropHoodieTableCommand(
    tableIdentifier: TableIdentifier,
    ifExists: Boolean,
    isView: Boolean,
    purge: Boolean)
extends HoodieLeafRunnableCommand {

  val MOR_SNAPSHOT_TABLE_SUFFIX = "_rt"
  val MOR_READ_OPTIMIZED_TABLE_SUFFIX = "_ro"

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fullTableName = s"${tableIdentifier.database}.${tableIdentifier.table}"
    logInfo(s"start execute drop table command for $fullTableName")
    sparkSession.catalog.refreshTable(tableIdentifier.unquotedString)

    try {
      // drop catalog table for this hoodie table
      dropTableInCatalog(sparkSession, tableIdentifier, ifExists, purge)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to drop catalog table in metastore: ${e.getMessage}")
    }

    logInfo(s"Finish execute drop table command for $fullTableName")
    Seq.empty[Row]
  }

  def dropTableInCatalog(sparkSession: SparkSession,
          tableIdentifier: TableIdentifier,
          ifExists: Boolean,
          purge: Boolean): Unit = {
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)
    val table = hoodieCatalogTable.table
    assert(table.tableType != CatalogTableType.VIEW)

    val basePath = hoodieCatalogTable.tableLocation
    val catalog = sparkSession.sessionState.catalog

    // Drop table in the catalog
    if (HoodieTableType.MERGE_ON_READ == hoodieCatalogTable.tableType && purge) {
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
      val targetPath = new Path(basePath)
      val engineContext = new HoodieSparkEngineContext(sparkSession.sparkContext)
      val fs = FSUtils.getFs(basePath, sparkSession.sparkContext.hadoopConfiguration)
      FSUtils.deleteDir(engineContext, fs, targetPath, sparkSession.sparkContext.defaultParallelism)
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
