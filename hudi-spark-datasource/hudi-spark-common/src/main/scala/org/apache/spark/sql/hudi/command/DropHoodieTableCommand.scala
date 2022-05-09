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
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.hive.HiveClientUtils
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isEnableHive

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
    val enableHive = isEnableHive(sparkSession)
    if (enableHive) {
      dropHiveDataSourceTable(sparkSession, hoodieCatalogTable)
    } else {
      if (catalog.tableExists(tableIdentifier)) {
        catalog.dropTable(tableIdentifier, ifExists, purge)
      }
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

  private def dropHiveDataSourceTable(
       sparkSession: SparkSession,
       hoodieCatalogTable: HoodieCatalogTable): Unit = {
    val table = hoodieCatalogTable.table
    val dbName = table.identifier.database.get
    val tableName = hoodieCatalogTable.tableName

    // check database exists
    val dbExists = sparkSession.sessionState.catalog.databaseExists(dbName)
    if (!dbExists) {
      throw new NoSuchDatabaseException(dbName)
    }

    if (HoodieTableType.MERGE_ON_READ == hoodieCatalogTable.tableType && purge) {
      val snapshotTableName = tableName + MOR_SNAPSHOT_TABLE_SUFFIX
      val roTableName = tableName + MOR_READ_OPTIMIZED_TABLE_SUFFIX

      dropHiveTable(sparkSession, dbName, snapshotTableName)
      dropHiveTable(sparkSession, dbName, roTableName)
    }

    dropHiveTable(sparkSession, dbName, tableName, purge)
  }

  private def dropHiveTable(
       sparkSession: SparkSession,
       dbName: String,
       tableName: String,
       purge: Boolean = false): Unit = {
    // check table exists
    if (sparkSession.sessionState.catalog.tableExists(new TableIdentifier(tableName, Option(dbName)))) {
      val client = HiveClientUtils.newClientForMetadata(sparkSession.sparkContext.conf,
        sparkSession.sessionState.newHadoopConf())

      // drop hive table.
      client.dropTable(dbName, tableName, ifExists, purge)
    }
  }
}
