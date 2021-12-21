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
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.fs.FSUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.HiveClientUtils
import org.apache.spark.sql.hudi.HoodieSqlUtils.isEnableHive

import scala.util.control.NonFatal

case class DropHoodieTableCommand(
          tableIdentifier: TableIdentifier,
          ifExists: Boolean,
          isView: Boolean,
          purge: Boolean) extends RunnableCommand
  with SparkAdapterSupport {

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
      dropHiveDataSourceTable(sparkSession, table, ifExists, purge)
    } else {
      catalog.dropTable(tableIdentifier, ifExists, purge)
    }

    // Recursively delete table directories
    if (purge) {
      logInfo("Clean up " + basePath)
      val targetPath = new Path(basePath)
      val fs = FSUtils.getFs(basePath, sparkSession.sparkContext.hadoopConfiguration)
      if (fs.exists(targetPath)) {
        fs.delete(targetPath, true)
      }
    }
  }

  private def dropHiveDataSourceTable(
        sparkSession: SparkSession,
        table: CatalogTable,
        ifExists: Boolean,
        purge: Boolean): Unit = {
    val dbName = table.identifier.database.get
    val tableName = table.identifier.table
    // check database exists
    val dbExists = sparkSession.sessionState.catalog.databaseExists(dbName)
    if (!dbExists) {
      throw new NoSuchDatabaseException(dbName)
    }
    // check table exists
    if (!sparkSession.sessionState.catalog.tableExists(table.identifier)) {
      throw new NoSuchTableException(dbName, table.identifier.table)
    }

    val client = HiveClientUtils.newClientForMetadata(sparkSession.sparkContext.conf,
      sparkSession.sessionState.newHadoopConf())
    // drop hive table.
    client.dropTable(dbName, tableName, ifExists, purge)
  }
}
