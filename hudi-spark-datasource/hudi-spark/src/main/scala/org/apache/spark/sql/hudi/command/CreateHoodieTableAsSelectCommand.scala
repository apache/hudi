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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.hive.HiveSyncConfig
import org.apache.hudi.hive.util.ConfigUtils
import org.apache.hudi.sql.InsertMode
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._

/**
 * Command for create table as query statement.
 */
case class CreateHoodieTableAsSelectCommand(
   table: CatalogTable,
   mode: SaveMode,
   query: LogicalPlan) extends HoodieLeafRunnableCommand {
  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableName has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new RuntimeException(s"Table $tableName already exists. You need to drop it first.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        // scalastyle:off
        return Seq.empty
        // scalastyle:on
      }
    }

    // ReOrder the query which move the partition columns to the last of the project list
    val reOrderedQuery = reOrderPartitionColumn(query, table.partitionColumnNames)
    val tableWithSchema = table.copy(schema = reOrderedQuery.schema)

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableWithSchema)
    val tablePath = hoodieCatalogTable.tableLocation
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    assert(HoodieSqlCommonUtils.isEmptyPath(tablePath, hadoopConf),
      s"Path '$tablePath' should be empty for CTAS")

    // Execute the insert query
    try {
      // init hoodie table
      hoodieCatalogTable.initHoodieTable()

      val tblProperties = hoodieCatalogTable.catalogProperties
      val options = Map(
        HiveSyncConfig.HIVE_CREATE_MANAGED_TABLE.key -> (table.tableType == CatalogTableType.MANAGED).toString,
        HiveSyncConfig.HIVE_TABLE_SERDE_PROPERTIES.key -> ConfigUtils.configToString(tblProperties.asJava),
        HiveSyncConfig.HIVE_TABLE_PROPERTIES.key -> ConfigUtils.configToString(table.properties.asJava),
        DataSourceWriteOptions.SQL_INSERT_MODE.key -> InsertMode.NON_STRICT.value(),
        DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key -> "true"
      )
      val success = InsertIntoHoodieTableCommand.run(sparkSession, tableWithSchema, reOrderedQuery, Map.empty,
        mode == SaveMode.Overwrite, refreshTable = false, extraOptions = options)
      if (success) {
        // If write success, create the table in catalog if it has not synced to the
        // catalog by the meta sync.
        if (!sparkSession.sessionState.catalog.tableExists(tableIdentWithDB)) {
          // create catalog table for this hoodie table
          CreateHoodieTableCommand.createTableInCatalog(sparkSession, hoodieCatalogTable, mode == SaveMode.Ignore)
        }
      } else { // failed to insert data, clear table path
        clearTablePath(tablePath, hadoopConf)
      }
    } catch {
      case e: Throwable => // failed to insert data, clear table path
        clearTablePath(tablePath, hadoopConf)
        throw e
    }
    Seq.empty[Row]
  }

  private def clearTablePath(tablePath: String, conf: Configuration): Unit = {
    val path = new Path(tablePath)
    val fs = path.getFileSystem(conf)
    fs.delete(path, true)
  }

  private def reOrderPartitionColumn(query: LogicalPlan,
    partitionColumns: Seq[String]): LogicalPlan = {
    if (partitionColumns.isEmpty) {
      query
    } else {
      val nonPartitionAttrs = query.output.filter(p => !partitionColumns.contains(p.name))
      val partitionAttrs = query.output.filter(p => partitionColumns.contains(p.name))
      val reorderAttrs = nonPartitionAttrs ++ partitionAttrs
      Project(reorderAttrs, query)
    }
  }
}
