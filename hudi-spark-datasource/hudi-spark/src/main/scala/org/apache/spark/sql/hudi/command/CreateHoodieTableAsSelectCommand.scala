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
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils.getTableLocation

/**
 * Command for create table as query statement.
 */
case class CreateHoodieTableAsSelectCommand(
   table: CatalogTable,
   mode: SaveMode,
   query: LogicalPlan) extends DataWritingCommand {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
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
    val tablePath = getTableLocation(table, sparkSession)
      .getOrElse(s"Missing path for table ${table.identifier}")
    val conf = sparkSession.sessionState.newHadoopConf()
    assert(CreateHoodieTableCommand.isEmptyPath(tablePath, conf),
      s"Path '$tablePath' should be empty for CTAS")

    // ReOrder the query which move the partition columns to the last of the project list
    val reOrderedQuery = reOrderPartitionColumn(query, table.partitionColumnNames)
    val tableWithSchema = table.copy(schema = reOrderedQuery.schema)

    // Execute the insert query
    try {
      val success = InsertIntoHoodieTableCommand.run(sparkSession, tableWithSchema, reOrderedQuery, Map.empty,
        mode == SaveMode.Overwrite, refreshTable = false)
      if (success) {
        // If write success, create the table in catalog if it has not synced to the
        // catalog by the meta sync.
        if (!sparkSession.sessionState.catalog.tableExists(tableIdentWithDB)) {
          // Create the table
          val createTableCommand = CreateHoodieTableCommand(tableWithSchema, mode == SaveMode.Ignore)
          createTableCommand.createTableInCatalog(sparkSession, checkPathForManagedTable = false)
        }
      } else { // failed to insert data, clear table path
        clearTablePath(tablePath, conf)
      }
    } catch {
      case e: Throwable => // failed to insert data, clear table path
        clearTablePath(tablePath, conf)
        throw e
    }
    Seq.empty[Row]
  }

  private def clearTablePath(tablePath: String, conf: Configuration): Unit = {
    val path = new Path(tablePath)
    val fs = path.getFileSystem(conf)
    fs.delete(path, true)
  }

  override def outputColumnNames: Seq[String] = query.output.map(_.name)

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
