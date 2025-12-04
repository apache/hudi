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

import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.execution.command.{AlterTableRenameCommand, AlterTableSetPropertiesCommand}

/**
 * Command for alter hudi table's table name.
 */
case class AlterHoodieTableRenameCommand(
     oldName: TableIdentifier,
     newName: TableIdentifier,
     isView: Boolean)
  extends HoodieLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (newName != oldName) {
      val hoodieCatalogTable = HoodieCatalogTable(sparkSession, oldName)

      // Init table with new name.
      HoodieTableMetaClient.newTableBuilder()
        .fromProperties(hoodieCatalogTable.tableConfig.getProps)
        .setTableName(newName.table)
        .initTable(
          hoodieCatalogTable.storage.getConf,
          hoodieCatalogTable.tableLocation)

      // Call AlterTableRenameCommand#run to rename table in meta.
      AlterTableRenameCommand(oldName, newName, isView).run(sparkSession)

      // update table properties path in every op
      if (hoodieCatalogTable.table.properties.contains("path")) {
        val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(newName)
        AlterTableSetPropertiesCommand(newName, Map("path" -> catalogTable.location.toString), isView).run(sparkSession)
      }

    }
    Seq.empty[Row]
  }
}
