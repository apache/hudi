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
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.getTableLocation
import org.apache.spark.sql.{Row, SparkSession}

case class AlterHoodieTableUnsetPropertiesCommand(
    tableIdentifier: TableIdentifier,
    propKeys: Seq[String],
    ifExists: Boolean,
    isView: Boolean) extends HoodieLeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    AlterHoodieTableUnsetPropertiesCommand.applyPropertyUnset(sparkSession, tableIdentifier, propKeys)
    Seq.empty[Row]
  }
}

object AlterHoodieTableUnsetPropertiesCommand extends Logging {
  // to do support unset default value to columns, and apply them to internalSchema
  def applyPropertyUnset(sparkSession: SparkSession,
      tableIdentifier: TableIdentifier,
      propKeys: Seq[String]): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val table = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    // ignore NonExist unset
    propKeys.foreach { k =>
      if (!table.properties.contains(k) && k != "comment") {
        logWarning(s"find non exist unset property: $k , ignore it")
      }
    }
    val tableComment = if (propKeys.contains("comment")) None else table.comment
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties, comment = tableComment)
    catalog.alterTable(newTable)

    // delete hoodie table's config file
    val deleteProps: java.util.Set[String] = new java.util.HashSet[String]()
    propKeys.foreach(v => deleteProps.add(v))
    deleteTableProperties(sparkSession, deleteProps, table)

    logInfo("Unset table properties change finished")
  }

  def deleteTableProperties(sparkSession: SparkSession, deletedProps: java.util.Set[String], table: CatalogTable): Any = {
    val path = getTableLocation(table, sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(hadoopConf).build()
    HoodieTableConfig.delete(metaClient.getFs, new Path(metaClient.getMetaPath), deletedProps)
  }
}
