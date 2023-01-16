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

import java.util.Properties

case class AlterHoodieTableSetPropertiesCommand(
      tableIdentifier: TableIdentifier,
      properties: Map[String, String],
      isView: Boolean) extends HoodieLeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    AlterHoodieTableSetPropertiesCommand.applyPropertySet(sparkSession, tableIdentifier, properties)
    Seq.empty[Row]
  }
}

object AlterHoodieTableSetPropertiesCommand extends Logging {
  // to do support set default value to columns, and apply them to internalSchema
  def applyPropertySet(sparkSession: SparkSession,
                       tableIdentifier: TableIdentifier,
                       properties: Map[String, String]): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val table = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    // This overrides old properties and update the comment parameter of CatalogTable
    // with the newly added/modified comment since CatalogTable also holds comment as its
    // direct property.
    val newTable = table.copy(
      properties = table.properties ++ properties,
      comment = properties.get("comment").orElse(table.comment))
    catalog.alterTable(newTable)

    // upserts the hoodie table's config file
    val updatedProps = new Properties
    properties.foreach(u => updatedProps.setProperty(u._1, u._2))
    updateTableProperties(sparkSession, updatedProps, table)

    logInfo("Set table properties change finished")
  }

  def updateTableProperties(sparkSession: SparkSession, updatedProps: Properties, table: CatalogTable): Unit = {
    val path = getTableLocation(table, sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(hadoopConf).build()
    HoodieTableConfig.update(metaClient.getFs, new Path(metaClient.getMetaPath), updatedProps)
  }
}
