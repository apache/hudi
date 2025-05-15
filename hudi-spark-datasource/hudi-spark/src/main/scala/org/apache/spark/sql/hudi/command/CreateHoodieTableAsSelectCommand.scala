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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.util.ConfigUtils
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.sql.InsertMode

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable.needFilterProps
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.collection.JavaConverters._

/**
 * Command for create table as query statement.
 */
case class CreateHoodieTableAsSelectCommand(
   table: CatalogTable,
   mode: SaveMode,
   query: LogicalPlan) extends DataWritingCommand {
  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override lazy val metrics: Map[String, SQLMetric] = HoodieCommandMetrics.metrics

  override def run(sparkSession: SparkSession, plan: SparkPlan): Seq[Row] = {
    checkState(table.tableType != CatalogTableType.VIEW)
    checkState(table.provider.isDefined)

    val hasQueryAsProp = (table.storage.properties ++ table.properties).contains(ConfigUtils.IS_QUERY_AS_RO_TABLE)
    if (hasQueryAsProp) {
      throw new AnalysisException("Not support CTAS for the ro/rt table")
    }

    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val qualifiedTableIdentifier = table.identifier.copy(database = Some(db))
    val tableName = qualifiedTableIdentifier.unquotedString

    if (sessionState.catalog.tableExists(qualifiedTableIdentifier)) {
      checkState(mode != SaveMode.Overwrite,
        s"Expect the table $tableName has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"Table $tableName already exists. You need to drop it first.")
      }

      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        // scalastyle:off
        return Seq.empty
        // scalastyle:on
      }
    }

    // Remove some properties should not be used
    val updatedStorageFormat = table.storage.copy(
      properties = table.storage.properties -- needFilterProps)

    val updatedTable = table.copy(
      identifier = qualifiedTableIdentifier,
      storage = updatedStorageFormat,
      // TODO need to add meta-fields here
      schema = query.schema,
      properties = table.properties -- needFilterProps
    )

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, updatedTable)
    val tablePath = hoodieCatalogTable.tableLocation
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    try {
      // Init hoodie table
      hoodieCatalogTable.initHoodieTable()

      val tableProperties = hoodieCatalogTable.catalogProperties
      val options = Map(
        HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE.key -> (table.tableType == CatalogTableType.MANAGED).toString,
        HiveSyncConfigHolder.HIVE_TABLE_SERDE_PROPERTIES.key -> ConfigUtils.configToString(tableProperties.asJava),
        HiveSyncConfigHolder.HIVE_TABLE_PROPERTIES.key -> ConfigUtils.configToString(updatedTable.properties.asJava),
        HoodieWriteConfig.COMBINE_BEFORE_INSERT.key -> "false",
        DataSourceWriteOptions.SQL_INSERT_MODE.key -> InsertMode.NON_STRICT.value(),
        DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key -> "true"
      )
      val partitionSpec = updatedTable.partitionColumnNames.map((_, None)).toMap
      val success = InsertIntoHoodieTableCommand.run(sparkSession, updatedTable, plan, partitionSpec,
        mode == SaveMode.Overwrite, refreshTable = false, extraOptions = options, metrics = metrics)
      DataWritingCommand.propogateMetrics(sparkSession.sparkContext, this, metrics)
      if (success) {
        // If write success, create the table in catalog if it has not synced to the
        // catalog by the meta sync.
        if (!sparkSession.sessionState.catalog.tableExists(qualifiedTableIdentifier)) {
          // create catalog table for this hoodie table
          CreateHoodieTableCommand.createTableInCatalog(sparkSession, hoodieCatalogTable, mode == SaveMode.Ignore)
        }
      } else { // failed to insert data, clear table path
        clearTablePath(tablePath, hadoopConf)
      }
    } catch {
      case e: Throwable =>
        // clear the hoodie table path only if it did not exist previously.
        if (!hoodieCatalogTable.hoodieTableExists) {
          clearTablePath(tablePath, hadoopConf)
        }
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

  override def withNewChildInternal(newChild: LogicalPlan) : LogicalPlan = copy(query = newChild)
}
