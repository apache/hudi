/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.execution

import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieWriteConfig.BULKINSERT_PARALLELISM
import org.apache.hudi.execution.HudiSQLUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.StaticSQLConf

import scala.collection.JavaConverters._

/**
 * Command for ctas
 */
case class CreateHudiTableAsSelectCommand(
    table: CatalogTable,
    mode: SaveMode,
    query: LogicalPlan,
    outputColumnNames: Seq[String]) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)
    val sessionState = sparkSession.sessionState
    var fs: FileSystem = null
    val conf = sessionState.newHadoopConf()

    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString
    val enableHive = "hive" == sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)
    val path = HudiSQLUtils.getTablePath(sparkSession, table)

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      assert(mode != SaveMode.Overwrite, s"Expect the table $tableName has been dropped when the save mode is OverWrite")
      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"Table $tableName already exists. You eed to drop it first.")
      }
      if (mode == SaveMode.Ignore) {
        // scalastyle:off
        return Seq.empty
        // scalastyle:on
      }
      // append table
      saveDataIntoHudiTable(sparkSession, table, path, table.storage.properties, enableHive)
    } else {
      val properties = table.storage.properties
      assert(table.schema.isEmpty)
      sparkSession.sessionState.catalog.validateTableLocation(table)
      // create table
      if (!enableHive) {
        val newTable = if (!HudiSQLUtils.tableExists(path, conf)) {
          table.copy(schema = query.schema)
        } else {
          table
        }
        CreateHudiTableCommand(newTable, true).run(sparkSession)
      }
      saveDataIntoHudiTable(sparkSession, table, path, properties, enableHive, "bulk_insert")
    }

    // save necessary parameter in hoodie.properties
    val newProperties = new Properties()
    newProperties.putAll(table.storage.properties.asJava)
    // add table partition
    newProperties.put(PARTITIONPATH_FIELD_OPT_KEY, table.partitionColumnNames.mkString(","))
    val metaPath = new Path(path, HoodieTableMetaClient.METAFOLDER_NAME)
    val propertyPath = new Path(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE)
    CreateHudiTableCommand.saveNeededPropertiesIntoHoodie(propertyPath.getFileSystem(conf), propertyPath, newProperties)
    Seq.empty[Row]
  }

  private def saveDataIntoHudiTable(
      session: SparkSession,
      table: CatalogTable,
      tablePath: String,
      tableOptions: Map[String, String],
      enableHive: Boolean,
      operationType: String = "upsert"): Unit = {
    val mode = if (operationType.equals("upsert")) {
      "append"
    } else {
      "overwrite"
    }
    val newTableOptions = if (enableHive) {
      // add hive sync properties
      val extraOptions = Map("hoodie.table.name" -> table.identifier.table,
        "path" -> tablePath,
        DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> table.identifier.database.getOrElse("default"),
        DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> table.identifier.table,
        PARTITIONPATH_FIELD_OPT_KEY -> table.partitionColumnNames.mkString(","),
        DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> table.partitionColumnNames.mkString(","),
        DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      HudiSQLUtils.buildDefaultParameter(extraOptions ++ tableOptions, true)
    } else {
      val extraOptions = Map("hoodie.table.name" -> table.identifier.table,
        "path" -> tablePath,
        PARTITIONPATH_FIELD_OPT_KEY -> table.partitionColumnNames.mkString(","))
      HudiSQLUtils.buildDefaultParameter(extraOptions ++ tableOptions, false)
    }

    HudiSQLUtils.checkWriteOptions(newTableOptions)
    val df = Dataset.ofRows(session, query)
    val par = session.sessionState.conf.getConfString("spark.hoodie.shuffle.parallelism", "1500")
    df.write
      .format("hudi")
      .options(newTableOptions)
      .option(BULKINSERT_PARALLELISM, par)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, "bulk_insert")
      .mode(mode)
      .save(tablePath)
  }
}

