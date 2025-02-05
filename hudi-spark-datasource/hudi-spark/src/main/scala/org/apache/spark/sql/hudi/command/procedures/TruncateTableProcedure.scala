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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.{HoodieCLIUtils, HoodieSparkSqlWriter}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class TruncateTableProcedure extends BaseProcedure
  with ProcedureBuilder
  with ProvidesHoodieConfig
  with Logging {
  def build: Procedure = new TruncateTableProcedure()

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "partitions", DataTypes.StringType)
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_cost", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(parameters, args)

    val tableNameStr = getArgValueOrDefault(args, parameters(0)).get.asInstanceOf[String]
    logInfo(s"start execute truncate table procedure for $tableNameStr")

    val partitionsStr = getArgValueOrDefault(args, parameters(1)).getOrElse("").asInstanceOf[String]

    val catalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, tableNameStr)

    val (db, tableName) = getDbAndTableName(tableNameStr)

    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(TableIdentifier(tableName, Some(db)))
    val tableId = table.identifier.quotedString

    if (table.tableType == CatalogTableType.VIEW) {
      throw new HoodieAnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on views: $tableId")
    }

    if (table.partitionColumnNames.isEmpty && partitionsStr.nonEmpty) {
      throw new HoodieAnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
          s"for tables that are not partitioned: $tableId")
    }

    val basePath = catalogTable.tableLocation
    val properties = catalogTable.tableConfig.getProps

    if (partitionsStr.isEmpty) {
      val targetPath = new StoragePath(basePath)
      val engineContext = new HoodieSparkEngineContext(sparkSession.sparkContext)
      val storage = HoodieStorageUtils.getStorage(
        basePath, HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf))

      val startTime = System.currentTimeMillis()
      FSUtils.deleteDir(engineContext, storage, targetPath, sparkSession.sparkContext.defaultParallelism)

      // ReInit hoodie.properties
      val metaClient = HoodieTableMetaClient.newTableBuilder()
        .fromProperties(properties)
        .initTable(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf), catalogTable.tableLocation)

      catalogTable.tableConfig.clearMetadataPartitions(metaClient)
      logInfo(s"Success to execute truncate table procedure for ${tableNameStr}")
      Seq(Row("SUCCESS", System.currentTimeMillis() - startTime))


    } else {
      val parameters = buildHoodieDropPartitionsConfig(sparkSession, catalogTable, partitionsStr)
      val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(
        sparkSession.sqlContext,
        SaveMode.Append,
        parameters,
        sparkSession.emptyDataFrame)
      if (!success) {
        throw new HoodieException("Truncate Hoodie Table procedure failed")
      }
    }

    // After deleting the data, refresh the table to make sure we don't keep around a stale
    // file relation in the metastore cache and cached table data in the cache manager.
    sparkSession.catalog.refreshTable(table.identifier.quotedString)
    logInfo(s"Finish execute truncate table procedure for $tableNameStr")
    Seq.empty[Row]
  }
}

object TruncateTableProcedure {
  val NAME = "truncate_table"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new TruncateTableProcedure
  }
}
