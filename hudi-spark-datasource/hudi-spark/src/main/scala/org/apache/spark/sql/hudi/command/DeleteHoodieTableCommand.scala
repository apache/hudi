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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.{DataSourceWriteOptions, SparkAdapterSupport}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.types.StructType

case class DeleteHoodieTableCommand(deleteTable: DeleteFromTable) extends RunnableCommand
  with SparkAdapterSupport {

  private val table = deleteTable.table

  private val tableId = getTableIdentify(table)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"start execute delete command for $tableId")

    // Remove meta fields from the data frame
    var df = removeMetaFields(Dataset.ofRows(sparkSession, table))
    if (deleteTable.condition.isDefined) {
      df = df.filter(Column(deleteTable.condition.get))
    }
    val config = buildHoodieConfig(sparkSession)
    df.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()
    sparkSession.catalog.refreshTable(tableId.unquotedString)
    logInfo(s"Finish execute delete command for $tableId")
    Seq.empty[Row]
  }

  private def buildHoodieConfig(sparkSession: SparkSession): Map[String, String] = {
    val targetTable = sparkSession.sessionState.catalog.getTableMetadata(tableId)
    val tblProperties = targetTable.storage.properties ++ targetTable.properties
    val path = getTableLocation(targetTable, sparkSession)
    val conf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(conf)
      .build()
    val tableConfig = metaClient.getTableConfig
    val tableSchema = getTableSqlSchema(metaClient).get
    val partitionColumns = tableConfig.getPartitionFieldProp.split(",").map(_.toLowerCase)
    val partitionSchema = StructType(tableSchema.filter(f => partitionColumns.contains(f.name)))
    val primaryColumns = tableConfig.getRecordKeyFields.get()

    assert(primaryColumns.nonEmpty,
      s"There are no primary key defined in table $tableId, cannot execute delete operator")
    withSparkConf(sparkSession, tblProperties) {
      Map(
        "path" -> path,
        TBL_NAME.key -> tableId.table,
        HIVE_STYLE_PARTITIONING.key -> tableConfig.getHiveStylePartitioningEnable,
        URL_ENCODE_PARTITIONING.key -> tableConfig.getUrlEncodePartitoning,
        KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getCanonicalName,
        SqlKeyGenerator.ORIGIN_KEYGEN_CLASS_NAME -> tableConfig.getKeyGeneratorClassName,
        OPERATION.key -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
        PARTITIONPATH_FIELD.key -> tableConfig.getPartitionFieldProp,
        HIVE_SYNC_MODE.key -> HiveSyncMode.HMS.name(),
        HIVE_SUPPORT_TIMESTAMP_TYPE.key -> "true",
        HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key -> "200",
        SqlKeyGenerator.PARTITION_SCHEMA -> partitionSchema.toDDL
      )
    }
  }
}
