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
import org.apache.hudi.HoodieSparkSqlWriter
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{getPartitionPathToDrop, normalizePartitionSpec}
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}

/**
 * Command for truncate hudi table.
 */
case class TruncateHoodieTableCommand(
   tableIdentifier: TableIdentifier,
   partitionSpec: Option[TablePartitionSpec])
  extends HoodieLeafRunnableCommand with ProvidesHoodieConfig {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fullTableName = s"${tableIdentifier.database}.${tableIdentifier.table}"
    logInfo(s"start execute truncate table command for $fullTableName")

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)

    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableIdentifier)
    val tableId = table.identifier.quotedString

    if (table.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on views: $tableId")
    }

    if (table.partitionColumnNames.isEmpty && partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
          s"for tables that are not partitioned: $tableId")
    }

    val basePath = hoodieCatalogTable.tableLocation
    val properties = hoodieCatalogTable.tableConfig.getProps
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    // If we have not specified the partition, truncate will delete all the data in the table path
    if (partitionSpec.isEmpty) {
      val targetPath = new Path(basePath)
      val engineContext = new HoodieSparkEngineContext(sparkSession.sparkContext)
      val fs = FSUtils.getFs(basePath, sparkSession.sparkContext.hadoopConfiguration)
      FSUtils.deleteDir(engineContext, fs, targetPath, sparkSession.sparkContext.defaultParallelism)

      // ReInit hoodie.properties
      HoodieTableMetaClient.withPropertyBuilder()
        .fromProperties(properties)
        .initTable(hadoopConf, hoodieCatalogTable.tableLocation)
    } else {
      val normalizedSpecs: Seq[Map[String, String]] = Seq(partitionSpec.map { spec =>
        normalizePartitionSpec(
          spec,
          hoodieCatalogTable.partitionFields,
          hoodieCatalogTable.tableName,
          sparkSession.sessionState.conf.resolver)
      }.get)

      // drop partitions to lazy clean
      val partitionsToDrop = getPartitionPathToDrop(hoodieCatalogTable, normalizedSpecs)
      val parameters = buildHoodieDropPartitionsConfig(sparkSession, hoodieCatalogTable, partitionsToDrop)
      HoodieSparkSqlWriter.write(
        sparkSession.sqlContext,
        SaveMode.Append,
        parameters,
        sparkSession.emptyDataFrame)
    }

    // After deleting the data, refresh the table to make sure we don't keep around a stale
    // file relation in the metastore cache and cached table data in the cache manager.
    sparkSession.catalog.refreshTable(table.identifier.quotedString)
    logInfo(s"Finish execute truncate table command for $fullTableName")
    Seq.empty[Row]
  }
}
