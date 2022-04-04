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
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{getPartitionPathToDrop, normalizePartitionSpec}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.util.control.NonFatal

/**
 * Command for truncate hudi table.
 */
case class TruncateHoodieTableCommand(
   tableIdentifier: TableIdentifier,
   partitionSpec: Option[TablePartitionSpec])
  extends HoodieLeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val fullTableName = s"${tableIdentifier.database}.${tableIdentifier.table}"
    logInfo(s"start execute truncate table command for $fullTableName")

    val hoodieCatalogTable = HoodieCatalogTable(spark, tableIdentifier)
    val properties = hoodieCatalogTable.tableConfig.getProps

    try {
      // Delete all data in the table directory
      val catalog = spark.sessionState.catalog
      val table = catalog.getTableMetadata(tableIdentifier)
      val tableIdentWithDB = table.identifier.quotedString

      if (table.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException(
          s"Operation not allowed: TRUNCATE TABLE on views: $tableIdentWithDB")
      }

      if (table.partitionColumnNames.isEmpty && partitionSpec.isDefined) {
        throw new AnalysisException(
          s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
            s"for tables that are not partitioned: $tableIdentWithDB")
      }

      val basePath = hoodieCatalogTable.tableLocation
      val partCols = table.partitionColumnNames
      val locations = if (partitionSpec.isEmpty || partCols.isEmpty) {
        Seq(basePath)
      } else {
        val normalizedSpec: Seq[Map[String, String]] = Seq(partitionSpec.map { spec =>
          normalizePartitionSpec(
            spec,
            partCols,
            table.identifier.quotedString,
            spark.sessionState.conf.resolver)
        }.get)

        val fullPartitionPath = FSUtils.getPartitionPath(basePath, getPartitionPathToDrop(hoodieCatalogTable, normalizedSpec))

        Seq(fullPartitionPath)
      }

      val hadoopConf = spark.sessionState.newHadoopConf()
      locations.foreach { location =>
        val path = new Path(location.toString)
        try {
          val fs = path.getFileSystem(hadoopConf)
          fs.delete(path, true)
          fs.mkdirs(path)
        } catch {
          case NonFatal(e) =>
            throw new AnalysisException(
              s"Failed to truncate table $tableIdentWithDB when removing data of the path: $path " +
                s"because of ${e.toString}")
        }
      }

      // Also try to drop the contents of the table from the columnar cache
      try {
        spark.sharedState.cacheManager.uncacheQuery(spark.table(table.identifier), cascade = true)
      } catch {
        case NonFatal(_) =>
      }

      if (table.stats.nonEmpty) {
        // empty table after truncation
        val newStats = CatalogStatistics(sizeInBytes = 0, rowCount = Some(0))
        catalog.alterTableStats(tableIdentifier, Some(newStats))
      }
      Seq.empty[Row]
    } catch {
      // TruncateTableCommand will delete the related directories first, and then refresh the table.
      // It will fail when refresh table, because the hudi meta directory(.hoodie) has been deleted at the first step.
      // So here ignore this failure, and refresh table later.
      case NonFatal(e) =>
        throw new AnalysisException(s"Exception when attempting to truncate table ${tableIdentifier.quotedString}: " + e)
    }

    // If we have not specified the partition, truncate will delete all the data in the table path
    // include the hoodie.properties. In this case we should reInit the table.
    if (partitionSpec.isEmpty) {
      val hadoopConf = spark.sessionState.newHadoopConf()
      // ReInit hoodie.properties
      HoodieTableMetaClient.withPropertyBuilder()
        .fromProperties(properties)
        .initTable(hadoopConf, hoodieCatalogTable.tableLocation)
    }

    // After deleting the data, refresh the table to make sure we don't keep around a stale
    // file relation in the metastore cache and cached table data in the cache manager.
    spark.catalog.refreshTable(hoodieCatalogTable.table.identifier.quotedString)
    Seq.empty[Row]
  }
}
