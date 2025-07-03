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
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.PartitionStatistics
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.language.postfixOps
import scala.util.control.NonFatal

/**
 * Command for repair hudi table's partitions.
 * Use the methods in HoodieSqlCommonUtils to obtain partitions and stats
 * instead of scanning the file system.
 */
case class RepairHoodieTableCommand(tableName: TableIdentifier,
                                    enableAddPartitions: Boolean,
                                    enableDropPartitions: Boolean,
                                    cmd: String = "MSCK REPAIR TABLE") extends HoodieLeafRunnableCommand {

  // These are list of statistics that can be collected quickly without requiring a scan of the data
  // see https://github.com/apache/hive/blob/master/
  //   common/src/java/org/apache/hadoop/hive/common/StatsSetupConst.java
  val NUM_FILES = "numFiles"
  val TOTAL_SIZE = "totalSize"
  val DDL_TIME = "transient_lastDdlTime"

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString
    if (table.partitionColumnNames.isEmpty) {
      throw new AnalysisException(
        s"Operation not allowed: $cmd only works on partitioned tables: $tableIdentWithDB")
    }

    if (table.storage.locationUri.isEmpty) {
      throw new AnalysisException(s"Operation not allowed: $cmd only works on table with " +
        s"location provided: $tableIdentWithDB")
    }

    val root = new Path(table.location)
    logInfo(s"Recover all the partitions in $root")

    val hoodieCatalogTable = HoodieCatalogTable(spark, table.identifier)
    val isHiveStyledPartitioning = hoodieCatalogTable.tableConfig.getHiveStylePartitioningEnable.toBoolean

    val partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)] = hoodieCatalogTable.
      getPartitionPaths.map(partitionPath => {
      var values = partitionPath.split('/')
        if (isHiveStyledPartitioning) {
        values = values.map(_.split('=')(1))
        }
        (table.partitionColumnNames.zip(values).toMap, new Path(root, partitionPath))
      })

    val droppedAmount = if (enableDropPartitions) {
      dropPartitions(catalog, partitionSpecsAndLocs)
    } else 0
    val addedAmount = if (enableAddPartitions) {
      val total = partitionSpecsAndLocs.length
      val partitionList = partitionSpecsAndLocs.map(_._2.toString)
      val partitionStats = if (spark.sqlContext.conf.gatherFastStats && total > 0) {
        HoodieSqlCommonUtils.getFilesInPartitions(spark, table, hoodieCatalogTable.metaClient, partitionList)
          .mapValues(statuses => PartitionStatistics(statuses.length, statuses.map(_.getLength).sum))
      } else {
        Map.empty[String, PartitionStatistics]
      }
      logInfo(s"Finished to gather the fast stats for all $total partitions.")
      addPartitions(spark, table, partitionSpecsAndLocs, partitionStats.toMap)
      total
    } else 0
    // Updates the table to indicate that its partition metadata is stored in the Hive metastore.
    // This is always the case for Hive format tables, but is not true for Datasource tables created
    // before Spark 2.1 unless they are converted via `msck repair table`.
    spark.sessionState.catalog.alterTable(table.copy(tracksPartitionsInCatalog = true))
    try {
      spark.catalog.refreshTable(tableIdentWithDB)
    } catch {
      case NonFatal(e) =>
        logError(s"Cannot refresh the table '$tableIdentWithDB'. A query of the table " +
          "might return wrong result if the table was cached. To avoid such issue, you should " +
          "uncache the table manually via the UNCACHE TABLE command after table recovering will " +
          "complete fully.", e)
    }
    logInfo(s"Recovered all partitions: added ($addedAmount), dropped ($droppedAmount).")
    Seq.empty[Row]
  }

  private def addPartitions(spark: SparkSession,
                            table: CatalogTable,
                            partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)],
                            partitionStats: Map[String, PartitionStatistics]): Unit = {
    val total = partitionSpecsAndLocs.length
    var done = 0L
    // Hive metastore may not have enough memory to handle millions of partitions in single RPC,
    // we should split them into smaller batches. Since Hive client is not thread safe, we cannot
    // do this in parallel.
    val batchSize = spark.sparkContext.conf.getInt("spark.sql.addPartitionInBatch.size", 100)
    partitionSpecsAndLocs.iterator.grouped(batchSize).foreach { batch =>
      val now = MILLISECONDS.toSeconds(System.currentTimeMillis())
      val parts = batch.map { case (spec, location) =>
        val params = partitionStats.get(location.toString).map {
          case PartitionStatistics(numFiles, totalSize) =>
            // This two fast stat could prevent Hive metastore to list the files again.
            Map(NUM_FILES -> numFiles.toString,
              TOTAL_SIZE -> totalSize.toString,
              // Workaround a bug in HiveMetastore that try to mutate a read-only parameters.
              // see metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java
              DDL_TIME -> now.toString)
        }.getOrElse(Map.empty)
        // inherit table storage format (possibly except for location)
        CatalogTablePartition(
          spec,
          table.storage.copy(locationUri = Some(location.toUri)),
          params)
      }
      spark.sessionState.catalog.createPartitions(tableName, parts, ignoreIfExists = true)
      done += parts.length
      logDebug(s"Recovered ${parts.length} partitions ($done/$total so far)")
    }
  }

  // Drops the partitions that do not exist in partitionSpecsAndLocs
  private def dropPartitions(catalog: SessionCatalog,
                             partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)]): Int = {
    val dropPartSpecs = ThreadUtils.parmap(
      catalog.listPartitions(tableName),
      "RepairTableCommand: non-existing partitions",
      maxThreads = 8) { partition =>
      partition.storage.locationUri.flatMap { uri =>
        if (partitionSpecsAndLocs.map(_._2).contains(new Path(uri))) None else Some(partition.spec)
      }
    }.flatten
    catalog.dropPartitions(
      tableName,
      dropPartSpecs,
      ignoreIfNotExists = true,
      purge = false,
      // Since we have already checked that partition directories do not exist, we can avoid
      // additional calls to the file system at the catalog side by setting this flag.
      retainData = true)
    dropPartSpecs.length
  }
}
