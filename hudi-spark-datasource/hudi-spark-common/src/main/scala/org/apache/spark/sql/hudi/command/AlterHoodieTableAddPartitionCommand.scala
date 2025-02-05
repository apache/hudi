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

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{makePartitionPath, normalizePartitionSpec}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException

import scala.util.control.NonFatal

case class AlterHoodieTableAddPartitionCommand(
   tableIdentifier: TableIdentifier,
   partitionSpecsAndLocs: Seq[(TablePartitionSpec, Option[String])],
   ifNotExists: Boolean)
  extends HoodieLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"start execute alter table add partition command for $tableIdentifier")

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)

    if (!hoodieCatalogTable.isPartitionedTable) {
      throw new HoodieAnalysisException(s"$tableIdentifier is a non-partitioned table that is not allowed to add partition")
    }

    val catalog = sparkSession.sessionState.catalog
    val table = hoodieCatalogTable.table
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)

    val normalizedSpecs: Seq[Map[String, String]] = partitionSpecsAndLocs.map { case (spec, location) =>
      if (location.isDefined) {
        throw new HoodieAnalysisException(s"Hoodie table does not support specify partition location explicitly")
      }
      normalizePartitionSpec(
        spec,
        hoodieCatalogTable.partitionFields,
        hoodieCatalogTable.tableName,
        sparkSession.sessionState.conf.resolver)
    }

    val basePath = new StoragePath(hoodieCatalogTable.tableLocation)
    val storage = hoodieCatalogTable.metaClient.getStorage
    val format = hoodieCatalogTable.tableConfig.getPartitionMetafileFormat
    val (partitionMetadata, parts) = normalizedSpecs.map { spec =>
      val partitionPath = makePartitionPath(hoodieCatalogTable, spec)
      val fullPartitionPath: StoragePath = FSUtils.constructAbsolutePath(basePath, partitionPath)
      val metadata = if (HoodiePartitionMetadata.hasPartitionMetadata(storage, fullPartitionPath)) {
        if (!ifNotExists) {
          throw new HoodieAnalysisException(s"Partition metadata already exists for path: $fullPartitionPath")
        }
        None
      } else Some(new HoodiePartitionMetadata(storage, HoodieTimeline.INIT_INSTANT_TS, basePath, fullPartitionPath, format))
      (metadata, CatalogTablePartition(spec, table.storage.copy(locationUri = Some(fullPartitionPath.toUri))))
    }.unzip
    partitionMetadata.flatten.foreach(_.trySave)

    // Sync new partitions in batch, enable ignoreIfExists to be silent for existing partitions.
    val batchSize = sparkSession.sparkContext.conf.getInt("spark.sql.addPartitionInBatch.size", 100)
    try {
      parts.toIterator.grouped(batchSize).foreach { batch =>
        catalog.createPartitions(tableIdentifier, batch, ignoreIfExists = true)
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to add partitions in external catalog", e)
    }
    sparkSession.catalog.refreshTable(tableIdentifier.unquotedString)

    Seq.empty[Row]
  }
}
