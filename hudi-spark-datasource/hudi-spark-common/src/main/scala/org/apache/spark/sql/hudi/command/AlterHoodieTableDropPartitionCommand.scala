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

import org.apache.hudi.HoodieSparkSqlWriter
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException

case class AlterHoodieTableDropPartitionCommand(
   tableIdentifier: TableIdentifier,
   partitionSpecs: Seq[TablePartitionSpec],
   ifExists : Boolean,
   purge : Boolean,
   retainData : Boolean)
  extends HoodieLeafRunnableCommand with ProvidesHoodieConfig {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val fullTableName = s"${tableIdentifier.database}.${tableIdentifier.table}"
    logInfo(s"start execute alter table drop partition command for $fullTableName")

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)

    if (!hoodieCatalogTable.isPartitionedTable) {
      throw new HoodieAnalysisException(s"$fullTableName is a non-partitioned table that is not allowed to drop partition")
    }

    DDLUtils.verifyAlterTableType(
      sparkSession.sessionState.catalog, hoodieCatalogTable.table, isView = false)

    val normalizedSpecs: Seq[Map[String, String]] = partitionSpecs.map { spec =>
      normalizePartitionSpec(
        spec,
        hoodieCatalogTable.partitionFields,
        hoodieCatalogTable.tableName,
        sparkSession.sessionState.conf.resolver)
    }

    // drop partitions to lazy clean (https://github.com/apache/hudi/pull/4489)
    // delete partition files by enabling cleaner and setting retention policies.
    val partitionsToDrop = getPartitionPathToDrop(hoodieCatalogTable, normalizedSpecs)
    val parameters = buildHoodieDropPartitionsConfig(sparkSession, hoodieCatalogTable, partitionsToDrop)
    val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(
      sparkSession.sqlContext,
      SaveMode.Append,
      parameters,
      sparkSession.emptyDataFrame)
    if (!success) {
      throw new HoodieException("Alter table command failed")
    }

    sparkSession.catalog.refreshTable(tableIdentifier.unquotedString)
    logInfo(s"Finish execute alter table drop partition command for $fullTableName")
    Seq.empty[Row]
  }
}
