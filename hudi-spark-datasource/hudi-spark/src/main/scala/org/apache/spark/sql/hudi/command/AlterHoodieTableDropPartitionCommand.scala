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
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkSqlWriter}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand}
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}

case class AlterHoodieTableDropPartitionCommand(
    tableIdentifier: TableIdentifier,
    specs: Seq[TablePartitionSpec])
extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)
    DDLUtils.verifyAlterTableType(
      sparkSession.sessionState.catalog, hoodieCatalogTable.table, isView = false)

    val normalizedSpecs: Seq[Map[String, String]] = specs.map { spec =>
      normalizePartitionSpec(
        spec,
        hoodieCatalogTable.partitionFields,
        hoodieCatalogTable.tableName,
        sparkSession.sessionState.conf.resolver)
    }

    val parameters = buildHoodieConfig(sparkSession, hoodieCatalogTable, normalizedSpecs)
    HoodieSparkSqlWriter.write(
      sparkSession.sqlContext,
      SaveMode.Append,
      parameters,
      sparkSession.emptyDataFrame)

    sparkSession.catalog.refreshTable(tableIdentifier.unquotedString)
    Seq.empty[Row]
  }

  private def buildHoodieConfig(
      sparkSession: SparkSession,
      hoodieCatalogTable: HoodieCatalogTable,
      normalizedSpecs: Seq[Map[String, String]]): Map[String, String] = {
    val table = hoodieCatalogTable.table
    val allPartitionPaths = hoodieCatalogTable.getAllPartitionPaths
    val enableHiveStylePartitioning = isHiveStyledPartitioning(allPartitionPaths, table)
    val enableEncodeUrl = isUrlEncodeEnabled(allPartitionPaths, table)
    val partitionFields = hoodieCatalogTable.partitionFields.mkString(",")
    val partitionsToDrop = normalizedSpecs.map { spec =>
      hoodieCatalogTable.partitionFields.map{ partitionColumn =>
        val encodedPartitionValue = if (enableEncodeUrl) {
          PartitionPathEncodeUtils.escapePathName(spec(partitionColumn))
        } else {
          spec(partitionColumn)
        }
        if (enableHiveStylePartitioning) {
          partitionColumn + "=" + encodedPartitionValue
        } else {
          encodedPartitionValue
        }
      }.mkString("/")
    }.mkString(",")

    val enableHive = isEnableHive(sparkSession)
    withSparkConf(sparkSession, Map.empty) {
      Map(
        "path" -> hoodieCatalogTable.tableLocation,
        TBL_NAME.key -> hoodieCatalogTable.tableName,
        TABLE_TYPE.key -> hoodieCatalogTable.tableTypeName,
        OPERATION.key -> DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL,
        PARTITIONS_TO_DELETE.key -> partitionsToDrop,
        RECORDKEY_FIELD.key -> hoodieCatalogTable.primaryKeys.mkString(","),
        PRECOMBINE_FIELD.key -> hoodieCatalogTable.preCombineKey.getOrElse(""),
        PARTITIONPATH_FIELD.key -> partitionFields,
        HIVE_SYNC_ENABLED.key -> enableHive.toString,
        META_SYNC_ENABLED.key -> enableHive.toString,
        HIVE_SYNC_MODE.key -> HiveSyncMode.HMS.name(),
        HIVE_USE_JDBC.key -> "false",
        HIVE_DATABASE.key -> hoodieCatalogTable.table.identifier.database.getOrElse("default"),
        HIVE_TABLE.key -> hoodieCatalogTable.table.identifier.table,
        HIVE_SUPPORT_TIMESTAMP_TYPE.key -> "true",
        HIVE_PARTITION_FIELDS.key -> partitionFields,
        HIVE_PARTITION_EXTRACTOR_CLASS.key -> classOf[MultiPartKeysValueExtractor].getCanonicalName
      )
    }
  }

  def normalizePartitionSpec[T](
      partitionSpec: Map[String, T],
      partColNames: Seq[String],
      tblName: String,
      resolver: Resolver): Map[String, T] = {
    val normalizedPartSpec = partitionSpec.toSeq.map { case (key, value) =>
      val normalizedKey = partColNames.find(resolver(_, key)).getOrElse {
        throw new AnalysisException(s"$key is not a valid partition column in table $tblName.")
      }
      normalizedKey -> value
    }

    if (normalizedPartSpec.size < partColNames.size) {
      throw new AnalysisException(
        "All partition columns need to be specified for Hoodie's dropping partition")
    }

    val lowerPartColNames = partColNames.map(_.toLowerCase)
    if (lowerPartColNames.distinct.length != lowerPartColNames.length) {
      val duplicateColumns = lowerPartColNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) in the partition schema: ${duplicateColumns.mkString(", ")}")
    }

    normalizedPartSpec.toMap
  }

}
