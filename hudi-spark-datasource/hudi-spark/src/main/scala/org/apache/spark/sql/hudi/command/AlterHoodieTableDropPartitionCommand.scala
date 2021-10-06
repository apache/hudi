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

import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkSqlWriter, HoodieWriterUtils}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand}
import org.apache.spark.sql.hudi.HoodieSqlUtils._

case class AlterHoodieTableDropPartitionCommand(
    tableIdentifier: TableIdentifier,
    specs: Seq[TablePartitionSpec])
extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableIdentifier)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)

    val path = getTableLocation(table, sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path).setConf(hadoopConf).build()
    val partitionColumns = metaClient.getTableConfig.getPartitionFields
    val normalizedSpecs: Seq[Map[String, String]] = specs.map { spec =>
      normalizePartitionSpec(
        spec,
        partitionColumns.get(),
        table.identifier.quotedString,
        sparkSession.sessionState.conf.resolver)
    }

    val parameters = buildHoodieConfig(sparkSession, path, partitionColumns.get, normalizedSpecs)

    HoodieSparkSqlWriter.write(
      sparkSession.sqlContext,
      SaveMode.Append,
      parameters,
      sparkSession.emptyDataFrame)

    Seq.empty[Row]
  }

  private def buildHoodieConfig(
      sparkSession: SparkSession,
      path: String,
      partitionColumns: Seq[String],
      normalizedSpecs: Seq[Map[String, String]]): Map[String, String] = {
    val table = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    val allPartitionPaths = getAllPartitionPaths(sparkSession, table)
    val notHiveStylePartitioning = isNotHiveStyledPartitionTable(allPartitionPaths, table)
    val notEncodeUrl = isUrlEncodeDisable(allPartitionPaths, table)

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(sparkSession.sessionState.newHadoopConf)
      .build()
    val tableConfig = metaClient.getTableConfig

    val partitionsToDelete = normalizedSpecs.map { spec =>
      partitionColumns.map{ partitionColumn =>
        val encodedPartitionValue = if (notEncodeUrl) {
          spec(partitionColumn)
        } else {
          PartitionPathEncodeUtils.escapePathName(spec(partitionColumn))
        }
        if (notHiveStylePartitioning) {
          encodedPartitionValue
        } else {
          partitionColumn + "=" + encodedPartitionValue
        }
      }.mkString("/")
    }.mkString(",")

    val optParams = withSparkConf(sparkSession, table.storage.properties) {
      Map(
        "path" -> path,
        TBL_NAME.key -> tableIdentifier.table,
        TABLE_TYPE.key -> tableConfig.getTableType.name,
        OPERATION.key -> DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL,
        PARTITIONS_TO_DELETE.key -> partitionsToDelete,
        RECORDKEY_FIELD.key -> tableConfig.getRecordKeyFieldProp,
        PRECOMBINE_FIELD.key -> tableConfig.getPreCombineField,
        PARTITIONPATH_FIELD.key -> tableConfig.getPartitionFieldProp
      )
    }

    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(optParams)
    val translatedOptions = DataSourceWriteOptions.translateSqlOptions(parameters)
    translatedOptions
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
