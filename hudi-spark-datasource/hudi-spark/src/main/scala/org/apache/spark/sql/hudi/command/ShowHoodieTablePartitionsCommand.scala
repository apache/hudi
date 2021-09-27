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

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.types.StringType

/**
 * Command for show hudi table's partitions.
 */
case class ShowHoodieTablePartitionsCommand(
    tableName: TableIdentifier,
    specOpt: Option[TablePartitionSpec])
extends RunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("partition", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val resolver = sparkSession.sessionState.conf.resolver
    val catalogTable = catalog.getTableMetadata(tableName)
    val tablePath = getTableLocation(catalogTable, sparkSession)

    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(tablePath)
      .setConf(hadoopConf).build()
    val schemaOpt = getTableSqlSchema(metaClient)
    val partitionColumnNamesOpt = metaClient.getTableConfig.getPartitionFields
    if (partitionColumnNamesOpt.isPresent && partitionColumnNamesOpt.get.nonEmpty
        && schemaOpt.isDefined && schemaOpt.nonEmpty) {

      val partitionColumnNames = partitionColumnNamesOpt.get
      val schema = schemaOpt.get
      val allPartitionPaths: Seq[String] = getAllPartitionPaths(sparkSession, catalogTable)

      if (specOpt.isEmpty) {
        allPartitionPaths.map(Row(_))
      } else {
        val spec = specOpt.get
        allPartitionPaths.filter { partitionPath =>
          val part = PartitioningUtils.parsePathFragment(partitionPath)
          spec.forall { case (col, value) =>
            PartitionPathEncodeUtils.escapePartitionValue(value) == part.getOrElse(col, null)
          }
        }.map(Row(_))
      }
    } else {
      Seq.empty[Row]
    }
  }
}
