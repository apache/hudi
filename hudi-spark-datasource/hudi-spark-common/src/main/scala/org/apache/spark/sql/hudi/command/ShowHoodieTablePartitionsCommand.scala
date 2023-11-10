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

import org.apache.hudi.common.util.PartitionPathEncodeUtils

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types.StringType

/**
 * Command for show hudi table's partitions.
 */
case class ShowHoodieTablePartitionsCommand(
    tableIdentifier: TableIdentifier,
    specOpt: Option[TablePartitionSpec])
  extends HoodieLeafRunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("partition", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)

    val schemaOpt = hoodieCatalogTable.tableSchema
    val partitionColumnNamesOpt = hoodieCatalogTable.tableConfig.getPartitionFields

    if (partitionColumnNamesOpt.isPresent && partitionColumnNamesOpt.get.nonEmpty && schemaOpt.nonEmpty) {
      specOpt.map { spec =>
        hoodieCatalogTable.getPartitionPaths.filter { partitionPath =>
          val part = PartitioningUtils.parsePathFragment(partitionPath)
          spec.forall { case (col, value) =>
            PartitionPathEncodeUtils.escapePartitionValue(value) == part.getOrElse(col, null)
          }
        }
      }
        .getOrElse(hoodieCatalogTable.getPartitionPaths)
        .map(Row(_))
    } else {
      Seq.empty[Row]
    }
  }
}
