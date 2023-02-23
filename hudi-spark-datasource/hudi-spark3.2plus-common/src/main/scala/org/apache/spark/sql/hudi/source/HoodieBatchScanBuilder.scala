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

package org.apache.spark.sql.hudi.source

import org.apache.hudi.DataSourceReadOptions.QUERY_TYPE
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.{DataSourceOptionsHelper, DefaultSource}
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType


class HoodieBatchScanBuilder(spark: SparkSession,
                             hoodieCatalogTable: HoodieCatalogTable,
                             options: Map[String, String])
    extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  @transient lazy val hadoopConf = {
    // Hadoop Configurations are case sensitive.
    spark.sessionState.newHadoopConfWithOptions(options)
  }

  private var filterExpressions: Option[Expression] = None

  private var filterArrays: Array[Filter] = Array.empty

  private var expectedSchema: StructType= hoodieCatalogTable.tableSchema

  override def build(): Scan = {
    val relation = new DefaultSource().createRelation(new SQLContext(spark), options)
    relation match {
      case HadoopFsRelation(location, partitionSchema, dataSchema, _, _, options) =>
        val selectedPartitions = location.listFiles(Seq.empty, filterExpressions.toList)
        SparkScan(spark, hoodieCatalogTable.catalogTableName, selectedPartitions, dataSchema, partitionSchema,
          expectedSchema, filterArrays, options, hadoopConf)
      case _ =>
        val isBootstrappedTable = hoodieCatalogTable.metaClient.getTableConfig.getBootstrapBasePath.isPresent
        val tableType = hoodieCatalogTable.metaClient.getTableType
        val parameters = DataSourceOptionsHelper.parametersWithReadDefaults(options)
        val queryType = parameters(QUERY_TYPE.key)
        throw new HoodieException("Hoodie do not support read with DataSource V2 for (tableType, queryType, " +
          "isBootstrappedTable) = (" + tableType + "," + queryType + "," + isBootstrappedTable + ")")
    }
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filterExpressions = HoodieCatalystExpressionUtils.convertToCatalystExpression(filters, expectedSchema)
    this.filterArrays = filters
    filters
  }

  override def pushedFilters(): Array[Filter] = {
    filterArrays
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    expectedSchema = requiredSchema
  }
}
