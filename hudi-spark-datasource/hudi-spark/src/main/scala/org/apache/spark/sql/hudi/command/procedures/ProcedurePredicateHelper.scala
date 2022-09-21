/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.hudi.{AvroConversionUtils, HoodieFileIndex}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.{resolveExpr, splitPartitionAndDataPredicates}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.datasources.FileStatusCache

trait ProcedurePredicateHelper extends PredicateHelper {

  def prunePartition(
    sparkSession: SparkSession,
    metaClient: HoodieTableMetaClient,
    predicate: String): Seq[String] = {
    val options = Map(QUERY_TYPE.key() -> QUERY_TYPE_SNAPSHOT_OPT_VAL, "path" -> metaClient.getBasePath)
    val hoodieFileIndex = HoodieFileIndex(sparkSession, metaClient, None, options,
      FileStatusCache.getOrCreate(sparkSession))

    // Resolve partition predicates
    val schemaResolver = new TableSchemaResolver(metaClient)
    val tableSchema = AvroConversionUtils.convertAvroSchemaToStructType(schemaResolver.getTableAvroSchema)
    val condition = resolveExpr(sparkSession, predicate, tableSchema)
    val partitionColumns = metaClient.getTableConfig.getPartitionFields.orElse(Array[String]())
    val (partitionPredicates, dataPredicates) = splitPartitionAndDataPredicates(
      sparkSession, splitConjunctivePredicates(condition).toArray, partitionColumns)
    checkArgument(dataPredicates.isEmpty, "Only partition predicates are allowed")

    // Get all partitions and prune partition by predicates
    val prunedPartitions = hoodieFileIndex.getPartitionPaths(partitionPredicates)
    prunedPartitions.map(partitionPath => partitionPath.getPath)
  }
}
