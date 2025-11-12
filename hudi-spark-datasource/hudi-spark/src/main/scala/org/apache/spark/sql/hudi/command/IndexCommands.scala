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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.common.model.HoodieIndexDefinition
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.{StringUtils, ValidationUtils}
import org.apache.hudi.exception.HoodieIndexException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieSparkIndexClient
import org.apache.hudi.index.expression.ExpressionIndexSparkFunctions
import org.apache.hudi.index.expression.HoodieExpressionIndex.EXPRESSION_OPTION
import org.apache.hudi.metadata.{HoodieTableMetadataUtil, MetadataPartitionType}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.getTableLocation

import java.util
import java.util.stream.Collectors

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter}

case class CreateIndexCommand(table: CatalogTable,
                              indexName: String,
                              indexType: String,
                              ignoreIfExists: Boolean,
                              columns: Seq[(Seq[String], Map[String, String])],
                              options: Map[String, String]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableId = table.identifier
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    val columnsMap: java.util.LinkedHashMap[String, java.util.Map[String, String]] =
      new util.LinkedHashMap[String, java.util.Map[String, String]]()
    columns.map(c => columnsMap.put(c._1.mkString("."), c._2.asJava))

    if (indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
      || indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS)) {
      if (indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS) &&
        options.asJava.getOrDefault(EXPRESSION_OPTION, ExpressionIndexSparkFunctions.IDENTITY_FUNCTION).equals(ExpressionIndexSparkFunctions.IDENTITY_FUNCTION)) {
        throw new HoodieIndexException("Column stats index without expression on any column can be created using datasource configs. " +
          "Please refer https://hudi.apache.org/docs/metadata for more info")
      }
      new HoodieSparkIndexClient(sparkSession).create(metaClient, indexName, indexType, columnsMap, options.asJava, table.properties.asJava)
    } else if (indexName.equals(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)) {
      ValidationUtils.checkArgument(CreateIndexCommand.matchesRecordKeys(columnsMap.keySet().asScala.toSet, metaClient.getTableConfig),
        "Input columns should match configured record key columns: " + metaClient.getTableConfig.getRecordKeyFieldProp)
      new HoodieSparkIndexClient(sparkSession).create(metaClient, indexName, HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX, columnsMap, options.asJava, table.properties.asJava)
    } else if (StringUtils.isNullOrEmpty(indexType)) {
      val columnNames = columnsMap.keySet().asScala.toSet
      val derivedIndexType: String = if (CreateIndexCommand.matchesRecordKeys(columnNames, metaClient.getTableConfig)) {
        HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX
      } else {
        HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX
      }
      new HoodieSparkIndexClient(sparkSession).create(metaClient, indexName, derivedIndexType, columnsMap, options.asJava, table.properties.asJava)
    } else {
      throw new HoodieIndexException(String.format("%s is not supported", indexType))
    }

    // Invalidate cached table for queries do not access related table
    // through {@code DefaultSource}
    sparkSession.sessionState.catalog.invalidateCachedTable(tableId)
    Seq.empty
  }
}

object CreateIndexCommand {

  /**
   * Returns true if the input columns are same as the set of the primary keys for the table.
   * Returns false if the input columns are all non primary keys.
   * Throws HoodieIndexException if the input columns are a subset of primary key columns or overlap
   * with both primary and non primary key columns in the table.
   */
  def matchesRecordKeys(columnNames: Set[String], tableConfig: HoodieTableConfig): Boolean = {
    val recordKeyFields = tableConfig.getRecordKeyFields.orElse(Array.empty).toSet
    if (columnNames.equals(recordKeyFields)) {
      true
    } else {
      val recordKeyColumns = columnNames.intersect(recordKeyFields)
      val nonRecordKeyColumns = columnNames -- recordKeyColumns
      if (recordKeyColumns.isEmpty) {
        // no record key columns
        false
      } else if (nonRecordKeyColumns.nonEmpty) {
        // partial record key columns are present along with non record key columns
        throw new HoodieIndexException("Index can be created either on all record key columns or a non record key column")
      } else {
        // only partial record key columns are present
        throw new HoodieIndexException(String.format("Index can be only be created on all record key columns. "
          + "Configured record key fields %s. Input columns: %s", recordKeyFields, columnNames))
      }
    }
  }
}

case class DropIndexCommand(table: CatalogTable,
                            indexName: String,
                            ignoreIfNotExists: Boolean) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableId = table.identifier
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    // need to ensure that the index name is for a valid partition type
    val indexMetadataOpt = metaClient.getIndexMetadata
    if (metaClient.getTableConfig.getMetadataPartitions.contains(indexName)) {
      new HoodieSparkIndexClient(sparkSession).drop(metaClient, indexName, ignoreIfNotExists)
    } else if (indexMetadataOpt.isPresent) {
      val indexMetadata = indexMetadataOpt.get
      val indexDefinitions = indexMetadata.getIndexDefinitions.values().stream()
        .filter(definition => {
          val partitionType = MetadataPartitionType.fromPartitionPath(definition.getIndexName)
          partitionType.getIndexNameWithoutPrefix(definition).equals(indexName)
        })
        .collect(Collectors.toList[HoodieIndexDefinition])
      if (indexDefinitions.isEmpty && !ignoreIfNotExists) {
        throw new HoodieIndexException(String.format("Index does not exist: %s", indexName))
      }
      indexDefinitions.forEach(definition => new HoodieSparkIndexClient(sparkSession).drop(metaClient, definition.getIndexName, ignoreIfNotExists))
    } else if (!ignoreIfNotExists) {
      throw new HoodieIndexException(String.format("Index does not exist: %s", indexName))
    }

    // Invalidate cached table for queries do not access related table
    // through {@code DefaultSource}
    sparkSession.sessionState.catalog.invalidateCachedTable(tableId)
    Seq.empty
  }
}

/**
 * Command to show available indexes in hudi. The corresponding logical plan is available at
 * org.apache.spark.sql.catalyst.plans.logical.ShowIndexes
 */
case class ShowIndexesCommand(table: CatalogTable,
                              override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = createHoodieTableMetaClient(table.identifier, sparkSession)
    // need to ensure that the index name is for a valid partition type
    metaClient.getTableConfig.getMetadataPartitions.asScala.map(
      partition => {
        if (MetadataPartitionType.isExpressionOrSecondaryIndex(partition)) {
          val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.get(partition)
          Row(partition, indexDefinition.getIndexType.toLowerCase, indexDefinition.getSourceFields.asScala.mkString(","))
        } else if (!partition.equals(MetadataPartitionType.FILES.getPartitionPath)) {
          Row(partition, partition, "")
        } else {
          Row.empty
        }
      }
    ).filter(row => row.length != 0)
     .toSeq
  }
}

case class RefreshIndexCommand(table: CatalogTable,
                               indexName: String) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty
  }
}

abstract class IndexBaseCommand extends HoodieLeafRunnableCommand with Logging {

  /**
   * Create hoodie table meta client according to given table identifier and
   * spark session
   *
   * @param tableId      The table identifier
   * @param sparkSession The spark session
   * @return The hoodie table meta client
   */
  def createHoodieTableMetaClient(
      tableId: TableIdentifier,
      sparkSession: SparkSession): HoodieTableMetaClient = {
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableId)
    val basePath = getTableLocation(catalogTable, sparkSession)
    HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf))
      .setBasePath(basePath)
      .build()
  }
}
