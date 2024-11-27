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

import org.apache.hudi.HoodieSparkIndexClient
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.{HoodieIndexDefinition, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.{StringUtils, ValidationUtils}
import org.apache.hudi.exception.HoodieIndexException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.functional.HoodieExpressionIndex.{EXPRESSION_OPTION, IDENTITY_FUNCTION}
import org.apache.hudi.metadata.{HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.getTableLocation
import org.apache.spark.sql.{Row, SparkSession}

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
    val extraOpts = options ++ table.properties

    if (indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX)
      || indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
      || indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS)) {
      // validate that only overwrite with latest payloads can enabled SI
      if (indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX)) {
        if ((metaClient.getTableConfig.getPayloadClass != null && !(metaClient.getTableConfig.getPayloadClass.equals(classOf[OverwriteWithLatestAvroPayload].getCanonicalName)))
          || (metaClient.getTableConfig.getRecordMergeMode ne RecordMergeMode.COMMIT_TIME_ORDERING)) {
          throw new HoodieIndexException("Secondary Index can only be enabled on table with OverwriteWithLatestAvroPayload payload class or " + "Merge mode set to OVERWRITE_WITH_LATEST")
        }
      }
      if (indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS) &&
        extraOpts.asJava.getOrDefault(EXPRESSION_OPTION, IDENTITY_FUNCTION).equals(IDENTITY_FUNCTION)) {
        throw new HoodieIndexException("Currently Column stats Index can only be created with a non identity expression")
      }
      HoodieSparkIndexClient.getInstance(sparkSession).create(
        metaClient, indexName, indexType, columnsMap, extraOpts.asJava)
    } else if (indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)) {
      ValidationUtils.checkArgument(matchesRecordKeys(columnsMap.keySet().asScala.toSet, metaClient.getTableConfig),
        "Input columns should match configured record key columns: " + metaClient.getTableConfig.getRecordKeyFieldProp)
      HoodieSparkIndexClient.getInstance(sparkSession).create(
        metaClient, indexName, indexType, columnsMap, extraOpts.asJava)
    } else if (StringUtils.isNullOrEmpty(indexType)) {
      val columnNames = columnsMap.keySet().asScala.toSet
      val derivedIndexType: String = if (matchesRecordKeys(columnNames, metaClient.getTableConfig)) {
        HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX
      } else if (columnNames.size == 1) {
        HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX
      } else {
        throw new HoodieIndexException("Can not create secondary index on more than one column at a time")
      }
      HoodieSparkIndexClient.getInstance(sparkSession).create(
        metaClient, indexName, derivedIndexType, columnsMap, extraOpts.asJava)
    } else {
      throw new HoodieIndexException(String.format("%s is not supported", indexType))
    }

    // Invalidate cached table for queries do not access related table
    // through {@code DefaultSource}
    sparkSession.sessionState.catalog.invalidateCachedTable(tableId)
    Seq.empty
  }

  def matchesRecordKeys(columnNames: Set[String], tableConfig: HoodieTableConfig): Boolean = {
    val recordKeyFields = tableConfig.getRecordKeyFields.orElse(Array.empty).toSet
    if (columnNames.equals(recordKeyFields) || columnNames.isEmpty) {
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
        throw new HoodieIndexException("Index can be only be created on all record key columns. Configured record key fields " + recordKeyFields)
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
      HoodieSparkIndexClient.getInstance(sparkSession).drop(metaClient, indexName, ignoreIfNotExists)
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
      indexDefinitions.forEach(definition => HoodieSparkIndexClient.getInstance(sparkSession).drop(metaClient, definition.getIndexName, ignoreIfNotExists))
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
        if (MetadataPartitionType.isGenericIndex(partition)) {
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
