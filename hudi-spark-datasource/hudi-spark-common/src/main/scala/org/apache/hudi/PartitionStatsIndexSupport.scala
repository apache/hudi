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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieFileIndex.collectReferencedColumns
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, HoodieMetadataRecord}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.{HoodieData, HoodieListData}
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.metadata.{ColumnStatsIndexPrefixRawKey, HoodieMetadataPayload, HoodieTableMetadataUtil}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.{PARTITION_NAME_COLUMN_STATS, getIndexableColumns, getLatestTableSchema}
import org.apache.hudi.util.{JFunction, Lazy}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, DateAdd, DateFormatClass, DateSub, Expression, FromUnixTime, ParseToDate, ParseToTimestamp, RegExpExtract, RegExpReplace, StringSplit, StringTrim, StringTrimLeft, StringTrimRight, Substring, UnaryExpression, UnixTimestamp}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.hudi.DataSkippingUtils.{containsNullOrValueCountBasedFilters, translateIntoColumnStatsIndexFilterExpr}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class PartitionStatsIndexSupport(spark: SparkSession,
                                 tableSchema: StructType,
                                 avroSchema: Schema,
                                 @transient metadataConfig: HoodieMetadataConfig,
                                 @transient metaClient: HoodieTableMetaClient,
                                 allowCaching: Boolean = false)
  extends ColumnStatsIndexSupport(spark, tableSchema, avroSchema, metadataConfig, metaClient, allowCaching) with Logging with SparkAdapterSupport {

  override def getIndexName: String = PartitionStatsIndexSupport.INDEX_NAME

  override def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled &&
      metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS)
  }

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    throw new UnsupportedOperationException("This method is not supported by PartitionStatsIndexSupport")
  }

  override def loadColumnStatsIndexRecords(targetColumns: Seq[String], prunedPartitions: Option[Set[String]] = None, shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    checkState(targetColumns.nonEmpty)
    logDebug(s"Loading column stats for columns: ${targetColumns.mkString(", ")}")
    // For partition stats, we only need column names (no partition name)
    val rawKeys = targetColumns.map(colName => new ColumnStatsIndexPrefixRawKey(colName))
    val metadataRecords: HoodieData[HoodieRecord[HoodieMetadataPayload]] =
      metadataTable.getRecordsByKeyPrefixes(
        HoodieListData.eager(rawKeys.asJava), HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, shouldReadInMemory)
    val columnStatsRecords: HoodieData[HoodieMetadataColumnStats] =
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      metadataRecords.map(JFunction.toJavaSerializableFunction(record => {
          toScalaOption(record.getData.getInsertValue(null, null))
            .map(metadataRecord => metadataRecord.asInstanceOf[HoodieMetadataRecord].getColumnStatsMetadata)
            .orNull
        }))
        .filter(JFunction.toJavaSerializableFunction(columnStatsRecord => columnStatsRecord != null))

    columnStatsRecords
  }

  def prunePartitions(fileIndex: HoodieFileIndex, queryFilters: Seq[Expression]): Option[Set[String]] = {
    if (isIndexAvailable && queryFilters.nonEmpty) {
      // Filter out sql queries. Partition stats only supports simple queries on field attribute without any operation on the field
      val nonSqlFilters = filterNonSqlExpressions(queryFilters)
      val filteredIndexedCols = {
        val indexDefinition = metaClient.getIndexMetadata.get()
          .getIndexDefinitions.get(PARTITION_NAME_COLUMN_STATS)

        getIndexableColumns(indexDefinition, avroSchema).asScala.toSeq
      }
      // Filter out queries involving null and value count checks
      val filteredQueryFilters: Seq[Expression] = filterExpressionsExcludingNullAndValue(nonSqlFilters, filteredIndexedCols)
      lazy val queryReferencedColumns = collectReferencedColumns(spark, filteredQueryFilters, tableSchema)

      if (filteredQueryFilters.nonEmpty && queryReferencedColumns.nonEmpty) {
        val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
        loadTransposed(queryReferencedColumns, readInMemory, Option.empty, Option.empty) {
          transposedPartitionStatsDF => {
            try {
              transposedPartitionStatsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
              val allPartitions = transposedPartitionStatsDF.select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
                .collect()
                .map(_.getString(0))
                .toSet
              if (allPartitions.nonEmpty) {
                // PARTITION_STATS index exist for all or some columns in the filters
                // NOTE: [[translateIntoColumnStatsIndexFilterExpr]] has covered the case where the
                //       column in a filter does not have the stats available, by making sure such a
                //       filter does not prune any partition.
                // to be fixed. HUDI-8836.
                val indexFilter = filteredQueryFilters.map(translateIntoColumnStatsIndexFilterExpr(_, indexedCols = filteredIndexedCols)).reduce(And)
                if (indexFilter.equals(TrueLiteral)) {
                  // if there are any non indexed cols or we can't translate source expr, we cannot prune partitions based on col stats lookup.
                  Some(allPartitions)
                } else {
                  Some(transposedPartitionStatsDF.where(sparkAdapter.createColumnFromExpression(indexFilter))
                    .select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
                    .collect()
                    .map(_.getString(0))
                    .toSet)
                }
              } else {
                // PARTITION_STATS index does not exist for any column in the filters, skip the pruning
                Option.empty
              }
            } finally {
              transposedPartitionStatsDF.unpersist()
            }
          }
        }
      } else {
        Option.empty
      }
    } else {
      Option.empty
    }
  }

  private def filterExpressionsExcludingNullAndValue(queryFilters: Seq[Expression], indexedCols: Seq[String]): Seq[Expression] = {
    // Filter queries which do not contain null/value-count filters
    queryFilters.filter(query => !containsNullOrValueCountBasedFilters(query, indexedCols))
  }

  private def filterNonSqlExpressions(queryFilters: Seq[Expression]): Seq[Expression] = {
    val isMatchingExpression = (expr: Expression) => {
      expr.find {
        case _: UnaryExpression => true
        case _: DateFormatClass => true
        case _: FromUnixTime => true
        case _: UnixTimestamp => true
        case _: ParseToDate => true
        case _: ParseToTimestamp => true
        case _: DateAdd => true
        case _: DateSub => true
        case _: Substring => true
        case _: StringTrim => true
        case _: StringTrimLeft => true
        case _: StringTrimRight => true
        case _: RegExpReplace => true
        case _: RegExpExtract => true
        case _: StringSplit => true
        case _ => false
      }.isDefined
    }
    queryFilters.filter(query => !isMatchingExpression(query))
  }
}

object PartitionStatsIndexSupport {
  val INDEX_NAME = "PARTITION_STATS"
}
