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

import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, HoodieMetadataRecord}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.hash.ColumnIndexID
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadataUtil}
import org.apache.hudi.util.JFunction

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.JavaConverters._

class PartitionStatsIndexSupport(spark: SparkSession,
                                 tableSchema: StructType,
                                 @transient metadataConfig: HoodieMetadataConfig,
                                 @transient metaClient: HoodieTableMetaClient,
                                 allowCaching: Boolean = false)
  extends ColumnStatsIndexSupport(spark, tableSchema, metadataConfig, metaClient, allowCaching) with Logging {

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

  override def loadColumnStatsIndexRecords(targetColumns: Seq[String], shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    checkState(targetColumns.nonEmpty)
    val encodedTargetColumnNames = targetColumns.map(colName => new ColumnIndexID(colName).asBase64EncodedString())
    logDebug(s"Loading column stats for columns: ${targetColumns.mkString(", ")},  Encoded column names: ${encodedTargetColumnNames.mkString(", ")}")
    val metadataRecords: HoodieData[HoodieRecord[HoodieMetadataPayload]] =
      metadataTable.getRecordsByKeyPrefixes(encodedTargetColumnNames.asJava, HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, shouldReadInMemory)
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

  def prunePartitions(fileIndex: HoodieFileIndex,
                      queryFilters: Seq[Expression],
                      queryReferencedColumns: Seq[String]): Option[Set[String]] = {
    if (isIndexAvailable && queryFilters.nonEmpty && queryReferencedColumns.nonEmpty) {
      val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
      loadTransposed(queryReferencedColumns, readInMemory, Option.empty) {
        transposedPartitionStatsDF => {
          val allPartitions = transposedPartitionStatsDF.select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
            .collect()
            .map(_.getString(0))
            .toSet
          if (allPartitions.nonEmpty) {
            // PARTITION_STATS index exist for all or some columns in the filters
            // NOTE: [[translateIntoColumnStatsIndexFilterExpr]] has covered the case where the
            //       column in a filter does not have the stats available, by making sure such a
            //       filter does not prune any partition.
            val indexSchema = transposedPartitionStatsDF.schema
            val indexFilter = queryFilters.map(translateIntoColumnStatsIndexFilterExpr(_, indexSchema)).reduce(And)
            Some(transposedPartitionStatsDF.where(new Column(indexFilter))
              .select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
              .collect()
              .map(_.getString(0))
              .toSet)
          } else {
            // PARTITION_STATS index does not exist for any column in the filters, skip the pruning
            Option.empty
          }
        }
      }
    } else {
      Option.empty
    }
  }
}

object PartitionStatsIndexSupport {
  val INDEX_NAME = "PARTITION_STATS"
}
