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
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.hash.ColumnIndexID
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadataUtil}
import org.apache.hudi.util.JFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class PartitionStatsIndexSupport(spark: SparkSession,
                                 tableSchema: StructType,
                                 @transient metadataConfig: HoodieMetadataConfig,
                                 @transient metaClient: HoodieTableMetaClient,
                                 allowCaching: Boolean = false)
  extends ColumnStatsIndexSupport(spark, tableSchema, metadataConfig, metaClient, allowCaching) {

  override def isIndexAvailable: Boolean = {
    checkState(metadataConfig.isEnabled, "Metadata Table support has to be enabled")
    metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS)
  }

  override def loadColumnStatsIndexRecords(targetColumns: Seq[String], shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    checkState(targetColumns.nonEmpty)
    val encodedTargetColumnNames = targetColumns.map(colName => new ColumnIndexID(colName).asBase64EncodedString())
    val metadataRecords: HoodieData[HoodieRecord[HoodieMetadataPayload]] =
      metadataTable.getRecordsByKeyPrefixes(encodedTargetColumnNames.asJava, HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, shouldReadInMemory)
    val columnStatsRecords: HoodieData[HoodieMetadataColumnStats] =
      // NOTE: Explicit conversion is required for Scala 2.11
      metadataRecords.map(JFunction.toJavaSerializableFunction(record => {
          toScalaOption(record.getData.getInsertValue(null, null))
            .map(metadataRecord => metadataRecord.asInstanceOf[HoodieMetadataRecord].getColumnStatsMetadata)
            .orNull
        }))
        .filter(JFunction.toJavaSerializableFunction(columnStatsRecord => columnStatsRecord != null))

    columnStatsRecords
  }
}
