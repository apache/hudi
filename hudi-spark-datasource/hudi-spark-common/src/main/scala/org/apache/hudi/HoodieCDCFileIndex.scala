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

import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.HoodieCDCExtractor
import org.apache.hudi.common.table.log.InstantRange.RangeType

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class HoodieCDCFileIndex(override val spark: SparkSession,
                         override val metaClient: HoodieTableMetaClient,
                         override val schemaSpec: Option[StructType],
                         override val options: Map[String, String],
                         @transient override val fileStatusCache: FileStatusCache = NoopCache,
                         override val includeLogFiles: Boolean,
                         val rangeType: RangeType)
  extends HoodieFileIndex(
    spark, metaClient, schemaSpec, options, fileStatusCache, includeLogFiles, shouldEmbedFileSlices = true
  ) with FileIndex  {
  private val emptyPartitionPath: String = "empty_partition_path";
  val cdcRelation: CDCRelation = CDCRelation.getCDCRelation(spark.sqlContext, metaClient, options, rangeType)
  val cdcExtractor: HoodieCDCExtractor = cdcRelation.cdcExtractor

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: HoodieCDCFileIndex =>
        super.equals(other) && this.rangeType == other.rangeType
      case _ => false
    }
  }

  override def hashCode(): Int = {
    31 * super.hashCode() + rangeType.hashCode()
  }

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    hasPushedDownPartitionPredicates = true
    cdcExtractor.extractCDCFileSplits().asScala.map {
      case (fileGroupId, fileSplits) =>
        val partitionPath = if (fileGroupId.getPartitionPath.isEmpty) emptyPartitionPath else fileGroupId.getPartitionPath
        val partitionFields = metaClient.getTableConfig.getPartitionFields
        val partitionValues: InternalRow = if (partitionFields.isPresent) {
          new GenericInternalRow(parsePartitionColumnValues(partitionFields.get(), partitionPath).asInstanceOf[Array[Any]])
        } else {
          InternalRow.empty
        }

        // Bogus file status, not used during read.
        val fileStatus = new FileStatus(0, true, 0, 0, 0,
          0, null, "", "", null,
          new Path(partitionPath, fileGroupId.getFileId))

        // Note that CDC file splits must be sorted based on their instant time.
        // Otherwise, the resulting records may not be correct.
        sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
          new HoodiePartitionCDCFileGroupMapping(
            partitionValues, fileSplits.asScala.sortBy(_.getInstant).toList),
          Seq(fileStatus)
        )
    }.toList
  }

  override def inputFiles: Array[String] = {
    cdcExtractor.extractCDCFileSplits().asScala.map { fileGroupSplit =>
      val fileGroupId = fileGroupSplit._1
      new Path(fileGroupId.getPartitionPath, fileGroupId.getFileId).toString
    }.toArray
  }
}
