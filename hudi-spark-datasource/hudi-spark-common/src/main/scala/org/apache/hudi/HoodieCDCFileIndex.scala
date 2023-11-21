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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.common.model.HoodieFileGroupId
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.HoodieCDCExtractor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

class HoodieCDCFileIndex (override val spark: SparkSession,
                          override val metaClient: HoodieTableMetaClient,
                          override val schemaSpec: Option[StructType],
                          override val options: Map[String, String],
                          @transient override val fileStatusCache: FileStatusCache = NoopCache,
                          override val includeLogFiles: Boolean,
                          override val shouldEmbedFileSlices: Boolean)
  extends HoodieIncrementalFileIndex(
    spark, metaClient, schemaSpec, options, fileStatusCache, includeLogFiles, shouldEmbedFileSlices
  ) with FileIndex {
  val cdcRelation: CDCRelation = CDCRelation.getCDCRelation(spark.sqlContext, metaClient, options)
  val cdcExtractor: HoodieCDCExtractor = cdcRelation.cdcExtractor

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val partitionToFileGroups = cdcExtractor.extractCDCFileSplits().asScala.groupBy(_._1.getPartitionPath).toSeq
    partitionToFileGroups.map {
      case (partitionPath, fileGroups) =>
        val fileGroupIds: List[FileStatus] = fileGroups.map { fileGroup => {
          // We create a fake FileStatus to wrap the information of HoodieFileGroupId, which are used
          // later to retrieve the corresponding CDC file group splits.
          val fileGroupId: HoodieFileGroupId = fileGroup._1
          new FileStatus(0, true, 0, 0, 0,
            0, null, "", "", null,
            new Path(fileGroupId.getPartitionPath, fileGroupId.getFileId))
        }}.toList
        val partitionValues: InternalRow = new GenericInternalRow(doParsePartitionColumnValues(
          metaClient.getTableConfig.getPartitionFields.get(), partitionPath).asInstanceOf[Array[Any]])
        sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
          new HoodiePartitionCDCFileGroupMapping(
            partitionValues, fileGroups.map(kv => kv._1 -> kv._2.asScala.toList).toMap),
          fileGroupIds
        )
    }
  }

  override def inputFiles: Array[String] = {
    cdcExtractor.extractCDCFileSplits().asScala.map { fileGroupSplit =>
      val fileGroupId = fileGroupSplit._1
      new Path(fileGroupId.getPartitionPath, fileGroupId.getFileId).toString
    }.toArray
  }
}
