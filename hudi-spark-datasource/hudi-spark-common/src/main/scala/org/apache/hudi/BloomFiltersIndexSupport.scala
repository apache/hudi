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
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.core.read.BaseHoodieTableFileIndex
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.metadata.HoodieTableMetadataUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.JavaConverters._

class BloomFiltersIndexSupport(spark: SparkSession,
                               metadataConfig: HoodieMetadataConfig,
                               metaClient: HoodieTableMetaClient) extends SparkBaseIndexSupport(spark, metadataConfig, metaClient) {

  // Like bucket pruning, trust the declared reader index type to match the writer. These Spark
  // indexes write new keys to base files. A custom class overrides the type and has no such guarantee.
  // Do not infer the writer's default SIMPLE index when the reader has not declared a type.
  private val canPruneLogSlices = {
    val indexType = metadataConfig.getStringOrDefault(HoodieIndexConfig.INDEX_TYPE, "")
    metadataConfig.getStringOrDefault(HoodieIndexConfig.INDEX_CLASS_NAME, "").isEmpty &&
      BloomFiltersIndexSupport.BASE_BLOOM_COVERED_INDEX_TYPES.exists(_.name().equalsIgnoreCase(indexType))
  }

  private def mustRetainSlice(fileSlice: FileSlice): Boolean =
    fileSlice.hasLogFiles && (fileSlice.getBaseFile.isEmpty || !canPruneLogSlices)

  override def getIndexName: String = BloomFiltersIndexSupport.INDEX_NAME

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    val (_, recordKeys) = filterQueriesWithRecordKey(queryFilters)
    if (recordKeys.isEmpty) {
      None
    } else {
      val candidateFileNames = prunedPartitionsAndFileSlices.flatMap { case (partition, fileSlices) =>
        val partitionPath = partition.map(_.getPath).getOrElse("")
        fileSlices.flatMap { slice =>
          if (mustRetainSlice(slice)) {
            // A log filename retains the entire slice without querying its base bloom.
            slice.getLogFiles.iterator().asScala.map(_.getFileName).toSeq
          } else {
            toScalaOption(slice.getBaseFile).filter { baseFile =>
              // A missing bloom cannot rule out a match.
              toScalaOption(metadataTable.getBloomFilter(partitionPath, baseFile.getFileName))
                .forall(bloom => recordKeys.exists(bloom.mightContain))
            }.map(_.getFileName).toSeq
          }
        }
      }.toSet
      Some(candidateFileNames)
    }
  }

  override def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled && metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS)
  }

  override def invalidateCaches(): Unit = {
    // no caches for this index type, do nothing
  }

}


object BloomFiltersIndexSupport {
  val INDEX_NAME = "BLOOM_FILTERS"

  // These Spark writer indexes write new keys to base files, so base blooms also cover keys in update logs.
  // canIndexLogFiles() == false is not sufficient: SIMPLE bucket tagging assigns new keys to existing
  // file groups, routing them through handleUpdate() into logs despite canIndexLogFiles() returning false.
  // Keep an explicit whitelist; other index types require conservative retention of slices with logs.
  private val BASE_BLOOM_COVERED_INDEX_TYPES = Set(
    IndexType.SIMPLE, IndexType.GLOBAL_SIMPLE, IndexType.BLOOM, IndexType.GLOBAL_BLOOM,
    IndexType.RECORD_INDEX, IndexType.RECORD_LEVEL_INDEX, IndexType.GLOBAL_RECORD_LEVEL_INDEX)
}
