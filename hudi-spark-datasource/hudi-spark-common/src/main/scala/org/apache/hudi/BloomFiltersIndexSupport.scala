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
import org.apache.hudi.metadata.HoodieTableMetadataUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

class BloomFiltersIndexSupport(spark: SparkSession,
                               metadataConfig: HoodieMetadataConfig,
                               metaClient: HoodieTableMetaClient) extends SparkBaseIndexSupport(spark, metadataConfig, metaClient) {

  override def getIndexName: String = BloomFiltersIndexSupport.INDEX_NAME

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    lazy val (_, recordKeys) = filterQueriesWithRecordKey(queryFilters)
    if (recordKeys.nonEmpty) {
      val prunedPartitionAndFileNames = getPrunedPartitionAndFileNames(prunedPartitionsAndFileSlices)
      Option.apply(getCandidateFilesForSecondaryKeys(prunedPartitionAndFileNames, recordKeys))
    } else {
      Option.empty
    }
  }

  private def getPrunedPartitionAndFileNames(prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])]): Seq[(String, String)] = {
    prunedPartitionsAndFileSlices
      .flatMap {
        case (Some(partitionPath), fileSlices) => fileSlices.map(fileSlice => (partitionPath.getPath, fileSlice))
        case (None, fileSlices) => fileSlices.map(fileSlice => ("", fileSlice))
      }
      .flatMap { case (partitionPath, fileSlice) =>
        val baseFileOption = Option(fileSlice.getBaseFile.orElse(null))
        baseFileOption.map(baseFile => (partitionPath, baseFile.getFileName))
      }
  }

  private def getCandidateFilesForSecondaryKeys(prunedPartitionAndFileNames: Seq[(String, String)], recordKeys: List[String]): Set[String] = {
    val candidateFiles = prunedPartitionAndFileNames.filter { partitionAndFileName =>
      val bloomFilterOpt = toScalaOption(metadataTable.getBloomFilter(partitionAndFileName._1, partitionAndFileName._2))
      bloomFilterOpt match {
        case Some(bloomFilter) =>
          recordKeys.exists(bloomFilter.mightContain)
        case None =>
          true // If bloom filter is empty or undefined, assume the file might contain the record key
      }
    }.map(_._2).toSet

    candidateFiles
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
}
