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
import org.apache.hudi.RecordLevelIndexSupport.filterQueryWithRecordKey
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.storage.StoragePath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

class BloomFiltersIndexSupport(spark: SparkSession,
                               metadataConfig: HoodieMetadataConfig,
                               metaClient: HoodieTableMetaClient) extends RecordLevelIndexSupport(spark, metadataConfig, metaClient) {

  override def getIndexName: String = BloomFiltersIndexSupport.INDEX_NAME

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    lazy val (_, recordKeys) = filterQueriesWithRecordKey(queryFilters)
    if (recordKeys.nonEmpty) {
      val allFiles = getPrunedFileNames(prunedPartitionsAndFileSlices).map(new StoragePath(_) ).toSeq
      Option.apply(getCandidateFilesForRecordKeys(allFiles, recordKeys))
    } else {
      Option.empty
    }
  }

  override def getCandidateFilesForRecordKeys(allFiles: Seq[StoragePath], recordKeys: List[String]): Set[String] = {
    val candidateFiles = allFiles.filter { file =>
      val relativePartitionPath = FSUtils.getRelativePartitionPath(metaClient.getBasePathV2, file)
      val fileName = FSUtils.getFileName(file.getName, relativePartitionPath)
      val bloomFilterOpt = toScalaOption(metadataTable.getBloomFilter(relativePartitionPath, fileName))

      bloomFilterOpt match {
        case Some(bloomFilter) =>
          recordKeys.exists(bloomFilter.mightContain)
        case None =>
          true // If bloom filter is empty or undefined, assume the file might contain the record key
      }
    }.map(_.getName).toSet

    candidateFiles
  }

  override def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled && metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS)
  }

  override def filterQueriesWithRecordKey(queryFilters: Seq[Expression]): (List[Expression], List[String]) = {
    val recordKeyOpt = getRecordKeyConfig
    recordKeyOpt.map { recordKey =>
      queryFilters.foldLeft((List.empty[Expression], List.empty[String])) {
        case ((accQueries, accKeys), query) =>
          filterQueryWithRecordKey(query, Some(recordKey)).foreach {
            case (exp: Expression, recKeys: List[String]) =>
              (accQueries :+ exp, accKeys ++ recKeys)
          }
          (accQueries, accKeys)
      }
    }.getOrElse((List.empty[Expression], List.empty[String]))
  }

}


object BloomFiltersIndexSupport {
  val INDEX_NAME = "BLOOM_FILTERS"
}
