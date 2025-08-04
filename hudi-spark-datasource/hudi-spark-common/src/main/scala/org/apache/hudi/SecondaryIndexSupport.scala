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

import org.apache.hudi.RecordLevelIndexSupport.{filterQueryWithRecordKey, getPrunedStoragePaths}
import org.apache.hudi.SecondaryIndexSupport.filterQueriesWithSecondaryKey
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.HoodieDataUtils
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.{mutable, JavaConverters}
import scala.collection.JavaConverters._

class SecondaryIndexSupport(spark: SparkSession,
                            metadataConfig: HoodieMetadataConfig,
                            metaClient: HoodieTableMetaClient) extends RecordLevelIndexSupport(spark, metadataConfig, metaClient) {
  override def getIndexName: String = SecondaryIndexSupport.INDEX_NAME

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    val secondaryKeyConfigOpt = getSecondaryKeyConfig(queryReferencedColumns, metaClient)
    if (secondaryKeyConfigOpt.isEmpty) {
      Option.empty
    }
    lazy val (_, secondaryKeys) = if (isIndexAvailable) filterQueriesWithSecondaryKey(queryFilters, secondaryKeyConfigOpt.map(_._2)) else (List.empty, List.empty)
    if (isIndexAvailable && queryFilters.nonEmpty && secondaryKeys.nonEmpty) {
      val prunedStoragePaths = getPrunedStoragePaths(prunedPartitionsAndFileSlices, fileIndex)
      Some(getCandidateFilesFromSecondaryIndex(prunedStoragePaths, secondaryKeys, secondaryKeyConfigOpt.get._1))
    } else {
      Option.empty
    }
  }

  override def invalidateCaches(): Unit = {
    // no caches for this index type, do nothing
  }

  /**
   * Return true if metadata table is enabled and expression index metadata partition is available.
   */
  override def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled && metaClient.getIndexMetadata.isPresent && !metaClient.getIndexMetadata.get().getIndexDefinitions.isEmpty
  }

  /**
   * Returns the list of candidate files which store the provided record keys based on Metadata Table Secondary Index
   * and Metadata Table Record Index.
   *
   * @param secondaryKeys - List of secondary keys.
   * @return Sequence of file names which need to be queried
   */
  private def getCandidateFilesFromSecondaryIndex(allFiles: Seq[StoragePath], secondaryKeys: List[String], secondaryIndexName: String): Set[String] = {
    val secondaryIndexData = metadataTable.readSecondaryIndexKeysAndLocation(
        HoodieListData.eager(JavaConverters.seqAsJavaListConverter(secondaryKeys).asJava), secondaryIndexName)
    try {
      val recordKeyLocationsMap = HoodieDataUtils.dedupeAndCollectAsMap(secondaryIndexData)
      val fileIdToPartitionMap: mutable.Map[String, String] = mutable.Map.empty
      val candidateFiles: mutable.Set[String] = mutable.Set.empty
      recordKeyLocationsMap.values().forEach(location => fileIdToPartitionMap.put(location.getFileId, location.getPartitionPath))

      for (file <- allFiles) {
        val fileId = FSUtils.getFileIdFromFilePath(file)
        val partitionOpt = fileIdToPartitionMap.get(fileId)
        if (partitionOpt.isDefined) {
          candidateFiles += file.getName
        }
      }
      candidateFiles.toSet
    } finally {
      // Clean up the RDD to avoid memory leaks
      secondaryIndexData.unpersistWithDependencies()
    }
  }

  /**
   * Returns the configured secondary key for the table
   * TODO: [HUDI-8302] Handle multiple secondary indexes (similar to expression index)
   */
  private def getSecondaryKeyConfig(queryReferencedColumns: Seq[String],
                                    metaClient: HoodieTableMetaClient): Option[(String, String)] = {
    val indexDefinitions = metaClient.getIndexMetadata.get.getIndexDefinitions.asScala
    indexDefinitions.values
      .find(indexDef => indexDef.getIndexType.equals(PARTITION_NAME_SECONDARY_INDEX) &&
        queryReferencedColumns.contains(indexDef.getSourceFields.get(0)))
      .map(indexDef => (indexDef.getIndexName, indexDef.getSourceFields.get(0)))
  }
}

object SecondaryIndexSupport {
  val INDEX_NAME = "secondary_index"

  def filterQueriesWithSecondaryKey(queryFilters: Seq[Expression],
                                    secondaryKeyConfigOpt: Option[String]): (List[Expression], List[String]) = {
    var secondaryKeyQueries: List[Expression] = List.empty
    var secondaryKeys: List[String] = List.empty
    if (secondaryKeyConfigOpt.isDefined) {
      for (query <- queryFilters) {
        filterQueryWithRecordKey(query, secondaryKeyConfigOpt).foreach({
          case (exp: Expression, recKeys: List[String]) =>
            secondaryKeys = secondaryKeys ++ recKeys
            secondaryKeyQueries = secondaryKeyQueries :+ exp
        })
      }
    }

    Tuple2.apply(secondaryKeyQueries, secondaryKeys)
  }

}
