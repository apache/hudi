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

import org.apache.hudi.RecordLevelIndexSupport.filterQueryWithRecordKey
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.util.JFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

class BloomFiltersIndexSupport(spark: SparkSession,
                               metadataConfig: HoodieMetadataConfig,
                               metaClient: HoodieTableMetaClient) extends RecordLevelIndexSupport(spark, metadataConfig, metaClient) {

  override def getCandidateFiles(allFiles: Seq[StoragePath], recordKeys: List[String]): Set[String] = {
    val fileToBloomFilterMap = allFiles.map { file =>
      val relativePartitionPath = FSUtils.getRelativePartitionPath(metaClient.getBasePathV2, file)
      val fileName = FSUtils.getFileName(file.getName, relativePartitionPath)
      val bloomFilter = metadataTable.getBloomFilter(relativePartitionPath, fileName)
      file -> bloomFilter
    }.toMap

    recordKeys.flatMap { recordKey =>
      fileToBloomFilterMap.filter { case (_, bloomFilter) =>
        // If bloom filter is empty, we assume conservatively that the file might contain the record key
        bloomFilter.isEmpty || bloomFilter.get.mightContain(recordKey)
      }.keys
    }.map(_.getName).toSet
  }

  override def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled && metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS)
  }

  override def filterQueriesWithRecordKey(queryFilters: Seq[Expression]): (List[Expression], List[String]) = {
    var recordKeyQueries: List[Expression] = List.empty
    var recordKeys: List[String] = List.empty
    for (query <- queryFilters) {
      val recordKeyOpt = getRecordKeyConfig
      filterQueryWithRecordKey(query, recordKeyOpt).foreach({
        case (exp: Expression, recKeys: List[String]) =>
          recordKeys = recordKeys ++ recKeys
          recordKeyQueries = recordKeyQueries :+ exp
      })
    }

    Tuple2.apply(recordKeyQueries, recordKeys)
  }

  private def getRecordKeyConfig: Option[String] = {
    val recordKeysOpt: org.apache.hudi.common.util.Option[Array[String]] = metaClient.getTableConfig.getRecordKeyFields
    val recordKeyOpt = recordKeysOpt.map[String](JFunction.toJavaFunction[Array[String], String](arr =>
      if (arr.length == 1) {
        arr(0)
      } else {
        null
      }))
    Option.apply(recordKeyOpt.orElse(null))
  }

}


object BloomFiltersIndexSupport {
  val INDEX_NAME = "RECORD_LEVEL"
}
