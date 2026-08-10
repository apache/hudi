/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.RecordLevelIndexSupport.getPrunedStoragePaths
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.core.read.BaseHoodieTableFileIndex

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * Data skipping based on a global Record Level Index (RLI), where a single set of file groups indexes
 * the record keys across the whole table. All record keys are resolved with one metadata table lookup.
 */
class GlobalRecordLevelIndexSupport(spark: SparkSession,
                                    metadataConfig: HoodieMetadataConfig,
                                    metaClient: HoodieTableMetaClient)
  extends RecordLevelIndexSupport(spark, metadataConfig, metaClient) {

  override protected def lookupCandidateFilesForRecordKeys(fileIndex: HoodieFileIndex,
                                                           prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                                           recordKeys: List[String]): Option[Set[String]] = {
    val prunedStoragePaths = getPrunedStoragePaths(prunedPartitionsAndFileSlices, fileIndex)
    val recordIndexData = metadataTable.readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys.asJava))
    try {
      val fileIdToPartitionMap = collectFileIdToPartitionMap(recordIndexData)
      Option.apply(filterCandidateFiles(prunedStoragePaths, fileIdToPartitionMap))
    } finally {
      // Clean up the RDD to avoid memory leaks
      recordIndexData.unpersistWithDependencies()
    }
  }
}
