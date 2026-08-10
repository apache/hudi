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

import org.apache.hudi.RecordLevelIndexSupport.{getPrunedStoragePaths, MAX_PARTITIONS}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.core.read.BaseHoodieTableFileIndex
import org.apache.hudi.index.PartitionedRecordIndexFileGroupLookupFunction
import org.apache.hudi.metadata.{BucketizedMetadataTableFileGroupIndexParser, HoodieTableMetadataUtil, MetadataPartitionType}

import org.apache.spark.Partitioner
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Data skipping based on a partitioned Record Level Index (RLI), where the file groups indexing the
 * record keys are sharded per data-table partition. The metadata lookup must therefore be scoped to each
 * candidate partition.
 *
 * The candidate partitions are derived from the already pruned partitions. Because each partition requires a
 * separate metadata table lookup, if the number of candidate partitions exceeds {@code MAX_PARTITIONS} the
 * record index filtering is skipped (returns [[None]]) and the reader falls back to other indexes.
 */
class PartitionedRecordLevelIndexSupport(spark: SparkSession,
                                         metadataConfig: HoodieMetadataConfig,
                                         metaClient: HoodieTableMetaClient)
  extends RecordLevelIndexSupport(spark, metadataConfig, metaClient) with Logging {

  override protected def lookupCandidateFilesForRecordKeys(fileIndex: HoodieFileIndex,
                                                           prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                                           recordKeys: List[String]): Option[Set[String]] = {
    val partitions = prunedPartitionsAndFileSlices.flatMap { case (partitionPathOpt, _) =>
      partitionPathOpt.map(_.getPath)
    }.toSet
    if (partitions.isEmpty) {
      // Cannot resolve candidate partitions, fall back to other indexes rather than over-pruning
      Option.empty
    } else if (partitions.size > MAX_PARTITIONS) {
      logInfo(s"The number of candidate partitions ${partitions.size} exceeds the partitioned record level index " +
        s"lookup threshold $MAX_PARTITIONS. Skipping record level index pruning.")
      Option.empty
    } else {
      lookupRecordKeys(partitions, recordKeys) match {
        case Some(fileIdToPartitionMap) =>
          val prunedStoragePaths = getPrunedStoragePaths(prunedPartitionsAndFileSlices, fileIndex)
          Option.apply(filterCandidateFiles(prunedStoragePaths, fileIdToPartitionMap))
        case None =>
          // None of the candidate partitions are indexed by the partitioned RLI (e.g. partitions
          // not yet indexed), so we cannot determine the matching files. Fall back to other indexes
          // rather than over-pruning to an empty candidate set.
          Option.empty
      }
    }
  }

  private def lookupRecordKeys(partitions: Set[String],
                               recordKeys: List[String]): Option[mutable.Map[String, String]] = {
    val fileGroups = metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX)
    val fileGroupCountPerDataPartition = fileGroups.asScala
      .filter { case (partition, _) => partitions.contains(partition) }
      .map { case (partition, slices) => partition -> Integer.valueOf(slices.size()) }
      .toMap
    if (fileGroupCountPerDataPartition.isEmpty) {
      None
    } else {
      val numFileGroups = BucketizedMetadataTableFileGroupIndexParser.calculateNumberOfFileGroups(fileGroupCountPerDataPartition.asJava)
      val partitionOffsetIndexes = BucketizedMetadataTableFileGroupIndexParser.generatePartitionToBaseIndexOffsets(fileGroupCountPerDataPartition.asJava).asScala
      // Like SparkMetadataTableRecordLevelIndex#lookupRecords: build (partition, recordKey)
      // pairs, key by the global partitioned-RLI shard id, and let each Spark partition look up
      // one record-index shard through PartitionedRecordIndexFileGroupLookupFunction.
      val partitionRecordKeys = fileGroupCountPerDataPartition.keys.toSeq.flatMap { partition =>
        recordKeys.map { recordKey => Pair.of(partition, recordKey) }
      }
      val partitionedKeyRDD = spark.sparkContext.parallelize(partitionRecordKeys, numFileGroups)
        .keyBy { partitionRecordKey =>
          val partition = partitionRecordKey.getLeft
          partitionOffsetIndexes(partition).intValue() +
            HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(partitionRecordKey.getRight, fileGroupCountPerDataPartition(partition).intValue())
        }
        .partitionBy(new PartitionIdPassthrough(numFileGroups))
        .map(_._2)
        .toJavaRDD()
      ValidationUtils.checkState(partitionedKeyRDD.getNumPartitions <= numFileGroups)
      val fileIdToPartitionMap = partitionedKeyRDD.mapPartitionsToPair(new PartitionedRecordIndexFileGroupLookupFunction(metadataTable))
        .collect()
        .asScala
        .foldLeft(mutable.Map.empty[String, String]) { (acc, location) =>
          acc.put(location._2.getFileId, location._2.getPartitionPath)
          acc
        }
      Some(fileIdToPartitionMap)
    }
  }
}

private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
