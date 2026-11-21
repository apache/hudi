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

package org.apache.hudi.dataframe

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.timeline.{HoodieTimeline, TimelineLayout}
import org.apache.hudi.common.util.CollectionUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.execution.estimator.RecordSizeEstimatorFactory
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.HoodieTable

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

/**
 * A small file group that takes inserts: the routing bucket of its merge and how many records it
 * still has room for.
 */
case class PaddingSlot(bucket: String, fileId: String, recordBudget: Long)

/**
 * Routing plan for insert records. A partition with a small file group sends its inserts to that
 * file group's bucket, where the write task merges them in up to the record budget and opens new
 * file groups for the rest; other partitions hash their inserts across new-file buckets. New
 * file groups take {@code recordsPerNewFile} records each.
 */
case class InsertRoutingPlan(paddingByPartition: Map[String, PaddingSlot],
                             newFileBuckets: Int,
                             recordsPerNewFile: Long) extends Serializable

/**
 * Plans insert routing from table metadata only, with no pass over the incoming data: the
 * average record size comes from commit metadata, small file groups come from the file-system
 * view, and both feed the same arithmetic the RDD write path sizes inserts with. Counting the
 * incoming records is left to the write task, which enforces the budgets as it consumes rows.
 */
object MetadataSizingProfiler extends Logging {

  def plan(table: HoodieTable[_, _, _, _],
           writeConfig: HoodieWriteConfig,
           newFileBuckets: Int,
           mergeOnRead: Boolean,
           paddingBucketPrefix: String,
           padSmallFiles: Boolean): InsertRoutingPlan = {
    val smallFileLimit = writeConfig.getParquetSmallFileLimit
    val maxFileSize = writeConfig.getParquetMaxFileSize
    val avgRecordSize = averageRecordSize(table, writeConfig)
    val recordsPerNewFile: Long = if (writeConfig.shouldAutoTuneInsertSplits) {
      math.ceil((1.0 * maxFileSize) / avgRecordSize).toLong
    } else {
      writeConfig.getCopyOnWriteInsertSplitSize.toLong
    }
    val completedCommits = table.getMetaClient.getActiveTimeline
      .getCommitsTimeline.filterCompletedInstants()
    if (!padSmallFiles || smallFileLimit <= 0 || completedCommits.countInstants() == 0) {
      InsertRoutingPlan(Map.empty, newFileBuckets, recordsPerNewFile)
    } else {
      val fsView = table.getHoodieView
      fsView.loadAllPartitions()
      // (partition, file id, size) of the small file groups, in the order they are considered.
      val candidates: Iterator[(String, String, Long)] = if (mergeOnRead) {
        // Slice-based sizing, and never pad a slice that has log files: the write-path merge
        // handle reads only the base file, so padding a logged slice would drop its log records.
        // Pending-compaction slices are excluded so the rewrite cannot race the compactor. The
        // smallest slices of a partition come first.
        val latestCompleted = completedCommits.lastInstant().get().requestedTime()
        table.getTableMetadata.getAllPartitionPaths.asScala.iterator.flatMap { partition =>
          fsView.getLatestFileSlicesBeforeOrOn(partition, latestCompleted, false).iterator().asScala
            .filter(slice => slice.getLogFiles.count() == 0
              && slice.getBaseFile.isPresent
              && slice.getBaseFile.get().getFileSize > 0
              && slice.getBaseFile.get().getFileSize < smallFileLimit)
            .toSeq.sortBy(_.getBaseFile.get().getFileSize)
            .map(slice => (partition, slice.getFileId, slice.getTotalFileSizeAsParquetFormat(writeConfig)))
        }
      } else {
        val basePath = new StoragePath(writeConfig.getBasePath)
        fsView.getLatestBaseFiles.iterator().asScala
          .filter(baseFile => baseFile.getFileSize > 0 && baseFile.getFileSize < smallFileLimit)
          .map(baseFile => (
            FSUtils.getRelativePartitionPath(basePath, baseFile.getStoragePath.getParent),
            baseFile.getFileId, baseFile.getFileSize))
      }
      // A file group that a pending clustering is rewriting takes no new records.
      val pendingClustering = fsView.getFileGroupsInPendingClustering.iterator().asScala
        .map(_.getKey).map(fileGroup => (fileGroup.getPartitionPath, fileGroup.getFileId)).toSet
      val padding = candidates
        .filterNot { case (partition, fileId, _) => pendingClustering.contains((partition, fileId)) }
        .map { case (partition, fileId, size) =>
          partition -> PaddingSlot(paddingBucketPrefix + fileId, fileId, (maxFileSize - size) / avgRecordSize)
        }
        .filter(_._2.recordBudget > 0)
        .toSeq
        // The first small file group of a partition with room takes the inserts.
        .groupBy(_._1).map { case (partition, slots) => partition -> slots.head._2 }
      log.info(s"Insert routing plan: ${padding.size} partitions pad a small file group, "
        + s"$newFileBuckets new-file buckets per partition, $recordsPerNewFile records per new file, "
        + s"avg record size $avgRecordSize")
      InsertRoutingPlan(padding, newFileBuckets, recordsPerNewFile)
    }
  }

  private def averageRecordSize(table: HoodieTable[_, _, _, _], writeConfig: HoodieWriteConfig): Long = {
    val layout = TimelineLayout.fromVersion(table.getActiveTimeline.getTimelineLayoutVersion)
    RecordSizeEstimatorFactory.createRecordSizeEstimator(writeConfig)
      .averageBytesPerRecord(
        table.getMetaClient.getActiveTimeline
          .getTimelineOfActions(CollectionUtils.createSet(
            HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION))
          .filterCompletedInstants(),
        layout.getCommitMetadataSerDe)
  }
}
