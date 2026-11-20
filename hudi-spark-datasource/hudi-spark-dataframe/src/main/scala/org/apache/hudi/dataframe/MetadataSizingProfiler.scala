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
 * Routing plan for insert records: per partition path, the routing-bucket slots that pad
 * existing small file groups (update buckets targeting those file groups) followed by
 * new-file buckets. Insert rows hash across the slots of their partition.
 */
case class InsertRoutingPlan(smallFileSlotsByPartition: Map[String, Array[String]],
                             newFileBuckets: Int) extends Serializable

/**
 * Plans insert routing from table metadata only, with no pass over the incoming data:
 * average record size comes from commit metadata (the same estimate the RDD-path
 * UpsertPartitioner uses) and small file groups come from the file-system view. Small
 * files with remaining capacity become padding slots so inserts fill them up through the
 * merge path instead of opening new file groups.
 */
object MetadataSizingProfiler extends Logging {

  def plan(table: HoodieTable[_, _, _, _],
           writeConfig: HoodieWriteConfig,
           newFileBuckets: Int,
           mergeOnRead: Boolean,
           paddingBucketPrefix: String): InsertRoutingPlan = {
    val smallFileLimit = writeConfig.getParquetSmallFileLimit
    val maxFileSize = writeConfig.getParquetMaxFileSize
    val completedCommits = table.getMetaClient.getActiveTimeline
      .getCommitsTimeline.filterCompletedInstants()
    if (smallFileLimit <= 0 || completedCommits.countInstants() == 0) {
      InsertRoutingPlan(Map.empty, newFileBuckets)
    } else {
      val avgRecordSize = averageRecordSize(table, writeConfig)
      val fsView = table.getHoodieView
      fsView.loadAllPartitions()
      val candidates: Iterator[(String, String)] = if (mergeOnRead) {
        // Slice-based sizing, and never pad a slice that has log files: the write-path merge
        // handle reads only the base file, so padding a logged slice would drop its log records.
        // Pending-compaction slices are excluded so the rewrite cannot race the compactor.
        val latestCompleted = completedCommits.lastInstant().get().requestedTime()
        table.getTableMetadata.getAllPartitionPaths.asScala.iterator.flatMap { partition =>
          fsView.getLatestFileSlicesBeforeOrOn(partition, latestCompleted, false).iterator().asScala
            .filter(slice => slice.getLogFiles.count() == 0
              && slice.getBaseFile.isPresent
              && slice.getBaseFile.get().getFileSize > 0
              && slice.getTotalFileSizeAsParquetFormat(writeConfig) < smallFileLimit
              && (maxFileSize - slice.getTotalFileSizeAsParquetFormat(writeConfig)) / avgRecordSize > 0)
            .map(slice => (partition, paddingBucketPrefix + slice.getFileId))
        }
      } else {
        val basePath = new StoragePath(writeConfig.getBasePath)
        fsView.getLatestBaseFiles.iterator().asScala
          .filter(baseFile => baseFile.getFileSize > 0
            && baseFile.getFileSize < smallFileLimit
            && (maxFileSize - baseFile.getFileSize) / avgRecordSize > 0)
          .map(baseFile => (
            FSUtils.getRelativePartitionPath(basePath, baseFile.getStoragePath.getParent),
            paddingBucketPrefix + baseFile.getFileId))
      }
      val smallFileSlots = candidates.toSeq
        .groupBy(_._1)
        .map { case (partition, slots) => (partition, slots.map(_._2).toArray) }
      log.info(s"Insert routing plan: ${smallFileSlots.values.map(_.length).sum} small-file "
        + s"padding slots across ${smallFileSlots.size} partitions, $newFileBuckets new-file "
        + s"buckets per partition, avg record size $avgRecordSize")
      InsertRoutingPlan(smallFileSlots, newFileBuckets)
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
