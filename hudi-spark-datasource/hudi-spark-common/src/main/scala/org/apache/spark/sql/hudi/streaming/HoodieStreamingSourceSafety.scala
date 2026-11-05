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

package org.apache.spark.sql.hudi.streaming

import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.CleanerUtils
import org.apache.hudi.exception.HoodieException

import scala.collection.JavaConverters._

/** Conservative reset and history validation for checkpointed latest-state streaming consumers. */
private[streaming] object HoodieStreamingSourceSafety {
  def validate(metaClient: HoodieTableMetaClient,
               start: String,
               end: String,
               completionTime: Boolean,
               resumed: Boolean): Unit = {
    if (start != HoodieTimeline.INIT_INSTANT_TS) {
      validateRetainedRange(metaClient, start, end, completionTime, resumed)
    }
  }

  private def validateRetainedRange(metaClient: HoodieTableMetaClient,
                                    start: String,
                                    end: String,
                                    completionTime: Boolean,
                                    resumed: Boolean): Unit = {
    val timeline = metaClient.reloadActiveTimeline()
    val completed = timeline.filterCompletedInstants().getInstants.asScala.toSeq
    def offset(instant: HoodieInstant): String = {
      if (completionTime) instant.getCompletionTime else instant.requestedTime()
    }
    def fail(reason: String): Nothing = {
      throw new HoodieException(s"Streaming source rebuild required for ${metaClient.getBasePath}: $reason. "
        + "Stop downstream consumers and rebuild affected downstream tables and checkpoints in dependency order.")
    }

    val checkpointInstant = completed.find(i => offset(i) == start)
    if (resumed && checkpointInstant.isEmpty) {
      fail(s"saved offset $start is absent from the active timeline; history expired or the source table was recreated")
    }
    if (end < start) {
      fail(s"source offset moved backwards from $start to $end")
    }

    val firstWrite = timeline.getWriteTimeline.filterCompletedInstants().firstInstant()
    val requestedStart = checkpointInstant.map(_.requestedTime())
    val unreadWrites = timeline.getCommitsTimeline.filterCompletedInstants().getInstants.asScala
      .filter(i => offset(i) > start && offset(i) <= end).toSeq
    val earliestUnread = unreadWrites.map(_.requestedTime()).sorted.headOption
    val completedByRequestedTime = completed.map(i => i.requestedTime() -> i).toMap
    // Usually only new cleans matter. Retain earlier cleans for late-completing writes,
    // or a user-specified initial offset whose requested-time boundary is unknown.
    timeline.getCleanerTimeline.filterCompletedInstants().getInstants.asScala
      .filter(clean => offset(clean) > start || requestedStart.isEmpty ||
        requestedStart.exists(_ < clean.requestedTime()) || earliestUnread.exists(_ < clean.requestedTime()))
      .foreach { clean =>
        val metadata = CleanerUtils.getCleanerMetadata(metaClient, clean)
        val retained = metadata.getEarliestCommitToRetain
        // File-version cleaning does not expose a retention boundary. Refuse to infer safety.
        if ((retained == null || retained.isEmpty) && metadata.getTotalFilesDeleted > 0 && offset(clean) > start) {
          fail("the cleaner did not record an earliest retained commit; KEEP_LATEST_FILE_VERSIONS "
            + "cannot prove incremental safety after deleting files")
        }
        val boundary = Option(retained).filter(_.nonEmpty)
        val retainedInstant = completedByRequestedTime.get(retained)
        val expired = boundary.exists { floor =>
          requestedStart.exists(_ < floor) || earliestUnread.exists(_ < floor) ||
            (requestedStart.isEmpty && retainedInstant.exists(i => start < offset(i))) ||
            (requestedStart.isEmpty && retainedInstant.isEmpty && firstWrite.isPresent)
        }
        if (expired) {
          fail(s"history after offset $start has been cleaned (earliest retained commit $retained)")
        }
      }

    completed.filter(i => i.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION &&
      offset(i) > start).foreach { instant =>
      val operation = timeline.readReplaceCommitMetadata(instant).getOperationType
      if (operation != WriteOperationType.CLUSTER) {
        fail(s"logical reset $operation at ${instant.requestedTime()} follows offset $start")
      }
    }
  }
}
