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

import org.apache.hudi.DataSourceReadOptions.{INCREMENTAL_READ_SKIP_COMPACT, STREAMING_MAX_COMMITS_PER_TRIGGER}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer
import org.apache.hudi.common.table.timeline.HoodieTimeline

import org.apache.spark.sql.connector.read.streaming.{Offset => ConnectorOffset, ReadLimit, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming.Source

/** Commit boundaries are indivisible: this limits commits, not rows or bytes. */
private[streaming] trait HoodieSourceAdmissionControl extends Source with SupportsTriggerAvailableNow {
  protected def admissionParameters: Map[String, String]
  protected def admissionInitialOffset: HoodieSourceOffset
  protected def completedWriteOffsets: Seq[String]

  private lazy val configuredLimit = HoodieSourceAdmissionControl.readLimit(admissionParameters)

  // Some(None) means AvailableNow started on an empty timeline. Do not admit later arrivals.
  private var availableNowEnd: Option[Option[HoodieSourceOffset]] = None

  override def getDefaultReadLimit: ReadLimit = configuredLimit

  override def prepareForTriggerAvailableNow(): Unit = {
    // Capture 'latest' initial offsets before pinning the end, rather than at the first getBatch.
    admissionInitialOffset
    availableNowEnd = Some(getOffset.map(HoodieSourceOffset(_)))
  }

  override def latestOffset(startOffset: ConnectorOffset, limit: ReadLimit): ConnectorOffset = {
    val start = Option(startOffset).map(offset => HoodieSourceOffset.fromJson(offset.json()))
      .getOrElse(admissionInitialOffset)
    val end = availableNowEnd.getOrElse(getOffset.map(HoodieSourceOffset(_)))
    end.map { latest =>
      limit match {
        case HoodieSourceCommitLimit(count) =>
          val candidates = completedWriteOffsets.sorted.filter(time =>
            time > start.offsetCommitTime && time <= latest.offsetCommitTime)
          // When the backlog fits, also advance past clean/rollback offsets without counting them.
          if (candidates.size > count) HoodieSourceOffset(candidates(count - 1)) else latest
        case all if all == ReadLimit.allAvailable() => latest
        case _ => throw new IllegalArgumentException(s"Unsupported Hudi streaming read limit: $limit")
      }
    }.orNull
  }

  override def reportLatestOffset(): ConnectorOffset = getOffset.orNull
}

private[streaming] case class HoodieSourceCommitLimit(commits: Int) extends ReadLimit

object HoodieSourceAdmissionControl {
  def readLimit(parameters: Map[String, String]): ReadLimit = parameters.get(STREAMING_MAX_COMMITS_PER_TRIGGER.key) match {
    case Some(value) =>
      val count = try {
        value.toInt
      } catch {
        case _: NumberFormatException => throw new IllegalArgumentException(
          s"${STREAMING_MAX_COMMITS_PER_TRIGGER.key} must be a positive integer: $value")
      }
      require(count > 0, s"${STREAMING_MAX_COMMITS_PER_TRIGGER.key} must be positive: $value")
      HoodieSourceCommitLimit(count)
    case None => ReadLimit.allAvailable()
  }

  private[streaming] def readTimeline(metaClient: HoodieTableMetaClient,
                                     parameters: Map[String, String]): HoodieTimeline = {
    // Include archived writes before selecting the boundary. Active-only selection would
    // admit the entire archived gap in the first micro-batch after a stale checkpoint.
    val timeline = metaClient.getArchivedTimeline("", false).getWriteTimeline.filterCompletedInstants()
      .mergeTimeline(metaClient.getActiveTimeline.getWriteTimeline.filterCompletedInstants())
    // Spark incremental reads do not support skipping clustering/insert-overwrite yet.
    IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(metaClient, timeline,
      parameters.getOrElse(INCREMENTAL_READ_SKIP_COMPACT.key,
        INCREMENTAL_READ_SKIP_COMPACT.defaultValue.toString).toBoolean, false, false)
  }
}
