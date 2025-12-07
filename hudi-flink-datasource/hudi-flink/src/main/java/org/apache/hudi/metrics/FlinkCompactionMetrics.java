/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sink.compact.CompactOperator;
import org.apache.hudi.sink.compact.CompactionPlanOperator;

import lombok.Setter;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;

/**
 * Metrics for flink compaction.
 */
public class FlinkCompactionMetrics extends FlinkWriteMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkCompactionMetrics.class);

  /**
   * Key for compaction timer.
   */
  private static final String COMPACTION_KEY = "compaction";

  /**
   * Number of pending compaction instants.
   *
   * @see CompactionPlanOperator
   */
  @Setter
  private int pendingCompactionCount;

  /**
   * Duration between the earliest pending compaction instant time and now in seconds.
   *
   *  @see CompactionPlanOperator
   */
  private long compactionDelay;

  /**
   * Cost for consuming a compaction operation in milliseconds.
   *
   * @see CompactOperator
   */
  private long compactionCost;

  /**
   * Flag saying whether the compaction is completed or been rolled back.
   *
   */
  private long compactionStateSignal;

  public FlinkCompactionMetrics(MetricGroup metricGroup) {
    super(metricGroup, HoodieTimeline.COMPACTION_ACTION);
  }

  @Override
  public void registerMetrics() {
    super.registerMetrics();
    metricGroup.gauge(getMetricsName(actionType, "pendingCompactionCount"), () -> pendingCompactionCount);
    metricGroup.gauge(getMetricsName(actionType, "compactionDelay"), () -> compactionDelay);
    metricGroup.gauge(getMetricsName(actionType, "compactionCost"), () -> compactionCost);
    metricGroup.gauge(getMetricsName(actionType, "compactionStateSignal"), () -> compactionStateSignal);
  }

  public void setFirstPendingCompactionInstant(Option<HoodieInstant> firstPendingCompactionInstant) {
    try {
      if (!firstPendingCompactionInstant.isPresent()) {
        this.compactionDelay = 0L;
      } else {
        Instant start = HoodieInstantTimeGenerator.parseDateFromInstantTime(firstPendingCompactionInstant.get().requestedTime()).toInstant();
        this.compactionDelay = Duration.between(start, Instant.now()).getSeconds();
      }
    } catch (ParseException e) {
      LOG.warn("Invalid input compaction instant: {}", firstPendingCompactionInstant);
    }
  }

  public void startCompaction() {
    startTimer(COMPACTION_KEY);
  }

  public void endCompaction() {
    this.compactionCost = stopTimer(COMPACTION_KEY);
  }

  public void markCompactionCompleted() {
    this.compactionStateSignal = 0L;
  }

  public void markCompactionRolledBack() {
    this.compactionStateSignal = 1L;
  }

}
