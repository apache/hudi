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

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.hudi.common.util.VisibleForTesting;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Metrics for flink bucket assign functions (BucketAssignFunction, MinibatchBucketAssignFunction,
 * DynamicBucketAssignFunction). Tracks record buffering time and RLI shard assignment distribution.
 */
public class FlinkBucketAssignMetrics extends HoodieFlinkMetrics {
  private static final int HISTOGRAM_WINDOW_SIZE = 100;
  private static final String RECORD_BUFFERING_KEY = "record_buffering";

  /**
   * Time records spend buffered before being processed, in milliseconds.
   * Only populated by MinibatchBucketAssignFunction.
   */
  private final Histogram recordBufferingTime;

  /**
   * Number of RLI file group shards assigned to this bucket assign task.
   * Set once during open() when global RLI is active; remains -1 otherwise.
   * Compare across task subtasks to detect skew in shard distribution.
   */
  private final AtomicInteger numShardsAssigned = new AtomicInteger(-1);

  public FlinkBucketAssignMetrics(MetricGroup metricGroup) {
    super(metricGroup);
    this.recordBufferingTime = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
  }

  @Override
  public void registerMetrics() {
    metricGroup.histogram("recordBufferingTime", recordBufferingTime);
    metricGroup.gauge("numShardsAssigned", numShardsAssigned::get);
  }

  /**
   * Sets the number of RLI shards owned by this task. Call once during {@code open()} when global
   * RLI is active; the gauge value changes from the sentinel -1 to {@code count}.
   */
  public void setNumShardsAssigned(int count) {
    numShardsAssigned.set(count);
  }

  public void startRecordBuffering() {
    startTimer(RECORD_BUFFERING_KEY);
  }

  public void endRecordBuffering() {
    recordBufferingTime.update(stopTimer(RECORD_BUFFERING_KEY));
  }

  @VisibleForTesting
  public long getRecordBufferingCount() {
    return recordBufferingTime.getCount();
  }

  @VisibleForTesting
  public int getNumShardsAssigned() {
    return numShardsAssigned.get();
  }
}
