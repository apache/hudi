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

import org.apache.hudi.common.util.VisibleForTesting;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;

/**
 * Metrics for flink bucket assign functions (BucketAssignFunction, MinibatchBucketAssignFunction,
 * DynamicBucketAssignFunction). Tracks record buffering time.
 */
public class FlinkBucketAssignMetrics extends HoodieFlinkMetrics {
  private static final int HISTOGRAM_WINDOW_SIZE = 100;
  private static final String RECORD_BUFFERING_KEY = "record_buffering";

  /**
   * Time records spend buffered before being processed, in milliseconds.
   * Only populated by MinibatchBucketAssignFunction.
   */
  private final Histogram recordBufferingTime;

  public FlinkBucketAssignMetrics(MetricGroup metricGroup) {
    super(metricGroup);
    this.recordBufferingTime = new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(HISTOGRAM_WINDOW_SIZE)));
  }

  @Override
  public void registerMetrics() {
    metricGroup.histogram("recordBufferingTime", recordBufferingTime);
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
}
