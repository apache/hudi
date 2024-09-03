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

package org.apache.hudi.common.util;

import java.util.function.Supplier;

public class SampleEstimator {

  public static final int DEFAULT_TRIGGER_SAMPLE_THRESHOLD = 100;

  public static final double DEFAULT_SAMPLE_WRIGHT = 0.1;

  private final int triggerSampleThreshold;

  private final double sampleWeight;

  private long perEstimatedSize;

  private int estimatedCount;

  private int sampleCount;

  public SampleEstimator() {
    this(DEFAULT_TRIGGER_SAMPLE_THRESHOLD, DEFAULT_SAMPLE_WRIGHT);
  }

  public SampleEstimator(int triggerSampleThreshold) {
    this(triggerSampleThreshold, DEFAULT_SAMPLE_WRIGHT);
  }

  // TODO: configure the triggerSampleThreshold and sampleWeight in the write config
  public SampleEstimator(int triggerSampleThreshold, double sampleWeight) {
    this.triggerSampleThreshold = triggerSampleThreshold;
    this.sampleWeight = sampleWeight;
    this.perEstimatedSize = 0;
    this.estimatedCount = 0;
    this.sampleCount = 0;
  }

  public long getPerEstimatedSize() {
    return perEstimatedSize;
  }

  public int getSampleCount() {
    return sampleCount;
  }

  /**
   * Sample the estimator's result.
   * @param estimator the estimator
   * @return the estimated size
   */
  public long estimate(Supplier<Long> estimator) {
    if (estimatedCount == 0) {
      // First sample, directly use the estimator's result
      perEstimatedSize = estimator.get();
      estimatedCount++;
      return perEstimatedSize;
    }
    if (estimatedCount++ % triggerSampleThreshold == 0) {
      // Trigger sample
      long sampleSize = estimator.get();
      perEstimatedSize = (long) (perEstimatedSize * (1 - sampleWeight) + sampleSize * sampleWeight);
      sampleCount++;
    }
    return perEstimatedSize;
  }
}
