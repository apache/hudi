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

public class SampleEstimator<R> implements SizeEstimator<R> {

  public static final int DEFAULT_TRIGGER_SAMPLE_THRESHOLD = 100;

  public static final double DEFAULT_SAMPLE_WRIGHT = 0.1;

  private final int triggerSampleThreshold;

  private final double sampleWeight;

  private final SizeEstimator<R> underlyingEstimator;

  private long perEstimatedSize;

  private int estimatedCount;

  private int sampleCount;

  public SampleEstimator(SizeEstimator<R> underlyingEstimator) {
    this(underlyingEstimator, DEFAULT_TRIGGER_SAMPLE_THRESHOLD);
  }

  public SampleEstimator(SizeEstimator<R> underlyingEstimator, int triggerSampleThreshold) {
    this(underlyingEstimator, triggerSampleThreshold, DEFAULT_SAMPLE_WRIGHT);
  }

  // TODO: configure the triggerSampleThreshold and sampleWeight in the write config
  public SampleEstimator(SizeEstimator<R> underlyingEstimator, int triggerSampleThreshold, double sampleWeight) {
    this.triggerSampleThreshold = triggerSampleThreshold;
    this.sampleWeight = sampleWeight;
    this.underlyingEstimator = underlyingEstimator;
    this.perEstimatedSize = 0;
    this.estimatedCount = 0;
    this.sampleCount = 0;
  }

  public SampleEstimator<R> newInstance() {
    return new SampleEstimator<>(underlyingEstimator, triggerSampleThreshold, sampleWeight);
  }

  public long getPerEstimatedSize() {
    return perEstimatedSize;
  }

  public int getSampleCount() {
    return sampleCount;
  }

  /**
   * Estimate the size of the record with sampling.
   * @param r the record
   * @return the estimated size based on the sampling
   */
  @Override
  public long sizeEstimate(R r) {
    if (estimatedCount == 0) {
      // First sample, directly use the estimator's result
      perEstimatedSize = underlyingEstimator.sizeEstimate(r);
      estimatedCount++;
      return perEstimatedSize;
    }
    if (estimatedCount++ % triggerSampleThreshold == 0) {
      // Trigger sample
      long sampleSize = underlyingEstimator.sizeEstimate(r);
      perEstimatedSize = (long) (perEstimatedSize * (1 - sampleWeight) + sampleSize * sampleWeight);
      sampleCount++;
    }
    return perEstimatedSize;
  }
}
