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

package org.apache.hudi.index.bucket.partition;

import org.apache.hudi.common.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * A utility class that encapsulates the logic for determining the number of buckets
 * for a given partition path, supporting both fixed bucket numbers and partition-specific
 * bucket numbers.
 */
public class NumBucketsFunction implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(NumBucketsFunction.class);

  /**
   * The default number of buckets to use when partition-specific buckets are not configured.
   */
  private final int defaultBucketNumber;

  /**
   * Flag indicating whether partition-level bucket index is enabled.
   */
  private final boolean isPartitionLevelBucketIndexEnabled;

  /**
   * Calculator for partition-specific bucket numbers.
   */
  private final PartitionBucketIndexCalculator calculator;
  private final String expressions;
  private final String ruleType;

  /**
   * Creates a NumBucketsFunction with the given configuration.
   *
   * @param config The Flink configuration containing bucket index settings.
   */
  public NumBucketsFunction(String expressions, String ruleType, int defaultBucketNumber) {
    this.defaultBucketNumber = defaultBucketNumber;
    this.expressions = expressions;
    this.ruleType = ruleType;
    this.isPartitionLevelBucketIndexEnabled = StringUtils.nonEmpty(expressions);
    if (isPartitionLevelBucketIndexEnabled) {
      this.calculator = PartitionBucketIndexCalculator.getInstance(
          expressions, ruleType, defaultBucketNumber);
      LOG.info("Initialized partition-level bucket index with expressions: {}, rule: {}, default bucket number: {}",
          expressions, ruleType, defaultBucketNumber);
    } else {
      this.calculator = null;
      LOG.info("Using fixed bucket number: {}", defaultBucketNumber);
    }
  }

  /**
   * Gets the number of buckets for the given partition path.
   *
   * @param partitionPath The partition path.
   * @return The number of buckets for the partition.
   */
  public int getNumBuckets(String partitionPath) {
    if (isPartitionLevelBucketIndexEnabled && calculator != null) {
      return calculator.computeNumBuckets(partitionPath);
    }
    return defaultBucketNumber;
  }

  /**
   * Gets the default bucket number.
   *
   * @return The default bucket number.
   */
  public int getDefaultBucketNumber() {
    return defaultBucketNumber;
  }

  /**
   * Checks if partition-level bucket index is enabled.
   *
   * @return True if partition-level bucket index is enabled, false otherwise.
   */
  public boolean isPartitionLevelBucketIndexEnabled() {
    return isPartitionLevelBucketIndexEnabled;
  }
}
