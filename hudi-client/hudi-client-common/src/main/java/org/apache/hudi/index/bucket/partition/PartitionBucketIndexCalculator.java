/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.index.bucket.partition;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A singleton implementation of PartitionBucketIndexCalculator that ensures only one instance
 * exists for each unique hashingInstantToLoad value.
 */
@Slf4j
public class PartitionBucketIndexCalculator implements Serializable {
  private static final long serialVersionUID = 1L;
  // Map to store singleton instances for each instantToLoad + configuration hash combination
  private static final HashMap<String, PartitionBucketIndexCalculator> INSTANCES = new HashMap<>();
  private static final int CACHE_SIZE = 100_000;
  @Getter
  private final int defaultBucketNumber;
  // Cache for partition to bucket number mapping
  private final Cache<String, Integer> partitionToBucketCache;
  private final RuleEngine ruleEngine;

  /**
   * Private constructor to prevent direct instantiation.
   */
  private PartitionBucketIndexCalculator(String expressions, String ruleType, int defaultBucketNumber) {
    this.defaultBucketNumber = defaultBucketNumber;
    this.ruleEngine = createRuleEngine(ruleType, expressions);
    this.partitionToBucketCache = Caffeine.newBuilder().maximumSize(CACHE_SIZE).build();
  }

  /**
   * Gets the singleton instance for the specified expressions.
   */
  public static PartitionBucketIndexCalculator getInstance(String expressions, String rule, int defaultBucketNumber) {
    return INSTANCES.computeIfAbsent(expressions,
        key -> {
          log.info("Creating new {} instance for expressions: {}", PartitionBucketIndexCalculator.class, key);
          return new PartitionBucketIndexCalculator(expressions, rule, defaultBucketNumber);
        });
  }

  /**
   * Computes the bucket number for a given partition path.
   *
   * @param partitionPath The partition path.
   * @return The computed bucket number.
   */
  public int computeNumBuckets(String partitionPath) {
    // Check cache first
    Integer cachedBucketNumber = partitionToBucketCache.getIfPresent(partitionPath);
    if (cachedBucketNumber != null) {
      return cachedBucketNumber;
    }

    // Calculate bucket number using the rule engine
    int bucketNumber = ruleEngine.calculateNumBuckets(partitionPath);

    // If no rule matched, use default bucket number
    if (bucketNumber == -1) {
      bucketNumber = defaultBucketNumber;
      log.debug("No rule matched for partition: {}. Using default bucket number: {}",
          partitionPath, defaultBucketNumber);
    }

    // Update cache
    partitionToBucketCache.put(partitionPath, bucketNumber);

    return bucketNumber;
  }

  public Map<String, Integer> getAllBucketNumbers(List<String> partitions) {
    for (String partition : partitions) {
      computeNumBuckets(partition);
    }
    return getPartitionToBucket();
  }

  public void cleanCache() {
    INSTANCES.clear();
  }

  public long getCacheSize() {
    return partitionToBucketCache.estimatedSize();
  }

  public Map<String, Integer> getPartitionToBucket() {
    return partitionToBucketCache.asMap();
  }

  /**
   * Factory method to create the appropriate rule engine based on rule type.
   */
  private static RuleEngine createRuleEngine(String ruleType, String expressions) {
    switch (PartitionBucketIndexRule.valueOf(ruleType.toUpperCase())) {
      case REGEX:
        return new RegexRuleEngine(expressions);
      default:
        log.error("Unsupported rule type: {}.", ruleType);
        throw new UnsupportedOperationException("Unsupported rule type " + ruleType);
    }
  }
}
