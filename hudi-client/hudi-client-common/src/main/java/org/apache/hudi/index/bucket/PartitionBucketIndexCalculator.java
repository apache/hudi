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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A singleton implementation of PartitionBucketIndexCalculator that ensures only one instance
 * exists for each unique hashingInstantToLoad value.
 */
public class PartitionBucketIndexCalculator implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketIndexCalculator.class);
  // Map to store singleton instances for each instantToLoad + configuration hash combination
  private static final ConcurrentMap<String, PartitionBucketIndexCalculator> INSTANCES = new ConcurrentHashMap<>();
  private static final int CACHE_SIZE = 100_000;
  private PartitionBucketIndexHashingConfig hashingConfig;
  private int defaultBucketNumber;
  private final String instantToLoad;
  // Cache for partition to bucket number mapping
  @SuppressWarnings("unchecked")
  private final Map<String, Integer> partitionToBucketCache = new LRUMap(CACHE_SIZE);
  private RuleEngine ruleEngine;

  /**
   * Private constructor to prevent direct instantiation
   *
   * @param instantToLoad The instant to load
   * @param hadoopConf The Hadoop configuration
   */
  private PartitionBucketIndexCalculator(String instantToLoad, Configuration hadoopConf, String basePath) {
    this.instantToLoad = instantToLoad;
    StoragePath metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    StoragePath hashingBase = new StoragePath(metaPath, HoodieTableMetaClient.BUCKET_INDEX_METAFOLDER_CONFIG_FOLDER);
    StoragePath hashingConfigPath =
        new StoragePath(hashingBase,
            instantToLoad + PartitionBucketIndexHashingConfig.HASHING_CONFIG_FILE_SUFFIX);

    try (HoodieHadoopStorage storage = new HoodieHadoopStorage(hashingConfigPath, HadoopFSUtils.getStorageConf(hadoopConf))) {
      init(storage, hashingConfigPath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize PartitionBucketIndexCalculator ", e);
    }
  }

  private PartitionBucketIndexCalculator(String instantToLoad, HoodieTableMetaClient client) {
    this.instantToLoad = instantToLoad;
    String metaPath = client.getHashingMetadataConfigPath();
    StoragePath hashingConfigPath = new StoragePath(metaPath, instantToLoad + PartitionBucketIndexHashingConfig.HASHING_CONFIG_FILE_SUFFIX);

    try (HoodieStorage storage = client.getStorage()) {
      init(storage, hashingConfigPath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize PartitionBucketIndexCalculator ", e);
    }
  }

  private void init(HoodieStorage storage, StoragePath hashingConfigPath) {
    Option<PartitionBucketIndexHashingConfig> config = PartitionBucketIndexUtils.loadHashingConfig(storage, hashingConfigPath);
    ValidationUtils.checkArgument(config.isPresent());
    this.hashingConfig = config.get();
    this.defaultBucketNumber = config.get().getDefaultBucketNumber();
    String expressions = config.get().getExpressions();
    String ruleType = config.get().getRule();
    this.ruleEngine = createRuleEngine(ruleType, expressions);
  }

  /**
   * Gets the singleton instance for the specified instantToLoad and configuration
   *
   * @param instantToLoad The instant to load
   * @param hadoopConf The Hadoop configuration
   * @return The singleton instance
   */
  public static PartitionBucketIndexCalculator getInstance(String instantToLoad, Configuration hadoopConf, String basePath) {
    // Using instantToLoad as the key for the cache
    return INSTANCES.computeIfAbsent(instantToLoad,
        key -> {
          LOG.info("Creating new PartitionBucketIndexCalculator instance for instantToLoad: {}", key);
          return new PartitionBucketIndexCalculator(key, hadoopConf, basePath);
        });
  }

  public static PartitionBucketIndexCalculator getInstance(String instantToLoad, HoodieTableMetaClient client) {
    // Using instantToLoad as the key for the cache
    return INSTANCES.computeIfAbsent(instantToLoad,
        key -> {
          LOG.info("Creating new PartitionBucketIndexCalculator instance for instantToLoad: {}", key);
          return new PartitionBucketIndexCalculator(key, client);
        });
  }

  /**
   * Computes the bucket number for a given partition path
   *
   * @param partitionPath The partition path
   * @return The computed bucket number
   */
  public int computeNumBuckets(String partitionPath) {
    // Check cache first
    Integer cachedBucketNumber = partitionToBucketCache.get(partitionPath);
    if (cachedBucketNumber != null) {
      return cachedBucketNumber;
    }

    // Calculate bucket number using the rule engine
    int bucketNumber = ruleEngine.calculateBucketNumber(partitionPath);

    // If no rule matched, use default bucket number
    if (bucketNumber == -1) {
      bucketNumber = defaultBucketNumber;
      LOG.debug("No rule matched for partition: {}. Using default bucket number: {}",
          partitionPath, defaultBucketNumber);
    }

    // Update cache
    partitionToBucketCache.put(partitionPath, bucketNumber);

    return bucketNumber;
  }

  /**
   * Gets the instant to load
   *
   * @return The instant to load
   */
  public String getInstantToLoad() {
    return instantToLoad;
  }

  /**
   * Gets the hashing configuration
   *
   * @return The hashing configuration
   */
  public PartitionBucketIndexHashingConfig getHashingConfig() {
    return hashingConfig;
  }

  /**
   * Clears the instance cache (useful for testing or memory management)
   */
  public static void clearInstanceCache() {
    INSTANCES.clear();
  }

  public int getCacheSize() {
    return partitionToBucketCache.size();
  }

  public void clearCache() {
    partitionToBucketCache.clear();
    LOG.info("Cleared partition to bucket number cache");
  }

  // -------------------------------------------------------------------------------------------------------

  /**
   * Interface for rule engines that calculate bucket numbers
   */
  private interface RuleEngine extends Serializable {
    /**
     * Calculate bucket number for a partition path
     * @param partitionPath The partition path
     * @return The calculated bucket number, or -1 if no rule matches
     */
    int calculateBucketNumber(String partitionPath);
  }

  /**
   * Factory method to create the appropriate rule engine based on rule type
   */
  private static PartitionBucketIndexCalculator.RuleEngine createRuleEngine(String ruleType, String expressions) {
    switch (PartitionBucketIndexRule.valueOf(ruleType.toUpperCase())) {
      case REGEX:
        return new PartitionBucketIndexCalculator.RegexRuleEngine(expressions);
      default:
        LOG.error("Unsupported rule type: {}.", ruleType);
        throw new UnsupportedOperationException("Unsupported rule type " + ruleType);
    }
  }

  /**
   * Regex-based rule engine implementation
   */
  private static class RegexRuleEngine implements PartitionBucketIndexCalculator.RuleEngine {
    private static final long serialVersionUID = 1L;
    private final List<RegexRule> rules = new ArrayList<>();

    /**
     * Represents a single regex rule with its pattern and bucket number
     */
    private static class RegexRule implements Serializable {
      private static final long serialVersionUID = 1L;
      private final Pattern pattern;
      private final int bucketNumber;

      public RegexRule(String regex, int bucketNumber) {
        this.pattern = Pattern.compile(regex);
        this.bucketNumber = bucketNumber;
      }

      public boolean matches(String input) {
        Matcher matcher = pattern.matcher(input);
        return matcher.find();
      }

      @Override
      public String toString() {
        return pattern.pattern() + " -> " + bucketNumber;
      }
    }

    /**
     * Initialize the regex rule engine with expressions
     * @param expressions Format: "expression1,bucketNumber1;expression2,bucketNumber2;..."
     */
    public RegexRuleEngine(String expressions) {
      parseExpressions(expressions);
    }

    /**
     * Parse the expressions string and create regex rules
     */
    private void parseExpressions(String expressions) {
      String[] ruleExpressions = expressions.split(";");

      for (String ruleExpression : ruleExpressions) {
        String[] parts = ruleExpression.trim().split(",");
        if (parts.length != 2) {
          throw new HoodieException("Invalid regex expression format. Expected 'pattern,bucketNumber' but got: " + ruleExpression);
        }

        String regex = parts[0].trim();
        int bucketNumber;
        try {
          bucketNumber = Integer.parseInt(parts[1].trim());
        } catch (NumberFormatException e) {
          throw new HoodieException("Invalid bucket number in expression: " + ruleExpression, e);
        }

        rules.add(new PartitionBucketIndexCalculator.RegexRuleEngine.RegexRule(regex, bucketNumber));
        LOG.info("Added regex rule: {} with bucket number: {}", regex, bucketNumber);
      }

      LOG.info("Initialized {} regex rules", rules.size());
    }

    @Override
    public int calculateBucketNumber(String partitionPath) {
      // Check each rule in order (priority by position)
      for (PartitionBucketIndexCalculator.RegexRuleEngine.RegexRule rule : rules) {
        if (rule.matches(partitionPath)) {
          LOG.debug("Partition '{}' matched regex rule: {}", partitionPath, rule);
          return rule.bucketNumber;
        }
      }

      // No rule matched
      return -1;
    }
  }
}
