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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexRule;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_TYPE;
import static org.apache.hudi.index.HoodieIndex.IndexType.BLOOM;
import static org.apache.hudi.index.HoodieIndex.IndexType.BUCKET;
import static org.apache.hudi.index.HoodieIndex.IndexType.FLINK_STATE;
import static org.apache.hudi.index.HoodieIndex.IndexType.GLOBAL_BLOOM;
import static org.apache.hudi.index.HoodieIndex.IndexType.GLOBAL_SIMPLE;
import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;
import static org.apache.hudi.index.HoodieIndex.IndexType.RECORD_INDEX;
import static org.apache.hudi.index.HoodieIndex.IndexType.PARTITIONED_RECORD_INDEX;
import static org.apache.hudi.index.HoodieIndex.IndexType.SIMPLE;

/**
 * Indexing related config.
 */
@Immutable
@ConfigClassProperty(name = "Common Index Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    subGroupName = ConfigGroups.SubGroupNames.INDEX,
    areCommonConfigs = true,
    description = "")
public class HoodieIndexConfig extends HoodieConfig {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieIndexConfig.class);

  public static final ConfigProperty<String> INDEX_TYPE = ConfigProperty
      .key("hoodie.index.type")
      // Builder#getDefaultIndexType has already set it according to engine type
      .noDefaultValue()
      .withValidValues(INMEMORY.name(), BLOOM.name(), GLOBAL_BLOOM.name(), SIMPLE.name(), GLOBAL_SIMPLE.name(),
          BUCKET.name(), FLINK_STATE.name(), RECORD_INDEX.name(), PARTITIONED_RECORD_INDEX.name())
      .withDocumentation(HoodieIndex.IndexType.class);


  public static final ConfigProperty<String> INDEX_CLASS_NAME = ConfigProperty
      .key("hoodie.index.class")
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("Full path of user-defined index class and must be a subclass of HoodieIndex class. "
          + "It will take precedence over the hoodie.index.type configuration if specified");

  // ***** Bloom Index configs *****

  public static final ConfigProperty<String> BLOOM_INDEX_PARALLELISM = ConfigProperty
      .key("hoodie.bloom.index.parallelism")
      .defaultValue("0")
      .markAdvanced()
      .withDocumentation("Only applies if index type is BLOOM. "
          + "This is the amount of parallelism for index lookup, which involves a shuffle. "
          + "By default, this is auto computed based on input workload characteristics. "
          + "If the parallelism is explicitly configured by the user, the user-configured "
          + "value is used in defining the actual parallelism. If the indexing stage is slow "
          + "due to the limited parallelism, you can increase this to tune the performance.");

  public static final ConfigProperty<String> BLOOM_INDEX_PRUNE_BY_RANGES = ConfigProperty
      .key("hoodie.bloom.index.prune.by.ranges")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, range information from files to leveraged speed up index lookups. Particularly helpful, "
          + "if the key has a monotonously increasing prefix, such as timestamp. "
          + "If the record key is completely random, it is better to turn this off, since range pruning will only "
          + " add extra overhead to the index lookup.");

  public static final ConfigProperty<String> BLOOM_INDEX_USE_CACHING = ConfigProperty
      .key("hoodie.bloom.index.use.caching")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Only applies if index type is BLOOM."
          + "When true, the input RDD will cached to speed up index lookup by reducing IO "
          + "for computing parallelism or affected partitions");

  public static final ConfigProperty<Boolean> BLOOM_INDEX_USE_METADATA = ConfigProperty
      .key("hoodie.bloom.index.use.metadata")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("Only applies if index type is BLOOM."
          + "When true, the index lookup uses bloom filters and column stats from metadata "
          + "table when available to speed up the process.");

  public static final ConfigProperty<String> BLOOM_INDEX_TREE_BASED_FILTER = ConfigProperty
      .key("hoodie.bloom.index.use.treebased.filter")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, interval tree based file pruning optimization is enabled. "
          + "This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode");

  // TODO: On by default. Once stable, we will remove the other mode.
  public static final ConfigProperty<String> BLOOM_INDEX_BUCKETIZED_CHECKING = ConfigProperty
      .key("hoodie.bloom.index.bucketized.checking")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, bucketized bloom filtering is enabled. "
          + "This reduces skew seen in sort based bloom index lookup");

  public static final ConfigProperty<String> BLOOM_INDEX_BUCKETIZED_CHECKING_ENABLE_DYNAMIC_PARALLELISM = ConfigProperty
      .key("hoodie.bloom.index.bucketized.checking.enable.dynamic.parallelism")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Only applies if index type is BLOOM and the bucketized bloom filtering "
          + "is enabled. When true, the index parallelism is determined by the number of file "
          + "groups to look up and the number of keys per bucket to split comparisons within a "
          + "file group; otherwise, the index parallelism is limited by the input parallelism. "
          + "PLEASE NOTE that if the bloom index parallelism (" + BLOOM_INDEX_PARALLELISM.key()
          + ") is configured, the bloom index parallelism takes effect instead of the input "
          + "parallelism and always limits the number of buckets calculated based on the number "
          + "of keys per bucket in the bucketized bloom filtering.");

  public static final ConfigProperty<String> BLOOM_INDEX_FILE_GROUP_ID_KEY_SORTING = ConfigProperty
      .key("hoodie.bloom.index.fileid.key.sorting.enable")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, the global sorting based on the fileId and key is enabled during key lookup. "
          + "This reduces skew in the key lookup in the bloom index.");

  public static final ConfigProperty<String> SIMPLE_INDEX_USE_CACHING = ConfigProperty
      .key("hoodie.simple.index.use.caching")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Only applies if index type is SIMPLE. "
          + "When true, the incoming writes will cached to speed up index lookup by reducing IO "
          + "for computing parallelism or affected partitions");

  public static final ConfigProperty<String> SIMPLE_INDEX_PARALLELISM = ConfigProperty
      .key("hoodie.simple.index.parallelism")
      .defaultValue("0")
      .markAdvanced()
      .withDocumentation("Only applies if index type is SIMPLE. "
          + "This limits the parallelism of fetching records from the base files of affected "
          + "partitions. By default, this is auto computed based on input workload characteristics. "
          + "If the parallelism is explicitly configured by the user, the user-configured "
          + "value is used in defining the actual parallelism. If the indexing stage is slow "
          + "due to the limited parallelism, you can increase this to tune the performance.");

  public static final ConfigProperty<String> GLOBAL_SIMPLE_INDEX_PARALLELISM = ConfigProperty
      .key("hoodie.global.simple.index.parallelism")
      .defaultValue("0")
      .markAdvanced()
      .withDocumentation("Only applies if index type is GLOBAL_SIMPLE. "
          + "This limits the parallelism of fetching records from the base files of all table "
          + "partitions. The index picks the configured parallelism if the number of base "
          + "files is larger than this configured value; otherwise, the number of base files "
          + "is used as the parallelism. If the indexing stage is slow due to the limited "
          + "parallelism, you can increase this to tune the performance.");

  // 1B bloom filter checks happen in 250 seconds. 500ms to read a bloom filter.
  // 10M checks in 2500ms, thus amortizing the cost of reading bloom filter across partitions.
  public static final ConfigProperty<String> BLOOM_INDEX_KEYS_PER_BUCKET = ConfigProperty
      .key("hoodie.bloom.index.keys.per.bucket")
      .defaultValue("10000000")
      .markAdvanced()
      .withDocumentation("Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. "
          + "This configuration controls the “bucket” size which tracks the number of record-key checks made against "
          + "a single file and is the unit of work allocated to each partition performing bloom filter lookup. "
          + "A higher value would amortize the fixed cost of reading a bloom filter to memory.");

  public static final ConfigProperty<String> BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE = ConfigProperty
      .key("hoodie.bloom.index.input.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .markAdvanced()
      .withDocumentation("Only applies when #bloomIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. "
          + "Refer to org.apache.spark.storage.StorageLevel for different values");

  public static final ConfigProperty<String> SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE = ConfigProperty
      .key("hoodie.simple.index.input.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .markAdvanced()
      .withDocumentation("Only applies when #simpleIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. "
          + "Refer to org.apache.spark.storage.StorageLevel for different values");

  /**
   * Only applies if index type is GLOBAL_BLOOM.
   * <p>
   * When set to true, an update to a record with a different partition from its existing one
   * will insert the record to the new partition and delete it from the old partition.
   * <p>
   * When set to false, a record will be updated to the old partition.
   */
  public static final ConfigProperty<String> BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE = ConfigProperty
      .key("hoodie.bloom.index.update.partition.path")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Only applies if index type is GLOBAL_BLOOM. "
          + "When set to true, an update including the partition path of a record that already exists will result in "
          + "inserting the incoming record into the new partition and deleting the original record in the old partition. "
          + "When set to false, the original record will only be updated in the old partition");

  public static final ConfigProperty<String> SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE = ConfigProperty
      .key("hoodie.simple.index.update.partition.path")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("Similar to " + BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE + ", but for simple index.");

  public static final ConfigProperty<String> RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE = ConfigProperty
      .key("hoodie.record.index.update.partition.path")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Similar to " + BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE + ", but for record index.");

  public static final ConfigProperty<String> GLOBAL_INDEX_RECONCILE_PARALLELISM = ConfigProperty
      .key("hoodie.global.index.reconcile.parallelism")
      .defaultValue("60")
      .markAdvanced()
      .withDocumentation("Only applies if index type is GLOBAL_BLOOM or GLOBAL_SIMPLE. "
          + "This controls the parallelism for deduplication during indexing where more than 1 record could be tagged due to partition update.");

  /**
   * ***** Bucket Index Configs *****
   * Bucket Index is targeted to locate the record fast by hash in big data scenarios.
   * A bucket size is recommended less than 3GB to avoid being too small.
   * For more details and progress, see [HUDI-3039].
   */

  /**
   * Bucket Index Engine Type: implementation of bucket index
   *
   * SIMPLE:
   *  0. Check `HoodieSimpleBucketLayout` for its supported operations.
   *  1. Bucket num is fixed and requires rewriting the partition if we want to change it.
   *
   * CONSISTENT_HASHING:
   *  0. Check `HoodieConsistentBucketLayout` for its supported operations.
   *  1. Bucket num will auto-adjust by running clustering (still in progress)
   */
  public static final ConfigProperty<String> BUCKET_INDEX_ENGINE_TYPE = ConfigProperty
      .key("hoodie.index.bucket.engine")
      .defaultValue(HoodieIndex.BucketIndexEngineType.SIMPLE.name())
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation(HoodieIndex.BucketIndexEngineType.class);

  /**
   * Bucket num equals file groups num in each partition.
   * Bucket num can be set according to partition size and file group size.
   *
   * In dynamic bucket index cases (e.g., using CONSISTENT_HASHING), this config of number of bucket serves as a initial bucket size
   */
  public static final ConfigProperty<Integer> BUCKET_INDEX_NUM_BUCKETS = ConfigProperty
      .key("hoodie.bucket.index.num.buckets")
      .defaultValue(256)
      .markAdvanced()
      .withDocumentation("Only applies if index type is BUCKET. Determine the number of buckets in the hudi table, "
          + "and each partition is divided to N buckets.");

  public static final ConfigProperty<Boolean> BUCKET_PARTITIONER = ConfigProperty
      .key("hoodie.bucket.index.remote.partitioner.enable")
      .defaultValue(false)
      .withDocumentation("Use Remote Partitioner using centralized allocation of partition "
          + "IDs to do repartition based on bucket aiming to resolve data skew. Default local hash partitioner");

  public static final ConfigProperty<String> BUCKET_INDEX_PARTITION_RULE_TYPE = ConfigProperty
      .key("hoodie.bucket.index.partition.rule.type")
      .defaultValue(PartitionBucketIndexRule.REGEX.name)
      .markAdvanced()
      .withDocumentation("Rule parser for expressions when using partition level bucket index, default regex.");

  public static final ConfigProperty<String> BUCKET_INDEX_PARTITION_EXPRESSIONS = ConfigProperty
      .key("hoodie.bucket.index.partition.expressions")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Users can use this parameter to specify expression and the corresponding bucket "
          + "numbers (separated by commas).Multiple rules are separated by semicolons like "
          + "hoodie.bucket.index.partition.expressions=expression1,bucket-number1;expression2,bucket-number2");

  public static final ConfigProperty<String> BUCKET_INDEX_MAX_NUM_BUCKETS = ConfigProperty
      .key("hoodie.bucket.index.max.num.buckets")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Only applies if bucket index engine is consistent hashing. Determine the upper bound of "
          + "the number of buckets in the hudi table. Bucket resizing cannot be done higher than this max limit.");

  public static final ConfigProperty<String> BUCKET_INDEX_MIN_NUM_BUCKETS = ConfigProperty
      .key("hoodie.bucket.index.min.num.buckets")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Only applies if bucket index engine is consistent hashing. Determine the lower bound of "
          + "the number of buckets in the hudi table. Bucket resizing cannot be done lower than this min limit.");

  public static final ConfigProperty<String> BUCKET_INDEX_HASH_FIELD = ConfigProperty
      .key("hoodie.bucket.index.hash.field")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Index key. It is used to index the record and find its file group. "
          + "If not set, use record key field as default");

  public static final ConfigProperty<Double> BUCKET_SPLIT_THRESHOLD = ConfigProperty
      .key("hoodie.bucket.index.split.threshold")
      .defaultValue(2.0)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Control if the bucket should be split when using consistent hashing bucket index."
          + "Specifically, if a file slice size reaches `hoodie.xxxx.max.file.size` * threshold, then split will be carried out.");

  public static final ConfigProperty<Double> BUCKET_MERGE_THRESHOLD = ConfigProperty
      .key("hoodie.bucket.index.merge.threshold")
      .defaultValue(0.2)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Control if buckets should be merged when using consistent hashing bucket index"
          + "Specifically, if a file slice size is smaller than `hoodie.xxxx.max.file.size` * threshold, then it will be considered"
          + "as a merge candidate.");

  public static final ConfigProperty<String> RECORD_INDEX_USE_CACHING = ConfigProperty
      .key("hoodie.record.index.use.caching")
      .defaultValue("true")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Only applies if index type is RECORD_INDEX."
          + "When true, the input RDD will be cached to speed up index lookup by reducing IO "
          + "for computing parallelism or affected partitions");

  public static final ConfigProperty<String> RECORD_INDEX_INPUT_STORAGE_LEVEL_VALUE = ConfigProperty
      .key("hoodie.record.index.input.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Only applies when #recordIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. "
          + "Refer to org.apache.spark.storage.StorageLevel for different values");

  public static final ConfigProperty<Boolean> BUCKET_QUERY_INDEX = ConfigProperty
      .key("hoodie.bucket.index.query.pruning")
      .defaultValue(true)
      .withDocumentation("Control if table with bucket index use bucket query or not");

  /** @deprecated Use {@link #INDEX_TYPE} and its methods instead */
  @Deprecated
  public static final String INDEX_TYPE_PROP = INDEX_TYPE.key();
  /**
   * @deprecated Use {@link #INDEX_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String INDEX_CLASS_PROP = INDEX_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #INDEX_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_INDEX_CLASS = INDEX_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link HoodieStorageConfig#BLOOM_FILTER_NUM_ENTRIES_VALUE} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_FILTER_NUM_ENTRIES = BLOOM_FILTER_NUM_ENTRIES_VALUE.key();
  /**
   * @deprecated Use {@link HoodieStorageConfig#BLOOM_FILTER_NUM_ENTRIES_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BLOOM_FILTER_NUM_ENTRIES = BLOOM_FILTER_NUM_ENTRIES_VALUE.defaultValue();
  /**
   * @deprecated Use {@link HoodieStorageConfig#BLOOM_FILTER_FPP_VALUE} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_FILTER_FPP = BLOOM_FILTER_FPP_VALUE.key();
  /**
   * @deprecated Use {@link HoodieStorageConfig#BLOOM_FILTER_FPP_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BLOOM_FILTER_FPP = BLOOM_FILTER_FPP_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_PARALLELISM} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_INDEX_PARALLELISM_PROP = BLOOM_INDEX_PARALLELISM.key();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_PARALLELISM} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_PARALLELISM = BLOOM_INDEX_PARALLELISM.defaultValue();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_PRUNE_BY_RANGES} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_INDEX_PRUNE_BY_RANGES_PROP = BLOOM_INDEX_PRUNE_BY_RANGES.key();
  /** @deprecated Use {@link #BLOOM_INDEX_PRUNE_BY_RANGES} and its methods instead */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_PRUNE_BY_RANGES = BLOOM_INDEX_PRUNE_BY_RANGES.defaultValue();
  /** @deprecated Use {@link #BLOOM_INDEX_USE_CACHING} and its methods instead */
  @Deprecated
  public static final String BLOOM_INDEX_USE_CACHING_PROP = BLOOM_INDEX_USE_CACHING.key();
  /** @deprecated Use {@link #BLOOM_INDEX_USE_CACHING} and its methods instead */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_USE_CACHING = BLOOM_INDEX_USE_CACHING.defaultValue();
  /** @deprecated Use {@link #BLOOM_INDEX_TREE_BASED_FILTER} and its methods instead */
  @Deprecated
  public static final String BLOOM_INDEX_TREE_BASED_FILTER_PROP = BLOOM_INDEX_TREE_BASED_FILTER.key();
  /** @deprecated Use {@link #BLOOM_INDEX_TREE_BASED_FILTER} and its methods instead */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_TREE_BASED_FILTER = BLOOM_INDEX_TREE_BASED_FILTER.defaultValue();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_BUCKETIZED_CHECKING} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_INDEX_BUCKETIZED_CHECKING_PROP = BLOOM_INDEX_BUCKETIZED_CHECKING.key();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_BUCKETIZED_CHECKING} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_BUCKETIZED_CHECKING = BLOOM_INDEX_BUCKETIZED_CHECKING.defaultValue();
  /**
   * @deprecated Use {@link HoodieStorageConfig#BLOOM_FILTER_TYPE} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_INDEX_FILTER_TYPE = BLOOM_FILTER_TYPE.key();
  /**
   * @deprecated Use {@link HoodieStorageConfig#BLOOM_FILTER_TYPE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_FILTER_TYPE = BLOOM_FILTER_TYPE.defaultValue();
  /**
   * @deprecated Use {@link HoodieStorageConfig#BLOOM_FILTER_DYNAMIC_MAX_ENTRIES} and its methods instead
   */
  @Deprecated
  public static final String HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES = BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.key();
  /**
   * @deprecated Use {@link HoodieStorageConfig#BLOOM_FILTER_DYNAMIC_MAX_ENTRIES} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES = BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.defaultValue();
  /**
   * @deprecated Use {@link #SIMPLE_INDEX_USE_CACHING} and its methods instead
   */
  @Deprecated
  public static final String SIMPLE_INDEX_USE_CACHING_PROP = SIMPLE_INDEX_USE_CACHING.key();
  /**
   * @deprecated Use {@link #SIMPLE_INDEX_USE_CACHING} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_SIMPLE_INDEX_USE_CACHING = SIMPLE_INDEX_USE_CACHING.defaultValue();
  /** @deprecated Use {@link #SIMPLE_INDEX_PARALLELISM} and its methods instead */
  @Deprecated
  public static final String SIMPLE_INDEX_PARALLELISM_PROP = SIMPLE_INDEX_PARALLELISM.key();
  /** @deprecated Use {@link #SIMPLE_INDEX_PARALLELISM} and its methods instead */
  @Deprecated
  public static final String DEFAULT_SIMPLE_INDEX_PARALLELISM = SIMPLE_INDEX_PARALLELISM.defaultValue();
  /** @deprecated Use {@link #GLOBAL_SIMPLE_INDEX_PARALLELISM} and its methods instead */
  @Deprecated
  public static final String GLOBAL_SIMPLE_INDEX_PARALLELISM_PROP = GLOBAL_SIMPLE_INDEX_PARALLELISM.key();
  /**
   * @deprecated Use {@link #GLOBAL_SIMPLE_INDEX_PARALLELISM} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_GLOBAL_SIMPLE_INDEX_PARALLELISM = GLOBAL_SIMPLE_INDEX_PARALLELISM.defaultValue();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_KEYS_PER_BUCKET} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_INDEX_KEYS_PER_BUCKET_PROP = BLOOM_INDEX_KEYS_PER_BUCKET.key();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_KEYS_PER_BUCKET} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_KEYS_PER_BUCKET = BLOOM_INDEX_KEYS_PER_BUCKET.defaultValue();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_INDEX_INPUT_STORAGE_LEVEL = BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE.key();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_INPUT_STORAGE_LEVEL = BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE} and its methods instead
   */
  @Deprecated
  public static final String SIMPLE_INDEX_INPUT_STORAGE_LEVEL = SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE.key();
  /**
   * @deprecated Use {@link #SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_SIMPLE_INDEX_INPUT_STORAGE_LEVEL = SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String BLOOM_INDEX_UPDATE_PARTITION_PATH = BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE.key();
  /**
   * @deprecated Use {@link #BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BLOOM_INDEX_UPDATE_PARTITION_PATH = BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String SIMPLE_INDEX_UPDATE_PARTITION_PATH = SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE.key();
  /**
   * @deprecated Use {@link #SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_SIMPLE_INDEX_UPDATE_PARTITION_PATH = SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE.defaultValue();

  private final EngineType engineType;

  /**
   * Use Spark engine by default.
   */

  private HoodieIndexConfig() {
    this(EngineType.SPARK);
  }

  private HoodieIndexConfig(EngineType engineType) {
    super();
    this.engineType = engineType;
  }

  public static HoodieIndexConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private EngineType engineType = EngineType.SPARK;
    private final HoodieIndexConfig hoodieIndexConfig = new HoodieIndexConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hoodieIndexConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.hoodieIndexConfig.getProps().putAll(props);
      return this;
    }

    public Builder withIndexType(HoodieIndex.IndexType indexType) {
      hoodieIndexConfig.setValue(INDEX_TYPE, indexType.name());
      return this;
    }

    public Builder withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType bucketType) {
      hoodieIndexConfig.setValue(BUCKET_INDEX_ENGINE_TYPE, bucketType.name());
      return this;
    }

    public Builder withIndexClass(String indexClass) {
      hoodieIndexConfig.setValue(INDEX_CLASS_NAME, indexClass);
      return this;
    }

    public Builder bloomFilterNumEntries(int numEntries) {
      hoodieIndexConfig.setValue(BLOOM_FILTER_NUM_ENTRIES_VALUE, String.valueOf(numEntries));
      return this;
    }

    public Builder bloomFilterFPP(double fpp) {
      hoodieIndexConfig.setValue(BLOOM_FILTER_FPP_VALUE, String.valueOf(fpp));
      return this;
    }

    public Builder bloomIndexParallelism(int parallelism) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder bloomIndexPruneByRanges(boolean pruneRanges) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_PRUNE_BY_RANGES, String.valueOf(pruneRanges));
      return this;
    }

    public Builder bloomIndexUseCaching(boolean useCaching) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_USE_CACHING, String.valueOf(useCaching));
      return this;
    }

    public Builder bloomIndexUseMetadata(boolean useMetadata) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_USE_METADATA, String.valueOf(useMetadata));
      return this;
    }

    public Builder bloomIndexTreebasedFilter(boolean useTreeFilter) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_TREE_BASED_FILTER, String.valueOf(useTreeFilter));
      return this;
    }

    public Builder bloomIndexBucketizedChecking(boolean bucketizedChecking) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_BUCKETIZED_CHECKING, String.valueOf(bucketizedChecking));
      return this;
    }

    public Builder enableBloomIndexFileGroupIdKeySorting(boolean fileGroupIdKeySorting) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_FILE_GROUP_ID_KEY_SORTING, String.valueOf(fileGroupIdKeySorting));
      return this;
    }

    public Builder bloomIndexKeysPerBucket(int keysPerBucket) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_KEYS_PER_BUCKET, String.valueOf(keysPerBucket));
      return this;
    }

    public Builder withBloomIndexInputStorageLevel(String level) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_INPUT_STORAGE_LEVEL_VALUE, level);
      return this;
    }

    public Builder withGlobalBloomIndexUpdatePartitionPath(boolean updatePartitionPath) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE, String.valueOf(updatePartitionPath));
      return this;
    }

    public Builder withSimpleIndexParallelism(int parallelism) {
      hoodieIndexConfig.setValue(SIMPLE_INDEX_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder simpleIndexUseCaching(boolean useCaching) {
      hoodieIndexConfig.setValue(SIMPLE_INDEX_USE_CACHING, String.valueOf(useCaching));
      return this;
    }

    public Builder withSimpleIndexInputStorageLevel(String level) {
      hoodieIndexConfig.setValue(SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE, level);
      return this;
    }

    public Builder withGlobalSimpleIndexParallelism(int parallelism) {
      hoodieIndexConfig.setValue(GLOBAL_SIMPLE_INDEX_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withGlobalSimpleIndexUpdatePartitionPath(boolean updatePartitionPath) {
      hoodieIndexConfig.setValue(SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE, String.valueOf(updatePartitionPath));
      return this;
    }

    public Builder withRecordIndexUpdatePartitionPath(boolean updatePartitionPath) {
      hoodieIndexConfig.setValue(RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE, String.valueOf(updatePartitionPath));
      return this;
    }

    public Builder withGlobalIndexReconcileParallelism(int parallelism) {
      hoodieIndexConfig.setValue(GLOBAL_INDEX_RECONCILE_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withEngineType(EngineType engineType) {
      this.engineType = engineType;
      return this;
    }

    public Builder withBucketNum(String bucketNum) {
      hoodieIndexConfig.setValue(BUCKET_INDEX_NUM_BUCKETS, bucketNum);
      return this;
    }

    public Builder withBucketMaxNum(int bucketMaxNum) {
      hoodieIndexConfig.setValue(BUCKET_INDEX_MAX_NUM_BUCKETS, String.valueOf(bucketMaxNum));
      return this;
    }

    public Builder enableBucketRemotePartitioner(boolean enableRemotePartitioner) {
      hoodieIndexConfig.setValue(BUCKET_PARTITIONER, String.valueOf(enableRemotePartitioner));
      return this;
    }

    public Builder withBucketMinNum(int bucketMinNum) {
      hoodieIndexConfig.setValue(BUCKET_INDEX_MIN_NUM_BUCKETS, String.valueOf(bucketMinNum));
      return this;
    }

    public Builder withIndexKeyField(String keyField) {
      hoodieIndexConfig.setValue(BUCKET_INDEX_HASH_FIELD, keyField);
      return this;
    }

    public Builder withRecordKeyField(String keyField) {
      hoodieIndexConfig.setValue(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, keyField);
      return this;
    }

    public Builder recordIndexUseCaching(boolean useCaching) {
      hoodieIndexConfig.setValue(RECORD_INDEX_USE_CACHING, String.valueOf(useCaching));
      return this;
    }

    public Builder withRecordIndexInputStorageLevel(String level) {
      hoodieIndexConfig.setValue(RECORD_INDEX_INPUT_STORAGE_LEVEL_VALUE, level);
      return this;
    }

    public HoodieIndexConfig build() {
      hoodieIndexConfig.setDefaultValue(INDEX_TYPE, getDefaultIndexType(engineType));
      hoodieIndexConfig.setDefaults(HoodieIndexConfig.class.getName());

      // Throws IllegalArgumentException if the value set is not a known Hoodie Index Type
      HoodieIndex.IndexType.valueOf(hoodieIndexConfig.getString(INDEX_TYPE));
      validateBucketIndexConfig();
      return hoodieIndexConfig;
    }

    private String getDefaultIndexType(EngineType engineType) {
      switch (engineType) {
        case SPARK:
        case JAVA:
          return HoodieIndex.IndexType.SIMPLE.name();
        case FLINK:
          return HoodieIndex.IndexType.INMEMORY.name();
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }

    public EngineType getEngineType() {
      return engineType;
    }

    private void validateBucketIndexConfig() {
      if (hoodieIndexConfig.getString(INDEX_TYPE).equalsIgnoreCase(HoodieIndex.IndexType.BUCKET.toString())) {
        // check the bucket index hash field
        if (StringUtils.isNullOrEmpty(hoodieIndexConfig.getString(BUCKET_INDEX_HASH_FIELD))) {
          hoodieIndexConfig.setValue(BUCKET_INDEX_HASH_FIELD,
              hoodieIndexConfig.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME));
        } else {
          boolean valid = Arrays
              .stream(hoodieIndexConfig.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME).split(","))
              .collect(Collectors.toSet())
              .containsAll(Arrays.asList(hoodieIndexConfig.getString(BUCKET_INDEX_HASH_FIELD).split(",")));
          if (!valid) {
            throw new HoodieIndexException("Bucket index key (if configured) must be subset of record key.");
          }
        }
        // check the bucket num
        if (hoodieIndexConfig.getIntOrDefault(BUCKET_INDEX_NUM_BUCKETS) <= 0) {
          throw new HoodieIndexException("When using bucket index, hoodie.bucket.index.num.buckets cannot be negative.");
        }
        int bucketNum = hoodieIndexConfig.getInt(BUCKET_INDEX_NUM_BUCKETS);
        if (StringUtils.isNullOrEmpty(hoodieIndexConfig.getString(BUCKET_INDEX_MAX_NUM_BUCKETS))) {
          hoodieIndexConfig.setValue(BUCKET_INDEX_MAX_NUM_BUCKETS, Integer.toString(bucketNum));
        } else if (hoodieIndexConfig.getInt(BUCKET_INDEX_MAX_NUM_BUCKETS) < bucketNum) {
          LOG.warn("Maximum bucket number is smaller than bucket number, maximum: " + hoodieIndexConfig.getInt(BUCKET_INDEX_MAX_NUM_BUCKETS) + ", bucketNum: " + bucketNum);
          hoodieIndexConfig.setValue(BUCKET_INDEX_MAX_NUM_BUCKETS, Integer.toString(bucketNum));
        }

        if (StringUtils.isNullOrEmpty(hoodieIndexConfig.getString(BUCKET_INDEX_MIN_NUM_BUCKETS))) {
          hoodieIndexConfig.setValue(BUCKET_INDEX_MIN_NUM_BUCKETS, Integer.toString(bucketNum));
        } else if (hoodieIndexConfig.getInt(BUCKET_INDEX_MIN_NUM_BUCKETS) > bucketNum) {
          LOG.warn("Minimum bucket number is larger than the bucket number, minimum: " + hoodieIndexConfig.getInt(BUCKET_INDEX_MIN_NUM_BUCKETS) + ", bucketNum: " + bucketNum);
          hoodieIndexConfig.setValue(BUCKET_INDEX_MIN_NUM_BUCKETS, Integer.toString(bucketNum));
        }
      }
    }
  }
}
