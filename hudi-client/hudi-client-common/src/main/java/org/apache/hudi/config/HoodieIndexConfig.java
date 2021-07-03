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

import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Indexing related config.
 */
@Immutable
public class HoodieIndexConfig extends HoodieConfig {

  public static final ConfigProperty<String> INDEX_TYPE_PROP = ConfigProperty
      .key("hoodie.index.type")
      .noDefaultValue()
      .withDocumentation("Type of index to use. Default is Bloom filter. "
          + "Possible options are [BLOOM | GLOBAL_BLOOM |SIMPLE | GLOBAL_SIMPLE | INMEMORY | HBASE]. "
          + "Bloom filters removes the dependency on a external system "
          + "and is stored in the footer of the Parquet Data Files");

  public static final ConfigProperty<String> INDEX_CLASS_PROP = ConfigProperty
      .key("hoodie.index.class")
      .defaultValue("")
      .withDocumentation("Full path of user-defined index class and must be a subclass of HoodieIndex class. "
          + "It will take precedence over the hoodie.index.type configuration if specified");

  // ***** Bloom Index configs *****
  public static final ConfigProperty<String> BLOOM_FILTER_NUM_ENTRIES = ConfigProperty
      .key("hoodie.index.bloom.num_entries")
      .defaultValue("60000")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "This is the number of entries to be stored in the bloom filter. "
          + "We assume the maxParquetFileSize is 128MB and averageRecordSize is 1024B and "
          + "hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. "
          + "HUDI-56 tracks computing this dynamically. Warning: Setting this very low, "
          + "will generate a lot of false positives and index lookup will have to scan a lot more files "
          + "than it has to and Setting this to a very high number will increase the size every data file linearly "
          + "(roughly 4KB for every 50000 entries). "
          + "This config is also used with DYNNAMIC bloom filter which determines the initial size for the bloom.");

  public static final ConfigProperty<String> BLOOM_FILTER_FPP = ConfigProperty
      .key("hoodie.index.bloom.fpp")
      .defaultValue("0.000000001")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "Error rate allowed given the number of entries. This is used to calculate how many bits should be "
          + "assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), "
          + "we like to tradeoff disk space for lower false positives. "
          + "If the number of entries added to bloom filter exceeds the congfigured value (hoodie.index.bloom.num_entries), "
          + "then this fpp may not be honored.");

  public static final ConfigProperty<String> BLOOM_INDEX_PARALLELISM_PROP = ConfigProperty
      .key("hoodie.bloom.index.parallelism")
      .defaultValue("0")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "This is the amount of parallelism for index lookup, which involves a Spark Shuffle. "
          + "By default, this is auto computed based on input workload characteristics. "
          + "Disable explicit bloom index parallelism setting by default - hoodie auto computes");

  public static final ConfigProperty<String> BLOOM_INDEX_PRUNE_BY_RANGES_PROP = ConfigProperty
      .key("hoodie.bloom.index.prune.by.ranges")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, range information from files to leveraged speed up index lookups. Particularly helpful, "
          + "if the key has a monotonously increasing prefix, such as timestamp. "
          + "If the record key is completely random, it is better to turn this off.");

  public static final ConfigProperty<String> BLOOM_INDEX_USE_CACHING_PROP = ConfigProperty
      .key("hoodie.bloom.index.use.caching")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is BLOOM."
          + "When true, the input RDD will cached to speed up index lookup by reducing IO "
          + "for computing parallelism or affected partitions");

  public static final ConfigProperty<String> BLOOM_INDEX_TREE_BASED_FILTER_PROP = ConfigProperty
      .key("hoodie.bloom.index.use.treebased.filter")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, interval tree based file pruning optimization is enabled. "
          + "This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode");

  // TODO: On by default. Once stable, we will remove the other mode.
  public static final ConfigProperty<String> BLOOM_INDEX_BUCKETIZED_CHECKING_PROP = ConfigProperty
      .key("hoodie.bloom.index.bucketized.checking")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is BLOOM. "
          + "When true, bucketized bloom filtering is enabled. "
          + "This reduces skew seen in sort based bloom index lookup");

  public static final ConfigProperty<String> BLOOM_INDEX_FILTER_TYPE = ConfigProperty
      .key("hoodie.bloom.index.filter.type")
      .defaultValue(BloomFilterTypeCode.SIMPLE.name())
      .withDocumentation("Filter type used. Default is BloomFilterTypeCode.SIMPLE. "
          + "Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. "
          + "Dynamic bloom filters auto size themselves based on number of keys.");

  public static final ConfigProperty<String> HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES = ConfigProperty
      .key("hoodie.bloom.index.filter.dynamic.max.entries")
      .defaultValue("100000")
      .withDocumentation("The threshold for the maximum number of keys to record in a dynamic Bloom filter row. "
          + "Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0.");

  public static final ConfigProperty<String> SIMPLE_INDEX_USE_CACHING_PROP = ConfigProperty
      .key("hoodie.simple.index.use.caching")
      .defaultValue("true")
      .withDocumentation("Only applies if index type is SIMPLE. "
          + "When true, the input RDD will cached to speed up index lookup by reducing IO "
          + "for computing parallelism or affected partitions");

  public static final ConfigProperty<String> SIMPLE_INDEX_PARALLELISM_PROP = ConfigProperty
      .key("hoodie.simple.index.parallelism")
      .defaultValue("50")
      .withDocumentation("Only applies if index type is SIMPLE. "
          + "This is the amount of parallelism for index lookup, which involves a Spark Shuffle");

  public static final ConfigProperty<String> GLOBAL_SIMPLE_INDEX_PARALLELISM_PROP = ConfigProperty
      .key("hoodie.global.simple.index.parallelism")
      .defaultValue("100")
      .withDocumentation("Only applies if index type is GLOBAL_SIMPLE. "
          + "This is the amount of parallelism for index lookup, which involves a Spark Shuffle");

  // 1B bloom filter checks happen in 250 seconds. 500ms to read a bloom filter.
  // 10M checks in 2500ms, thus amortizing the cost of reading bloom filter across partitions.
  public static final ConfigProperty<String> BLOOM_INDEX_KEYS_PER_BUCKET_PROP = ConfigProperty
      .key("hoodie.bloom.index.keys.per.bucket")
      .defaultValue("10000000")
      .withDocumentation("Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. "
          + "This configuration controls the “bucket” size which tracks the number of record-key checks made against "
          + "a single file and is the unit of work allocated to each partition performing bloom filter lookup. "
          + "A higher value would amortize the fixed cost of reading a bloom filter to memory.");

  public static final ConfigProperty<String> BLOOM_INDEX_INPUT_STORAGE_LEVEL = ConfigProperty
      .key("hoodie.bloom.index.input.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .withDocumentation("Only applies when #bloomIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. "
          + "Refer to org.apache.spark.storage.StorageLevel for different values");

  public static final ConfigProperty<String> SIMPLE_INDEX_INPUT_STORAGE_LEVEL = ConfigProperty
      .key("hoodie.simple.index.input.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
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
  public static final ConfigProperty<String> BLOOM_INDEX_UPDATE_PARTITION_PATH = ConfigProperty
      .key("hoodie.bloom.index.update.partition.path")
      .defaultValue("false")
      .withDocumentation("Only applies if index type is GLOBAL_BLOOM. "
          + "When set to true, an update including the partition path of a record that already exists will result in "
          + "inserting the incoming record into the new partition and deleting the original record in the old partition. "
          + "When set to false, the original record will only be updated in the old partition");

  public static final ConfigProperty<String> SIMPLE_INDEX_UPDATE_PARTITION_PATH = ConfigProperty
      .key("hoodie.simple.index.update.partition.path")
      .defaultValue("false")
      .withDocumentation("");

  private EngineType engineType;

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
      hoodieIndexConfig.setValue(INDEX_TYPE_PROP, indexType.name());
      return this;
    }

    public Builder withIndexClass(String indexClass) {
      hoodieIndexConfig.setValue(INDEX_CLASS_PROP, indexClass);
      return this;
    }

    public Builder withHBaseIndexConfig(HoodieHBaseIndexConfig hBaseIndexConfig) {
      hoodieIndexConfig.getProps().putAll(hBaseIndexConfig.getProps());
      return this;
    }

    public Builder bloomFilterNumEntries(int numEntries) {
      hoodieIndexConfig.setValue(BLOOM_FILTER_NUM_ENTRIES, String.valueOf(numEntries));
      return this;
    }

    public Builder bloomFilterFPP(double fpp) {
      hoodieIndexConfig.setValue(BLOOM_FILTER_FPP, String.valueOf(fpp));
      return this;
    }

    public Builder bloomIndexParallelism(int parallelism) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_PARALLELISM_PROP, String.valueOf(parallelism));
      return this;
    }

    public Builder bloomIndexPruneByRanges(boolean pruneRanges) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_PRUNE_BY_RANGES_PROP, String.valueOf(pruneRanges));
      return this;
    }

    public Builder bloomIndexUseCaching(boolean useCaching) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_USE_CACHING_PROP, String.valueOf(useCaching));
      return this;
    }

    public Builder bloomIndexTreebasedFilter(boolean useTreeFilter) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_TREE_BASED_FILTER_PROP, String.valueOf(useTreeFilter));
      return this;
    }

    public Builder bloomIndexBucketizedChecking(boolean bucketizedChecking) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_BUCKETIZED_CHECKING_PROP, String.valueOf(bucketizedChecking));
      return this;
    }

    public Builder bloomIndexKeysPerBucket(int keysPerBucket) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_KEYS_PER_BUCKET_PROP, String.valueOf(keysPerBucket));
      return this;
    }

    public Builder withBloomIndexInputStorageLevel(String level) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_INPUT_STORAGE_LEVEL, level);
      return this;
    }

    public Builder withBloomIndexUpdatePartitionPath(boolean updatePartitionPath) {
      hoodieIndexConfig.setValue(BLOOM_INDEX_UPDATE_PARTITION_PATH, String.valueOf(updatePartitionPath));
      return this;
    }

    public Builder withSimpleIndexParallelism(int parallelism) {
      hoodieIndexConfig.setValue(SIMPLE_INDEX_PARALLELISM_PROP, String.valueOf(parallelism));
      return this;
    }

    public Builder simpleIndexUseCaching(boolean useCaching) {
      hoodieIndexConfig.setValue(SIMPLE_INDEX_USE_CACHING_PROP, String.valueOf(useCaching));
      return this;
    }

    public Builder withSimpleIndexInputStorageLevel(String level) {
      hoodieIndexConfig.setValue(SIMPLE_INDEX_INPUT_STORAGE_LEVEL, level);
      return this;
    }

    public Builder withGlobalSimpleIndexParallelism(int parallelism) {
      hoodieIndexConfig.setValue(GLOBAL_SIMPLE_INDEX_PARALLELISM_PROP, String.valueOf(parallelism));
      return this;
    }

    public Builder withGlobalSimpleIndexUpdatePartitionPath(boolean updatePartitionPath) {
      hoodieIndexConfig.setValue(SIMPLE_INDEX_UPDATE_PARTITION_PATH, String.valueOf(updatePartitionPath));
      return this;
    }

    public Builder withEngineType(EngineType engineType) {
      this.engineType = engineType;
      return this;
    }

    public HoodieIndexConfig build() {
      hoodieIndexConfig.setDefaultValue(INDEX_TYPE_PROP, getDefaultIndexType(engineType));
      hoodieIndexConfig.setDefaults(HoodieIndexConfig.class.getName());

      // Throws IllegalArgumentException if the value set is not a known Hoodie Index Type
      HoodieIndex.IndexType.valueOf(hoodieIndexConfig.getString(INDEX_TYPE_PROP));
      return hoodieIndexConfig;
    }

    private String getDefaultIndexType(EngineType engineType) {
      switch (engineType) {
        case SPARK:
          return HoodieIndex.IndexType.BLOOM.name();
        case FLINK:
        case JAVA:
          return HoodieIndex.IndexType.INMEMORY.name();
        default:
          throw new HoodieNotSupportedException("Unsupported engine " + engineType);
      }
    }

    public EngineType getEngineType() {
      return engineType;
    }
  }
}
