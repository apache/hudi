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

package com.uber.hoodie.config;

import com.uber.hoodie.index.HoodieIndex;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

/**
 * Indexing related config
 */
@Immutable
public class HoodieIndexConfig extends DefaultHoodieConfig {

  public static final String INDEX_TYPE_PROP = "hoodie.index.type";
  public static final String DEFAULT_INDEX_TYPE = HoodieIndex.IndexType.BLOOM.name();

  // *****  Bloom Index configs *****
  public static final String BLOOM_FILTER_NUM_ENTRIES = "hoodie.index.bloom.num_entries";
  public static final String DEFAULT_BLOOM_FILTER_NUM_ENTRIES = "60000";
  public static final String BLOOM_FILTER_FPP = "hoodie.index.bloom.fpp";
  public static final String DEFAULT_BLOOM_FILTER_FPP = "0.000000001";
  public static final String BLOOM_INDEX_PARALLELISM_PROP = "hoodie.bloom.index.parallelism";
  // Disable explicit bloom index parallelism setting by default - hoodie auto computes
  public static final String DEFAULT_BLOOM_INDEX_PARALLELISM = "0";
  public static final String BLOOM_INDEX_PRUNE_BY_RANGES_PROP =
      "hoodie.bloom.index.prune.by" + ".ranges";
  public static final String DEFAULT_BLOOM_INDEX_PRUNE_BY_RANGES = "true";
  public static final String BLOOM_INDEX_USE_CACHING_PROP = "hoodie.bloom.index.use.caching";
  public static final String DEFAULT_BLOOM_INDEX_USE_CACHING = "true";
  public static final String BLOOM_INDEX_TREE_BASED_FILTER_PROP = "hoodie.bloom.index.use.treebased.filter";
  public static final String DEFAULT_BLOOM_INDEX_TREE_BASED_FILTER = "true";
  // TODO: On by default. Once stable, we will remove the other mode.
  public static final String BLOOM_INDEX_BUCKETIZED_CHECKING_PROP = "hoodie.bloom.index.bucketized.checking";
  public static final String DEFAULT_BLOOM_INDEX_BUCKETIZED_CHECKING = "true";
  // 1B bloom filter checks happen in 250 seconds. 500ms to read a bloom filter.
  // 10M checks in 2500ms, thus amortizing the cost of reading bloom filter across partitions.
  public static final String BLOOM_INDEX_KEYS_PER_BUCKET_PROP = "hoodie.bloom.index.keys.per.bucket";
  public static final String DEFAULT_BLOOM_INDEX_KEYS_PER_BUCKET = "10000000";


  public static final String BLOOM_INDEX_INPUT_STORAGE_LEVEL =
      "hoodie.bloom.index.input.storage" + ".level";
  public static final String DEFAULT_BLOOM_INDEX_INPUT_STORAGE_LEVEL = "MEMORY_AND_DISK_SER";

  private HoodieIndexConfig(Properties props) {
    super(props);
  }

  public static HoodieIndexConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      FileReader reader = new FileReader(propertiesFile);
      try {
        this.props.load(reader);
        return this;
      } finally {
        reader.close();
      }
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withIndexType(HoodieIndex.IndexType indexType) {
      props.setProperty(INDEX_TYPE_PROP, indexType.name());
      return this;
    }

    public Builder withHBaseIndexConfig(HoodieHBaseIndexConfig hBaseIndexConfig) {
      props.putAll(hBaseIndexConfig.getProps());
      return this;
    }

    public Builder bloomFilterNumEntries(int numEntries) {
      props.setProperty(BLOOM_FILTER_NUM_ENTRIES, String.valueOf(numEntries));
      return this;
    }

    public Builder bloomFilterFPP(double fpp) {
      props.setProperty(BLOOM_FILTER_FPP, String.valueOf(fpp));
      return this;
    }

    public Builder bloomIndexParallelism(int parallelism) {
      props.setProperty(BLOOM_INDEX_PARALLELISM_PROP, String.valueOf(parallelism));
      return this;
    }

    public Builder bloomIndexPruneByRanges(boolean pruneRanges) {
      props.setProperty(BLOOM_INDEX_PRUNE_BY_RANGES_PROP, String.valueOf(pruneRanges));
      return this;
    }

    public Builder bloomIndexUseCaching(boolean useCaching) {
      props.setProperty(BLOOM_INDEX_USE_CACHING_PROP, String.valueOf(useCaching));
      return this;
    }

    public Builder bloomIndexTreebasedFilter(boolean useTreeFilter) {
      props.setProperty(BLOOM_INDEX_TREE_BASED_FILTER_PROP, String.valueOf(useTreeFilter));
      return this;
    }

    public Builder bloomIndexBucketizedChecking(boolean bucketizedChecking) {
      props.setProperty(BLOOM_INDEX_BUCKETIZED_CHECKING_PROP, String.valueOf(bucketizedChecking));
      return this;
    }

    public Builder bloomIndexKeysPerBucket(int keysPerBucket) {
      props.setProperty(BLOOM_INDEX_KEYS_PER_BUCKET_PROP, String.valueOf(keysPerBucket));
      return this;
    }

    public Builder withBloomIndexInputStorageLevel(String level) {
      props.setProperty(BLOOM_INDEX_INPUT_STORAGE_LEVEL, level);
      return this;
    }

    public HoodieIndexConfig build() {
      HoodieIndexConfig config = new HoodieIndexConfig(props);
      setDefaultOnCondition(props, !props.containsKey(INDEX_TYPE_PROP), INDEX_TYPE_PROP,
          DEFAULT_INDEX_TYPE);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_FILTER_NUM_ENTRIES),
          BLOOM_FILTER_NUM_ENTRIES, DEFAULT_BLOOM_FILTER_NUM_ENTRIES);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_FILTER_FPP), BLOOM_FILTER_FPP,
          DEFAULT_BLOOM_FILTER_FPP);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_INDEX_PARALLELISM_PROP),
          BLOOM_INDEX_PARALLELISM_PROP, DEFAULT_BLOOM_INDEX_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_INDEX_PRUNE_BY_RANGES_PROP),
          BLOOM_INDEX_PRUNE_BY_RANGES_PROP, DEFAULT_BLOOM_INDEX_PRUNE_BY_RANGES);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_INDEX_USE_CACHING_PROP),
          BLOOM_INDEX_USE_CACHING_PROP, DEFAULT_BLOOM_INDEX_USE_CACHING);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_INDEX_INPUT_STORAGE_LEVEL),
          BLOOM_INDEX_INPUT_STORAGE_LEVEL, DEFAULT_BLOOM_INDEX_INPUT_STORAGE_LEVEL);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_INDEX_TREE_BASED_FILTER_PROP),
          BLOOM_INDEX_TREE_BASED_FILTER_PROP, DEFAULT_BLOOM_INDEX_TREE_BASED_FILTER);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_INDEX_BUCKETIZED_CHECKING_PROP),
          BLOOM_INDEX_BUCKETIZED_CHECKING_PROP, DEFAULT_BLOOM_INDEX_BUCKETIZED_CHECKING);
      setDefaultOnCondition(props, !props.containsKey(BLOOM_INDEX_KEYS_PER_BUCKET_PROP),
          BLOOM_INDEX_KEYS_PER_BUCKET_PROP, DEFAULT_BLOOM_INDEX_KEYS_PER_BUCKET);
      // Throws IllegalArgumentException if the value set is not a known Hoodie Index Type
      HoodieIndex.IndexType.valueOf(props.getProperty(INDEX_TYPE_PROP));
      return config;
    }
  }
}
