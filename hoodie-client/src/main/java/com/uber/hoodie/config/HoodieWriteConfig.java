/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.config;

import com.google.common.base.Preconditions;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.compact.strategy.CompactionStrategy;
import com.uber.hoodie.metrics.MetricsReporterType;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;
import org.apache.spark.storage.StorageLevel;

/**
 * Class storing configs for the {@link com.uber.hoodie.HoodieWriteClient}
 */
@Immutable
public class HoodieWriteConfig extends DefaultHoodieConfig {

  public static final String TABLE_NAME = "hoodie.table.name";
  private static final String BASE_PATH_PROP = "hoodie.base.path";
  private static final String AVRO_SCHEMA = "hoodie.avro.schema";
  private static final String DEFAULT_PARALLELISM = "1500";
  private static final String INSERT_PARALLELISM = "hoodie.insert.shuffle.parallelism";
  private static final String BULKINSERT_PARALLELISM = "hoodie.bulkinsert.shuffle.parallelism";
  private static final String UPSERT_PARALLELISM = "hoodie.upsert.shuffle.parallelism";
  private static final String WRITE_BUFFER_LIMIT_BYTES = "hoodie.write.buffer.limit.bytes";
  private static final String DEFAULT_WRITE_BUFFER_LIMIT_BYTES = String.valueOf(4 * 1024 * 1024);
  private static final String COMBINE_BEFORE_INSERT_PROP = "hoodie.combine.before.insert";
  private static final String DEFAULT_COMBINE_BEFORE_INSERT = "false";
  private static final String COMBINE_BEFORE_UPSERT_PROP = "hoodie.combine.before.upsert";
  private static final String DEFAULT_COMBINE_BEFORE_UPSERT = "true";
  private static final String WRITE_STATUS_STORAGE_LEVEL = "hoodie.write.status.storage.level";
  private static final String DEFAULT_WRITE_STATUS_STORAGE_LEVEL = "MEMORY_AND_DISK_SER";
  private static final String HOODIE_AUTO_COMMIT_PROP = "hoodie.auto.commit";
  private static final String DEFAULT_HOODIE_AUTO_COMMIT = "true";
  private static final String HOODIE_ASSUME_DATE_PARTITIONING_PROP =
      "hoodie.assume.date" + ".partitioning";
  private static final String DEFAULT_ASSUME_DATE_PARTITIONING = "false";
  private static final String HOODIE_WRITE_STATUS_CLASS_PROP = "hoodie.writestatus.class";
  private static final String DEFAULT_HOODIE_WRITE_STATUS_CLASS = WriteStatus.class.getName();
  private static final String HOODIE_COPYONWRITE_USE_TEMP_FOLDER_CREATE =
      "hoodie.copyonwrite.use" + ".temp.folder.for.create";
  private static final String DEFAULT_HOODIE_COPYONWRITE_USE_TEMP_FOLDER_CREATE = "false";
  private static final String HOODIE_COPYONWRITE_USE_TEMP_FOLDER_MERGE =
      "hoodie.copyonwrite.use" + ".temp.folder.for.merge";
  private static final String DEFAULT_HOODIE_COPYONWRITE_USE_TEMP_FOLDER_MERGE = "false";
  private static final String FINALIZE_WRITE_PARALLELISM = "hoodie.finalize.write.parallelism";
  private static final String DEFAULT_FINALIZE_WRITE_PARALLELISM = DEFAULT_PARALLELISM;

  private HoodieWriteConfig(Properties props) {
    super(props);
  }

  public static HoodieWriteConfig.Builder newBuilder() {
    return new Builder();
  }

  /**
   * base properties
   **/
  public String getBasePath() {
    return props.getProperty(BASE_PATH_PROP);
  }

  public String getSchema() {
    return props.getProperty(AVRO_SCHEMA);
  }

  public String getTableName() {
    return props.getProperty(TABLE_NAME);
  }

  public Boolean shouldAutoCommit() {
    return Boolean.parseBoolean(props.getProperty(HOODIE_AUTO_COMMIT_PROP));
  }

  public Boolean shouldAssumeDatePartitioning() {
    return Boolean.parseBoolean(props.getProperty(HOODIE_ASSUME_DATE_PARTITIONING_PROP));
  }

  public int getBulkInsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(BULKINSERT_PARALLELISM));
  }

  public int getInsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(INSERT_PARALLELISM));
  }

  public int getUpsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(UPSERT_PARALLELISM));
  }

  public int getWriteBufferLimitBytes() {
    return Integer
        .parseInt(props.getProperty(WRITE_BUFFER_LIMIT_BYTES, DEFAULT_WRITE_BUFFER_LIMIT_BYTES));
  }

  public boolean shouldCombineBeforeInsert() {
    return Boolean.parseBoolean(props.getProperty(COMBINE_BEFORE_INSERT_PROP));
  }

  public boolean shouldCombineBeforeUpsert() {
    return Boolean.parseBoolean(props.getProperty(COMBINE_BEFORE_UPSERT_PROP));
  }

  public StorageLevel getWriteStatusStorageLevel() {
    return StorageLevel.fromString(props.getProperty(WRITE_STATUS_STORAGE_LEVEL));
  }

  public String getWriteStatusClassName() {
    return props.getProperty(HOODIE_WRITE_STATUS_CLASS_PROP);
  }

  public boolean shouldUseTempFolderForCopyOnWriteForCreate() {
    return Boolean.parseBoolean(props.getProperty(HOODIE_COPYONWRITE_USE_TEMP_FOLDER_CREATE));
  }

  public boolean shouldUseTempFolderForCopyOnWriteForMerge() {
    return Boolean.parseBoolean(props.getProperty(HOODIE_COPYONWRITE_USE_TEMP_FOLDER_MERGE));
  }

  public boolean shouldUseTempFolderForCopyOnWrite() {
    return shouldUseTempFolderForCopyOnWriteForCreate()
        || shouldUseTempFolderForCopyOnWriteForMerge();
  }

  public int getFinalizeWriteParallelism() {
    return Integer.parseInt(props.getProperty(FINALIZE_WRITE_PARALLELISM));
  }

  /**
   * compaction properties
   **/
  public HoodieCleaningPolicy getCleanerPolicy() {
    return HoodieCleaningPolicy
        .valueOf(props.getProperty(HoodieCompactionConfig.CLEANER_POLICY_PROP));
  }

  public int getCleanerFileVersionsRetained() {
    return Integer
        .parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_FILE_VERSIONS_RETAINED_PROP));
  }

  public int getCleanerCommitsRetained() {
    return Integer
        .parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP));
  }

  public int getMaxCommitsToKeep() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP));
  }

  public int getMinCommitsToKeep() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP));
  }

  public int getParquetSmallFileLimit() {
    return Integer
        .parseInt(props.getProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT_BYTES));
  }

  public int getCopyOnWriteInsertSplitSize() {
    return Integer
        .parseInt(props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE));
  }

  public int getCopyOnWriteRecordSizeEstimate() {
    return Integer.parseInt(
        props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE));
  }

  public boolean shouldAutoTuneInsertSplits() {
    return Boolean.parseBoolean(
        props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS));
  }

  public int getCleanerParallelism() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_PARALLELISM));
  }

  public boolean isAutoClean() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.AUTO_CLEAN_PROP));
  }

  public boolean isInlineCompaction() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_PROP));
  }

  public int getInlineCompactDeltaCommitMax() {
    return Integer
        .parseInt(props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS_PROP));
  }

  public CompactionStrategy getCompactionStrategy() {
    return ReflectionUtils
        .loadClass(props.getProperty(HoodieCompactionConfig.COMPACTION_STRATEGY_PROP));
  }

  public Long getTargetIOPerCompactionInMB() {
    return Long
        .parseLong(props.getProperty(HoodieCompactionConfig.TARGET_IO_PER_COMPACTION_IN_MB_PROP));
  }

  public Boolean getCompactionLazyBlockReadEnabled() {
    return Boolean
        .valueOf(props.getProperty(HoodieCompactionConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP));
  }

  public Boolean getCompactionReverseLogReadEnabled() {
    return Boolean.valueOf(
        props.getProperty(HoodieCompactionConfig.COMPACTION_REVERSE_LOG_READ_ENABLED_PROP));
  }

  public String getPayloadClass() {
    return props.getProperty(HoodieCompactionConfig.PAYLOAD_CLASS_PROP);
  }

  /**
   * index properties
   **/
  public HoodieIndex.IndexType getIndexType() {
    return HoodieIndex.IndexType.valueOf(props.getProperty(HoodieIndexConfig.INDEX_TYPE_PROP));
  }

  public int getBloomFilterNumEntries() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES));
  }

  public double getBloomFilterFPP() {
    return Double.parseDouble(props.getProperty(HoodieIndexConfig.BLOOM_FILTER_FPP));
  }

  public String getHbaseZkQuorum() {
    return props.getProperty(HoodieIndexConfig.HBASE_ZKQUORUM_PROP);
  }

  public int getHbaseZkPort() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.HBASE_ZKPORT_PROP));
  }

  public String getHbaseTableName() {
    return props.getProperty(HoodieIndexConfig.HBASE_TABLENAME_PROP);
  }

  public int getHbaseIndexGetBatchSize() {
    return Integer.valueOf(props.getProperty(HoodieIndexConfig.HBASE_GET_BATCH_SIZE_PROP));
  }

  public int getHbaseIndexPutBatchSize() {
    return Integer.valueOf(props.getProperty(HoodieIndexConfig.HBASE_PUT_BATCH_SIZE_PROP));
  }

  public int getBloomIndexParallelism() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_PARALLELISM_PROP));
  }

  public boolean getBloomIndexPruneByRanges() {
    return Boolean
        .parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_PRUNE_BY_RANGES_PROP));
  }

  public boolean getBloomIndexUseCaching() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_USE_CACHING_PROP));
  }

  public int getNumBucketsPerPartition() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.BUCKETED_INDEX_NUM_BUCKETS_PROP));
  }

  public StorageLevel getBloomIndexInputStorageLevel() {
    return StorageLevel
        .fromString(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_INPUT_STORAGE_LEVEL));
  }

  /**
   * storage properties
   **/
  public long getParquetMaxFileSize() {
    return Long.parseLong(props.getProperty(HoodieStorageConfig.PARQUET_FILE_MAX_BYTES));
  }

  public int getParquetBlockSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE_BYTES));
  }

  public int getParquetPageSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.PARQUET_PAGE_SIZE_BYTES));
  }

  public int getLogFileDataBlockMaxSize() {
    return Integer
        .parseInt(props.getProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES));
  }

  public int getLogFileMaxSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.LOGFILE_SIZE_MAX_BYTES));
  }

  public double getParquetCompressionRatio() {
    return Double.valueOf(props.getProperty(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO));
  }

  public double getLogFileToParquetCompressionRatio() {
    return Double.valueOf(props.getProperty(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO));
  }

  /**
   * metrics properties
   **/
  public boolean isMetricsOn() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsConfig.METRICS_ON));
  }

  public MetricsReporterType getMetricsReporterType() {
    return MetricsReporterType
        .valueOf(props.getProperty(HoodieMetricsConfig.METRICS_REPORTER_TYPE));
  }

  public String getGraphiteServerHost() {
    return props.getProperty(HoodieMetricsConfig.GRAPHITE_SERVER_HOST);
  }

  public int getGraphiteServerPort() {
    return Integer.parseInt(props.getProperty(HoodieMetricsConfig.GRAPHITE_SERVER_PORT));
  }

  public String getGraphiteMetricPrefix() {
    return props.getProperty(HoodieMetricsConfig.GRAPHITE_METRIC_PREFIX);
  }

  /**
   * memory configs
   */
  public Double getMaxMemoryFractionPerPartitionMerge() {
    return Double.valueOf(props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_MERGE_PROP));
  }

  public Double getMaxMemoryFractionPerCompaction() {
    return Double
        .valueOf(
            props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP));
  }

  public Long getMaxMemoryPerPartitionMerge() {
    return Long.valueOf(props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE_PROP));
  }

  public Long getMaxMemoryPerCompaction() {
    return Long
        .valueOf(
            props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION_PROP));
  }

  public int getMaxDFSStreamBufferSize() {
    return Integer
        .valueOf(
            props.getProperty(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP));
  }

  public String getSpillableMapBasePath() {
    return props.getProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH_PROP);
  }

  public static class Builder {

    private final Properties props = new Properties();
    private boolean isIndexConfigSet = false;
    private boolean isStorageConfigSet = false;
    private boolean isCompactionConfigSet = false;
    private boolean isMetricsConfigSet = false;
    private boolean isAutoCommit = true;
    private boolean isMemoryConfigSet = false;

    public Builder fromFile(File propertiesFile) throws IOException {
      FileReader reader = new FileReader(propertiesFile);
      try {
        this.props.load(reader);
        return this;
      } finally {
        reader.close();
      }
    }

    public Builder fromInputStream(InputStream inputStream) throws IOException {
      try {
        this.props.load(inputStream);
        return this;
      } finally {
        inputStream.close();
      }
    }

    public Builder withProps(Map kvprops) {
      props.putAll(kvprops);
      return this;
    }

    public Builder withPath(String basePath) {
      props.setProperty(BASE_PATH_PROP, basePath);
      return this;
    }

    public Builder withSchema(String schemaStr) {
      props.setProperty(AVRO_SCHEMA, schemaStr);
      return this;
    }

    public Builder forTable(String tableName) {
      props.setProperty(TABLE_NAME, tableName);
      return this;
    }

    public Builder withBulkInsertParallelism(int bulkInsertParallelism) {
      props.setProperty(BULKINSERT_PARALLELISM, String.valueOf(bulkInsertParallelism));
      return this;
    }

    public Builder withParallelism(int insertShuffleParallelism, int upsertShuffleParallelism) {
      props.setProperty(INSERT_PARALLELISM, String.valueOf(insertShuffleParallelism));
      props.setProperty(UPSERT_PARALLELISM, String.valueOf(upsertShuffleParallelism));
      return this;
    }

    public Builder withWriteBufferLimitBytes(int writeBufferLimit) {
      props.setProperty(WRITE_BUFFER_LIMIT_BYTES, String.valueOf(writeBufferLimit));
      return this;
    }

    public Builder combineInput(boolean onInsert, boolean onUpsert) {
      props.setProperty(COMBINE_BEFORE_INSERT_PROP, String.valueOf(onInsert));
      props.setProperty(COMBINE_BEFORE_UPSERT_PROP, String.valueOf(onUpsert));
      return this;
    }

    public Builder withWriteStatusStorageLevel(String level) {
      props.setProperty(WRITE_STATUS_STORAGE_LEVEL, level);
      return this;
    }

    public Builder withIndexConfig(HoodieIndexConfig indexConfig) {
      props.putAll(indexConfig.getProps());
      isIndexConfigSet = true;
      return this;
    }

    public Builder withStorageConfig(HoodieStorageConfig storageConfig) {
      props.putAll(storageConfig.getProps());
      isStorageConfigSet = true;
      return this;
    }

    public Builder withCompactionConfig(HoodieCompactionConfig compactionConfig) {
      props.putAll(compactionConfig.getProps());
      isCompactionConfigSet = true;
      return this;
    }

    public Builder withMetricsConfig(HoodieMetricsConfig metricsConfig) {
      props.putAll(metricsConfig.getProps());
      isMetricsConfigSet = true;
      return this;
    }

    public Builder withMemoryConfig(HoodieMemoryConfig memoryConfig) {
      props.putAll(memoryConfig.getProps());
      isMemoryConfigSet = true;
      return this;
    }

    public Builder withAutoCommit(boolean autoCommit) {
      props.setProperty(HOODIE_AUTO_COMMIT_PROP, String.valueOf(autoCommit));
      return this;
    }

    public Builder withAssumeDatePartitioning(boolean assumeDatePartitioning) {
      props.setProperty(HOODIE_ASSUME_DATE_PARTITIONING_PROP,
          String.valueOf(assumeDatePartitioning));
      return this;
    }

    public Builder withWriteStatusClass(Class<? extends WriteStatus> writeStatusClass) {
      props.setProperty(HOODIE_WRITE_STATUS_CLASS_PROP, writeStatusClass.getName());
      return this;
    }

    public Builder withUseTempFolderCopyOnWriteForCreate(
        boolean shouldUseTempFolderCopyOnWriteForCreate) {
      props.setProperty(HOODIE_COPYONWRITE_USE_TEMP_FOLDER_CREATE,
          String.valueOf(shouldUseTempFolderCopyOnWriteForCreate));
      return this;
    }

    public Builder withUseTempFolderCopyOnWriteForMerge(
        boolean shouldUseTempFolderCopyOnWriteForMerge) {
      props.setProperty(HOODIE_COPYONWRITE_USE_TEMP_FOLDER_MERGE,
          String.valueOf(shouldUseTempFolderCopyOnWriteForMerge));
      return this;
    }

    public Builder withFinalizeWriteParallelism(int parallelism) {
      props.setProperty(FINALIZE_WRITE_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public HoodieWriteConfig build() {
      HoodieWriteConfig config = new HoodieWriteConfig(props);
      // Check for mandatory properties
      Preconditions.checkArgument(config.getBasePath() != null);
      setDefaultOnCondition(props, !props.containsKey(INSERT_PARALLELISM), INSERT_PARALLELISM,
          DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(BULKINSERT_PARALLELISM),
          BULKINSERT_PARALLELISM, DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(UPSERT_PARALLELISM), UPSERT_PARALLELISM,
          DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(COMBINE_BEFORE_INSERT_PROP),
          COMBINE_BEFORE_INSERT_PROP, DEFAULT_COMBINE_BEFORE_INSERT);
      setDefaultOnCondition(props, !props.containsKey(COMBINE_BEFORE_UPSERT_PROP),
          COMBINE_BEFORE_UPSERT_PROP, DEFAULT_COMBINE_BEFORE_UPSERT);
      setDefaultOnCondition(props, !props.containsKey(WRITE_STATUS_STORAGE_LEVEL),
          WRITE_STATUS_STORAGE_LEVEL, DEFAULT_WRITE_STATUS_STORAGE_LEVEL);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_AUTO_COMMIT_PROP),
          HOODIE_AUTO_COMMIT_PROP, DEFAULT_HOODIE_AUTO_COMMIT);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_ASSUME_DATE_PARTITIONING_PROP),
          HOODIE_ASSUME_DATE_PARTITIONING_PROP, DEFAULT_ASSUME_DATE_PARTITIONING);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_WRITE_STATUS_CLASS_PROP),
          HOODIE_WRITE_STATUS_CLASS_PROP, DEFAULT_HOODIE_WRITE_STATUS_CLASS);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_COPYONWRITE_USE_TEMP_FOLDER_CREATE),
          HOODIE_COPYONWRITE_USE_TEMP_FOLDER_CREATE,
          DEFAULT_HOODIE_COPYONWRITE_USE_TEMP_FOLDER_CREATE);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_COPYONWRITE_USE_TEMP_FOLDER_MERGE),
          HOODIE_COPYONWRITE_USE_TEMP_FOLDER_MERGE,
          DEFAULT_HOODIE_COPYONWRITE_USE_TEMP_FOLDER_MERGE);
      setDefaultOnCondition(props, !props.containsKey(FINALIZE_WRITE_PARALLELISM),
          FINALIZE_WRITE_PARALLELISM, DEFAULT_FINALIZE_WRITE_PARALLELISM);

      // Make sure the props is propagated
      setDefaultOnCondition(props, !isIndexConfigSet,
          HoodieIndexConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isStorageConfigSet,
          HoodieStorageConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isCompactionConfigSet,
          HoodieCompactionConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMetricsConfigSet,
          HoodieMetricsConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMemoryConfigSet,
          HoodieMemoryConfig.newBuilder().fromProperties(props).build());
      return config;
    }
  }
}
