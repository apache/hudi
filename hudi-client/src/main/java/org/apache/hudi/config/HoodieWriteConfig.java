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

import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.compact.strategy.CompactionStrategy;
import org.apache.hudi.metrics.MetricsReporterType;

import com.google.common.base.Preconditions;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.storage.StorageLevel;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Class storing configs for the {@link HoodieWriteClient}.
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
  private static final String DELETE_PARALLELISM = "hoodie.delete.shuffle.parallelism";
  private static final String DEFAULT_ROLLBACK_PARALLELISM = "100";
  private static final String ROLLBACK_PARALLELISM = "hoodie.rollback.parallelism";
  private static final String WRITE_BUFFER_LIMIT_BYTES = "hoodie.write.buffer.limit.bytes";
  private static final String DEFAULT_WRITE_BUFFER_LIMIT_BYTES = String.valueOf(4 * 1024 * 1024);
  private static final String COMBINE_BEFORE_INSERT_PROP = "hoodie.combine.before.insert";
  private static final String DEFAULT_COMBINE_BEFORE_INSERT = "false";
  private static final String COMBINE_BEFORE_UPSERT_PROP = "hoodie.combine.before.upsert";
  private static final String DEFAULT_COMBINE_BEFORE_UPSERT = "true";
  private static final String COMBINE_BEFORE_DELETE_PROP = "hoodie.combine.before.delete";
  private static final String DEFAULT_COMBINE_BEFORE_DELETE = "true";
  private static final String WRITE_STATUS_STORAGE_LEVEL = "hoodie.write.status.storage.level";
  private static final String DEFAULT_WRITE_STATUS_STORAGE_LEVEL = "MEMORY_AND_DISK_SER";
  private static final String HOODIE_AUTO_COMMIT_PROP = "hoodie.auto.commit";
  private static final String DEFAULT_HOODIE_AUTO_COMMIT = "true";
  private static final String HOODIE_ASSUME_DATE_PARTITIONING_PROP = "hoodie.assume.date" + ".partitioning";
  private static final String DEFAULT_ASSUME_DATE_PARTITIONING = "false";
  private static final String HOODIE_WRITE_STATUS_CLASS_PROP = "hoodie.writestatus.class";
  private static final String DEFAULT_HOODIE_WRITE_STATUS_CLASS = WriteStatus.class.getName();
  private static final String FINALIZE_WRITE_PARALLELISM = "hoodie.finalize.write.parallelism";
  private static final String DEFAULT_FINALIZE_WRITE_PARALLELISM = DEFAULT_PARALLELISM;

  private static final String EMBEDDED_TIMELINE_SERVER_ENABLED = "hoodie.embed.timeline.server";
  private static final String DEFAULT_EMBEDDED_TIMELINE_SERVER_ENABLED = "false";

  private static final String FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP = "hoodie.fail.on.timeline.archiving";
  private static final String DEFAULT_FAIL_ON_TIMELINE_ARCHIVING_ENABLED = "true";
  // time between successive attempts to ensure written data's metadata is consistent on storage
  private static final String INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP =
      "hoodie.consistency.check.initial_interval_ms";
  private static long DEFAULT_INITIAL_CONSISTENCY_CHECK_INTERVAL_MS = 2000L;

  // max interval time
  private static final String MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = "hoodie.consistency.check.max_interval_ms";
  private static long DEFAULT_MAX_CONSISTENCY_CHECK_INTERVAL_MS = 300000L;

  // maximum number of checks, for consistency of written data. Will wait upto 256 Secs
  private static final String MAX_CONSISTENCY_CHECKS_PROP = "hoodie.consistency.check.max_checks";
  private static int DEFAULT_MAX_CONSISTENCY_CHECKS = 7;

  private ConsistencyGuardConfig consistencyGuardConfig;

  // Hoodie Write Client transparently rewrites File System View config when embedded mode is enabled
  // We keep track of original config and rewritten config
  private final FileSystemViewStorageConfig clientSpecifiedViewStorageConfig;
  private FileSystemViewStorageConfig viewStorageConfig;

  private HoodieWriteConfig(Properties props) {
    super(props);
    Properties newProps = new Properties();
    newProps.putAll(props);
    this.consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().fromProperties(newProps).build();
    this.clientSpecifiedViewStorageConfig = FileSystemViewStorageConfig.newBuilder().fromProperties(newProps).build();
    this.viewStorageConfig = clientSpecifiedViewStorageConfig;
  }

  public static HoodieWriteConfig.Builder newBuilder() {
    return new Builder();
  }

  /**
   * base properties.
   */
  public String getBasePath() {
    return props.getProperty(BASE_PATH_PROP);
  }

  public String getSchema() {
    return props.getProperty(AVRO_SCHEMA);
  }

  public void setSchema(String schemaStr) {
    props.setProperty(AVRO_SCHEMA, schemaStr);
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

  public int getDeleteShuffleParallelism() {
    return Integer.parseInt(props.getProperty(DELETE_PARALLELISM));
  }

  public int getRollbackParallelism() {
    return Integer.parseInt(props.getProperty(ROLLBACK_PARALLELISM));
  }

  public int getWriteBufferLimitBytes() {
    return Integer.parseInt(props.getProperty(WRITE_BUFFER_LIMIT_BYTES, DEFAULT_WRITE_BUFFER_LIMIT_BYTES));
  }

  public boolean shouldCombineBeforeInsert() {
    return Boolean.parseBoolean(props.getProperty(COMBINE_BEFORE_INSERT_PROP));
  }

  public boolean shouldCombineBeforeUpsert() {
    return Boolean.parseBoolean(props.getProperty(COMBINE_BEFORE_UPSERT_PROP));
  }

  public boolean shouldCombineBeforeDelete() {
    return Boolean.parseBoolean(props.getProperty(COMBINE_BEFORE_DELETE_PROP));
  }

  public StorageLevel getWriteStatusStorageLevel() {
    return StorageLevel.fromString(props.getProperty(WRITE_STATUS_STORAGE_LEVEL));
  }

  public String getWriteStatusClassName() {
    return props.getProperty(HOODIE_WRITE_STATUS_CLASS_PROP);
  }

  public int getFinalizeWriteParallelism() {
    return Integer.parseInt(props.getProperty(FINALIZE_WRITE_PARALLELISM));
  }

  public boolean isEmbeddedTimelineServerEnabled() {
    return Boolean.parseBoolean(props.getProperty(EMBEDDED_TIMELINE_SERVER_ENABLED));
  }

  public boolean isFailOnTimelineArchivingEnabled() {
    return Boolean.parseBoolean(props.getProperty(FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP));
  }

  public int getMaxConsistencyChecks() {
    return Integer.parseInt(props.getProperty(MAX_CONSISTENCY_CHECKS_PROP));
  }

  public int getInitialConsistencyCheckIntervalMs() {
    return Integer.parseInt(props.getProperty(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP));
  }

  public int getMaxConsistencyCheckIntervalMs() {
    return Integer.parseInt(props.getProperty(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP));
  }

  /**
   * compaction properties.
   */
  public HoodieCleaningPolicy getCleanerPolicy() {
    return HoodieCleaningPolicy.valueOf(props.getProperty(HoodieCompactionConfig.CLEANER_POLICY_PROP));
  }

  public int getCleanerFileVersionsRetained() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_FILE_VERSIONS_RETAINED_PROP));
  }

  public int getCleanerCommitsRetained() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP));
  }

  public int getMaxCommitsToKeep() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP));
  }

  public int getMinCommitsToKeep() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP));
  }

  public int getParquetSmallFileLimit() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT_BYTES));
  }

  public int getCopyOnWriteInsertSplitSize() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE));
  }

  public int getCopyOnWriteRecordSizeEstimate() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE));
  }

  public boolean shouldAutoTuneInsertSplits() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS));
  }

  public int getCleanerParallelism() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_PARALLELISM));
  }

  public boolean isAutoClean() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.AUTO_CLEAN_PROP));
  }

  public boolean incrementalCleanerModeEnabled() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.CLEANER_INCREMENTAL_MODE));
  }

  public boolean isInlineCompaction() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_PROP));
  }

  public int getInlineCompactDeltaCommitMax() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS_PROP));
  }

  public CompactionStrategy getCompactionStrategy() {
    return ReflectionUtils.loadClass(props.getProperty(HoodieCompactionConfig.COMPACTION_STRATEGY_PROP));
  }

  public Long getTargetIOPerCompactionInMB() {
    return Long.parseLong(props.getProperty(HoodieCompactionConfig.TARGET_IO_PER_COMPACTION_IN_MB_PROP));
  }

  public Boolean getCompactionLazyBlockReadEnabled() {
    return Boolean.valueOf(props.getProperty(HoodieCompactionConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP));
  }

  public Boolean getCompactionReverseLogReadEnabled() {
    return Boolean.valueOf(props.getProperty(HoodieCompactionConfig.COMPACTION_REVERSE_LOG_READ_ENABLED_PROP));
  }

  public String getPayloadClass() {
    return props.getProperty(HoodieCompactionConfig.PAYLOAD_CLASS_PROP);
  }

  public int getTargetPartitionsPerDayBasedCompaction() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP));
  }

  public int getCommitArchivalBatchSize() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.COMMITS_ARCHIVAL_BATCH_SIZE_PROP));
  }

  /**
   * index properties.
   */
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
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_ZKQUORUM_PROP);
  }

  public int getHbaseZkPort() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HBASE_ZKPORT_PROP));
  }

  public String getHBaseZkZnodeParent() {
    return props.getProperty(HoodieIndexConfig.HBASE_ZK_ZNODEPARENT);
  }

  public String getHbaseTableName() {
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_TABLENAME_PROP);
  }

  public int getHbaseIndexGetBatchSize() {
    return Integer.valueOf(props.getProperty(HoodieHBaseIndexConfig.HBASE_GET_BATCH_SIZE_PROP));
  }

  public int getHbaseIndexPutBatchSize() {
    return Integer.valueOf(props.getProperty(HoodieHBaseIndexConfig.HBASE_PUT_BATCH_SIZE_PROP));
  }

  public Boolean getHbaseIndexPutBatchSizeAutoCompute() {
    return Boolean.valueOf(props.getProperty(HoodieHBaseIndexConfig.HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP));
  }

  public String getHBaseQPSResourceAllocatorClass() {
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_INDEX_QPS_ALLOCATOR_CLASS);
  }

  public String getHBaseQPSZKnodePath() {
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_ZK_PATH_QPS_ROOT);
  }

  public String getHBaseZkZnodeSessionTimeout() {
    return props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS);
  }

  public String getHBaseZkZnodeConnectionTimeout() {
    return props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS);
  }

  public boolean getHBaseIndexShouldComputeQPSDynamically() {
    return Boolean.valueOf(props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY));
  }

  public int getHBaseIndexDesiredPutsTime() {
    return Integer.valueOf(props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS));
  }

  /**
   * Fraction of the global share of QPS that should be allocated to this job. Let's say there are 3 jobs which have
   * input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then this fraction for
   * the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively.
   */
  public float getHbaseIndexQPSFraction() {
    return Float.parseFloat(props.getProperty(HoodieHBaseIndexConfig.HBASE_QPS_FRACTION_PROP));
  }

  public float getHBaseIndexMinQPSFraction() {
    return Float.parseFloat(props.getProperty(HoodieHBaseIndexConfig.HBASE_MIN_QPS_FRACTION_PROP));
  }

  public float getHBaseIndexMaxQPSFraction() {
    return Float.parseFloat(props.getProperty(HoodieHBaseIndexConfig.HBASE_MAX_QPS_FRACTION_PROP));
  }

  /**
   * This should be same across various jobs. This is intended to limit the aggregate QPS generated across various
   * Hoodie jobs to an Hbase Region Server
   */
  public int getHbaseIndexMaxQPSPerRegionServer() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HBASE_MAX_QPS_PER_REGION_SERVER_PROP));
  }

  public int getBloomIndexParallelism() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_PARALLELISM_PROP));
  }

  public boolean getBloomIndexPruneByRanges() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_PRUNE_BY_RANGES_PROP));
  }

  public boolean getBloomIndexUseCaching() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_USE_CACHING_PROP));
  }

  public boolean useBloomIndexTreebasedFilter() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_TREE_BASED_FILTER_PROP));
  }

  public boolean useBloomIndexBucketizedChecking() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_BUCKETIZED_CHECKING_PROP));
  }

  public int getBloomIndexKeysPerBucket() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_KEYS_PER_BUCKET_PROP));
  }

  public StorageLevel getBloomIndexInputStorageLevel() {
    return StorageLevel.fromString(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_INPUT_STORAGE_LEVEL));
  }

  /**
   * storage properties.
   */
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
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES));
  }

  public int getLogFileMaxSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.LOGFILE_SIZE_MAX_BYTES));
  }

  public double getParquetCompressionRatio() {
    return Double.valueOf(props.getProperty(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO));
  }

  public CompressionCodecName getParquetCompressionCodec() {
    return CompressionCodecName.fromConf(props.getProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC));
  }

  public double getLogFileToParquetCompressionRatio() {
    return Double.valueOf(props.getProperty(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO));
  }

  /**
   * metrics properties.
   */
  public boolean isMetricsOn() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsConfig.METRICS_ON));
  }

  public MetricsReporterType getMetricsReporterType() {
    return MetricsReporterType.valueOf(props.getProperty(HoodieMetricsConfig.METRICS_REPORTER_TYPE));
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

  public String getJmxHost() {
    return props.getProperty(HoodieMetricsConfig.JMX_HOST);
  }

  public int getJmxPort() {
    return Integer.parseInt(props.getProperty(HoodieMetricsConfig.JMX_PORT));
  }

  /**
   * memory configs.
   */
  public Double getMaxMemoryFractionPerPartitionMerge() {
    return Double.valueOf(props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_MERGE_PROP));
  }

  public Double getMaxMemoryFractionPerCompaction() {
    return Double.valueOf(props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP));
  }

  public Long getMaxMemoryPerPartitionMerge() {
    return Long.valueOf(props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE_PROP));
  }

  public Long getMaxMemoryPerCompaction() {
    return Long.valueOf(props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION_PROP));
  }

  public int getMaxDFSStreamBufferSize() {
    return Integer.valueOf(props.getProperty(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP));
  }

  public String getSpillableMapBasePath() {
    return props.getProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH_PROP);
  }

  public double getWriteStatusFailureFraction() {
    return Double.valueOf(props.getProperty(HoodieMemoryConfig.WRITESTATUS_FAILURE_FRACTION_PROP));
  }

  public ConsistencyGuardConfig getConsistencyGuardConfig() {
    return consistencyGuardConfig;
  }

  public void setConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
    this.consistencyGuardConfig = consistencyGuardConfig;
  }

  public FileSystemViewStorageConfig getViewStorageConfig() {
    return viewStorageConfig;
  }

  public void setViewStorageConfig(FileSystemViewStorageConfig viewStorageConfig) {
    this.viewStorageConfig = viewStorageConfig;
  }

  public void resetViewStorageConfig() {
    this.setViewStorageConfig(getClientSpecifiedViewStorageConfig());
  }

  public FileSystemViewStorageConfig getClientSpecifiedViewStorageConfig() {
    return clientSpecifiedViewStorageConfig;
  }

  public static class Builder {

    private final Properties props = new Properties();
    private boolean isIndexConfigSet = false;
    private boolean isStorageConfigSet = false;
    private boolean isCompactionConfigSet = false;
    private boolean isMetricsConfigSet = false;
    private boolean isMemoryConfigSet = false;
    private boolean isViewConfigSet = false;
    private boolean isConsistencyGuardSet = false;

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

    public Builder withRollbackParallelism(int rollbackParallelism) {
      props.setProperty(ROLLBACK_PARALLELISM, String.valueOf(rollbackParallelism));
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
      props.setProperty(HOODIE_ASSUME_DATE_PARTITIONING_PROP, String.valueOf(assumeDatePartitioning));
      return this;
    }

    public Builder withWriteStatusClass(Class<? extends WriteStatus> writeStatusClass) {
      props.setProperty(HOODIE_WRITE_STATUS_CLASS_PROP, writeStatusClass.getName());
      return this;
    }

    public Builder withFileSystemViewConfig(FileSystemViewStorageConfig viewStorageConfig) {
      props.putAll(viewStorageConfig.getProps());
      isViewConfigSet = true;
      return this;
    }

    public Builder withConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
      props.putAll(consistencyGuardConfig.getProps());
      isConsistencyGuardSet = true;
      return this;
    }

    public Builder withFinalizeWriteParallelism(int parallelism) {
      props.setProperty(FINALIZE_WRITE_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withEmbeddedTimelineServerEnabled(boolean enabled) {
      props.setProperty(EMBEDDED_TIMELINE_SERVER_ENABLED, String.valueOf(enabled));
      return this;
    }

    public HoodieWriteConfig build() {
      // Check for mandatory properties
      setDefaultOnCondition(props, !props.containsKey(INSERT_PARALLELISM), INSERT_PARALLELISM, DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(BULKINSERT_PARALLELISM), BULKINSERT_PARALLELISM,
          DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(UPSERT_PARALLELISM), UPSERT_PARALLELISM, DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(DELETE_PARALLELISM), DELETE_PARALLELISM, DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(ROLLBACK_PARALLELISM), ROLLBACK_PARALLELISM, DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(COMBINE_BEFORE_INSERT_PROP), COMBINE_BEFORE_INSERT_PROP,
          DEFAULT_COMBINE_BEFORE_INSERT);
      setDefaultOnCondition(props, !props.containsKey(COMBINE_BEFORE_UPSERT_PROP), COMBINE_BEFORE_UPSERT_PROP,
          DEFAULT_COMBINE_BEFORE_UPSERT);
      setDefaultOnCondition(props, !props.containsKey(COMBINE_BEFORE_DELETE_PROP), COMBINE_BEFORE_DELETE_PROP,
          DEFAULT_COMBINE_BEFORE_DELETE);
      setDefaultOnCondition(props, !props.containsKey(WRITE_STATUS_STORAGE_LEVEL), WRITE_STATUS_STORAGE_LEVEL,
          DEFAULT_WRITE_STATUS_STORAGE_LEVEL);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_AUTO_COMMIT_PROP), HOODIE_AUTO_COMMIT_PROP,
          DEFAULT_HOODIE_AUTO_COMMIT);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_ASSUME_DATE_PARTITIONING_PROP),
          HOODIE_ASSUME_DATE_PARTITIONING_PROP, DEFAULT_ASSUME_DATE_PARTITIONING);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_WRITE_STATUS_CLASS_PROP), HOODIE_WRITE_STATUS_CLASS_PROP,
          DEFAULT_HOODIE_WRITE_STATUS_CLASS);
      setDefaultOnCondition(props, !props.containsKey(FINALIZE_WRITE_PARALLELISM), FINALIZE_WRITE_PARALLELISM,
          DEFAULT_FINALIZE_WRITE_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(EMBEDDED_TIMELINE_SERVER_ENABLED),
          EMBEDDED_TIMELINE_SERVER_ENABLED, DEFAULT_EMBEDDED_TIMELINE_SERVER_ENABLED);
      setDefaultOnCondition(props, !props.containsKey(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP),
          INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(DEFAULT_INITIAL_CONSISTENCY_CHECK_INTERVAL_MS));
      setDefaultOnCondition(props, !props.containsKey(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP),
          MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(DEFAULT_MAX_CONSISTENCY_CHECK_INTERVAL_MS));
      setDefaultOnCondition(props, !props.containsKey(MAX_CONSISTENCY_CHECKS_PROP), MAX_CONSISTENCY_CHECKS_PROP,
          String.valueOf(DEFAULT_MAX_CONSISTENCY_CHECKS));
      setDefaultOnCondition(props, !props.containsKey(FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP),
          FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP, DEFAULT_FAIL_ON_TIMELINE_ARCHIVING_ENABLED);

      // Make sure the props is propagated
      setDefaultOnCondition(props, !isIndexConfigSet, HoodieIndexConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isStorageConfigSet, HoodieStorageConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isCompactionConfigSet,
          HoodieCompactionConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMetricsConfigSet, HoodieMetricsConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMemoryConfigSet, HoodieMemoryConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isViewConfigSet,
          FileSystemViewStorageConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isConsistencyGuardSet,
          ConsistencyGuardConfig.newBuilder().fromProperties(props).build());

      // Build WriteConfig at the end
      HoodieWriteConfig config = new HoodieWriteConfig(props);
      Preconditions.checkArgument(config.getBasePath() != null);
      return config;
    }
  }
}
