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

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerFactory.BulkInsertSortMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Class storing configs for the {@link HoodieWriteClient}.
 */
@Immutable
public class HoodieWriteConfig extends DefaultHoodieConfig {

  public static final String TABLE_NAME = "hoodie.table.name";
  public static final String DEFAULT_ROLLBACK_USING_MARKERS = "false";
  public static final String ROLLBACK_USING_MARKERS = "hoodie.rollback.using.markers";
  public static final String TIMELINE_LAYOUT_VERSION = "hoodie.timeline.layout.version";
  public static final String BASE_PATH_PROP = "hoodie.base.path";
  public static final String AVRO_SCHEMA = "hoodie.avro.schema";
  public static final String AVRO_SCHEMA_VALIDATE = "hoodie.avro.schema.validate";
  public static final String DEFAULT_AVRO_SCHEMA_VALIDATE = "false";
  public static final String DEFAULT_PARALLELISM = "1500";
  public static final String INSERT_PARALLELISM = "hoodie.insert.shuffle.parallelism";
  public static final String BULKINSERT_PARALLELISM = "hoodie.bulkinsert.shuffle.parallelism";
  public static final String BULKINSERT_USER_DEFINED_PARTITIONER_CLASS = "hoodie.bulkinsert.user.defined.partitioner.class";
  public static final String UPSERT_PARALLELISM = "hoodie.upsert.shuffle.parallelism";
  public static final String DELETE_PARALLELISM = "hoodie.delete.shuffle.parallelism";
  public static final String DEFAULT_ROLLBACK_PARALLELISM = "100";
  public static final String ROLLBACK_PARALLELISM = "hoodie.rollback.parallelism";
  public static final String WRITE_BUFFER_LIMIT_BYTES = "hoodie.write.buffer.limit.bytes";
  public static final String DEFAULT_WRITE_BUFFER_LIMIT_BYTES = String.valueOf(4 * 1024 * 1024);
  public static final String COMBINE_BEFORE_INSERT_PROP = "hoodie.combine.before.insert";
  public static final String DEFAULT_COMBINE_BEFORE_INSERT = "false";
  public static final String COMBINE_BEFORE_UPSERT_PROP = "hoodie.combine.before.upsert";
  public static final String DEFAULT_COMBINE_BEFORE_UPSERT = "true";
  public static final String COMBINE_BEFORE_DELETE_PROP = "hoodie.combine.before.delete";
  public static final String DEFAULT_COMBINE_BEFORE_DELETE = "true";
  public static final String WRITE_STATUS_STORAGE_LEVEL = "hoodie.write.status.storage.level";
  public static final String DEFAULT_WRITE_STATUS_STORAGE_LEVEL = "MEMORY_AND_DISK_SER";
  public static final String HOODIE_AUTO_COMMIT_PROP = "hoodie.auto.commit";
  public static final String DEFAULT_HOODIE_AUTO_COMMIT = "true";
  public static final String HOODIE_ASSUME_DATE_PARTITIONING_PROP = "hoodie.assume.date.partitioning";
  public static final String DEFAULT_ASSUME_DATE_PARTITIONING = "false";
  public static final String HOODIE_WRITE_STATUS_CLASS_PROP = "hoodie.writestatus.class";
  public static final String DEFAULT_HOODIE_WRITE_STATUS_CLASS = WriteStatus.class.getName();
  public static final String FINALIZE_WRITE_PARALLELISM = "hoodie.finalize.write.parallelism";
  public static final String DEFAULT_FINALIZE_WRITE_PARALLELISM = DEFAULT_PARALLELISM;
  public static final String MARKERS_DELETE_PARALLELISM = "hoodie.markers.delete.parallelism";
  public static final String DEFAULT_MARKERS_DELETE_PARALLELISM = "100";
  public static final String BULKINSERT_SORT_MODE = "hoodie.bulkinsert.sort.mode";
  public static final String DEFAULT_BULKINSERT_SORT_MODE = BulkInsertSortMode.GLOBAL_SORT
      .toString();

  public static final String EMBEDDED_TIMELINE_SERVER_ENABLED = "hoodie.embed.timeline.server";
  public static final String DEFAULT_EMBEDDED_TIMELINE_SERVER_ENABLED = "true";

  public static final String FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP = "hoodie.fail.on.timeline.archiving";
  public static final String DEFAULT_FAIL_ON_TIMELINE_ARCHIVING_ENABLED = "true";
  // time between successive attempts to ensure written data's metadata is consistent on storage
  public static final String INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP =
      "hoodie.consistency.check.initial_interval_ms";
  public static long DEFAULT_INITIAL_CONSISTENCY_CHECK_INTERVAL_MS = 2000L;

  // max interval time
  public static final String MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = "hoodie.consistency.check.max_interval_ms";
  public static long DEFAULT_MAX_CONSISTENCY_CHECK_INTERVAL_MS = 300000L;

  // maximum number of checks, for consistency of written data. Will wait upto 256 Secs
  public static final String MAX_CONSISTENCY_CHECKS_PROP = "hoodie.consistency.check.max_checks";
  public static int DEFAULT_MAX_CONSISTENCY_CHECKS = 7;

  /**
   * HUDI-858 : There are users who had been directly using RDD APIs and have relied on a behavior in 0.4.x to allow
   * multiple write operations (upsert/buk-insert/...) to be executed within a single commit.
   *
   * Given Hudi commit protocol, these are generally unsafe operations and user need to handle failure scenarios. It
   * only works with COW table. Hudi 0.5.x had stopped this behavior.
   *
   * Given the importance of supporting such cases for the user's migration to 0.5.x, we are proposing a safety flag
   * (disabled by default) which will allow this old behavior.
   */
  public static final String ALLOW_MULTI_WRITE_ON_SAME_INSTANT =
      "_.hoodie.allow.multi.write.on.same.instant";
  public static final String DEFAULT_ALLOW_MULTI_WRITE_ON_SAME_INSTANT = "false";

  public static final String EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION = AVRO_SCHEMA + ".externalTransformation";
  public static final String DEFAULT_EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION = "false";

  private ConsistencyGuardConfig consistencyGuardConfig;

  // Hoodie Write Client transparently rewrites File System View config when embedded mode is enabled
  // We keep track of original config and rewritten config
  private final FileSystemViewStorageConfig clientSpecifiedViewStorageConfig;
  private FileSystemViewStorageConfig viewStorageConfig;

  protected HoodieWriteConfig(Properties props) {
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

  public boolean getAvroSchemaValidate() {
    return Boolean.parseBoolean(props.getProperty(AVRO_SCHEMA_VALIDATE));
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

  public boolean shouldUseExternalSchemaTransformation() {
    return Boolean.parseBoolean(props.getProperty(EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION));
  }

  public Integer getTimelineLayoutVersion() {
    return Integer.parseInt(props.getProperty(TIMELINE_LAYOUT_VERSION));
  }

  public int getBulkInsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(BULKINSERT_PARALLELISM));
  }

  public String getUserDefinedBulkInsertPartitionerClass() {
    return props.getProperty(BULKINSERT_USER_DEFINED_PARTITIONER_CLASS);
  }

  public int getInsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(INSERT_PARALLELISM));
  }

  public int getUpsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(UPSERT_PARALLELISM));
  }

  public int getDeleteShuffleParallelism() {
    return Math.max(Integer.parseInt(props.getProperty(DELETE_PARALLELISM)), 1);
  }

  public int getRollbackParallelism() {
    return Integer.parseInt(props.getProperty(ROLLBACK_PARALLELISM));
  }

  public boolean shouldRollbackUsingMarkers() {
    return Boolean.parseBoolean(props.getProperty(ROLLBACK_USING_MARKERS));
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

  public boolean shouldAllowMultiWriteOnSameInstant() {
    return Boolean.parseBoolean(props.getProperty(ALLOW_MULTI_WRITE_ON_SAME_INSTANT));
  }

  public String getWriteStatusClassName() {
    return props.getProperty(HOODIE_WRITE_STATUS_CLASS_PROP);
  }

  public int getFinalizeWriteParallelism() {
    return Integer.parseInt(props.getProperty(FINALIZE_WRITE_PARALLELISM));
  }

  public int getMarkersDeleteParallelism() {
    return Integer.parseInt(props.getProperty(MARKERS_DELETE_PARALLELISM));
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

  public BulkInsertSortMode getBulkInsertSortMode() {
    String sortMode = props.getProperty(BULKINSERT_SORT_MODE);
    return BulkInsertSortMode.valueOf(sortMode.toUpperCase());
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

  public double getRecordSizeEstimationThreshold() {
    return Double.parseDouble(props.getProperty(HoodieCompactionConfig.RECORD_SIZE_ESTIMATION_THRESHOLD_PROP));
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

  public boolean isAsyncClean() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.ASYNC_CLEAN_PROP));
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

  public Boolean shouldCleanBootstrapBaseFile() {
    return Boolean.valueOf(props.getProperty(HoodieCompactionConfig.CLEANER_BOOTSTRAP_BASE_FILE_ENABLED));
  }

  /**
   * index properties.
   */
  public HoodieIndex.IndexType getIndexType() {
    return HoodieIndex.IndexType.valueOf(props.getProperty(HoodieIndexConfig.INDEX_TYPE_PROP));
  }

  public String getIndexClass() {
    return props.getProperty(HoodieIndexConfig.INDEX_CLASS_PROP);
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
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HBASE_GET_BATCH_SIZE_PROP));
  }

  public int getHbaseIndexPutBatchSize() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HBASE_PUT_BATCH_SIZE_PROP));
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
    return Boolean.parseBoolean(props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY));
  }

  public int getHBaseIndexDesiredPutsTime() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS));
  }

  public String getBloomFilterType() {
    return props.getProperty(HoodieIndexConfig.BLOOM_INDEX_FILTER_TYPE);
  }

  public int getDynamicBloomFilterMaxNumEntries() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES));
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

  public boolean getBloomIndexUpdatePartitionPath() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH));
  }

  public int getSimpleIndexParallelism() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.SIMPLE_INDEX_PARALLELISM_PROP));
  }

  public boolean getSimpleIndexUseCaching() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.SIMPLE_INDEX_USE_CACHING_PROP));
  }

  public int getGlobalSimpleIndexParallelism() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.GLOBAL_SIMPLE_INDEX_PARALLELISM_PROP));
  }

  public boolean getGlobalSimpleIndexUpdatePartitionPath() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH));
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
    return Double.parseDouble(props.getProperty(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO));
  }

  public CompressionCodecName getParquetCompressionCodec() {
    return CompressionCodecName.fromConf(props.getProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC));
  }

  public double getLogFileToParquetCompressionRatio() {
    return Double.parseDouble(props.getProperty(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO));
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

  public String getJmxPort() {
    return props.getProperty(HoodieMetricsConfig.JMX_PORT);
  }

  public int getDatadogReportPeriodSeconds() {
    return Integer.parseInt(props.getProperty(HoodieMetricsDatadogConfig.DATADOG_REPORT_PERIOD_SECONDS));
  }

  public ApiSite getDatadogApiSite() {
    return ApiSite.valueOf(props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_SITE));
  }

  public String getDatadogApiKey() {
    if (props.containsKey(HoodieMetricsDatadogConfig.DATADOG_API_KEY)) {
      return props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_KEY);
    } else {
      Supplier<String> apiKeySupplier = ReflectionUtils.loadClass(
          props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_KEY_SUPPLIER));
      return apiKeySupplier.get();
    }
  }

  public boolean getDatadogApiKeySkipValidation() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_KEY_SKIP_VALIDATION));
  }

  public int getDatadogApiTimeoutSeconds() {
    return Integer.parseInt(props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_TIMEOUT_SECONDS));
  }

  public String getDatadogMetricPrefix() {
    return props.getProperty(HoodieMetricsDatadogConfig.DATADOG_METRIC_PREFIX);
  }

  public String getDatadogMetricHost() {
    return props.getProperty(HoodieMetricsDatadogConfig.DATADOG_METRIC_HOST);
  }

  public List<String> getDatadogMetricTags() {
    return Arrays.stream(props.getProperty(
        HoodieMetricsDatadogConfig.DATADOG_METRIC_TAGS).split("\\s*,\\s*")).collect(Collectors.toList());
  }

  public String getMetricReporterClassName() {
    return props.getProperty(HoodieMetricsConfig.METRICS_REPORTER_CLASS);
  }

  public int getPrometheusPort() {
    return Integer.parseInt(props.getProperty(HoodieMetricsPrometheusConfig.PROMETHEUS_PORT));
  }

  public String getPushGatewayHost() {
    return props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_HOST);
  }

  public int getPushGatewayPort() {
    return Integer.parseInt(props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_PORT));
  }

  public int getPushGatewayReportPeriodSeconds() {
    return Integer.parseInt(props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_REPORT_PERIOD_SECONDS));
  }

  public boolean getPushGatewayDeleteOnShutdown() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_DELETE_ON_SHUTDOWN));
  }

  public String getPushGatewayJobName() {
    return props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_JOB_NAME);
  }

  public boolean getPushGatewayRandomJobNameSuffix() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX));
  }
  
  /**
   * memory configs.
   */
  public int getMaxDFSStreamBufferSize() {
    return Integer.parseInt(props.getProperty(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP));
  }

  public String getSpillableMapBasePath() {
    return props.getProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH_PROP);
  }

  public double getWriteStatusFailureFraction() {
    return Double.parseDouble(props.getProperty(HoodieMemoryConfig.WRITESTATUS_FAILURE_FRACTION_PROP));
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

  /**
   * Commit call back configs.
   */
  public boolean writeCommitCallbackOn() {
    return Boolean.parseBoolean(props.getProperty(HoodieWriteCommitCallbackConfig.CALLBACK_ON));
  }

  public String getCallbackClass() {
    return props.getProperty(HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_PROP);
  }

  public String getBootstrapSourceBasePath() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP);
  }

  public String getBootstrapModeSelectorClass() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR);
  }

  public String getFullBootstrapInputProvider() {
    return props.getProperty(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER);
  }

  public String getBootstrapKeyGeneratorClass() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS);
  }

  public String getBootstrapModeSelectorRegex() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR_REGEX);
  }

  public BootstrapMode getBootstrapModeForRegexMatch() {
    return BootstrapMode.valueOf(props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR_REGEX_MODE));
  }

  public String getBootstrapPartitionPathTranslatorClass() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS);
  }

  public int getBootstrapParallelism() {
    return Integer.parseInt(props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_PARALLELISM));
  }

  public static class Builder {

    protected final Properties props = new Properties();
    private boolean isIndexConfigSet = false;
    private boolean isStorageConfigSet = false;
    private boolean isCompactionConfigSet = false;
    private boolean isMetricsConfigSet = false;
    private boolean isBootstrapConfigSet = false;
    private boolean isMemoryConfigSet = false;
    private boolean isViewConfigSet = false;
    private boolean isConsistencyGuardSet = false;
    private boolean isCallbackConfigSet = false;

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
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

    public Builder withAvroSchemaValidate(boolean enable) {
      props.setProperty(AVRO_SCHEMA_VALIDATE, String.valueOf(enable));
      return this;
    }

    public Builder forTable(String tableName) {
      props.setProperty(TABLE_NAME, tableName);
      return this;
    }

    public Builder withTimelineLayoutVersion(int version) {
      props.setProperty(TIMELINE_LAYOUT_VERSION, String.valueOf(version));
      return this;
    }

    public Builder withBulkInsertParallelism(int bulkInsertParallelism) {
      props.setProperty(BULKINSERT_PARALLELISM, String.valueOf(bulkInsertParallelism));
      return this;
    }

    public Builder withUserDefinedBulkInsertPartitionerClass(String className) {
      props.setProperty(BULKINSERT_USER_DEFINED_PARTITIONER_CLASS, className);
      return this;
    }

    public Builder withDeleteParallelism(int parallelism) {
      props.setProperty(DELETE_PARALLELISM, String.valueOf(parallelism));
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

    public Builder withRollbackUsingMarkers(boolean rollbackUsingMarkers) {
      props.setProperty(ROLLBACK_USING_MARKERS, String.valueOf(rollbackUsingMarkers));
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

    public Builder combineDeleteInput(boolean onDelete) {
      props.setProperty(COMBINE_BEFORE_DELETE_PROP, String.valueOf(onDelete));
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

    public Builder withBootstrapConfig(HoodieBootstrapConfig bootstrapConfig) {
      props.putAll(bootstrapConfig.getProps());
      isBootstrapConfigSet = true;
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

    public Builder withCallbackConfig(HoodieWriteCommitCallbackConfig callbackConfig) {
      props.putAll(callbackConfig.getProps());
      isCallbackConfigSet = true;
      return this;
    }

    public Builder withFinalizeWriteParallelism(int parallelism) {
      props.setProperty(FINALIZE_WRITE_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withMarkersDeleteParallelism(int parallelism) {
      props.setProperty(MARKERS_DELETE_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withEmbeddedTimelineServerEnabled(boolean enabled) {
      props.setProperty(EMBEDDED_TIMELINE_SERVER_ENABLED, String.valueOf(enabled));
      return this;
    }

    public Builder withBulkInsertSortMode(String mode) {
      props.setProperty(BULKINSERT_SORT_MODE, mode);
      return this;
    }

    public Builder withAllowMultiWriteOnSameInstant(boolean allow) {
      props.setProperty(ALLOW_MULTI_WRITE_ON_SAME_INSTANT, String.valueOf(allow));
      return this;
    }

    public Builder withExternalSchemaTrasformation(boolean enabled) {
      props.setProperty(EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION, String.valueOf(enabled));
      return this;
    }

    public Builder withProperties(Properties properties) {
      this.props.putAll(properties);
      return this;
    }

    protected void setDefaults() {
      // Check for mandatory properties
      setDefaultOnCondition(props, !props.containsKey(INSERT_PARALLELISM), INSERT_PARALLELISM, DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(BULKINSERT_PARALLELISM), BULKINSERT_PARALLELISM,
          DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(UPSERT_PARALLELISM), UPSERT_PARALLELISM, DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(DELETE_PARALLELISM), DELETE_PARALLELISM, DEFAULT_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(ROLLBACK_PARALLELISM), ROLLBACK_PARALLELISM,
          DEFAULT_ROLLBACK_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(ROLLBACK_USING_MARKERS), ROLLBACK_USING_MARKERS,
          DEFAULT_ROLLBACK_USING_MARKERS);
      setDefaultOnCondition(props, !props.containsKey(COMBINE_BEFORE_INSERT_PROP), COMBINE_BEFORE_INSERT_PROP,
          DEFAULT_COMBINE_BEFORE_INSERT);
      setDefaultOnCondition(props, !props.containsKey(COMBINE_BEFORE_UPSERT_PROP), COMBINE_BEFORE_UPSERT_PROP,
          DEFAULT_COMBINE_BEFORE_UPSERT);
      setDefaultOnCondition(props, !props.containsKey(COMBINE_BEFORE_DELETE_PROP), COMBINE_BEFORE_DELETE_PROP,
          DEFAULT_COMBINE_BEFORE_DELETE);
      setDefaultOnCondition(props, !props.containsKey(ALLOW_MULTI_WRITE_ON_SAME_INSTANT),
          ALLOW_MULTI_WRITE_ON_SAME_INSTANT, DEFAULT_ALLOW_MULTI_WRITE_ON_SAME_INSTANT);
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
      setDefaultOnCondition(props, !props.containsKey(MARKERS_DELETE_PARALLELISM), MARKERS_DELETE_PARALLELISM,
          DEFAULT_MARKERS_DELETE_PARALLELISM);
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
      setDefaultOnCondition(props, !props.containsKey(AVRO_SCHEMA_VALIDATE), AVRO_SCHEMA_VALIDATE, DEFAULT_AVRO_SCHEMA_VALIDATE);
      setDefaultOnCondition(props, !props.containsKey(BULKINSERT_SORT_MODE),
          BULKINSERT_SORT_MODE, DEFAULT_BULKINSERT_SORT_MODE);

      // Make sure the props is propagated
      setDefaultOnCondition(props, !isIndexConfigSet, HoodieIndexConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isStorageConfigSet, HoodieStorageConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isCompactionConfigSet,
          HoodieCompactionConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMetricsConfigSet, HoodieMetricsConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isBootstrapConfigSet,
          HoodieBootstrapConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMemoryConfigSet, HoodieMemoryConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isViewConfigSet,
          FileSystemViewStorageConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isConsistencyGuardSet,
          ConsistencyGuardConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isCallbackConfigSet,
          HoodieWriteCommitCallbackConfig.newBuilder().fromProperties(props).build());

      setDefaultOnCondition(props, !props.containsKey(EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION),
          EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION, DEFAULT_EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION);
      setDefaultOnCondition(props, !props.containsKey(TIMELINE_LAYOUT_VERSION), TIMELINE_LAYOUT_VERSION,
          String.valueOf(TimelineLayoutVersion.CURR_VERSION));
    }

    private void validate() {
      String layoutVersion = props.getProperty(TIMELINE_LAYOUT_VERSION);
      // Ensure Layout Version is good
      new TimelineLayoutVersion(Integer.parseInt(layoutVersion));
      Objects.requireNonNull(props.getProperty(BASE_PATH_PROP));
    }

    public HoodieWriteConfig build() {
      setDefaults();
      validate();
      // Build WriteConfig at the end
      HoodieWriteConfig config = new HoodieWriteConfig(props);
      return config;
    }
  }
}
