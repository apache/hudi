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
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.compact.strategy.CompactionStrategy;
import com.uber.hoodie.metrics.MetricsReporterType;
import org.apache.spark.storage.StorageLevel;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Class storing configs for the {@link com.uber.hoodie.HoodieWriteClient}
 */
@Immutable
public class HoodieWriteConfig extends DefaultHoodieConfig {
    private static final String BASE_PATH_PROP = "hoodie.base.path";
    private static final String AVRO_SCHEMA = "hoodie.avro.schema";
    public static final String TABLE_NAME = "hoodie.table.name";
    private static final String DEFAULT_PARALLELISM = "200";
    private static final String INSERT_PARALLELISM = "hoodie.insert.shuffle.parallelism";
    private static final String UPSERT_PARALLELISM = "hoodie.upsert.shuffle.parallelism";
    private static final String COMBINE_BEFORE_INSERT_PROP = "hoodie.combine.before.insert";
    private static final String DEFAULT_COMBINE_BEFORE_INSERT = "false";
    private static final String COMBINE_BEFORE_UPSERT_PROP = "hoodie.combine.before.upsert";
    private static final String DEFAULT_COMBINE_BEFORE_UPSERT = "true";
    private static final String WRITE_STATUS_STORAGE_LEVEL = "hoodie.write.status.storage.level";
    private static final String DEFAULT_WRITE_STATUS_STORAGE_LEVEL = "MEMORY_AND_DISK_SER";
    private static final String HOODIE_AUTO_COMMIT_PROP = "hoodie.auto.commit";
    private static final String DEFAULT_HOODIE_AUTO_COMMIT = "true";
    private static final String HOODIE_ASSUME_DATE_PARTITIONING_PROP = "hoodie.assume.date.partitioning";
    private static final String DEFAULT_ASSUME_DATE_PARTITIONING = "false";


    private HoodieWriteConfig(Properties props) {
        super(props);
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

    public int getInsertShuffleParallelism() {
        return Integer.parseInt(props.getProperty(INSERT_PARALLELISM));
    }

    public int getUpsertShuffleParallelism() {
        return Integer.parseInt(props.getProperty(UPSERT_PARALLELISM));
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

    /**
     * compaction properties
     **/
    public HoodieCleaningPolicy getCleanerPolicy() {
        return HoodieCleaningPolicy
            .valueOf(props.getProperty(HoodieCompactionConfig.CLEANER_POLICY_PROP));
    }

    public int getCleanerFileVersionsRetained() {
        return Integer.parseInt(
            props.getProperty(HoodieCompactionConfig.CLEANER_FILE_VERSIONS_RETAINED_PROP));
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
        return Integer.parseInt(props.getProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT_BYTES));
    }

    public int getCopyOnWriteInsertSplitSize() {
        return Integer.parseInt(
            props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE));
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
        return Integer.parseInt(
            props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS_PROP));
    }

    public CompactionStrategy getCompactionStrategy() {
        return ReflectionUtils.loadClass(props.getProperty(HoodieCompactionConfig.COMPACTION_STRATEGY_PROP));
    }

    public Long getTargetIOPerCompactionInMB() {
        return Long.parseLong(props.getProperty(HoodieCompactionConfig.TARGET_IO_PER_COMPACTION_IN_MB_PROP));
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

    public int getBloomIndexParallelism() {
        return Integer.parseInt(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_PARALLELISM_PROP));
    }

    public int getNumBucketsPerPartition() {
        return Integer.parseInt(props.getProperty(HoodieIndexConfig.BUCKETED_INDEX_NUM_BUCKETS_PROP));
    }

    /**
     * storage properties
     **/
    public int getParquetMaxFileSize() {
        return Integer.parseInt(props.getProperty(HoodieStorageConfig.PARQUET_FILE_MAX_BYTES));
    }

    public int getParquetBlockSize() {
        return Integer.parseInt(props.getProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE_BYTES));
    }

    public int getParquetPageSize() {
        return Integer.parseInt(props.getProperty(HoodieStorageConfig.PARQUET_PAGE_SIZE_BYTES));
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

    public static HoodieWriteConfig.Builder newBuilder() {
        return new Builder();
    }



  public static class Builder {
        private final Properties props = new Properties();
        private boolean isIndexConfigSet = false;
        private boolean isStorageConfigSet = false;
        private boolean isCompactionConfigSet = false;
        private boolean isMetricsConfigSet = false;
        private boolean isAutoCommit = true;

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

        public Builder withParallelism(int insertShuffleParallelism, int upsertShuffleParallelism) {
            props.setProperty(INSERT_PARALLELISM, String.valueOf(insertShuffleParallelism));
            props.setProperty(UPSERT_PARALLELISM, String.valueOf(upsertShuffleParallelism));
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

        public Builder withAutoCommit(boolean autoCommit) {
            props.setProperty(HOODIE_AUTO_COMMIT_PROP, String.valueOf(autoCommit));
            return this;
        }

        public Builder withAssumeDatePartitioning(boolean assumeDatePartitioning) {
            props.setProperty(HOODIE_ASSUME_DATE_PARTITIONING_PROP, String.valueOf(assumeDatePartitioning));
            return this;
        }

        public HoodieWriteConfig build() {
            HoodieWriteConfig config = new HoodieWriteConfig(props);
            // Check for mandatory properties
            Preconditions.checkArgument(config.getBasePath() != null);
            setDefaultOnCondition(props, !props.containsKey(INSERT_PARALLELISM), INSERT_PARALLELISM,
                DEFAULT_PARALLELISM);
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

            // Make sure the props is propagated
            setDefaultOnCondition(props, !isIndexConfigSet,
                HoodieIndexConfig.newBuilder().fromProperties(props).build());
            setDefaultOnCondition(props, !isStorageConfigSet,
                HoodieStorageConfig.newBuilder().fromProperties(props).build());
            setDefaultOnCondition(props, !isCompactionConfigSet,
                HoodieCompactionConfig.newBuilder().fromProperties(props).build());
            setDefaultOnCondition(props, !isMetricsConfigSet,
                HoodieMetricsConfig.newBuilder().fromProperties(props).build());
            return config;
        }
    }
}
