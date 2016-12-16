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
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.HoodieCleaner;
import com.uber.hoodie.metrics.MetricsReporterType;
import org.apache.spark.storage.StorageLevel;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Class storing configs for the {@link com.uber.hoodie.HoodieWriteClient}
 */
@Immutable
public class HoodieWriteConfig extends DefaultHoodieConfig {
    private static final String BASE_PATH_PROP = "hoodie.base.path";
    private static final String AVRO_SCHEMA = "hoodie.avro.schema";
    private static final String TABLE_NAME = "hoodie.table.name";
    private static final String DEFAULT_PARALLELISM = "200";
    private static final String INSERT_PARALLELISM = "hoodie.insert.shuffle.parallelism";
    private static final String UPSERT_PARALLELISM = "hoodie.upsert.shuffle.parallelism";
    private static final String COMBINE_BEFORE_INSERT_PROP = "hoodie.combine.before.insert";
    private static final String DEFAULT_COMBINE_BEFORE_INSERT = "false";
    private static final String COMBINE_BEFORE_UPSERT_PROP = "hoodie.combine.before.upsert";
    private static final String DEFAULT_COMBINE_BEFORE_UPSERT = "true";
    private static final String WRITE_STATUS_STORAGE_LEVEL = "hoodie.write.status.storage.level";
    private static final String DEFAULT_WRITE_STATUS_STORAGE_LEVEL = "MEMORY_AND_DISK_SER";

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
    public HoodieCleaner.CleaningPolicy getCleanerPolicy() {
        return HoodieCleaner.CleaningPolicy
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

        public Builder fromFile(File propertiesFile) throws IOException {
            FileReader reader = new FileReader(propertiesFile);
            try {
                this.props.load(reader);
                return this;
            } finally {
                reader.close();
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

        public Builder withWriteStatusStorageLevel(StorageLevel level) {
            props.setProperty(WRITE_STATUS_STORAGE_LEVEL, level.toString());
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


            setDefaultOnCondition(props, !isIndexConfigSet, HoodieIndexConfig.newBuilder().build());
            setDefaultOnCondition(props, !isStorageConfigSet,
                HoodieStorageConfig.newBuilder().build());
            setDefaultOnCondition(props, !isCompactionConfigSet,
                HoodieCompactionConfig.newBuilder().build());
            setDefaultOnCondition(props, !isMetricsConfigSet,
                HoodieMetricsConfig.newBuilder().build());
            return config;
        }
    }
}
