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
import com.uber.hoodie.io.HoodieCleaner;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Compaction related config
 */
@Immutable
public class HoodieCompactionConfig extends DefaultHoodieConfig {
    public static final String CLEANER_POLICY_PROP = "hoodie.cleaner.policy";
    private static final String DEFAULT_CLEANER_POLICY =
        HoodieCleaner.CleaningPolicy.KEEP_LATEST_COMMITS.name();

    public static final String CLEANER_FILE_VERSIONS_RETAINED_PROP =
        "hoodie.cleaner.fileversions.retained";
    private static final String DEFAULT_CLEANER_FILE_VERSIONS_RETAINED = "3";

    public static final String AUTO_CLEAN_PROP = "hoodie.clean.automatic";
    private static final String DEFAULT_AUTO_CLEAN = "true";

    public static final String CLEANER_COMMITS_RETAINED_PROP = "hoodie.cleaner.commits.retained";
    private static final String DEFAULT_CLEANER_COMMITS_RETAINED = "24";

    public static final String MAX_COMMITS_TO_KEEP = "hoodie.keep.max.commits";
    private static final String DEFAULT_MAX_COMMITS_TO_KEEP = String.valueOf(128);
    public static final String MIN_COMMITS_TO_KEEP = "hoodie.keep.min.commits";
    private static final String DEFAULT_MIN_COMMITS_TO_KEEP = String.valueOf(96);
    // Upsert uses this file size to compact new data onto existing files..
    public static final String PARQUET_SMALL_FILE_LIMIT_BYTES = "hoodie.parquet.small.file.limit";
    // Turned off by default
    public static final String DEFAULT_PARQUET_SMALL_FILE_LIMIT_BYTES = String.valueOf(0);


    /** Configs related to specific table types **/
    // Number of inserts, that will be put each partition/bucket for writing
    public static final String COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE = "hoodie.copyonwrite.insert.split.size";
    // The rationale to pick the insert parallelism is the following. Writing out 100MB files,
    // with atleast 1kb records, means 100K records per file. we just overprovision to 500K
    public static final String DEFAULT_COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE = String.valueOf(500000);

    // Config to control whether we control insert split sizes automatically based on average record sizes
    public static final String COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS = "hoodie.copyonwrite.insert.auto.split";
    // its off by default
    public static final String DEFAULT_COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS = String.valueOf(false);


    // This value is used as a guessimate for the record size, if we can't determine this from previous commits
    public static final String COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE = "hoodie.copyonwrite.record.size.estimate";
    // Used to determine how much more can be packed into a small file, before it exceeds the size limit.
    public static final String DEFAULT_COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE = String.valueOf(1024);

    public static final String CLEANER_PARALLELISM = "hoodie.cleaner.parallelism";
    public static final String DEFAULT_CLEANER_PARALLELISM = String.valueOf(200);


    private HoodieCompactionConfig(Properties props) {
        super(props);
    }

    public static HoodieCompactionConfig.Builder newBuilder() {
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

        public Builder withAutoClean(Boolean autoClean) {
            props.setProperty(AUTO_CLEAN_PROP, String.valueOf(Boolean.TRUE));
            return this;
        }

        public Builder withCleanerPolicy(HoodieCleaner.CleaningPolicy policy) {
            props.setProperty(CLEANER_POLICY_PROP, policy.name());
            return this;
        }

        public Builder retainFileVersions(int fileVersionsRetained) {
            props.setProperty(CLEANER_FILE_VERSIONS_RETAINED_PROP,
                String.valueOf(fileVersionsRetained));
            return this;
        }

        public Builder retainCommits(int commitsRetained) {
            props.setProperty(CLEANER_COMMITS_RETAINED_PROP, String.valueOf(commitsRetained));
            return this;
        }

        public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
            props.setProperty(MIN_COMMITS_TO_KEEP, String.valueOf(minToKeep));
            props.setProperty(MAX_COMMITS_TO_KEEP, String.valueOf(maxToKeep));
            return this;
        }

        public Builder compactionSmallFileSize(long smallFileLimitBytes) {
            props.setProperty(PARQUET_SMALL_FILE_LIMIT_BYTES, String.valueOf(smallFileLimitBytes));
            return this;
        }

        public Builder insertSplitSize(int insertSplitSize) {
            props.setProperty(COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE, String.valueOf(insertSplitSize));
            return this;
        }

        public Builder autoTuneInsertSplits(boolean autoTuneInsertSplits) {
            props.setProperty(COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS, String.valueOf(autoTuneInsertSplits));
            return this;
        }

        public Builder approxRecordSize(int recordSizeEstimate) {
            props.setProperty(COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE, String.valueOf(recordSizeEstimate));
            return this;
        }

        public Builder withCleanerParallelism(int cleanerParallelism) {
            props.setProperty(CLEANER_PARALLELISM, String.valueOf(cleanerParallelism));
            return this;
        }

        public HoodieCompactionConfig build() {
            HoodieCompactionConfig config = new HoodieCompactionConfig(props);
            setDefaultOnCondition(props, !props.containsKey(AUTO_CLEAN_PROP),
                    AUTO_CLEAN_PROP, DEFAULT_AUTO_CLEAN);
            setDefaultOnCondition(props, !props.containsKey(CLEANER_POLICY_PROP),
                CLEANER_POLICY_PROP, DEFAULT_CLEANER_POLICY);
            setDefaultOnCondition(props, !props.containsKey(CLEANER_FILE_VERSIONS_RETAINED_PROP),
                CLEANER_FILE_VERSIONS_RETAINED_PROP, DEFAULT_CLEANER_FILE_VERSIONS_RETAINED);
            setDefaultOnCondition(props, !props.containsKey(CLEANER_COMMITS_RETAINED_PROP),
                CLEANER_COMMITS_RETAINED_PROP, DEFAULT_CLEANER_COMMITS_RETAINED);
            setDefaultOnCondition(props, !props.containsKey(MAX_COMMITS_TO_KEEP),
                MAX_COMMITS_TO_KEEP, DEFAULT_MAX_COMMITS_TO_KEEP);
            setDefaultOnCondition(props, !props.containsKey(MIN_COMMITS_TO_KEEP),
                MIN_COMMITS_TO_KEEP, DEFAULT_MIN_COMMITS_TO_KEEP);
            setDefaultOnCondition(props, !props.containsKey(PARQUET_SMALL_FILE_LIMIT_BYTES),
                PARQUET_SMALL_FILE_LIMIT_BYTES, DEFAULT_PARQUET_SMALL_FILE_LIMIT_BYTES);
            setDefaultOnCondition(props, !props.containsKey(COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE),
                COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE, DEFAULT_COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE);
            setDefaultOnCondition(props, !props.containsKey(COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS),
                COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS, DEFAULT_COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS);
            setDefaultOnCondition(props, !props.containsKey(COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE),
                COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE, DEFAULT_COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE);
            setDefaultOnCondition(props, !props.containsKey(CLEANER_PARALLELISM),
                CLEANER_PARALLELISM, DEFAULT_CLEANER_PARALLELISM);

            HoodieCleaner.CleaningPolicy.valueOf(props.getProperty(CLEANER_POLICY_PROP));
            Preconditions.checkArgument(
                Integer.parseInt(props.getProperty(MAX_COMMITS_TO_KEEP)) > Integer
                    .parseInt(props.getProperty(MIN_COMMITS_TO_KEEP)));
            return config;
        }

    }
}
