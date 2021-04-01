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

package org.apache.hudi.common.config;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Configurations used by the HUDI Metadata Table.
 */
@Immutable
public final class HoodieMetadataConfig extends DefaultHoodieConfig {

  public static final String METADATA_PREFIX = "hoodie.metadata";

  // Enable the internal Metadata Table which saves file listings
  public static final String METADATA_ENABLE_PROP = METADATA_PREFIX + ".enable";
  public static final boolean DEFAULT_METADATA_ENABLE = false;

  // Validate contents of Metadata Table on each access against the actual filesystem
  public static final String METADATA_VALIDATE_PROP = METADATA_PREFIX + ".validate";
  public static final boolean DEFAULT_METADATA_VALIDATE = false;
  public static final boolean DEFAULT_METADATA_ENABLE_FOR_READERS = false;

  // Enable metrics for internal Metadata Table
  public static final String METADATA_METRICS_ENABLE_PROP = METADATA_PREFIX + ".metrics.enable";
  public static final boolean DEFAULT_METADATA_METRICS_ENABLE = false;

  // Parallelism for inserts
  public static final String METADATA_INSERT_PARALLELISM_PROP = METADATA_PREFIX + ".insert.parallelism";
  public static final int DEFAULT_METADATA_INSERT_PARALLELISM = 1;

  // Async clean
  public static final String METADATA_ASYNC_CLEAN_PROP = METADATA_PREFIX + ".clean.async";
  public static final boolean DEFAULT_METADATA_ASYNC_CLEAN = false;

  // Maximum delta commits before compaction occurs
  public static final String METADATA_COMPACT_NUM_DELTA_COMMITS_PROP = METADATA_PREFIX + ".compact.max.delta.commits";
  public static final int DEFAULT_METADATA_COMPACT_NUM_DELTA_COMMITS = 24;

  // Archival settings
  public static final String MIN_COMMITS_TO_KEEP_PROP = METADATA_PREFIX + ".keep.min.commits";
  public static final int DEFAULT_MIN_COMMITS_TO_KEEP = 20;
  public static final String MAX_COMMITS_TO_KEEP_PROP = METADATA_PREFIX + ".keep.max.commits";
  public static final int DEFAULT_MAX_COMMITS_TO_KEEP = 30;

  // Cleaner commits retained
  public static final String CLEANER_COMMITS_RETAINED_PROP = METADATA_PREFIX + ".cleaner.commits.retained";
  public static final int DEFAULT_CLEANER_COMMITS_RETAINED = 3;

  // Controls whether or not, upon failure to fetch from metadata table, should fallback to listing.
  public static final String ENABLE_FALLBACK_PROP = METADATA_PREFIX + ".fallback.enable";
  public static final String DEFAULT_ENABLE_FALLBACK = "true";

  // Regex to filter out matching directories during bootstrap
  public static final String DIRECTORY_FILTER_REGEX = METADATA_PREFIX + ".dir.filter.regex";
  public static final String DEFAULT_DIRECTORY_FILTER_REGEX = "";

  public static final String HOODIE_ASSUME_DATE_PARTITIONING_PROP = "hoodie.assume.date.partitioning";
  public static final String DEFAULT_ASSUME_DATE_PARTITIONING = "false";

  public static final String FILE_LISTING_PARALLELISM_PROP = "hoodie.file.listing.parallelism";
  public static final int DEFAULT_FILE_LISTING_PARALLELISM = 1500;

  private HoodieMetadataConfig(Properties props) {
    super(props);
  }

  public static HoodieMetadataConfig.Builder newBuilder() {
    return new Builder();
  }

  public int getFileListingParallelism() {
    return Math.max(Integer.parseInt(props.getProperty(HoodieMetadataConfig.FILE_LISTING_PARALLELISM_PROP)), 1);
  }

  public Boolean shouldAssumeDatePartitioning() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetadataConfig.HOODIE_ASSUME_DATE_PARTITIONING_PROP));
  }

  public boolean useFileListingMetadata() {
    return Boolean.parseBoolean(props.getProperty(METADATA_ENABLE_PROP));
  }

  public boolean enableFallback() {
    return Boolean.parseBoolean(props.getProperty(ENABLE_FALLBACK_PROP));
  }

  public boolean validateFileListingMetadata() {
    return Boolean.parseBoolean(props.getProperty(METADATA_VALIDATE_PROP));
  }

  public boolean enableMetrics() {
    return Boolean.parseBoolean(props.getProperty(METADATA_METRICS_ENABLE_PROP));
  }

  public String getDirectoryFilterRegex() {
    return props.getProperty(DIRECTORY_FILTER_REGEX);
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder enable(boolean enable) {
      props.setProperty(METADATA_ENABLE_PROP, String.valueOf(enable));
      return this;
    }

    public Builder enableMetrics(boolean enableMetrics) {
      props.setProperty(METADATA_METRICS_ENABLE_PROP, String.valueOf(enableMetrics));
      return this;
    }

    public Builder enableFallback(boolean fallback) {
      props.setProperty(ENABLE_FALLBACK_PROP, String.valueOf(fallback));
      return this;
    }

    public Builder validate(boolean validate) {
      props.setProperty(METADATA_VALIDATE_PROP, String.valueOf(validate));
      return this;
    }

    public Builder withInsertParallelism(int parallelism) {
      props.setProperty(METADATA_INSERT_PARALLELISM_PROP, String.valueOf(parallelism));
      return this;
    }

    public Builder withAsyncClean(boolean asyncClean) {
      props.setProperty(METADATA_ASYNC_CLEAN_PROP, String.valueOf(asyncClean));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      props.setProperty(METADATA_COMPACT_NUM_DELTA_COMMITS_PROP, String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      props.setProperty(MIN_COMMITS_TO_KEEP_PROP, String.valueOf(minToKeep));
      props.setProperty(MAX_COMMITS_TO_KEEP_PROP, String.valueOf(maxToKeep));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      props.setProperty(CLEANER_COMMITS_RETAINED_PROP, String.valueOf(commitsRetained));
      return this;
    }

    public Builder withFileListingParallelism(int parallelism) {
      props.setProperty(FILE_LISTING_PARALLELISM_PROP, String.valueOf(parallelism));
      return this;
    }

    public Builder withAssumeDatePartitioning(boolean assumeDatePartitioning) {
      props.setProperty(HOODIE_ASSUME_DATE_PARTITIONING_PROP, String.valueOf(assumeDatePartitioning));
      return this;
    }

    public Builder withDirectoryFilterRegex(String regex) {
      props.setProperty(DIRECTORY_FILTER_REGEX, regex);
      return this;
    }

    public HoodieMetadataConfig build() {
      HoodieMetadataConfig config = new HoodieMetadataConfig(props);
      setDefaultOnCondition(props, !props.containsKey(METADATA_ENABLE_PROP), METADATA_ENABLE_PROP,
          String.valueOf(DEFAULT_METADATA_ENABLE));
      setDefaultOnCondition(props, !props.containsKey(METADATA_METRICS_ENABLE_PROP), METADATA_METRICS_ENABLE_PROP,
          String.valueOf(DEFAULT_METADATA_METRICS_ENABLE));
      setDefaultOnCondition(props, !props.containsKey(METADATA_VALIDATE_PROP), METADATA_VALIDATE_PROP,
          String.valueOf(DEFAULT_METADATA_VALIDATE));
      setDefaultOnCondition(props, !props.containsKey(METADATA_INSERT_PARALLELISM_PROP), METADATA_INSERT_PARALLELISM_PROP,
          String.valueOf(DEFAULT_METADATA_INSERT_PARALLELISM));
      setDefaultOnCondition(props, !props.containsKey(METADATA_ASYNC_CLEAN_PROP), METADATA_ASYNC_CLEAN_PROP,
          String.valueOf(DEFAULT_METADATA_ASYNC_CLEAN));
      setDefaultOnCondition(props, !props.containsKey(METADATA_COMPACT_NUM_DELTA_COMMITS_PROP),
          METADATA_COMPACT_NUM_DELTA_COMMITS_PROP, String.valueOf(DEFAULT_METADATA_COMPACT_NUM_DELTA_COMMITS));
      setDefaultOnCondition(props, !props.containsKey(CLEANER_COMMITS_RETAINED_PROP), CLEANER_COMMITS_RETAINED_PROP,
          String.valueOf(DEFAULT_CLEANER_COMMITS_RETAINED));
      setDefaultOnCondition(props, !props.containsKey(MAX_COMMITS_TO_KEEP_PROP), MAX_COMMITS_TO_KEEP_PROP,
          String.valueOf(DEFAULT_MAX_COMMITS_TO_KEEP));
      setDefaultOnCondition(props, !props.containsKey(MIN_COMMITS_TO_KEEP_PROP), MIN_COMMITS_TO_KEEP_PROP,
          String.valueOf(DEFAULT_MIN_COMMITS_TO_KEEP));
      setDefaultOnCondition(props, !props.containsKey(FILE_LISTING_PARALLELISM_PROP), FILE_LISTING_PARALLELISM_PROP,
          String.valueOf(DEFAULT_FILE_LISTING_PARALLELISM));
      setDefaultOnCondition(props, !props.containsKey(HOODIE_ASSUME_DATE_PARTITIONING_PROP),
          HOODIE_ASSUME_DATE_PARTITIONING_PROP, DEFAULT_ASSUME_DATE_PARTITIONING);
      setDefaultOnCondition(props, !props.containsKey(ENABLE_FALLBACK_PROP), ENABLE_FALLBACK_PROP,
          DEFAULT_ENABLE_FALLBACK);
      setDefaultOnCondition(props, !props.containsKey(DIRECTORY_FILTER_REGEX), DIRECTORY_FILTER_REGEX,
          DEFAULT_DIRECTORY_FILTER_REGEX);
      return config;
    }
  }
}
