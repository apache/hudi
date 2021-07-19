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
@ConfigClassProperty(name = "Metadata Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations used by the HUDI Metadata Table. "
        + "This table maintains the meta information stored in hudi dataset "
        + "so that listing can be avoided during queries.")
public final class HoodieMetadataConfig extends HoodieConfig {

  public static final String METADATA_PREFIX = "hoodie.metadata";

  // Enable the internal Metadata Table which saves file listings
  public static final ConfigProperty<Boolean> METADATA_ENABLE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".enable")
      .defaultValue(false)
      .sinceVersion("0.7.0")
      .withDocumentation("Enable the internal metadata table which serves table metadata like level file listings");

  // Validate contents of Metadata Table on each access against the actual filesystem
  public static final ConfigProperty<Boolean> METADATA_VALIDATE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".validate")
      .defaultValue(false)
      .sinceVersion("0.7.0")
      .withDocumentation("Validate contents of metadata table on each access; e.g against the actual listings from lake storage");

  public static final boolean DEFAULT_METADATA_ENABLE_FOR_READERS = false;

  // Enable metrics for internal Metadata Table
  public static final ConfigProperty<Boolean> METADATA_METRICS_ENABLE_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".metrics.enable")
      .defaultValue(false)
      .sinceVersion("0.7.0")
      .withDocumentation("Enable publishing of metrics around metadata table.");

  // Parallelism for inserts
  public static final ConfigProperty<Integer> METADATA_INSERT_PARALLELISM_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".insert.parallelism")
      .defaultValue(1)
      .sinceVersion("0.7.0")
      .withDocumentation("Parallelism to use when inserting to the metadata table");

  // Async clean
  public static final ConfigProperty<Boolean> METADATA_ASYNC_CLEAN_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".clean.async")
      .defaultValue(false)
      .sinceVersion("0.7.0")
      .withDocumentation("Enable asynchronous cleaning for metadata table");

  // Maximum delta commits before compaction occurs
  public static final ConfigProperty<Integer> METADATA_COMPACT_NUM_DELTA_COMMITS_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".compact.max.delta.commits")
      .defaultValue(24)
      .sinceVersion("0.7.0")
      .withDocumentation("Controls how often the metadata table is compacted.");

  // Archival settings
  public static final ConfigProperty<Integer> MIN_COMMITS_TO_KEEP_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".keep.min.commits")
      .defaultValue(20)
      .sinceVersion("0.7.0")
      .withDocumentation("Controls the archival of the metadata table’s timeline.");

  public static final ConfigProperty<Integer> MAX_COMMITS_TO_KEEP_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".keep.max.commits")
      .defaultValue(30)
      .sinceVersion("0.7.0")
      .withDocumentation("Controls the archival of the metadata table’s timeline.");

  // Cleaner commits retained
  public static final ConfigProperty<Integer> CLEANER_COMMITS_RETAINED_PROP = ConfigProperty
      .key(METADATA_PREFIX + ".cleaner.commits.retained")
      .defaultValue(3)
      .sinceVersion("0.7.0")
      .withDocumentation("Controls retention/history for metadata table.");

  // Regex to filter out matching directories during bootstrap
  public static final ConfigProperty<String> DIRECTORY_FILTER_REGEX = ConfigProperty
      .key(METADATA_PREFIX + ".dir.filter.regex")
      .defaultValue("")
      .sinceVersion("0.7.0")
      .withDocumentation("Directories matching this regex, will be filtered out when initializing metadata table from lake storage for the first time.");

  public static final ConfigProperty<String> HOODIE_ASSUME_DATE_PARTITIONING_PROP = ConfigProperty
      .key("hoodie.assume.date.partitioning")
      .defaultValue("false")
      .sinceVersion("0.3.0")
      .withDocumentation("Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. "
          + "This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually");

  public static final ConfigProperty<Integer> FILE_LISTING_PARALLELISM_PROP = ConfigProperty
      .key("hoodie.file.listing.parallelism")
      .defaultValue(1500)
      .sinceVersion("0.7.0")
      .withDocumentation("Parallelism to use, when listing the table on lake storage.");

  private HoodieMetadataConfig() {
    super();
  }

  public static HoodieMetadataConfig.Builder newBuilder() {
    return new Builder();
  }

  public int getFileListingParallelism() {
    return Math.max(getInt(HoodieMetadataConfig.FILE_LISTING_PARALLELISM_PROP), 1);
  }

  public Boolean shouldAssumeDatePartitioning() {
    return getBoolean(HoodieMetadataConfig.HOODIE_ASSUME_DATE_PARTITIONING_PROP);
  }

  public boolean useFileListingMetadata() {
    return getBoolean(METADATA_ENABLE_PROP);
  }

  public boolean validateFileListingMetadata() {
    return getBoolean(METADATA_VALIDATE_PROP);
  }

  public boolean enableMetrics() {
    return getBoolean(METADATA_METRICS_ENABLE_PROP);
  }

  public String getDirectoryFilterRegex() {
    return getString(DIRECTORY_FILTER_REGEX);
  }

  public static class Builder {

    private final HoodieMetadataConfig metadataConfig = new HoodieMetadataConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.metadataConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.metadataConfig.getProps().putAll(props);
      return this;
    }

    public Builder enable(boolean enable) {
      metadataConfig.setValue(METADATA_ENABLE_PROP, String.valueOf(enable));
      return this;
    }

    public Builder enableMetrics(boolean enableMetrics) {
      metadataConfig.setValue(METADATA_METRICS_ENABLE_PROP, String.valueOf(enableMetrics));
      return this;
    }

    public Builder validate(boolean validate) {
      metadataConfig.setValue(METADATA_VALIDATE_PROP, String.valueOf(validate));
      return this;
    }

    public Builder withInsertParallelism(int parallelism) {
      metadataConfig.setValue(METADATA_INSERT_PARALLELISM_PROP, String.valueOf(parallelism));
      return this;
    }

    public Builder withAsyncClean(boolean asyncClean) {
      metadataConfig.setValue(METADATA_ASYNC_CLEAN_PROP, String.valueOf(asyncClean));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      metadataConfig.setValue(METADATA_COMPACT_NUM_DELTA_COMMITS_PROP, String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      metadataConfig.setValue(MIN_COMMITS_TO_KEEP_PROP, String.valueOf(minToKeep));
      metadataConfig.setValue(MAX_COMMITS_TO_KEEP_PROP, String.valueOf(maxToKeep));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      metadataConfig.setValue(CLEANER_COMMITS_RETAINED_PROP, String.valueOf(commitsRetained));
      return this;
    }

    public Builder withFileListingParallelism(int parallelism) {
      metadataConfig.setValue(FILE_LISTING_PARALLELISM_PROP, String.valueOf(parallelism));
      return this;
    }

    public Builder withAssumeDatePartitioning(boolean assumeDatePartitioning) {
      metadataConfig.setValue(HOODIE_ASSUME_DATE_PARTITIONING_PROP, String.valueOf(assumeDatePartitioning));
      return this;
    }

    public Builder withDirectoryFilterRegex(String regex) {
      metadataConfig.setValue(DIRECTORY_FILTER_REGEX, regex);
      return this;
    }

    public HoodieMetadataConfig build() {
      metadataConfig.setDefaults(HoodieMetadataConfig.class.getName());
      return metadataConfig;
    }
  }
}
