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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Archival related config.
 */
@Immutable
@ConfigClassProperty(name = "Archival Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control archival.")
public class HoodieArchivalConfig extends HoodieConfig {

  public static final ConfigProperty<String> AUTO_ARCHIVE = ConfigProperty
      .key("hoodie.archive.automatic")
      .defaultValue("true")
      .withDocumentation("When enabled, the archival table service is invoked immediately after each commit,"
          + " to archive commits if we cross a maximum value of commits."
          + " It's recommended to enable this, to ensure number of active commits is bounded.");

  public static final ConfigProperty<String> ASYNC_ARCHIVE = ConfigProperty
      .key("hoodie.archive.async")
      .defaultValue("false")
      .sinceVersion("0.11.0")
      .withDocumentation("Only applies when " + AUTO_ARCHIVE.key() + " is turned on. "
          + "When turned on runs archiver async with writing, which can speed up overall write performance.");

  public static final ConfigProperty<String> MAX_COMMITS_TO_KEEP = ConfigProperty
      .key("hoodie.keep.max.commits")
      .defaultValue("30")
      .withDocumentation("Archiving service moves older entries from timeline into an archived log after each write, to "
          + " keep the metadata overhead constant, even as the table size grows."
          + "This config controls the maximum number of instants to retain in the active timeline. ");

  public static final ConfigProperty<Integer> DELETE_ARCHIVED_INSTANT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.archive.delete.parallelism")
      .defaultValue(100)
      .withDocumentation("Parallelism for deleting archived hoodie commits.");

  public static final ConfigProperty<String> MIN_COMMITS_TO_KEEP = ConfigProperty
      .key("hoodie.keep.min.commits")
      .defaultValue("20")
      .withDocumentation("Similar to " + MAX_COMMITS_TO_KEEP.key() + ", but controls the minimum number of"
          + "instants to retain in the active timeline.");

  public static final ConfigProperty<String> COMMITS_ARCHIVAL_BATCH_SIZE = ConfigProperty
      .key("hoodie.commits.archival.batch")
      .defaultValue(String.valueOf(10))
      .withDocumentation("Archiving of instants is batched in best-effort manner, to pack more instants into a single"
          + " archive log. This config controls such archival batch size.");

  public static final ConfigProperty<Integer> ARCHIVE_MERGE_FILES_BATCH_SIZE = ConfigProperty
      .key("hoodie.archive.merge.files.batch.size")
      .defaultValue(10)
      .withDocumentation("The number of small archive files to be merged at once.");

  public static final ConfigProperty<Long> ARCHIVE_MERGE_SMALL_FILE_LIMIT_BYTES = ConfigProperty
      .key("hoodie.archive.merge.small.file.limit.bytes")
      .defaultValue(20L * 1024 * 1024)
      .withDocumentation("This config sets the archive file size limit below which an archive file becomes a candidate to be selected as such a small file.");

  public static final ConfigProperty<Boolean> ARCHIVE_MERGE_ENABLE = ConfigProperty
      .key("hoodie.archive.merge.enable")
      .defaultValue(false)
      .withDocumentation("When enable, hoodie will auto merge several small archive files into larger one. It's"
          + " useful when storage scheme doesn't support append operation.");

  public static final ConfigProperty<Boolean> ARCHIVE_BEYOND_SAVEPOINT = ConfigProperty
      .key("hoodie.archive.proceed.savepoint")
      .defaultValue(false)
      .sinceVersion("0.12.0")
      .withDocumentation("If enabled, archival will proceed beyond savepoint, skipping savepoint commits. "
          + "If disabled, archival will stop at the earliest savepoint commit.");

  /**
   * @deprecated Use {@link #MAX_COMMITS_TO_KEEP} and its methods instead
   */
  @Deprecated
  public static final String MAX_COMMITS_TO_KEEP_PROP = MAX_COMMITS_TO_KEEP.key();
  /**
   * @deprecated Use {@link #MIN_COMMITS_TO_KEEP} and its methods instead
   */
  @Deprecated
  public static final String MIN_COMMITS_TO_KEEP_PROP = MIN_COMMITS_TO_KEEP.key();
  /**
   * @deprecated Use {@link #COMMITS_ARCHIVAL_BATCH_SIZE} and its methods instead
   */
  @Deprecated
  public static final String COMMITS_ARCHIVAL_BATCH_SIZE_PROP = COMMITS_ARCHIVAL_BATCH_SIZE.key();
  /**
   * @deprecated Use {@link #MAX_COMMITS_TO_KEEP} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_MAX_COMMITS_TO_KEEP = MAX_COMMITS_TO_KEEP.defaultValue();
  /**
   * @deprecated Use {@link #MIN_COMMITS_TO_KEEP} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_MIN_COMMITS_TO_KEEP = MIN_COMMITS_TO_KEEP.defaultValue();
  /**
   * @deprecated Use {@link #COMMITS_ARCHIVAL_BATCH_SIZE} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_COMMITS_ARCHIVAL_BATCH_SIZE = COMMITS_ARCHIVAL_BATCH_SIZE.defaultValue();

  private HoodieArchivalConfig() {
    super();
  }

  public static HoodieArchivalConfig.Builder newBuilder() {
    return new HoodieArchivalConfig.Builder();
  }

  public static class Builder {

    private final HoodieArchivalConfig archivalConfig = new HoodieArchivalConfig();

    public HoodieArchivalConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.archivalConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieArchivalConfig.Builder fromProperties(Properties props) {
      this.archivalConfig.getProps().putAll(props);
      return this;
    }

    public HoodieArchivalConfig.Builder withAutoArchive(Boolean autoArchive) {
      archivalConfig.setValue(AUTO_ARCHIVE, String.valueOf(autoArchive));
      return this;
    }

    public HoodieArchivalConfig.Builder withAsyncArchive(Boolean asyncArchive) {
      archivalConfig.setValue(ASYNC_ARCHIVE, String.valueOf(asyncArchive));
      return this;
    }

    public HoodieArchivalConfig.Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      archivalConfig.setValue(MIN_COMMITS_TO_KEEP, String.valueOf(minToKeep));
      archivalConfig.setValue(MAX_COMMITS_TO_KEEP, String.valueOf(maxToKeep));
      return this;
    }

    public HoodieArchivalConfig.Builder withArchiveMergeFilesBatchSize(int number) {
      archivalConfig.setValue(ARCHIVE_MERGE_FILES_BATCH_SIZE, String.valueOf(number));
      return this;
    }

    public HoodieArchivalConfig.Builder withArchiveMergeSmallFileLimit(long size) {
      archivalConfig.setValue(ARCHIVE_MERGE_SMALL_FILE_LIMIT_BYTES, String.valueOf(size));
      return this;
    }

    public HoodieArchivalConfig.Builder withArchiveMergeEnable(boolean enable) {
      archivalConfig.setValue(ARCHIVE_MERGE_ENABLE, String.valueOf(enable));
      return this;
    }

    public HoodieArchivalConfig.Builder withArchiveDeleteParallelism(int archiveDeleteParallelism) {
      archivalConfig.setValue(DELETE_ARCHIVED_INSTANT_PARALLELISM_VALUE, String.valueOf(archiveDeleteParallelism));
      return this;
    }

    public HoodieArchivalConfig.Builder withCommitsArchivalBatchSize(int batchSize) {
      archivalConfig.setValue(COMMITS_ARCHIVAL_BATCH_SIZE, String.valueOf(batchSize));
      return this;
    }

    public Builder withArchiveBeyondSavepoint(boolean archiveBeyondSavepoint) {
      archivalConfig.setValue(ARCHIVE_BEYOND_SAVEPOINT, String.valueOf(archiveBeyondSavepoint));
      return this;
    }

    public HoodieArchivalConfig build() {
      archivalConfig.setDefaults(HoodieArchivalConfig.class.getName());
      return archivalConfig;
    }
  }
}
