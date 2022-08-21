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
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.action.clean.CleaningTriggerStrategy;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Clean related config.
 */
@Immutable
@ConfigClassProperty(name = "Clean Configs",
        groupName = ConfigGroups.Names.WRITE_CLIENT,
        description = "Cleaning (reclamation of older/unused file groups/slices).")
public class HoodieCleanConfig extends HoodieConfig {

  public static final ConfigProperty<String> AUTO_CLEAN = ConfigProperty
      .key("hoodie.clean.automatic")
      .defaultValue("true")
      .withDocumentation("When enabled, the cleaner table service is invoked immediately after each commit,"
          + " to delete older file slices. It's recommended to enable this, to ensure metadata and data storage"
          + " growth is bounded.");

  public static final ConfigProperty<String> ASYNC_CLEAN = ConfigProperty
      .key("hoodie.clean.async")
      .defaultValue("false")
      .withDocumentation("Only applies when " + AUTO_CLEAN.key() + " is turned on. "
          + "When turned on runs cleaner async with writing, which can speed up overall write performance.");

  public static final ConfigProperty<String> CLEANER_COMMITS_RETAINED = ConfigProperty
      .key("hoodie.cleaner.commits.retained")
      .defaultValue("10")
      .withDocumentation("Number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits "
          + "(scheduled). This also directly translates into how much data retention the table supports for incremental queries.");

  public static final ConfigProperty<String> CLEANER_HOURS_RETAINED = ConfigProperty.key("hoodie.cleaner.hours.retained")
      .defaultValue("24")
      .withDocumentation("Number of hours for which commits need to be retained. This config provides a more flexible option as"
          + "compared to number of commits retained for cleaning service. Setting this property ensures all the files, but the latest in a file group,"
          + " corresponding to commits with commit times older than the configured number of hours to be retained are cleaned.");

  public static final ConfigProperty<String> CLEANER_POLICY = ConfigProperty
      .key("hoodie.cleaner.policy")
      .defaultValue(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
      .withDocumentation("Cleaning policy to be used. The cleaner service deletes older file slices files to re-claim space."
          + " By default, cleaner spares the file slices written by the last N commits, determined by  " + CLEANER_COMMITS_RETAINED.key()
          + " Long running query plans may often refer to older file slices and will break if those are cleaned, before the query has had"
          + "   a chance to run. So, it is good to make sure that the data is retained for more than the maximum query execution time");

  public static final ConfigProperty<String> CLEAN_TRIGGER_STRATEGY = ConfigProperty
      .key("hoodie.clean.trigger.strategy")
      .defaultValue(CleaningTriggerStrategy.NUM_COMMITS.name())
      .withDocumentation("Controls how cleaning is scheduled. Valid options: "
          + Arrays.stream(CleaningTriggerStrategy.values()).map(Enum::name).collect(Collectors.joining(",")));

  public static final ConfigProperty<String> CLEAN_MAX_COMMITS = ConfigProperty
      .key("hoodie.clean.max.commits")
      .defaultValue("1")
      .withDocumentation("Number of commits after the last clean operation, before scheduling of a new clean is attempted.");

  public static final ConfigProperty<String> CLEANER_FILE_VERSIONS_RETAINED = ConfigProperty
      .key("hoodie.cleaner.fileversions.retained")
      .defaultValue("3")
      .withDocumentation("When " + HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name() + " cleaning policy is used, "
          + " the minimum number of file slices to retain in each file group, during cleaning.");

  public static final ConfigProperty<String> CLEANER_INCREMENTAL_MODE_ENABLE = ConfigProperty
      .key("hoodie.cleaner.incremental.mode")
      .defaultValue("true")
      .withDocumentation("When enabled, the plans for each cleaner service run is computed incrementally off the events "
          + " in the timeline, since the last cleaner run. This is much more efficient than obtaining listings for the full"
          + " table for each planning (even with a metadata table).");

  public static final ConfigProperty<String> FAILED_WRITES_CLEANER_POLICY = ConfigProperty
      .key("hoodie.cleaner.policy.failed.writes")
      .defaultValue(HoodieFailedWritesCleaningPolicy.EAGER.name())
      .withInferFunction(cfg -> {
        Option<String> writeConcurrencyModeOpt = Option.ofNullable(cfg.getString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE));
        if (!writeConcurrencyModeOpt.isPresent() ||
            !writeConcurrencyModeOpt.get().equals(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name())) {
          return Option.empty();
        }
        return Option.of(HoodieFailedWritesCleaningPolicy.LAZY.name());
      })
      .withDocumentation("Cleaning policy for failed writes to be used. Hudi will delete any files written by "
          + "failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before "
          + "every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers)");

  public static final ConfigProperty<String> CLEANER_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.cleaner.parallelism")
      .defaultValue("200")
      .withDocumentation("Parallelism for the cleaning operation. Increase this if cleaning becomes slow.");

  public static final ConfigProperty<Boolean> ALLOW_MULTIPLE_CLEANS = ConfigProperty
      .key("hoodie.clean.allow.multiple")
      .defaultValue(true)
      .sinceVersion("0.11.0")
      .withDocumentation("Allows scheduling/executing multiple cleans by enabling this config. If users prefer to strictly ensure clean requests should be mutually exclusive, "
          + ".i.e. a 2nd clean will not be scheduled if another clean is not yet completed to avoid repeat cleaning of same files, they might want to disable this config.");

  public static final ConfigProperty<String> CLEANER_BOOTSTRAP_BASE_FILE_ENABLE = ConfigProperty
      .key("hoodie.cleaner.delete.bootstrap.base.file")
      .defaultValue("false")
      .withDocumentation("When set to true, cleaner also deletes the bootstrap base file when it's skeleton base file is "
          + " cleaned. Turn this to true, if you want to ensure the bootstrap dataset storage is reclaimed over time, as the"
          + " table receives updates/deletes. Another reason to turn this on, would be to ensure data residing in bootstrap "
          + " base files are also physically deleted, to comply with data privacy enforcement processes.");


  /** @deprecated Use {@link #CLEANER_POLICY} and its methods instead */
  @Deprecated
  public static final String CLEANER_POLICY_PROP = CLEANER_POLICY.key();
  /** @deprecated Use {@link #AUTO_CLEAN} and its methods instead */
  @Deprecated
  public static final String AUTO_CLEAN_PROP = AUTO_CLEAN.key();
  /** @deprecated Use {@link #ASYNC_CLEAN} and its methods instead */
  @Deprecated
  public static final String ASYNC_CLEAN_PROP = ASYNC_CLEAN.key();
  /** @deprecated Use {@link #CLEANER_FILE_VERSIONS_RETAINED} and its methods instead */
  @Deprecated
  public static final String CLEANER_FILE_VERSIONS_RETAINED_PROP = CLEANER_FILE_VERSIONS_RETAINED.key();
  /**
   * @deprecated Use {@link #CLEANER_COMMITS_RETAINED} and its methods instead
   */
  @Deprecated
  public static final String CLEANER_COMMITS_RETAINED_PROP = CLEANER_COMMITS_RETAINED.key();
  /**
   * @deprecated Use {@link #CLEANER_INCREMENTAL_MODE_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String CLEANER_INCREMENTAL_MODE = CLEANER_INCREMENTAL_MODE_ENABLE.key();
  /**
   * @deprecated Use {@link #CLEANER_BOOTSTRAP_BASE_FILE_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String CLEANER_BOOTSTRAP_BASE_FILE_ENABLED = CLEANER_BOOTSTRAP_BASE_FILE_ENABLE.key();
  /**
   * @deprecated Use {@link #CLEANER_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String CLEANER_PARALLELISM = CLEANER_PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #CLEANER_PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CLEANER_PARALLELISM = CLEANER_PARALLELISM_VALUE.defaultValue();
  /** @deprecated Use {@link #CLEANER_POLICY} and its methods instead */
  @Deprecated
  private static final String DEFAULT_CLEANER_POLICY = CLEANER_POLICY.defaultValue();
  /** @deprecated Use {@link #FAILED_WRITES_CLEANER_POLICY} and its methods instead */
  @Deprecated
  public static final String FAILED_WRITES_CLEANER_POLICY_PROP = FAILED_WRITES_CLEANER_POLICY.key();
  /** @deprecated Use {@link #FAILED_WRITES_CLEANER_POLICY} and its methods instead */
  @Deprecated
  private  static final String DEFAULT_FAILED_WRITES_CLEANER_POLICY = FAILED_WRITES_CLEANER_POLICY.defaultValue();
  /** @deprecated Use {@link #AUTO_CLEAN} and its methods instead */
  @Deprecated
  private static final String DEFAULT_AUTO_CLEAN = AUTO_CLEAN.defaultValue();
  /**
   * @deprecated Use {@link #ASYNC_CLEAN} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_ASYNC_CLEAN = ASYNC_CLEAN.defaultValue();
  /**
   * @deprecated Use {@link #CLEANER_INCREMENTAL_MODE_ENABLE} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_INCREMENTAL_CLEANER = CLEANER_INCREMENTAL_MODE_ENABLE.defaultValue();
  /** @deprecated Use {@link #CLEANER_FILE_VERSIONS_RETAINED} and its methods instead */
  @Deprecated
  private static final String DEFAULT_CLEANER_FILE_VERSIONS_RETAINED = CLEANER_FILE_VERSIONS_RETAINED.defaultValue();
  /** @deprecated Use {@link #CLEANER_COMMITS_RETAINED} and its methods instead */
  @Deprecated
  private static final String DEFAULT_CLEANER_COMMITS_RETAINED = CLEANER_COMMITS_RETAINED.defaultValue();
  /**
   * @deprecated Use {@link #CLEANER_BOOTSTRAP_BASE_FILE_ENABLE} and its methods instead
   */
  @Deprecated
  private static final String DEFAULT_CLEANER_BOOTSTRAP_BASE_FILE_ENABLED = CLEANER_BOOTSTRAP_BASE_FILE_ENABLE.defaultValue();

  private HoodieCleanConfig() {
    super();
  }

  public static HoodieCleanConfig.Builder newBuilder() {
    return new HoodieCleanConfig.Builder();
  }

  public static class Builder {

    private final HoodieCleanConfig cleanConfig = new HoodieCleanConfig();

    public HoodieCleanConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.cleanConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieCleanConfig.Builder fromProperties(Properties props) {
      this.cleanConfig.getProps().putAll(props);
      return this;
    }

    public HoodieCleanConfig.Builder withAutoClean(Boolean autoClean) {
      cleanConfig.setValue(AUTO_CLEAN, String.valueOf(autoClean));
      return this;
    }

    public HoodieCleanConfig.Builder withAsyncClean(Boolean asyncClean) {
      cleanConfig.setValue(ASYNC_CLEAN, String.valueOf(asyncClean));
      return this;
    }

    public HoodieCleanConfig.Builder withIncrementalCleaningMode(Boolean incrementalCleaningMode) {
      cleanConfig.setValue(CLEANER_INCREMENTAL_MODE_ENABLE, String.valueOf(incrementalCleaningMode));
      return this;
    }

    public HoodieCleanConfig.Builder withCleaningTriggerStrategy(String cleaningTriggerStrategy) {
      cleanConfig.setValue(CLEAN_TRIGGER_STRATEGY, cleaningTriggerStrategy);
      return this;
    }

    public HoodieCleanConfig.Builder withMaxCommitsBeforeCleaning(int maxCommitsBeforeCleaning) {
      cleanConfig.setValue(CLEAN_MAX_COMMITS, String.valueOf(maxCommitsBeforeCleaning));
      return this;
    }

    public HoodieCleanConfig.Builder withCleanerPolicy(HoodieCleaningPolicy policy) {
      cleanConfig.setValue(CLEANER_POLICY, policy.name());
      return this;
    }

    public HoodieCleanConfig.Builder retainFileVersions(int fileVersionsRetained) {
      cleanConfig.setValue(CLEANER_FILE_VERSIONS_RETAINED, String.valueOf(fileVersionsRetained));
      return this;
    }

    public HoodieCleanConfig.Builder retainCommits(int commitsRetained) {
      cleanConfig.setValue(CLEANER_COMMITS_RETAINED, String.valueOf(commitsRetained));
      return this;
    }

    public HoodieCleanConfig.Builder cleanerNumHoursRetained(int cleanerHoursRetained) {
      cleanConfig.setValue(CLEANER_HOURS_RETAINED, String.valueOf(cleanerHoursRetained));
      return this;
    }

    public HoodieCleanConfig.Builder allowMultipleCleans(boolean allowMultipleCleanSchedules) {
      cleanConfig.setValue(ALLOW_MULTIPLE_CLEANS, String.valueOf(allowMultipleCleanSchedules));
      return this;
    }

    public HoodieCleanConfig.Builder withCleanerParallelism(int cleanerParallelism) {
      cleanConfig.setValue(CLEANER_PARALLELISM_VALUE, String.valueOf(cleanerParallelism));
      return this;
    }

    public HoodieCleanConfig.Builder withCleanBootstrapBaseFileEnabled(Boolean cleanBootstrapSourceFileEnabled) {
      cleanConfig.setValue(CLEANER_BOOTSTRAP_BASE_FILE_ENABLE, String.valueOf(cleanBootstrapSourceFileEnabled));
      return this;
    }

    public HoodieCleanConfig.Builder withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy failedWritesPolicy) {
      cleanConfig.setValue(FAILED_WRITES_CLEANER_POLICY, failedWritesPolicy.name());
      return this;
    }

    public HoodieCleanConfig build() {
      cleanConfig.setDefaults(HoodieCleanConfig.class.getName());
      HoodieCleaningPolicy.valueOf(cleanConfig.getString(CLEANER_POLICY));
      return cleanConfig;
    }
  }
}
