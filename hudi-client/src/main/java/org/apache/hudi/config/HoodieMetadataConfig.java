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

import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.config.HoodieCompactionConfig.Builder;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Configurations used by the HUDI Metadata Table.
 */
@Immutable
public class HoodieMetadataConfig extends DefaultHoodieConfig {

  public static final String METADATA_PREFIX = "hoodie.metadata";

  // Enable the internal Metadata Table which saves file listings
  public static final String METADATA_ENABLE = METADATA_PREFIX + ".enable";
  public static final boolean DEFAULT_METADATA_ENABLE = false;

  // Validate contents of Metadata Table on each access against the actual filesystem
  public static final String METADATA_VALIDATE = METADATA_PREFIX + ".validate";
  public static final boolean DEFAULT_METADATA_VALIDATE = false;

  // Parallelism for inserts
  public static final String INSERT_PARALLELISM = METADATA_PREFIX + ".insert.parallelism";
  public static final int DEFAULT_INSERT_PARALLELISM = 1;

  // Async clean
  public static final String ASYNC_CLEAN = METADATA_PREFIX + ".clean.async";
  public static final boolean DEFAULT_ASYNC_CLEAN = false;

  // Maximum delta commits before compaction occurs
  public static final String COMPACT_NUM_DELTA_COMMITS = METADATA_PREFIX + ".compact.max.delta.commits";
  public static final int DEFAULT_COMPACT_NUM_DELTA_COMMITS = 24;

  // Archival settings
  public static final String MIN_COMMITS_TO_KEEP = METADATA_PREFIX + ".keep.min.commits";
  public static final int DEFAULT_MIN_COMMITS_TO_KEEP = 20;
  public static final String MAX_COMMITS_TO_KEEP = METADATA_PREFIX + ".keep.max.commits";
  public static final int DEFAULT_MAX_COMMITS_TO_KEEP = 30;

  // Cleaner commits retained
  public static final String CLEANER_COMMITS_RETAINED = METADATA_PREFIX + ".cleaner.commits.retained";
  public static final int DEFAULT_CLEANER_COMMITS_RETAINED = 3;

  private HoodieMetadataConfig(Properties props) {
    super(props);
  }

  public static HoodieMetadataConfig.Builder newBuilder() {
    return new Builder();
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
      props.setProperty(METADATA_ENABLE, String.valueOf(enable));
      return this;
    }

    public Builder validate(boolean validate) {
      props.setProperty(METADATA_VALIDATE, String.valueOf(validate));
      return this;
    }

    public Builder withInsertParallelism(int parallelism) {
      props.setProperty(INSERT_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withAsyncClean(boolean asyncClean) {
      props.setProperty(ASYNC_CLEAN, String.valueOf(asyncClean));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      props.setProperty(COMPACT_NUM_DELTA_COMMITS, String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      props.setProperty(MIN_COMMITS_TO_KEEP, String.valueOf(minToKeep));
      props.setProperty(MAX_COMMITS_TO_KEEP, String.valueOf(maxToKeep));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      props.setProperty(CLEANER_COMMITS_RETAINED, String.valueOf(commitsRetained));
      return this;
    }

    public HoodieMetadataConfig build() {
      HoodieMetadataConfig config = new HoodieMetadataConfig(props);
      setDefaultOnCondition(props, !props.containsKey(METADATA_ENABLE), METADATA_ENABLE,
          String.valueOf(DEFAULT_METADATA_ENABLE));
      setDefaultOnCondition(props, !props.containsKey(METADATA_VALIDATE), METADATA_VALIDATE,
          String.valueOf(DEFAULT_METADATA_VALIDATE));
      setDefaultOnCondition(props, !props.containsKey(INSERT_PARALLELISM), INSERT_PARALLELISM,
          String.valueOf(DEFAULT_INSERT_PARALLELISM));
      setDefaultOnCondition(props, !props.containsKey(ASYNC_CLEAN), ASYNC_CLEAN,
          String.valueOf(DEFAULT_ASYNC_CLEAN));
      setDefaultOnCondition(props, !props.containsKey(COMPACT_NUM_DELTA_COMMITS),
          COMPACT_NUM_DELTA_COMMITS, String.valueOf(DEFAULT_COMPACT_NUM_DELTA_COMMITS));
      setDefaultOnCondition(props, !props.containsKey(CLEANER_COMMITS_RETAINED), CLEANER_COMMITS_RETAINED,
          String.valueOf(DEFAULT_CLEANER_COMMITS_RETAINED));
      setDefaultOnCondition(props, !props.containsKey(MAX_COMMITS_TO_KEEP), MAX_COMMITS_TO_KEEP,
          String.valueOf(DEFAULT_MAX_COMMITS_TO_KEEP));
      setDefaultOnCondition(props, !props.containsKey(MIN_COMMITS_TO_KEEP), MIN_COMMITS_TO_KEEP,
          String.valueOf(DEFAULT_MIN_COMMITS_TO_KEEP));

      return config;
    }
  }

}
