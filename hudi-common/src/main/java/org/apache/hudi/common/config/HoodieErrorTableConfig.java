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

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Configurations used by the hudi error table.
 */
@Immutable
public final class HoodieErrorTableConfig extends DefaultHoodieConfig {

  // Error table name suffix
  public static final String ERROR_TABLE_NAME_SUFFIX = "_error";

  // Error table base path
  public static final String ERROR_TABLE_BASE_PATH = HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR + "error";

  // Error table schema
  public static final String ERROR_TABLE_SCHEMA = "{\"type\": \"record\",\"namespace\": \"org.apache.hudi.common\",\"name\": \"ErrorRecord\",\"fields\": "
                             + "[{\"name\": \"uuid\",\"type\": \"string\"},{\"name\": \"ts\",\"type\": \"string\"},"
                             + "{\"name\": \"schema\",\"type\": [\"null\", \"string\"],\"default\": null},{\"name\": \"record\",\"type\": "
                             + "[\"null\", \"string\"],\"default\": null},{\"name\": \"message\",\"type\": [\"null\", \"string\"],\"default\": null},"
                             + "{\"name\": \"context\",\"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}],\"default\": null}]}";


  // Enable the internal Error Table which saves error record
  public static final String ERROR_TABLE_PREFIX = "hoodie.write.error.table";

  public static final String ERROR_TABLE_ENABLE_PROP = ERROR_TABLE_PREFIX + ".enabled";
  public static final boolean DEFAULT_ERROR_TABLE_ENABLE = false;

  // Parallelism for inserts
  public static final String ERROR_TABLE_INSERT_PARALLELISM_PROP = ERROR_TABLE_PREFIX + ".insert.parallelism";
  public static final int DEFAULT_ERROR_TABLE_INSERT_PARALLELISM = 1;

  // Archival settings
  public static final String MIN_COMMITS_TO_KEEP_PROP = ERROR_TABLE_PREFIX + ".keep.min.commits";
  public static final int DEFAULT_MIN_COMMITS_TO_KEEP = 20;
  public static final String MAX_COMMITS_TO_KEEP_PROP = ERROR_TABLE_PREFIX + ".keep.max.commits";
  public static final int DEFAULT_MAX_COMMITS_TO_KEEP = 30;

  // Cleaner commits retained
  public static final String CLEANER_COMMITS_RETAINED_PROP = ERROR_TABLE_PREFIX + ".cleaner.commits.retained";
  public static final int DEFAULT_CLEANER_COMMITS_RETAINED = 3;

  private HoodieErrorTableConfig(Properties props) {
    super(props);
  }

  public static HoodieErrorTableConfig.Builder newBuilder() {
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
      props.setProperty(ERROR_TABLE_ENABLE_PROP, String.valueOf(enable));
      return this;
    }

    public Builder withInsertParallelism(int parallelism) {
      props.setProperty(ERROR_TABLE_INSERT_PARALLELISM_PROP, String.valueOf(parallelism));
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

    public HoodieErrorTableConfig build() {
      HoodieErrorTableConfig config = new HoodieErrorTableConfig(props);
      setDefaultOnCondition(props, !props.containsKey(ERROR_TABLE_ENABLE_PROP), ERROR_TABLE_ENABLE_PROP,
          String.valueOf(DEFAULT_ERROR_TABLE_ENABLE));
      setDefaultOnCondition(props, !props.containsKey(ERROR_TABLE_INSERT_PARALLELISM_PROP), ERROR_TABLE_INSERT_PARALLELISM_PROP,
          String.valueOf(DEFAULT_ERROR_TABLE_INSERT_PARALLELISM));
      setDefaultOnCondition(props, !props.containsKey(CLEANER_COMMITS_RETAINED_PROP), CLEANER_COMMITS_RETAINED_PROP,
          String.valueOf(DEFAULT_CLEANER_COMMITS_RETAINED));
      setDefaultOnCondition(props, !props.containsKey(MAX_COMMITS_TO_KEEP_PROP), MAX_COMMITS_TO_KEEP_PROP,
          String.valueOf(DEFAULT_MAX_COMMITS_TO_KEEP));
      setDefaultOnCondition(props, !props.containsKey(MIN_COMMITS_TO_KEEP_PROP), MIN_COMMITS_TO_KEEP_PROP,
          String.valueOf(DEFAULT_MIN_COMMITS_TO_KEEP));

      return config;
    }
  }
}
