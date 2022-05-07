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

package org.apache.hudi.common.fs;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * The file system retry relevant config options.
 */
@ConfigClassProperty(name = "FileSystem Guard Configurations",
        groupName = ConfigGroups.Names.WRITE_CLIENT,
        description = "The filesystem retry related config options, to help deal with runtime exception like list/get/put/delete performance issues.")
public class FileSystemRetryConfig  extends HoodieConfig {

  public static final ConfigProperty<String> FILESYSTEM_RETRY_ENABLE = ConfigProperty
      .key("hoodie.filesystem.operation.retry.enable")
      .defaultValue("false")
      .sinceVersion("0.11.0")
      .withDocumentation("Enabled to handle list/get/delete etc file system performance issue.");

  public static final ConfigProperty<Long> INITIAL_RETRY_INTERVAL_MS = ConfigProperty
      .key("hoodie.filesystem.operation.retry.initial_interval_ms")
      .defaultValue(100L)
      .sinceVersion("0.11.0")
      .withDocumentation("Amount of time (in ms) to wait, before retry to do operations on storage.");

  public static final ConfigProperty<Long> MAX_RETRY_INTERVAL_MS = ConfigProperty
      .key("hoodie.filesystem.operation.retry.max_interval_ms")
      .defaultValue(2000L)
      .sinceVersion("0.11.0")
      .withDocumentation("Maximum amount of time (in ms), to wait for next retry.");

  public static final ConfigProperty<Integer> MAX_RETRY_NUMBERS = ConfigProperty
      .key("hoodie.filesystem.operation.retry.max_numbers")
      .defaultValue(4)
      .sinceVersion("0.11.0")
      .withDocumentation("Maximum number of retry actions to perform, with exponential backoff.");

  public static final ConfigProperty<String> RETRY_EXCEPTIONS = ConfigProperty
      .key("hoodie.filesystem.operation.retry.exceptions")
      .defaultValue("")
      .sinceVersion("0.11.0")
      .withDocumentation("The class name of the Exception that needs to be re-tryed, separated by commas. "
          + "Default is empty which means retry all the IOException and RuntimeException from FileSystem");

  private FileSystemRetryConfig() {
    super();
  }

  public long getInitialRetryIntervalMs() {
    return getLong(INITIAL_RETRY_INTERVAL_MS);
  }

  public long getMaxRetryIntervalMs() {
    return getLong(MAX_RETRY_INTERVAL_MS);
  }

  public int getMaxRetryNumbers() {
    return getInt(MAX_RETRY_NUMBERS);
  }

  public boolean isFileSystemActionRetryEnable() {
    return Boolean.parseBoolean(getStringOrDefault(FILESYSTEM_RETRY_ENABLE));
  }

  public static FileSystemRetryConfig.Builder newBuilder() {
    return new Builder();
  }

  public String getRetryExceptions() {
    return getString(RETRY_EXCEPTIONS);
  }

  /**
   * The builder used to build filesystem configurations.
   */
  public static class Builder {

    private final FileSystemRetryConfig fileSystemRetryConfig = new FileSystemRetryConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        fileSystemRetryConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.fileSystemRetryConfig.getProps().putAll(props);
      return this;
    }

    public Builder withMaxRetryNumbers(int numbers) {
      fileSystemRetryConfig.setValue(MAX_RETRY_NUMBERS, String.valueOf(numbers));
      return this;
    }

    public Builder withInitialRetryIntervalMs(long intervalMs) {
      fileSystemRetryConfig.setValue(INITIAL_RETRY_INTERVAL_MS, String.valueOf(intervalMs));
      return this;
    }

    public Builder withMaxRetryIntervalMs(long intervalMs) {
      fileSystemRetryConfig.setValue(MAX_RETRY_INTERVAL_MS, String.valueOf(intervalMs));
      return this;
    }

    public Builder withFileSystemActionRetryEnabled(boolean enabled) {
      fileSystemRetryConfig.setValue(FILESYSTEM_RETRY_ENABLE, String.valueOf(enabled));
      return this;
    }

    public FileSystemRetryConfig build() {
      fileSystemRetryConfig.setDefaults(FileSystemRetryConfig.class.getName());
      return fileSystemRetryConfig;
    }
  }
}
