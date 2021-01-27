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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Hoodie payload related configs.
 */
public class HoodieWrapperFileSystemConfig extends DefaultHoodieConfig {

  // Buffering for IO streams
  private static final String CONFIG_IO_BUFFER_PREFIX = "hoodie.fs.io.buffer";
  public static final String FILE_IO_BUFFER_ENABLED = CONFIG_IO_BUFFER_PREFIX + ".enabled";
  private static final String DEFAULT_FILE_IO_BUFFER_ENABLED = "true";

  // Minimum buffer size of data files and log files which are generally large in size
  public static final String MIN_DATA_FILE_IO_BUFFER_SIZE = CONFIG_IO_BUFFER_PREFIX + ".data.min.size";
  private static final String DEFAULT_MIN_DATA_FILE_IO_BUFFER_SIZE_BYTES = String.valueOf(16 * 1024 * 1024); // 16MB;

  // Minimum size of IO buffer for non-data files
  public static final String MIN_FILE_IO_BUFFER_SIZE = CONFIG_IO_BUFFER_PREFIX + ".min.size";
  public static final String DEFAULT_MIN_FILE_IO_BUFFER_SIZE_BYTES = String.valueOf(1 * 1024 * 1024); // 1 MB

  public HoodieWrapperFileSystemConfig(Properties props) {
    super(props);
  }

  public static HoodieWrapperFileSystemConfig.Builder newBuilder() {
    return new HoodieWrapperFileSystemConfig.Builder();
  }

  public boolean isFileIOBufferingEnabled() {
    return Boolean.parseBoolean(props.getProperty(FILE_IO_BUFFER_ENABLED, DEFAULT_FILE_IO_BUFFER_ENABLED));
  }

  public int getFileIOBufferMinSize() {
    return Integer.parseInt(props.getProperty(MIN_FILE_IO_BUFFER_SIZE, DEFAULT_MIN_FILE_IO_BUFFER_SIZE_BYTES));
  }

  public int getDataFileIOBufferMinSize() {
    return Integer.parseInt(props.getProperty(MIN_DATA_FILE_IO_BUFFER_SIZE, DEFAULT_MIN_DATA_FILE_IO_BUFFER_SIZE_BYTES));
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

    public Builder withFileIOBufferingEnabled(boolean enabled) {
      props.setProperty(FILE_IO_BUFFER_ENABLED, String.valueOf(enabled));
      return this;
    }

    public Builder withMinDataFileIOBufferSize(int sizeInBytes) {
      props.setProperty(MIN_DATA_FILE_IO_BUFFER_SIZE, String.valueOf(sizeInBytes));
      return this;
    }

    public Builder withMinFileIOBufferSize(int sizeInBytes) {
      props.setProperty(MIN_FILE_IO_BUFFER_SIZE, String.valueOf(sizeInBytes));
      return this;
    }

    public HoodieWrapperFileSystemConfig build() {
      HoodieWrapperFileSystemConfig config = new HoodieWrapperFileSystemConfig(props);
      setDefaultOnCondition(props, !props.containsKey(FILE_IO_BUFFER_ENABLED), FILE_IO_BUFFER_ENABLED,
          String.valueOf(DEFAULT_FILE_IO_BUFFER_ENABLED));
      setDefaultOnCondition(props, !props.containsKey(MIN_DATA_FILE_IO_BUFFER_SIZE), MIN_DATA_FILE_IO_BUFFER_SIZE,
          String.valueOf(DEFAULT_MIN_DATA_FILE_IO_BUFFER_SIZE_BYTES));
      setDefaultOnCondition(props, !props.containsKey(MIN_FILE_IO_BUFFER_SIZE), MIN_FILE_IO_BUFFER_SIZE,
          String.valueOf(DEFAULT_MIN_FILE_IO_BUFFER_SIZE_BYTES));
      return config;
    }
  }

}
