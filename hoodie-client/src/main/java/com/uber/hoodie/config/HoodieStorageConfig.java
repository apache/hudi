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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

/**
 * Storage related config
 */
@Immutable
public class HoodieStorageConfig extends DefaultHoodieConfig {

  public static final String PARQUET_FILE_MAX_BYTES = "hoodie.parquet.max.file.size";
  public static final String DEFAULT_PARQUET_FILE_MAX_BYTES = String.valueOf(120 * 1024 * 1024);
  public static final String PARQUET_BLOCK_SIZE_BYTES = "hoodie.parquet.block.size";
  public static final String DEFAULT_PARQUET_BLOCK_SIZE_BYTES = DEFAULT_PARQUET_FILE_MAX_BYTES;
  public static final String PARQUET_PAGE_SIZE_BYTES = "hoodie.parquet.page.size";
  public static final String DEFAULT_PARQUET_PAGE_SIZE_BYTES = String.valueOf(1 * 1024 * 1024);
  // used to size log files
  public static final String LOGFILE_SIZE_MAX_BYTES = "hoodie.logfile.max.size";
  public static final String DEFAULT_LOGFILE_SIZE_MAX_BYTES = String
      .valueOf(1024 * 1024 * 1024); // 1 GB
  // used to size data blocks in log file
  public static final String LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES = "hoodie.logfile.data.block.max.size";
  public static final String DEFAULT_LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES = String.valueOf(256 * 1024 * 1024); // 256 MB
  public static final String PARQUET_COMPRESSION_RATIO = "hoodie.parquet.compression.ratio";
  // Default compression ratio for parquet
  public static final String DEFAULT_STREAM_COMPRESSION_RATIO = String.valueOf(0.1);
  public static final String LOGFILE_TO_PARQUET_COMPRESSION_RATIO = "hoodie.logfile.to.parquet.compression.ratio";
  // Default compression ratio for log file to parquet, general 3x
  public static final String DEFAULT_LOGFILE_TO_PARQUET_COMPRESSION_RATIO = String.valueOf(0.35);

  private HoodieStorageConfig(Properties props) {
    super(props);
  }

  public static HoodieStorageConfig.Builder newBuilder() {
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

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder limitFileSize(long maxFileSize) {
      props.setProperty(PARQUET_FILE_MAX_BYTES, String.valueOf(maxFileSize));
      return this;
    }

    public Builder parquetBlockSize(int blockSize) {
      props.setProperty(PARQUET_BLOCK_SIZE_BYTES, String.valueOf(blockSize));
      return this;
    }

    public Builder parquetPageSize(int pageSize) {
      props.setProperty(PARQUET_PAGE_SIZE_BYTES, String.valueOf(pageSize));
      return this;
    }

    public Builder logFileDataBlockMaxSize(int dataBlockSize) {
      props.setProperty(LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES, String.valueOf(dataBlockSize));
      return this;
    }

    public Builder logFileMaxSize(int logFileSize) {
      props.setProperty(LOGFILE_SIZE_MAX_BYTES, String.valueOf(logFileSize));
      return this;
    }

    public Builder parquetCompressionRatio(double parquetCompressionRatio) {
      props.setProperty(PARQUET_COMPRESSION_RATIO, String.valueOf(parquetCompressionRatio));
      return this;
    }

    public Builder logFileToParquetCompressionRatio(double logFileToParquetCompressionRatio) {
      props.setProperty(LOGFILE_TO_PARQUET_COMPRESSION_RATIO, String.valueOf(logFileToParquetCompressionRatio));
      return this;
    }

    public HoodieStorageConfig build() {
      HoodieStorageConfig config = new HoodieStorageConfig(props);
      setDefaultOnCondition(props, !props.containsKey(PARQUET_FILE_MAX_BYTES),
          PARQUET_FILE_MAX_BYTES, DEFAULT_PARQUET_FILE_MAX_BYTES);
      setDefaultOnCondition(props, !props.containsKey(PARQUET_BLOCK_SIZE_BYTES),
          PARQUET_BLOCK_SIZE_BYTES, DEFAULT_PARQUET_BLOCK_SIZE_BYTES);
      setDefaultOnCondition(props, !props.containsKey(PARQUET_PAGE_SIZE_BYTES),
          PARQUET_PAGE_SIZE_BYTES, DEFAULT_PARQUET_PAGE_SIZE_BYTES);
      setDefaultOnCondition(props, !props.containsKey(LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES),
          LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES, DEFAULT_LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES);
      setDefaultOnCondition(props, !props.containsKey(LOGFILE_SIZE_MAX_BYTES),
          LOGFILE_SIZE_MAX_BYTES, DEFAULT_LOGFILE_SIZE_MAX_BYTES);
      setDefaultOnCondition(props, !props.containsKey(PARQUET_COMPRESSION_RATIO),
          PARQUET_COMPRESSION_RATIO, DEFAULT_STREAM_COMPRESSION_RATIO);
      setDefaultOnCondition(props, !props.containsKey(LOGFILE_TO_PARQUET_COMPRESSION_RATIO),
          LOGFILE_TO_PARQUET_COMPRESSION_RATIO, DEFAULT_LOGFILE_TO_PARQUET_COMPRESSION_RATIO);
      return config;
    }
  }

}
