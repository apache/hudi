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
 * Storage related config.
 */
@Immutable
@ConfigClassProperty(name = "Storage Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control aspects around writing, sizing, reading base and log files.")
public class HoodieStorageConfig extends HoodieConfig {

  public static final ConfigProperty<String> PARQUET_MAX_FILE_SIZE = ConfigProperty
      .key("hoodie.parquet.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Target size in bytes for parquet files produced by Hudi write phases. "
          + "For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.");

  public static final ConfigProperty<String> PARQUET_BLOCK_SIZE = ConfigProperty
      .key("hoodie.parquet.block.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Parquet RowGroup size in bytes. It's recommended to make this large enough that scan costs can be"
          + " amortized by packing enough column values into a single row group.");

  public static final ConfigProperty<String> PARQUET_PAGE_SIZE = ConfigProperty
      .key("hoodie.parquet.page.size")
      .defaultValue(String.valueOf(1 * 1024 * 1024))
      .withDocumentation("Parquet page size in bytes. Page is the unit of read within a parquet file. "
          + "Within a block, pages are compressed separately.");

  public static final ConfigProperty<String> ORC_FILE_MAX_SIZE = ConfigProperty
      .key("hoodie.orc.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Target file size in bytes for ORC base files.");

  public static final ConfigProperty<String> ORC_STRIPE_SIZE = ConfigProperty
      .key("hoodie.orc.stripe.size")
      .defaultValue(String.valueOf(64 * 1024 * 1024))
      .withDocumentation("Size of the memory buffer in bytes for writing");

  public static final ConfigProperty<String> ORC_BLOCK_SIZE = ConfigProperty
      .key("hoodie.orc.block.size")
      .defaultValue(ORC_FILE_MAX_SIZE.defaultValue())
      .withDocumentation("ORC block size, recommended to be aligned with the target file size.");

  public static final ConfigProperty<String> HFILE_MAX_FILE_SIZE = ConfigProperty
      .key("hoodie.hfile.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Target file size in bytes for HFile base files.");

  public static final ConfigProperty<String> HFILE_BLOCK_SIZE = ConfigProperty
      .key("hoodie.hfile.block.size")
      .defaultValue(String.valueOf(1024 * 1024))
      .withDocumentation("Lower values increase the size in bytes of metadata tracked within HFile, but can offer potentially "
          + "faster lookup times.");

  public static final ConfigProperty<String> LOGFILE_DATA_BLOCK_FORMAT = ConfigProperty
      .key("hoodie.logfile.data.block.format")
      .noDefaultValue()
      .withDocumentation("Format of the data block within delta logs. Following formats are currently supported \"avro\", \"hfile\", \"parquet\"");

  public static final ConfigProperty<String> LOGFILE_MAX_SIZE = ConfigProperty
      .key("hoodie.logfile.max.size")
      .defaultValue(String.valueOf(1024 * 1024 * 1024)) // 1 GB
      .withDocumentation("LogFile max size in bytes. This is the maximum size allowed for a log file "
          + "before it is rolled over to the next version.");

  public static final ConfigProperty<String> LOGFILE_DATA_BLOCK_MAX_SIZE = ConfigProperty
      .key("hoodie.logfile.data.block.max.size")
      .defaultValue(String.valueOf(256 * 1024 * 1024))
      .withDocumentation("LogFile Data block max size in bytes. This is the maximum size allowed for a single data block "
          + "to be appended to a log file. This helps to make sure the data appended to the log file is broken up "
          + "into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory.");

  public static final ConfigProperty<String> PARQUET_COMPRESSION_RATIO_FRACTION = ConfigProperty
      .key("hoodie.parquet.compression.ratio")
      .defaultValue(String.valueOf(0.1))
      .withDocumentation("Expected compression of parquet data used by Hudi, when it tries to size new parquet files. "
          + "Increase this value, if bulk_insert is producing smaller than expected sized files");

  // Default compression codec for parquet
  public static final ConfigProperty<String> PARQUET_COMPRESSION_CODEC_NAME = ConfigProperty
      .key("hoodie.parquet.compression.codec")
      .defaultValue("gzip")
      .withDocumentation("Compression Codec for parquet files");

  public static final ConfigProperty<Boolean> PARQUET_DICTIONARY_ENABLED = ConfigProperty
      .key("hoodie.parquet.dictionary.enabled")
      .defaultValue(true)
      .withDocumentation("Whether to use dictionary encoding");

  public static final ConfigProperty<String> HFILE_COMPRESSION_ALGORITHM_NAME = ConfigProperty
      .key("hoodie.hfile.compression.algorithm")
      .defaultValue("GZ")
      .withDocumentation("Compression codec to use for hfile base files.");

  public static final ConfigProperty<String> ORC_COMPRESSION_CODEC_NAME = ConfigProperty
      .key("hoodie.orc.compression.codec")
      .defaultValue("ZLIB")
      .withDocumentation("Compression codec to use for ORC base files.");

  // Default compression ratio for log file to parquet, general 3x
  public static final ConfigProperty<String> LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION = ConfigProperty
      .key("hoodie.logfile.to.parquet.compression.ratio")
      .defaultValue(String.valueOf(0.35))
      .withDocumentation("Expected additional compression as records move from log files to parquet. Used for merge_on_read "
          + "table to send inserts into log files & control the size of compacted parquet file.");

  /**
   * @deprecated Use {@link #PARQUET_MAX_FILE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_FILE_MAX_BYTES = PARQUET_MAX_FILE_SIZE.key();
  /**
   * @deprecated Use {@link #PARQUET_MAX_FILE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_FILE_MAX_BYTES = PARQUET_MAX_FILE_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #PARQUET_BLOCK_SIZE} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_BLOCK_SIZE_BYTES = PARQUET_BLOCK_SIZE.key();
  /**
   * @deprecated Use {@link #PARQUET_BLOCK_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_BLOCK_SIZE_BYTES = PARQUET_BLOCK_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #PARQUET_PAGE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_PAGE_SIZE_BYTES = PARQUET_PAGE_SIZE.key();
  /**
   * @deprecated Use {@link #PARQUET_PAGE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_PAGE_SIZE_BYTES = PARQUET_PAGE_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #HFILE_MAX_FILE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String HFILE_FILE_MAX_BYTES = HFILE_MAX_FILE_SIZE.key();
  /**
   * @deprecated Use {@link #HFILE_MAX_FILE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HFILE_FILE_MAX_BYTES = HFILE_MAX_FILE_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #HFILE_BLOCK_SIZE} and its methods instead
   */
  @Deprecated
  public static final String HFILE_BLOCK_SIZE_BYTES = HFILE_BLOCK_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #HFILE_BLOCK_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HFILE_BLOCK_SIZE_BYTES = HFILE_BLOCK_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #LOGFILE_MAX_SIZE} and its methods instead
   */
  @Deprecated
  public static final String LOGFILE_SIZE_MAX_BYTES = LOGFILE_MAX_SIZE.key();
  /**
   * @deprecated Use {@link #LOGFILE_MAX_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_LOGFILE_SIZE_MAX_BYTES = LOGFILE_MAX_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #LOGFILE_DATA_BLOCK_MAX_SIZE} and its methods instead
   */
  @Deprecated
  public static final String LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES = LOGFILE_DATA_BLOCK_MAX_SIZE.key();
  /**
   * @deprecated Use {@link #LOGFILE_DATA_BLOCK_MAX_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES = LOGFILE_DATA_BLOCK_MAX_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #PARQUET_COMPRESSION_RATIO_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_COMPRESSION_RATIO = PARQUET_COMPRESSION_RATIO_FRACTION.key();
  /**
   * @deprecated Use {@link #PARQUET_COMPRESSION_RATIO_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_STREAM_COMPRESSION_RATIO = PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue();
  /**
   * @deprecated Use {@link #PARQUET_COMPRESSION_CODEC_NAME} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_COMPRESSION_CODEC = PARQUET_COMPRESSION_CODEC_NAME.key();
  /**
   * @deprecated Use {@link #HFILE_COMPRESSION_ALGORITHM_NAME} and its methods instead
   */
  @Deprecated
  public static final String HFILE_COMPRESSION_ALGORITHM = HFILE_COMPRESSION_ALGORITHM_NAME.key();
  /**
   * @deprecated Use {@link #PARQUET_COMPRESSION_CODEC_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_COMPRESSION_CODEC = PARQUET_COMPRESSION_CODEC_NAME.defaultValue();
  /**
   * @deprecated Use {@link #HFILE_COMPRESSION_ALGORITHM_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HFILE_COMPRESSION_ALGORITHM = HFILE_COMPRESSION_ALGORITHM_NAME.defaultValue();
  /**
   * @deprecated Use {@link #LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String LOGFILE_TO_PARQUET_COMPRESSION_RATIO = LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.key();
  /**
   * @deprecated Use {@link #LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_LOGFILE_TO_PARQUET_COMPRESSION_RATIO = LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue();

  private HoodieStorageConfig() {
    super();
  }

  public static HoodieStorageConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieStorageConfig storageConfig = new HoodieStorageConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.storageConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.storageConfig.getProps().putAll(props);
      return this;
    }

    public Builder parquetMaxFileSize(long maxFileSize) {
      storageConfig.setValue(PARQUET_MAX_FILE_SIZE, String.valueOf(maxFileSize));
      return this;
    }

    public Builder parquetBlockSize(int blockSize) {
      storageConfig.setValue(PARQUET_BLOCK_SIZE, String.valueOf(blockSize));
      return this;
    }

    public Builder parquetPageSize(int pageSize) {
      storageConfig.setValue(PARQUET_PAGE_SIZE, String.valueOf(pageSize));
      return this;
    }

    public Builder hfileMaxFileSize(long maxFileSize) {
      storageConfig.setValue(HFILE_MAX_FILE_SIZE, String.valueOf(maxFileSize));
      return this;
    }

    public Builder hfileBlockSize(int blockSize) {
      storageConfig.setValue(HFILE_BLOCK_SIZE, String.valueOf(blockSize));
      return this;
    }

    public Builder logFileDataBlockMaxSize(int dataBlockSize) {
      storageConfig.setValue(LOGFILE_DATA_BLOCK_MAX_SIZE, String.valueOf(dataBlockSize));
      return this;
    }

    public Builder logFileMaxSize(long logFileSize) {
      storageConfig.setValue(LOGFILE_MAX_SIZE, String.valueOf(logFileSize));
      return this;
    }

    public Builder parquetCompressionRatio(double parquetCompressionRatio) {
      storageConfig.setValue(PARQUET_COMPRESSION_RATIO_FRACTION, String.valueOf(parquetCompressionRatio));
      return this;
    }

    public Builder parquetCompressionCodec(String parquetCompressionCodec) {
      storageConfig.setValue(PARQUET_COMPRESSION_CODEC_NAME, parquetCompressionCodec);
      return this;
    }

    public Builder hfileCompressionAlgorithm(String hfileCompressionAlgorithm) {
      storageConfig.setValue(HFILE_COMPRESSION_ALGORITHM_NAME, hfileCompressionAlgorithm);
      return this;
    }

    public Builder logFileToParquetCompressionRatio(double logFileToParquetCompressionRatio) {
      storageConfig.setValue(LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION, String.valueOf(logFileToParquetCompressionRatio));
      return this;
    }

    public Builder orcMaxFileSize(long maxFileSize) {
      storageConfig.setValue(ORC_FILE_MAX_SIZE, String.valueOf(maxFileSize));
      return this;
    }

    public Builder orcStripeSize(int orcStripeSize) {
      storageConfig.setValue(ORC_STRIPE_SIZE, String.valueOf(orcStripeSize));
      return this;
    }

    public Builder orcBlockSize(int orcBlockSize) {
      storageConfig.setValue(ORC_BLOCK_SIZE, String.valueOf(orcBlockSize));
      return this;
    }

    public Builder orcCompressionCodec(String orcCompressionCodec) {
      storageConfig.setValue(ORC_COMPRESSION_CODEC_NAME, orcCompressionCodec);
      return this;
    }

    public HoodieStorageConfig build() {
      storageConfig.setDefaults(HoodieStorageConfig.class.getName());
      return storageConfig;
    }
  }
}
