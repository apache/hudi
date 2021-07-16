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

import org.apache.hudi.common.config.ConfigGroupProperty;
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
@ConfigGroupProperty(name = "Storage Configs", description = "Configurations that control aspects around sizing parquet and log files.")
public class HoodieStorageConfig extends HoodieConfig {

  public static final ConfigProperty<String> PARQUET_FILE_MAX_BYTES = ConfigProperty
      .key("hoodie.parquet.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Target size for parquet files produced by Hudi write phases. "
          + "For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.");

  public static final ConfigProperty<String> PARQUET_BLOCK_SIZE_BYTES = ConfigProperty
      .key("hoodie.parquet.block.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Parquet RowGroup size. It's recommended to make this large enough that scan costs can be"
          + " amortized by packing enough column values into a single row group.");

  public static final ConfigProperty<String> PARQUET_PAGE_SIZE_BYTES = ConfigProperty
      .key("hoodie.parquet.page.size")
      .defaultValue(String.valueOf(1 * 1024 * 1024))
      .withDocumentation("Parquet page size. Page is the unit of read within a parquet file. "
          + "Within a block, pages are compressed separately.");

  public static final ConfigProperty<String> ORC_FILE_MAX_BYTES = ConfigProperty
      .key("hoodie.orc.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Target file size for ORC base files.");

  public static final ConfigProperty<String> ORC_STRIPE_SIZE = ConfigProperty
      .key("hoodie.orc.stripe.size")
      .defaultValue(String.valueOf(64 * 1024 * 1024))
      .withDocumentation("Size of the memory buffer in bytes for writing");

  public static final ConfigProperty<String> ORC_BLOCK_SIZE = ConfigProperty
      .key("hoodie.orc.block.size")
      .defaultValue(ORC_FILE_MAX_BYTES.defaultValue())
      .withDocumentation("ORC block size, recommended to be aligned with the target file size.");

  public static final ConfigProperty<String> HFILE_FILE_MAX_BYTES = ConfigProperty
      .key("hoodie.hfile.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Target file size for HFile base files.");

  public static final ConfigProperty<String> HFILE_BLOCK_SIZE_BYTES = ConfigProperty
      .key("hoodie.hfile.block.size")
      .defaultValue(String.valueOf(1024 * 1024))
      .withDocumentation("Lower values increase the size of metadata tracked within HFile, but can offer potentially "
          + "faster lookup times.");

  // used to size log files
  public static final ConfigProperty<String> LOGFILE_SIZE_MAX_BYTES = ConfigProperty
      .key("hoodie.logfile.max.size")
      .defaultValue(String.valueOf(1024 * 1024 * 1024)) // 1 GB
      .withDocumentation("LogFile max size. This is the maximum size allowed for a log file "
          + "before it is rolled over to the next version.");

  // used to size data blocks in log file
  public static final ConfigProperty<String> LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES = ConfigProperty
      .key("hoodie.logfile.data.block.max.size")
      .defaultValue(String.valueOf(256 * 1024 * 1024))
      .withDocumentation("LogFile Data block max size. This is the maximum size allowed for a single data block "
          + "to be appended to a log file. This helps to make sure the data appended to the log file is broken up "
          + "into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory.");

  public static final ConfigProperty<String> PARQUET_COMPRESSION_RATIO = ConfigProperty
      .key("hoodie.parquet.compression.ratio")
      .defaultValue(String.valueOf(0.1))
      .withDocumentation("Expected compression of parquet data used by Hudi, when it tries to size new parquet files. "
          + "Increase this value, if bulk_insert is producing smaller than expected sized files");

  // Default compression codec for parquet
  public static final ConfigProperty<String> PARQUET_COMPRESSION_CODEC = ConfigProperty
      .key("hoodie.parquet.compression.codec")
      .defaultValue("gzip")
      .withDocumentation("Compression Codec for parquet files");

  public static final ConfigProperty<String> HFILE_COMPRESSION_ALGORITHM = ConfigProperty
      .key("hoodie.hfile.compression.algorithm")
      .defaultValue("GZ")
      .withDocumentation("Compression codec to use for hfile base files.");

  public static final ConfigProperty<String> ORC_COMPRESSION_CODEC = ConfigProperty
      .key("hoodie.orc.compression.codec")
      .defaultValue("ZLIB")
      .withDocumentation("Compression codec to use for ORC base files.");

  // Default compression ratio for log file to parquet, general 3x
  public static final ConfigProperty<String> LOGFILE_TO_PARQUET_COMPRESSION_RATIO = ConfigProperty
      .key("hoodie.logfile.to.parquet.compression.ratio")
      .defaultValue(String.valueOf(0.35))
      .withDocumentation("Expected additional compression as records move from log files to parquet. Used for merge_on_read "
          + "table to send inserts into log files & control the size of compacted parquet file.");

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
      storageConfig.setValue(PARQUET_FILE_MAX_BYTES, String.valueOf(maxFileSize));
      return this;
    }

    public Builder parquetBlockSize(int blockSize) {
      storageConfig.setValue(PARQUET_BLOCK_SIZE_BYTES, String.valueOf(blockSize));
      return this;
    }

    public Builder parquetPageSize(int pageSize) {
      storageConfig.setValue(PARQUET_PAGE_SIZE_BYTES, String.valueOf(pageSize));
      return this;
    }

    public Builder hfileMaxFileSize(long maxFileSize) {
      storageConfig.setValue(HFILE_FILE_MAX_BYTES, String.valueOf(maxFileSize));
      return this;
    }

    public Builder hfileBlockSize(int blockSize) {
      storageConfig.setValue(HFILE_BLOCK_SIZE_BYTES, String.valueOf(blockSize));
      return this;
    }

    public Builder logFileDataBlockMaxSize(int dataBlockSize) {
      storageConfig.setValue(LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES, String.valueOf(dataBlockSize));
      return this;
    }

    public Builder logFileMaxSize(int logFileSize) {
      storageConfig.setValue(LOGFILE_SIZE_MAX_BYTES, String.valueOf(logFileSize));
      return this;
    }

    public Builder parquetCompressionRatio(double parquetCompressionRatio) {
      storageConfig.setValue(PARQUET_COMPRESSION_RATIO, String.valueOf(parquetCompressionRatio));
      return this;
    }

    public Builder parquetCompressionCodec(String parquetCompressionCodec) {
      storageConfig.setValue(PARQUET_COMPRESSION_CODEC, parquetCompressionCodec);
      return this;
    }

    public Builder hfileCompressionAlgorithm(String hfileCompressionAlgorithm) {
      storageConfig.setValue(HFILE_COMPRESSION_ALGORITHM, hfileCompressionAlgorithm);
      return this;
    }

    public Builder logFileToParquetCompressionRatio(double logFileToParquetCompressionRatio) {
      storageConfig.setValue(LOGFILE_TO_PARQUET_COMPRESSION_RATIO, String.valueOf(logFileToParquetCompressionRatio));
      return this;
    }

    public Builder orcMaxFileSize(long maxFileSize) {
      storageConfig.setValue(ORC_FILE_MAX_BYTES, String.valueOf(maxFileSize));
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
      storageConfig.setValue(ORC_COMPRESSION_CODEC, orcCompressionCodec);
      return this;
    }

    public HoodieStorageConfig build() {
      storageConfig.setDefaults(HoodieStorageConfig.class.getName());
      return storageConfig;
    }
  }

}
