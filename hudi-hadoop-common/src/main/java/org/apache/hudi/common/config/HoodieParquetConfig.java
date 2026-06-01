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

import org.apache.hudi.storage.StorageConfiguration;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Base ParquetConfig to hold config params for writing to Parquet.
 * @param <T>
 */
@Getter
public class HoodieParquetConfig<T> {

  // Matches parquet-mr's ZstandardCodec default. See HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL.
  public static final int DEFAULT_ZSTD_LEVEL = 3;

  // parquet-mr ZstandardCodec reads this key off the Hadoop Configuration at write time.
  public static final String PARQUET_COMPRESS_ZSTD_LEVEL = "parquet.compression.codec.zstd.level";

  private final T writeSupport;
  private final CompressionCodecName compressionCodecName;
  private final int blockSize;
  private final int pageSize;
  private final long maxFileSize;
  private final StorageConfiguration<?> storageConf;
  private final double compressionRatio;
  private final boolean dictionaryEnabled;
  private final int zstdLevel;

  public HoodieParquetConfig(T writeSupport,
                             CompressionCodecName compressionCodecName,
                             int blockSize,
                             int pageSize,
                             long maxFileSize,
                             StorageConfiguration<?> storageConf,
                             double compressionRatio,
                             boolean dictionaryEnabled) {
    this(writeSupport, compressionCodecName, blockSize, pageSize, maxFileSize, storageConf,
        compressionRatio, dictionaryEnabled, DEFAULT_ZSTD_LEVEL);
  }

  public HoodieParquetConfig(T writeSupport,
                             CompressionCodecName compressionCodecName,
                             int blockSize,
                             int pageSize,
                             long maxFileSize,
                             StorageConfiguration<?> storageConf,
                             double compressionRatio,
                             boolean dictionaryEnabled,
                             int zstdLevel) {
    this.writeSupport = writeSupport;
    this.compressionCodecName = compressionCodecName;
    this.blockSize = blockSize;
    this.pageSize = pageSize;
    this.maxFileSize = maxFileSize;
    this.storageConf = storageConf;
    this.compressionRatio = compressionRatio;
    this.dictionaryEnabled = dictionaryEnabled;
    this.zstdLevel = zstdLevel;
  }

  /**
   * Returns a Hadoop {@link Configuration} carrying the zstd compression level when {@code codec} is ZSTD.
   * If the codec is not ZSTD, returns {@code conf} unchanged. Otherwise returns a defensive copy with the
   * level stamped on it, so callers don't mutate the shared task-level Configuration.
   */
  public static Configuration applyZstdLevel(Configuration conf, CompressionCodecName codec, int zstdLevel) {
    if (codec != CompressionCodecName.ZSTD) {
      return conf;
    }
    Configuration copy = new Configuration(conf);
    copy.setInt(PARQUET_COMPRESS_ZSTD_LEVEL, zstdLevel);
    return copy;
  }
}
