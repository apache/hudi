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

package org.apache.hudi.io.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;

/**
 * Base ParquetConfig to hold config params for writing to Parquet.
 * @param <T>
 */
public class HoodieParquetConfig<T> {
  private final T writeSupport;
  private final CompressionCodecName compressionCodecName;
  private final int blockSize;
  private final int pageSize;
  private final long maxFileSize;
  private final Configuration hadoopConf;
  private final double compressionRatio;
  private final boolean dictionaryEnabled;
  private final long minRowCountForSizeCheck = DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
  private final long maxRowCountForSizeCheck = DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK;

  public HoodieParquetConfig(T writeSupport, CompressionCodecName compressionCodecName, int blockSize,
                             int pageSize, long maxFileSize, Configuration hadoopConf, double compressionRatio, boolean dictionaryEnabled) {
    this.writeSupport = writeSupport;
    this.compressionCodecName = compressionCodecName;
    this.blockSize = blockSize;
    this.pageSize = pageSize;
    this.maxFileSize = maxFileSize;
    this.hadoopConf = hadoopConf;
    this.compressionRatio = compressionRatio;
    this.dictionaryEnabled = dictionaryEnabled;
  }

  public CompressionCodecName getCompressionCodecName() {
    return compressionCodecName;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public int getPageSize() {
    return pageSize;
  }

  public long getMaxFileSize() {
    return maxFileSize;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public double getCompressionRatio() {
    return compressionRatio;
  }

  public T getWriteSupport() {
    return writeSupport;
  }

  public boolean dictionaryEnabled() {
    return dictionaryEnabled;
  }

  public long getMinRowCountForSizeCheck() {
    return minRowCountForSizeCheck;
  }

  public long getMaxRowCountForSizeCheck() {
    return maxRowCountForSizeCheck;
  }
}
