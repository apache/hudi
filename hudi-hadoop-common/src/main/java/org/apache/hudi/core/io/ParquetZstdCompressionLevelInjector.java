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

package org.apache.hudi.core.io;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import static org.apache.parquet.hadoop.codec.ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL;

/**
 * Built-in injector that configures the ZSTD compression level for native Parquet log files.
 */
public class ParquetZstdCompressionLevelInjector implements HoodieParquetConfigInjector {

  public static ParquetZstdCompressionLevelInjector INSTANCE = new ParquetZstdCompressionLevelInjector();

  @Override
  public Pair<StorageConfiguration, HoodieConfig> injectConfig(
      StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig) {
    // Keep the global Parquet writer configuration unchanged for base files and non-native log files.
    if (!FSUtils.isNativeLogFile(path.getName())) {
      return Pair.of(storageConf, hoodieConfig);
    }

    int nativeLogZstdLevel =
        hoodieConfig.getIntOrDefault(HoodieStorageConfig.LOGFILE_PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL);
    Option<String> globalZstdLevel = storageConf.getString(PARQUET_COMPRESS_ZSTD_LEVEL);
    if (globalZstdLevel.isPresent()) {
      // Parse an explicitly configured global level before overriding it so that malformed user
      // configuration is not silently hidden by the native-log-specific value.
      int parsedGlobalZstdLevel;
      try {
        parsedGlobalZstdLevel = Integer.parseInt(globalZstdLevel.get());
      } catch (NumberFormatException e) {
        throw new HoodieException("Invalid value for " + PARQUET_COMPRESS_ZSTD_LEVEL + ": "
            + globalZstdLevel.get() + ". Expected an integer.", e);
      }
      // Reuse the original configuration when the desired level is already effective.
      if (nativeLogZstdLevel == parsedGlobalZstdLevel) {
        return Pair.of(storageConf, hoodieConfig);
      }
    }

    // Isolate the native log override from other Parquet writers sharing the storage configuration.
    StorageConfiguration nativeLogStorageConf = storageConf.newInstance();
    nativeLogStorageConf.set(PARQUET_COMPRESS_ZSTD_LEVEL, String.valueOf(nativeLogZstdLevel));
    return Pair.of(nativeLogStorageConf, hoodieConfig);
  }
}
