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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.apache.parquet.hadoop.codec.ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for built-in and user-defined {@link HoodieParquetConfigInjector} composition.
 */
class TestHoodieParquetConfigInjector {

  @Test
  void testApplyBuiltInZstdCompressionLevelInjector() {
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration(false));
    storageConf.set(PARQUET_COMPRESS_ZSTD_LEVEL, "7");

    Pair<StorageConfiguration, HoodieConfig> nativeLogConfigs =
        ParquetUtils.injectParquetWriterConfigs(
            new StoragePath("/tmp/file-id_1-0-1_001_1.log.parquet"), storageConf, new HoodieConfig());

    assertNotSame(storageConf, nativeLogConfigs.getLeft());
    assertEquals(7, storageConf.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1));
    assertEquals(1, nativeLogConfigs.getLeft().getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1));

    Pair<StorageConfiguration, HoodieConfig> baseFileConfigs =
        ParquetUtils.injectParquetWriterConfigs(
            new StoragePath("/tmp/file-id_001.parquet"), storageConf, new HoodieConfig());
    assertSame(storageConf, baseFileConfigs.getLeft());

    HoodieConfig configuredZstdLevel = new HoodieConfig();
    configuredZstdLevel.setValue(HoodieStorageConfig.LOGFILE_PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL, "3");
    Pair<StorageConfiguration, HoodieConfig> configuredNativeLogConfigs =
        ParquetUtils.injectParquetWriterConfigs(
            new StoragePath("/tmp/file-id_1-0-1_001_1.log.parquet"), storageConf, configuredZstdLevel);
    assertEquals(3, configuredNativeLogConfigs.getLeft().getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1));

    configuredZstdLevel.setValue(HoodieStorageConfig.LOGFILE_PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL, "7");
    Pair<StorageConfiguration, HoodieConfig> sameLevelNativeLogConfigs =
        ParquetUtils.injectParquetWriterConfigs(
            new StoragePath("/tmp/file-id_1-0-1_001_1.log.parquet"), storageConf, configuredZstdLevel);
    assertSame(storageConf, sameLevelNativeLogConfigs.getLeft());

    HadoopStorageConfiguration storageConfWithoutGlobalLevel =
        new HadoopStorageConfiguration(new Configuration(false));
    configuredZstdLevel.setValue(HoodieStorageConfig.LOGFILE_PARQUET_COMPRESSION_CODEC_ZSTD_LEVEL, "3");
    Pair<StorageConfiguration, HoodieConfig> configsWithExplicitNativeLevel =
        ParquetUtils.injectParquetWriterConfigs(
            new StoragePath("/tmp/file-id_1-0-1_001_1.log.parquet"),
            storageConfWithoutGlobalLevel, configuredZstdLevel);
    assertNotSame(storageConfWithoutGlobalLevel, configsWithExplicitNativeLevel.getLeft());
    assertEquals(3, configsWithExplicitNativeLevel.getLeft().getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1));
  }

  @Test
  void testBuiltInInjectorFailsForInvalidGlobalLevel() {
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration(false));
    storageConf.set(PARQUET_COMPRESS_ZSTD_LEVEL, "invalid");

    HoodieException exception = assertThrows(
        HoodieException.class,
        () -> ParquetUtils.injectParquetWriterConfigs(
            new StoragePath("/tmp/file-id_1-0-1_001_1.log.parquet"),
            storageConf, new HoodieConfig()));
    assertTrue(exception.getMessage().contains(PARQUET_COMPRESS_ZSTD_LEVEL));
  }

  @Test
  void testApplyUserDefinedInjectorAfterBuiltInInjector() {
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration(false));
    storageConf.set(PARQUET_COMPRESS_ZSTD_LEVEL, "7");
    HoodieConfig hoodieConfig = new HoodieConfig();
    hoodieConfig.setValue(
        HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS,
        OverrideNativeLogZstdLevelInjector.class.getName());

    Pair<StorageConfiguration, HoodieConfig> injectedConfigs =
        ParquetUtils.injectParquetWriterConfigs(
            new StoragePath("/tmp/file-id_1-0-1_001_1.log.parquet"), storageConf, hoodieConfig);

    assertEquals(9, injectedConfigs.getLeft().getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1));
  }

  public static class OverrideNativeLogZstdLevelInjector implements HoodieParquetConfigInjector {
    @Override
    public Pair<StorageConfiguration, HoodieConfig> injectConfig(
        StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig) {
      assertEquals(1, storageConf.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, -1));
      StorageConfiguration storageConfCopy = storageConf.newInstance();
      storageConfCopy.set(PARQUET_COMPRESS_ZSTD_LEVEL, "9");
      HoodieConfig hoodieConfigCopy = new HoodieConfig(TypedProperties.copy(hoodieConfig.getProps()));
      return Pair.of(storageConfCopy, hoodieConfigCopy);
    }
  }
}
