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

import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

@ConfigClassProperty(name = "Common Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "The following set of configurations are common across Hudi.")
public class HoodieCommonConfig extends HoodieConfig {

  public static final ConfigProperty<Boolean> SCHEMA_EVOLUTION_ENABLE = ConfigProperty
      .key("hoodie.schema.on.read.enable")
      .defaultValue(false)
      .withDocumentation("Enables support for Schema Evolution feature");

  public static final ConfigProperty<Boolean> RECONCILE_SCHEMA = ConfigProperty
      .key("hoodie.datasource.write.reconcile.schema")
      .defaultValue(false)
      .withDocumentation("When a new batch of write has records with old schema, but latest table schema got "
          + "evolved, this config will upgrade the records to leverage latest table schema(default values will be "
          + "injected to missing fields). If not, the write batch would fail.");

  public static final ConfigProperty<ExternalSpillableMap.DiskMapType> SPILLABLE_DISK_MAP_TYPE = ConfigProperty
      .key("hoodie.common.spillable.diskmap.type")
      .defaultValue(ExternalSpillableMap.DiskMapType.BITCASK)
      .withDocumentation(
          "When handling input data that cannot be held in memory, to merge with a file on storage, a spillable diskmap is employed.  "
              + "By default, we use a persistent hashmap based loosely on bitcask, that offers O(1) inserts, lookups. "
              + "Change this to `ROCKS_DB` to prefer using rocksDB, for handling the spill.");

  public static final ConfigProperty<Long> LOG_FILE_BLOCK_SIZE = ConfigProperty
      .key("hoodie.log.file.block.size")
      .defaultValue(128 * 1024 * 1024L)
      .withDocumentation("Log file block size");

  public static final ConfigProperty<Boolean> DISK_MAP_BITCASK_COMPRESSION_ENABLED = ConfigProperty
      .key("hoodie.common.diskmap.compression.enabled")
      .defaultValue(true)
      .withDocumentation("Turn on compression for BITCASK disk map used by the External Spillable Map");

  public ExternalSpillableMap.DiskMapType getSpillableDiskMapType() {
    return ExternalSpillableMap.DiskMapType.valueOf(getString(SPILLABLE_DISK_MAP_TYPE).toUpperCase(Locale.ROOT));
  }

  public boolean isBitCaskDiskMapCompressionEnabled() {
    return getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED);
  }

  public Long getLogFileBlockSize() {
    return getLong(LOG_FILE_BLOCK_SIZE);
  }

  private HoodieCommonConfig() {
    super();
  }

  public static HoodieCommonConfig.Builder newBuilder() {
    return new HoodieCommonConfig.Builder();
  }

  public static class Builder {

    private final HoodieCommonConfig commonConfig = new HoodieCommonConfig();

    public HoodieCommonConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        commonConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieCommonConfig.Builder fromProperties(Properties props) {
      commonConfig.getProps().putAll(props);
      return this;
    }

    public Builder withSpillableDiskMapType(ExternalSpillableMap.DiskMapType diskMapType) {
      commonConfig.setValue(SPILLABLE_DISK_MAP_TYPE, diskMapType.name());
      return this;
    }

    public Builder withLogFileBlockSize(Long logFileBlockSize) {
      commonConfig.setValue(LOG_FILE_BLOCK_SIZE, String.valueOf(logFileBlockSize));
      return this;
    }

    public Builder withBitcaskDiskMapCompressionEnabled(boolean bitcaskDiskMapCompressionEnabled) {
      commonConfig.setValue(DISK_MAP_BITCASK_COMPRESSION_ENABLED, String.valueOf(bitcaskDiskMapCompressionEnabled));
      return this;
    }

    public HoodieCommonConfig build() {
      commonConfig.setDefaults(HoodieCommonConfig.class.getName());
      return commonConfig;
    }
  }
}
