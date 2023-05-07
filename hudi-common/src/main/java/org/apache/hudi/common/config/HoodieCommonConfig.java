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

/**
 * Hudi configs used across engines.
 */
@ConfigClassProperty(name = "Common Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "The following set of configurations are common across Hudi.")
public class HoodieCommonConfig extends HoodieConfig {

  public static final ConfigProperty<Boolean> SCHEMA_EVOLUTION_ENABLE = ConfigProperty
      .key("hoodie.schema.on.read.enable")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("Enables support for Schema Evolution feature");

  public static final ConfigProperty<String> TIMESTAMP_AS_OF = ConfigProperty
      .key("as.of.instant")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("The query instant for time travel. Without specified this option, we query the latest snapshot.");

  public static final ConfigProperty<Boolean> RECONCILE_SCHEMA = ConfigProperty
      .key("hoodie.datasource.write.reconcile.schema")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("This config controls how writer's schema will be selected based on the incoming batch's "
          + "schema as well as existing table's one. When schema reconciliation is DISABLED, incoming batch's "
          + "schema will be picked as a writer-schema (therefore updating table's schema). When schema reconciliation "
          + "is ENABLED, writer-schema will be picked such that table's schema (after txn) is either kept the same "
          + "or extended, meaning that we'll always prefer the schema that either adds new columns or stays the same. "
          + "This enables us, to always extend the table's schema during evolution and never lose the data (when, for "
          + "ex, existing column is being dropped in a new batch)");

  public static final ConfigProperty<ExternalSpillableMap.DiskMapType> SPILLABLE_DISK_MAP_TYPE = ConfigProperty
      .key("hoodie.common.spillable.diskmap.type")
      .defaultValue(ExternalSpillableMap.DiskMapType.BITCASK)
      .markAdvanced()
      .withDocumentation("When handling input data that cannot be held in memory, to merge with a file on storage, a spillable diskmap is employed.  "
          + "By default, we use a persistent hashmap based loosely on bitcask, that offers O(1) inserts, lookups. "
          + "Change this to `ROCKS_DB` to prefer using rocksDB, for handling the spill.");

  public static final ConfigProperty<Boolean> DISK_MAP_BITCASK_COMPRESSION_ENABLED = ConfigProperty
      .key("hoodie.common.diskmap.compression.enabled")
      .defaultValue(true)
      .markAdvanced()
      .withDocumentation("Turn on compression for BITCASK disk map used by the External Spillable Map");

  public static final ConfigProperty<Boolean> READ_BY_STATE_TRANSITION_TIME = ConfigProperty
      .key("hoodie.datasource.read.by.state.transition.time")
      .defaultValue(false)
      .sinceVersion("0.14.0")
      .withDocumentation("For incremental mode, whether to enable to pulling commits in range by state transition time(completion time) "
          + "instead of commit time(start time). Please be aware that enabling this will result in"
          + "`begin.instanttime` and `end.instanttime` using `stateTransitionTime` instead of the instant's commit time.");

  public static final ConfigProperty<String> HOODIE_FS_ATOMIC_CREATION_SUPPORT = ConfigProperty
      .key("hoodie.fs.atomic_creation.support")
      .defaultValue("")
      .withDocumentation("This config is used to specify the file system which supports atomic file creation . "
          + "atomic means that an operation either succeeds and has an effect or has fails and has no effect;"
          + " now this feature is used by FileSystemLockProvider to guaranteeing that only one writer can create the lock file at a time."
          + " since some FS does not support atomic file creation (eg: S3), we decide the FileSystemLockProvider only support HDFS,local FS"
          + " and View FS as default. if you want to use FileSystemLockProvider with other FS, you can set this config with the FS scheme, eg: fs1,fs2");

  public ExternalSpillableMap.DiskMapType getSpillableDiskMapType() {
    return ExternalSpillableMap.DiskMapType.valueOf(getString(SPILLABLE_DISK_MAP_TYPE).toUpperCase(Locale.ROOT));
  }

  public boolean isBitCaskDiskMapCompressionEnabled() {
    return getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED);
  }

  private HoodieCommonConfig() {
    super();
  }

  public static HoodieCommonConfig.Builder newBuilder() {
    return new HoodieCommonConfig.Builder();
  }

  /**
   * Builder for {@link HoodieCommonConfig}.
   */
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
