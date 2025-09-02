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

import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import static org.apache.hudi.common.util.ConfigUtils.enumNames;

/**
 * Hudi configs used across engines.
 */
@ConfigClassProperty(name = "Common Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "The following set of configurations are common across Hudi.")
public class HoodieCommonConfig extends HoodieConfig {

  public static final String META_SYNC_BASE_PATH_KEY = "hoodie.datasource.meta.sync.base.path";

  public static final ConfigProperty<String> BASE_PATH = ConfigProperty
      .key("hoodie.base.path")
      .noDefaultValue()
      .withDocumentation("Base path on lake storage, under which all the table data is stored. "
          + "Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). "
          + "Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs "
          + "etc in .hoodie directory under this base path directory.");

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

  @Deprecated
  public static final ConfigProperty<Boolean> RECONCILE_SCHEMA = ConfigProperty
      .key("hoodie.datasource.write.reconcile.schema")
      .defaultValue(false)
      .markAdvanced()
      .deprecatedAfter("0.14.1")
      .withDocumentation("This config controls how writer's schema will be selected based on the incoming batch's "
          + "schema as well as existing table's one. When schema reconciliation is DISABLED, incoming batch's "
          + "schema will be picked as a writer-schema (therefore updating table's schema). When schema reconciliation "
          + "is ENABLED, writer-schema will be picked such that table's schema (after txn) is either kept the same "
          + "or extended, meaning that we'll always prefer the schema that either adds new columns or stays the same. "
          + "This enables us, to always extend the table's schema during evolution and never lose the data (when, for "
          + "ex, existing column is being dropped in a new batch)");

  public static final ConfigProperty<String> SET_NULL_FOR_MISSING_COLUMNS = ConfigProperty
      .key("hoodie.write.set.null.for.missing.columns")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("0.14.1")
      .withDocumentation("When a nullable column is missing from incoming batch during a write operation, the write "
          + " operation will fail schema compatibility check. Set this option to true will make the missing "
          + " column be filled with null values to successfully complete the write operation.");

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

  public static final ConfigProperty<String> INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT = ConfigProperty
      .key("hoodie.read.timeline.holes.resolution.policy")
      .defaultValue(HollowCommitHandling.FAIL.name())
      .sinceVersion("0.14.0")
      .markAdvanced()
      .withValidValues(enumNames(HollowCommitHandling.class))
      .withDocumentation("When doing incremental queries, there could be hollow commits (requested or inflight commits that are not the latest)"
          + " that are produced by concurrent writers and could lead to potential data loss. This config allows users to have different ways of handling this situation."
          + " The valid values are " + Arrays.toString(enumNames(HollowCommitHandling.class)) + ":"
          + " Use `" + HollowCommitHandling.FAIL + "` to throw an exception when hollow commit is detected. This is helpful when hollow commits"
          + " are not expected."
          + " Use `" + HollowCommitHandling.BLOCK + "` to block processing commits from going beyond the hollow ones. This fits the case where waiting for hollow commits"
          + " to finish is acceptable."
          + " Use `" + HollowCommitHandling.USE_TRANSITION_TIME + "` (experimental) to query commits in range by state transition time (completion time), instead"
          + " of commit time (start time). Using this mode will result in `begin.instanttime` and `end.instanttime` using `stateTransitionTime` "
          + " instead of the instant's commit time."
      );

  public static final ConfigProperty<String> HOODIE_FS_ATOMIC_CREATION_SUPPORT = ConfigProperty
      .key("hoodie.fs.atomic_creation.support")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("This config is used to specify the file system which supports atomic file creation . "
          + "atomic means that an operation either succeeds and has an effect or has fails and has no effect;"
          + " now this feature is used by FileSystemLockProvider to guaranteeing that only one writer can create the lock file at a time."
          + " since some FS does not support atomic file creation (eg: S3), we decide the FileSystemLockProvider only support HDFS,local FS"
          + " and View FS as default. if you want to use FileSystemLockProvider with other FS, you can set this config with the FS scheme, eg: fs1,fs2");

  public static final ConfigProperty<String> MAX_MEMORY_FOR_COMPACTION = ConfigProperty
      .key("hoodie.memory.compaction.max.size")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Maximum amount of memory used  in bytes for compaction operations in bytes , before spilling to local storage.");

  public static final ConfigProperty<Integer> MAX_DFS_STREAM_BUFFER_SIZE = ConfigProperty
      .key("hoodie.memory.dfs.buffer.max.size")
      .defaultValue(16 * 1024 * 1024)
      .markAdvanced()
      .withDocumentation("Property to control the max memory in bytes for dfs input stream buffer size");

  public static final ConfigProperty<Boolean> HOODIE_FILE_INDEX_USE_SPILLABLE_MAP = ConfigProperty
      .key("hoodie.file.index.cache.use.spillable.map")
      .defaultValue(false)
      .sinceVersion("1.1.0")
      .markAdvanced()
      .withDocumentation("Property to enable spillable map for caching input file slices in org.apache.hudi.BaseHoodieTableFileIndex");

  public static final ConfigProperty<Long> HOODIE_FILE_INDEX_SPILLABLE_MEMORY = ConfigProperty
      .key("hoodie.file.index.cache.spillable.mem")
      .defaultValue(500 * 1024L * 1024L) // 500 MB
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Amount of memory to be used in bytes for holding cachedAllInputFileSlices in org.apache.hudi.BaseHoodieTableFileIndex.");
  
  public static final long DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES = 1024 * 1024 * 1024L;

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
