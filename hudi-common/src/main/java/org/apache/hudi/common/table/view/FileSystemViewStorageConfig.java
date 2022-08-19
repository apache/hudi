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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * File System View Storage Configurations.
 */
@ConfigClassProperty(name = "File System View Storage Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control how file metadata is stored by Hudi, for transaction processing and queries.")
public class FileSystemViewStorageConfig extends HoodieConfig {

  // Property Names
  public static final ConfigProperty<FileSystemViewStorageType> VIEW_TYPE = ConfigProperty
      .key("hoodie.filesystem.view.type")
      .defaultValue(FileSystemViewStorageType.MEMORY)
      .withDocumentation("File system view provides APIs for viewing the files on the underlying lake storage, "
          + " as file groups and file slices. This config controls how such a view is held. Options include "
          + Arrays.stream(FileSystemViewStorageType.values()).map(Enum::name).collect(Collectors.joining(","))
          + " which provide different trade offs for memory usage and API request performance.");

  public static final ConfigProperty<String> INCREMENTAL_TIMELINE_SYNC_ENABLE = ConfigProperty
      .key("hoodie.filesystem.view.incr.timeline.sync.enable")
      .defaultValue("false")
      .withDocumentation("Controls whether or not, the file system view is incrementally updated as "
          + "new actions are performed on the timeline.");

  public static final ConfigProperty<FileSystemViewStorageType> SECONDARY_VIEW_TYPE = ConfigProperty
      .key("hoodie.filesystem.view.secondary.type")
      .defaultValue(FileSystemViewStorageType.MEMORY)
      .withDocumentation("Specifies the secondary form of storage for file system view, if the primary (e.g timeline server) "
          + " is unavailable.");

  public static final ConfigProperty<String> REMOTE_HOST_NAME = ConfigProperty
      .key("hoodie.filesystem.view.remote.host")
      .defaultValue("localhost")
      .withDocumentation("We expect this to be rarely hand configured.");

  public static final ConfigProperty<Integer> REMOTE_PORT_NUM = ConfigProperty
      .key("hoodie.filesystem.view.remote.port")
      .defaultValue(26754)
      .withDocumentation("Port to serve file system view queries, when remote. We expect this to be rarely hand configured.");

  public static final ConfigProperty<String> SPILLABLE_DIR = ConfigProperty
      .key("hoodie.filesystem.view.spillable.dir")
      .defaultValue("/tmp/")
      .withDocumentation("Path on local storage to use, when file system view is held in a spillable map.");

  public static final ConfigProperty<Long> SPILLABLE_MEMORY = ConfigProperty
      .key("hoodie.filesystem.view.spillable.mem")
      .defaultValue(100 * 1024 * 1024L) // 100 MB
      .withDocumentation("Amount of memory to be used in bytes for holding file system view, before spilling to disk.");

  public static final ConfigProperty<Double> SPILLABLE_COMPACTION_MEM_FRACTION = ConfigProperty
      .key("hoodie.filesystem.view.spillable.compaction.mem.fraction")
      .defaultValue(0.8)
      .withDocumentation("Fraction of the file system view memory, to be used for holding compaction related metadata.");

  public static final ConfigProperty<Double> BOOTSTRAP_BASE_FILE_MEM_FRACTION = ConfigProperty
      .key("hoodie.filesystem.view.spillable.bootstrap.base.file.mem.fraction")
      .defaultValue(0.05)
      .withDocumentation("Fraction of the file system view memory, to be used for holding mapping to bootstrap base files.");

  public static final ConfigProperty<Double> SPILLABLE_REPLACED_MEM_FRACTION = ConfigProperty
      .key("hoodie.filesystem.view.spillable.replaced.mem.fraction")
      .defaultValue(0.01)
      .withDocumentation("Fraction of the file system view memory, to be used for holding replace commit related metadata.");

  public static final ConfigProperty<Double> SPILLABLE_CLUSTERING_MEM_FRACTION = ConfigProperty
      .key("hoodie.filesystem.view.spillable.clustering.mem.fraction")
      .defaultValue(0.01)
      .withDocumentation("Fraction of the file system view memory, to be used for holding clustering related metadata.");

  public static final ConfigProperty<String> ROCKSDB_BASE_PATH = ConfigProperty
      .key("hoodie.filesystem.view.rocksdb.base.path")
      .defaultValue("/tmp/hoodie_timeline_rocksdb")
      .withDocumentation("Path on local storage to use, when storing file system view in embedded kv store/rocksdb.");

  public static final ConfigProperty<Integer> REMOTE_TIMEOUT_SECS = ConfigProperty
      .key("hoodie.filesystem.view.remote.timeout.secs")
      .defaultValue(5 * 60) // 5 min
      .withDocumentation("Timeout in seconds, to wait for API requests against a remote file system view. e.g timeline server.");

  public static final ConfigProperty<String> REMOTE_RETRY_ENABLE = ConfigProperty
          .key("hoodie.filesystem.view.remote.retry.enable")
          .defaultValue("false")
          .sinceVersion("0.12.0")
          .withDocumentation("Whether to enable API request retry for remote file system view.");

  public static final ConfigProperty<Integer> REMOTE_MAX_RETRY_NUMBERS = ConfigProperty
      .key("hoodie.filesystem.view.remote.retry.max_numbers")
      .defaultValue(3) // 3 times
      .sinceVersion("0.12.0")
      .withDocumentation("Maximum number of retry for API requests against a remote file system view. e.g timeline server.");

  public static final ConfigProperty<Long> REMOTE_INITIAL_RETRY_INTERVAL_MS = ConfigProperty
      .key("hoodie.filesystem.view.remote.retry.initial_interval_ms")
      .defaultValue(100L)
      .sinceVersion("0.12.0")
      .withDocumentation("Amount of time (in ms) to wait, before retry to do operations on storage.");

  public static final ConfigProperty<Long> REMOTE_MAX_RETRY_INTERVAL_MS = ConfigProperty
      .key("hoodie.filesystem.view.remote.retry.max_interval_ms")
      .defaultValue(2000L)
      .sinceVersion("0.12.0")
      .withDocumentation("Maximum amount of time (in ms), to wait for next retry.");

  public static final ConfigProperty<String> RETRY_EXCEPTIONS = ConfigProperty
          .key("hoodie.filesystem.view.remote.retry.exceptions")
          .defaultValue("")
          .sinceVersion("0.12.0")
          .withDocumentation("The class name of the Exception that needs to be re-tryed, separated by commas. "
                  + "Default is empty which means retry all the IOException and RuntimeException from Remote Request.");

  public static final ConfigProperty<String> REMOTE_BACKUP_VIEW_ENABLE = ConfigProperty
      .key("hoodie.filesystem.remote.backup.view.enable")
      .defaultValue("true") // Need to be disabled only for tests.
      .withDocumentation("Config to control whether backup needs to be configured if clients were not able to reach"
          + " timeline service.");

  public static FileSystemViewStorageConfig.Builder newBuilder() {
    return new Builder();
  }

  private FileSystemViewStorageConfig() {
    super();
  }

  public FileSystemViewStorageType getStorageType() {
    return FileSystemViewStorageType.valueOf(getString(VIEW_TYPE));
  }

  public boolean isIncrementalTimelineSyncEnabled() {
    return getBoolean(INCREMENTAL_TIMELINE_SYNC_ENABLE);
  }

  public String getRemoteViewServerHost() {
    return getString(REMOTE_HOST_NAME);
  }

  public Integer getRemoteViewServerPort() {
    return getInt(REMOTE_PORT_NUM);
  }

  public Integer getRemoteTimelineClientTimeoutSecs() {
    return getInt(REMOTE_TIMEOUT_SECS);
  }

  public boolean isRemoteTimelineClientRetryEnabled() {
    return getBoolean(REMOTE_RETRY_ENABLE);
  }

  public Integer getRemoteTimelineClientMaxRetryNumbers() {
    return getInt(REMOTE_MAX_RETRY_NUMBERS);
  }

  public Long getRemoteTimelineInitialRetryIntervalMs() {
    return getLong(REMOTE_INITIAL_RETRY_INTERVAL_MS);
  }

  public Long getRemoteTimelineClientMaxRetryIntervalMs() {
    return getLong(REMOTE_MAX_RETRY_INTERVAL_MS);
  }

  public String getRemoteTimelineClientRetryExceptions() {
    return getString(RETRY_EXCEPTIONS);
  }

  public long getMaxMemoryForFileGroupMap() {
    long totalMemory = getLong(SPILLABLE_MEMORY);
    return totalMemory - getMaxMemoryForPendingCompaction() - getMaxMemoryForBootstrapBaseFile();
  }

  public long getMaxMemoryForPendingCompaction() {
    long totalMemory = getLong(SPILLABLE_MEMORY);
    return new Double(totalMemory * getDouble(SPILLABLE_COMPACTION_MEM_FRACTION))
        .longValue();
  }

  public long getMaxMemoryForBootstrapBaseFile() {
    long totalMemory = getLong(SPILLABLE_MEMORY);
    long reservedForExternalDataFile =
        new Double(totalMemory * getDouble(BOOTSTRAP_BASE_FILE_MEM_FRACTION))
            .longValue();
    return reservedForExternalDataFile;
  }

  public long getMaxMemoryForReplacedFileGroups() {
    long totalMemory = getLong(SPILLABLE_MEMORY);
    return new Double(totalMemory * getDouble(SPILLABLE_REPLACED_MEM_FRACTION))
        .longValue();
  }

  public long getMaxMemoryForPendingClusteringFileGroups() {
    long totalMemory = getLong(SPILLABLE_MEMORY);
    return new Double(totalMemory * getDouble(SPILLABLE_CLUSTERING_MEM_FRACTION))
        .longValue();
  }

  public String getSpillableDir() {
    return getString(SPILLABLE_DIR);
  }

  public FileSystemViewStorageType getSecondaryStorageType() {
    return FileSystemViewStorageType.valueOf(getString(SECONDARY_VIEW_TYPE));
  }

  public boolean shouldEnableBackupForRemoteFileSystemView() {
    return getBoolean(REMOTE_BACKUP_VIEW_ENABLE);
  }

  public String getRocksdbBasePath() {
    return getString(ROCKSDB_BASE_PATH);
  }

  /**
   * The builder used to build {@link FileSystemViewStorageConfig}.
   */
  public static class Builder {

    private final FileSystemViewStorageConfig fileSystemViewStorageConfig = new FileSystemViewStorageConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        fileSystemViewStorageConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.fileSystemViewStorageConfig.getProps().putAll(props);
      return this;
    }

    public Builder withStorageType(FileSystemViewStorageType storageType) {
      fileSystemViewStorageConfig.setValue(VIEW_TYPE, storageType.name());
      return this;
    }

    public Builder withSecondaryStorageType(FileSystemViewStorageType storageType) {
      fileSystemViewStorageConfig.setValue(SECONDARY_VIEW_TYPE, storageType.name());
      return this;
    }

    public Builder withIncrementalTimelineSync(boolean enableIncrTimelineSync) {
      fileSystemViewStorageConfig.setValue(INCREMENTAL_TIMELINE_SYNC_ENABLE, Boolean.toString(enableIncrTimelineSync));
      return this;
    }

    public Builder withRemoteServerHost(String remoteServerHost) {
      fileSystemViewStorageConfig.setValue(REMOTE_HOST_NAME, remoteServerHost);
      return this;
    }

    public Builder withRemoteServerPort(Integer remoteServerPort) {
      fileSystemViewStorageConfig.setValue(REMOTE_PORT_NUM, remoteServerPort.toString());
      return this;
    }

    public Builder withMaxMemoryForView(Long maxMemoryForView) {
      fileSystemViewStorageConfig.setValue(SPILLABLE_MEMORY, maxMemoryForView.toString());
      return this;
    }

    public Builder withRemoteTimelineClientTimeoutSecs(Integer timelineClientTimeoutSecs) {
      fileSystemViewStorageConfig.setValue(REMOTE_TIMEOUT_SECS, timelineClientTimeoutSecs.toString());
      return this;
    }

    public Builder withRemoteTimelineClientRetry(boolean enableRetry) {
      fileSystemViewStorageConfig.setValue(REMOTE_RETRY_ENABLE, Boolean.toString(enableRetry));
      return this;
    }

    public Builder withRemoteTimelineClientMaxRetryNumbers(Integer maxRetryNumbers) {
      fileSystemViewStorageConfig.setValue(REMOTE_MAX_RETRY_NUMBERS, maxRetryNumbers.toString());
      return this;
    }

    public Builder withRemoteTimelineInitialRetryIntervalMs(Long initialRetryIntervalMs) {
      fileSystemViewStorageConfig.setValue(REMOTE_INITIAL_RETRY_INTERVAL_MS, initialRetryIntervalMs.toString());
      return this;
    }

    public Builder withRemoteTimelineClientMaxRetryIntervalMs(Long maxRetryIntervalMs) {
      fileSystemViewStorageConfig.setValue(REMOTE_MAX_RETRY_INTERVAL_MS, maxRetryIntervalMs.toString());
      return this;
    }

    public Builder withRemoteTimelineClientRetryExceptions(String retryExceptions) {
      fileSystemViewStorageConfig.setValue(RETRY_EXCEPTIONS, retryExceptions);
      return this;
    }

    public Builder withMemFractionForPendingCompaction(Double memFractionForPendingCompaction) {
      fileSystemViewStorageConfig.setValue(SPILLABLE_COMPACTION_MEM_FRACTION, memFractionForPendingCompaction.toString());
      return this;
    }

    public Builder withMemFractionForExternalDataFile(Double memFractionForExternalDataFile) {
      fileSystemViewStorageConfig.setValue(BOOTSTRAP_BASE_FILE_MEM_FRACTION, memFractionForExternalDataFile.toString());
      return this;
    }

    public Builder withBaseStoreDir(String baseStorePath) {
      fileSystemViewStorageConfig.setValue(SPILLABLE_DIR, baseStorePath);
      return this;
    }

    public Builder withRocksDBPath(String basePath) {
      fileSystemViewStorageConfig.setValue(ROCKSDB_BASE_PATH, basePath);
      return this;
    }

    public Builder withEnableBackupForRemoteFileSystemView(boolean enable) {
      fileSystemViewStorageConfig.setValue(REMOTE_BACKUP_VIEW_ENABLE, Boolean.toString(enable));
      return this;
    }

    public FileSystemViewStorageConfig build() {
      fileSystemViewStorageConfig.setDefaults(FileSystemViewStorageConfig.class.getName());
      // Validations
      FileSystemViewStorageType.valueOf(fileSystemViewStorageConfig.getString(VIEW_TYPE));
      FileSystemViewStorageType.valueOf(fileSystemViewStorageConfig.getString(SECONDARY_VIEW_TYPE));
      ValidationUtils.checkArgument(fileSystemViewStorageConfig.getInt(REMOTE_PORT_NUM) > 0);
      return fileSystemViewStorageConfig;
    }
  }

  /**
   * @deprecated Use {@link #VIEW_TYPE} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_STORAGE_TYPE = VIEW_TYPE.key();
  /**
   * @deprecated Use {@link #VIEW_TYPE} and its methods.
   */
  @Deprecated
  public static final FileSystemViewStorageType DEFAULT_VIEW_STORAGE_TYPE = VIEW_TYPE.defaultValue();
  /**
   * @deprecated Use {@link #INCREMENTAL_TIMELINE_SYNC_ENABLE} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_INCREMENTAL_SYNC_MODE = INCREMENTAL_TIMELINE_SYNC_ENABLE.key();
  /**
   * @deprecated Use {@link #INCREMENTAL_TIMELINE_SYNC_ENABLE} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_FILESYSTEM_VIEW_INCREMENTAL_SYNC_MODE = INCREMENTAL_TIMELINE_SYNC_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #SECONDARY_VIEW_TYPE} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE = SECONDARY_VIEW_TYPE.key();
  /**
   * @deprecated Use {@link #SECONDARY_VIEW_TYPE} and its methods.
   */
  @Deprecated
  public static final FileSystemViewStorageType DEFAULT_SECONDARY_VIEW_STORAGE_TYPE = SECONDARY_VIEW_TYPE.defaultValue();
  /**
   * @deprecated Use {@link #REMOTE_HOST_NAME} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_REMOTE_HOST = REMOTE_HOST_NAME.key();
  /**
   * @deprecated Use {@link #REMOTE_HOST_NAME} and its methods.
   */
  @Deprecated
  public static final String DEFUALT_REMOTE_VIEW_SERVER_HOST = REMOTE_HOST_NAME.defaultValue();
  /**
   * @deprecated Use {@link #REMOTE_PORT_NUM} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_REMOTE_PORT = REMOTE_PORT_NUM.key();
  /**
   * @deprecated Use {@link #REMOTE_PORT_NUM} and its methods.
   */
  @Deprecated
  public static final Integer DEFAULT_REMOTE_VIEW_SERVER_PORT = REMOTE_PORT_NUM.defaultValue();
  /**
   * @deprecated Use {@link #SPILLABLE_DIR} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_SPILLABLE_DIR = SPILLABLE_DIR.key();
  /**
   * @deprecated Use {@link #SPILLABLE_DIR} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_VIEW_SPILLABLE_DIR = SPILLABLE_DIR.defaultValue();
  /**
   * @deprecated Use {@link #SPILLABLE_MEMORY} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_SPILLABLE_MEM = SPILLABLE_MEMORY.key();
  /**
   * @deprecated Use {@link #SPILLABLE_MEMORY} and its methods.
   */
  @Deprecated
  private static final Long DEFAULT_MAX_MEMORY_FOR_VIEW = SPILLABLE_MEMORY.defaultValue();
  /**
   * @deprecated Use {@link #SPILLABLE_COMPACTION_MEM_FRACTION} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION = SPILLABLE_COMPACTION_MEM_FRACTION.key();
  /**
   * @deprecated Use {@link #SPILLABLE_COMPACTION_MEM_FRACTION} and its methods.
   */
  @Deprecated
  private static final Double DEFAULT_MEM_FRACTION_FOR_PENDING_COMPACTION = SPILLABLE_COMPACTION_MEM_FRACTION.defaultValue();
  /**
   * @deprecated Use {@link #BOOTSTRAP_BASE_FILE_MEM_FRACTION} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_BOOTSTRAP_BASE_FILE_FRACTION = BOOTSTRAP_BASE_FILE_MEM_FRACTION.key();
  /**
   * @deprecated Use {@link #SPILLABLE_REPLACED_MEM_FRACTION} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_REPLACED_MEM_FRACTION = SPILLABLE_REPLACED_MEM_FRACTION.key();
  /**
   * @deprecated Use {@link #SPILLABLE_REPLACED_MEM_FRACTION} and its methods.
   */
  @Deprecated
  private static final Double DEFAULT_MEM_FRACTION_FOR_REPLACED_FILEGROUPS = SPILLABLE_REPLACED_MEM_FRACTION.defaultValue();
  /**
   * @deprecated Use {@link #SPILLABLE_CLUSTERING_MEM_FRACTION} and its methods.
   */
  @Deprecated
  public static final String FILESYSTEM_VIEW_PENDING_CLUSTERING_MEM_FRACTION = SPILLABLE_CLUSTERING_MEM_FRACTION.key();
  /**
   * @deprecated Use {@link #SPILLABLE_CLUSTERING_MEM_FRACTION} and its methods.
   */
  @Deprecated
  private static final Double DEFAULT_MEM_FRACTION_FOR_PENDING_CLUSTERING_FILEGROUPS = SPILLABLE_CLUSTERING_MEM_FRACTION.defaultValue();
  /**
   * @deprecated Use {@link #ROCKSDB_BASE_PATH} and its methods.
   */
  @Deprecated
  private static final String ROCKSDB_BASE_PATH_PROP = ROCKSDB_BASE_PATH.key();
  /**
   * @deprecated Use {@link #ROCKSDB_BASE_PATH} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_ROCKSDB_BASE_PATH = ROCKSDB_BASE_PATH.defaultValue();
  /**
   * @deprecated Use {@link #REMOTE_TIMEOUT_SECS} and its methods.
   */
  @Deprecated
  public static final String FILESTYSTEM_REMOTE_TIMELINE_CLIENT_TIMEOUT_SECS = REMOTE_TIMEOUT_SECS.key();
  /**
   * @deprecated Use {@link #REMOTE_TIMEOUT_SECS} and its methods.
   */
  @Deprecated
  public static final Integer DEFAULT_REMOTE_TIMELINE_CLIENT_TIMEOUT_SECS = REMOTE_TIMEOUT_SECS.defaultValue();
  /**
   * @deprecated Use {@link #BOOTSTRAP_BASE_FILE_MEM_FRACTION} and its methods.
   */
  @Deprecated
  private static final Double DEFAULT_MEM_FRACTION_FOR_EXTERNAL_DATA_FILE = BOOTSTRAP_BASE_FILE_MEM_FRACTION.defaultValue();
}
