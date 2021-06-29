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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.ValidationUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * File System View Storage Configurations.
 */
public class FileSystemViewStorageConfig extends HoodieConfig {

  // Property Names
  public static final ConfigProperty<FileSystemViewStorageType> FILESYSTEM_VIEW_STORAGE_TYPE = ConfigProperty
      .key("hoodie.filesystem.view.type")
      .defaultValue(FileSystemViewStorageType.MEMORY)
      .withDocumentation("");

  public static final ConfigProperty<String> FILESYSTEM_VIEW_INCREMENTAL_SYNC_MODE = ConfigProperty
      .key("hoodie.filesystem.view.incr.timeline.sync.enable")
      .defaultValue("false")
      .withDocumentation("");

  public static final ConfigProperty<FileSystemViewStorageType> FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE = ConfigProperty
      .key("hoodie.filesystem.view.secondary.type")
      .defaultValue(FileSystemViewStorageType.MEMORY)
      .withDocumentation("");

  public static final ConfigProperty<String> FILESYSTEM_VIEW_REMOTE_HOST = ConfigProperty
      .key("hoodie.filesystem.view.remote.host")
      .defaultValue("localhost")
      .withDocumentation("");

  public static final ConfigProperty<Integer> FILESYSTEM_VIEW_REMOTE_PORT = ConfigProperty
      .key("hoodie.filesystem.view.remote.port")
      .defaultValue(26754)
      .withDocumentation("");

  public static final ConfigProperty<String> FILESYSTEM_VIEW_SPILLABLE_DIR = ConfigProperty
      .key("hoodie.filesystem.view.spillable.dir")
      .defaultValue("/tmp/view_map/")
      .withDocumentation("");

  public static final ConfigProperty<Long> FILESYSTEM_VIEW_SPILLABLE_MEM = ConfigProperty
      .key("hoodie.filesystem.view.spillable.mem")
      .defaultValue(100 * 1024 * 1024L) // 100 MB
      .withDocumentation("");

  public static final ConfigProperty<Double> FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION = ConfigProperty
      .key("hoodie.filesystem.view.spillable.compaction.mem.fraction")
      .defaultValue(0.8)
      .withDocumentation("");

  public static final ConfigProperty<Double> FILESYSTEM_VIEW_BOOTSTRAP_BASE_FILE_FRACTION = ConfigProperty
      .key("hoodie.filesystem.view.spillable.bootstrap.base.file.mem.fraction")
      .defaultValue(0.05)
      .withDocumentation("");

  public static final ConfigProperty<Double> FILESYSTEM_VIEW_REPLACED_MEM_FRACTION = ConfigProperty
      .key("hoodie.filesystem.view.spillable.replaced.mem.fraction")
      .defaultValue(0.01)
      .withDocumentation("");

  public static final ConfigProperty<Double> FILESYSTEM_VIEW_PENDING_CLUSTERING_MEM_FRACTION = ConfigProperty
      .key("hoodie.filesystem.view.spillable.clustering.mem.fraction")
      .defaultValue(0.01)
      .withDocumentation("");

  public static final ConfigProperty<String> ROCKSDB_BASE_PATH_PROP = ConfigProperty
      .key("hoodie.filesystem.view.rocksdb.base.path")
      .defaultValue("/tmp/hoodie_timeline_rocksdb")
      .withDocumentation("");

  public static final ConfigProperty<Integer> FILESTYSTEM_REMOTE_TIMELINE_CLIENT_TIMEOUT_SECS = ConfigProperty
      .key("hoodie.filesystem.view.remote.timeout.secs")
      .defaultValue(5 * 60) // 5 min
      .withDocumentation("");

  /**
   * Configs to control whether backup needs to be configured if clients were not able to reach
   * timeline service.
   */
  public static final ConfigProperty<String> REMOTE_BACKUP_VIEW_HANDLER_ENABLE = ConfigProperty
      .key("hoodie.filesystem.remote.backup.view.enable")
      .defaultValue("true") // Need to be disabled only for tests.
      .withDocumentation("");

  public static FileSystemViewStorageConfig.Builder newBuilder() {
    return new Builder();
  }

  private FileSystemViewStorageConfig() {
    super();
  }

  public FileSystemViewStorageType getStorageType() {
    return FileSystemViewStorageType.valueOf(getString(FILESYSTEM_VIEW_STORAGE_TYPE));
  }

  public boolean isIncrementalTimelineSyncEnabled() {
    return getBoolean(FILESYSTEM_VIEW_INCREMENTAL_SYNC_MODE);
  }

  public String getRemoteViewServerHost() {
    return getString(FILESYSTEM_VIEW_REMOTE_HOST);
  }

  public Integer getRemoteViewServerPort() {
    return getInt(FILESYSTEM_VIEW_REMOTE_PORT);
  }

  public Integer getRemoteTimelineClientTimeoutSecs() {
    return getInt(FILESTYSTEM_REMOTE_TIMELINE_CLIENT_TIMEOUT_SECS);
  }

  public long getMaxMemoryForFileGroupMap() {
    long totalMemory = getLong(FILESYSTEM_VIEW_SPILLABLE_MEM);
    return totalMemory - getMaxMemoryForPendingCompaction() - getMaxMemoryForBootstrapBaseFile();
  }

  public long getMaxMemoryForPendingCompaction() {
    long totalMemory = getLong(FILESYSTEM_VIEW_SPILLABLE_MEM);
    return new Double(totalMemory * getDouble(FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION))
        .longValue();
  }

  public long getMaxMemoryForBootstrapBaseFile() {
    long totalMemory = getLong(FILESYSTEM_VIEW_SPILLABLE_MEM);
    long reservedForExternalDataFile =
        new Double(totalMemory * getDouble(FILESYSTEM_VIEW_BOOTSTRAP_BASE_FILE_FRACTION))
            .longValue();
    return reservedForExternalDataFile;
  }

  public long getMaxMemoryForReplacedFileGroups() {
    long totalMemory = getLong(FILESYSTEM_VIEW_SPILLABLE_MEM);
    return new Double(totalMemory * getDouble(FILESYSTEM_VIEW_REPLACED_MEM_FRACTION))
        .longValue();
  }

  public long getMaxMemoryForPendingClusteringFileGroups() {
    long totalMemory = getLong(FILESYSTEM_VIEW_SPILLABLE_MEM);
    return new Double(totalMemory * getDouble(FILESYSTEM_VIEW_PENDING_CLUSTERING_MEM_FRACTION))
        .longValue();
  }

  public String getSpillableDir() {
    return getString(FILESYSTEM_VIEW_SPILLABLE_DIR);
  }

  public FileSystemViewStorageType getSecondaryStorageType() {
    return FileSystemViewStorageType.valueOf(getString(FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE));
  }

  public boolean shouldEnableBackupForRemoteFileSystemView() {
    return getBoolean(REMOTE_BACKUP_VIEW_HANDLER_ENABLE);
  }

  public String getRocksdbBasePath() {
    return getString(ROCKSDB_BASE_PATH_PROP);
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
      fileSystemViewStorageConfig.setValue(FILESYSTEM_VIEW_STORAGE_TYPE, storageType.name());
      return this;
    }

    public Builder withSecondaryStorageType(FileSystemViewStorageType storageType) {
      fileSystemViewStorageConfig.setValue(FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE, storageType.name());
      return this;
    }

    public Builder withIncrementalTimelineSync(boolean enableIncrTimelineSync) {
      fileSystemViewStorageConfig.setValue(FILESYSTEM_VIEW_INCREMENTAL_SYNC_MODE, Boolean.toString(enableIncrTimelineSync));
      return this;
    }

    public Builder withRemoteServerHost(String remoteServerHost) {
      fileSystemViewStorageConfig.setValue(FILESYSTEM_VIEW_REMOTE_HOST, remoteServerHost);
      return this;
    }

    public Builder withRemoteServerPort(Integer remoteServerPort) {
      fileSystemViewStorageConfig.setValue(FILESYSTEM_VIEW_REMOTE_PORT, remoteServerPort.toString());
      return this;
    }

    public Builder withMaxMemoryForView(Long maxMemoryForView) {
      fileSystemViewStorageConfig.setValue(FILESYSTEM_VIEW_SPILLABLE_MEM, maxMemoryForView.toString());
      return this;
    }

    public Builder withRemoteTimelineClientTimeoutSecs(Long timelineClientTimeoutSecs) {
      fileSystemViewStorageConfig.setValue(FILESTYSTEM_REMOTE_TIMELINE_CLIENT_TIMEOUT_SECS, timelineClientTimeoutSecs.toString());
      return this;
    }

    public Builder withMemFractionForPendingCompaction(Double memFractionForPendingCompaction) {
      fileSystemViewStorageConfig.setValue(FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION, memFractionForPendingCompaction.toString());
      return this;
    }

    public Builder withMemFractionForExternalDataFile(Double memFractionForExternalDataFile) {
      fileSystemViewStorageConfig.setValue(FILESYSTEM_VIEW_BOOTSTRAP_BASE_FILE_FRACTION, memFractionForExternalDataFile.toString());
      return this;
    }

    public Builder withBaseStoreDir(String baseStorePath) {
      fileSystemViewStorageConfig.setValue(FILESYSTEM_VIEW_SPILLABLE_DIR, baseStorePath);
      return this;
    }

    public Builder withRocksDBPath(String basePath) {
      fileSystemViewStorageConfig.setValue(ROCKSDB_BASE_PATH_PROP, basePath);
      return this;
    }

    public Builder withEnableBackupForRemoteFileSystemView(boolean enable) {
      fileSystemViewStorageConfig.setValue(REMOTE_BACKUP_VIEW_HANDLER_ENABLE, Boolean.toString(enable));
      return this;
    }

    public FileSystemViewStorageConfig build() {
      fileSystemViewStorageConfig.setDefaults(FileSystemViewStorageConfig.class.getName());
      // Validations
      FileSystemViewStorageType.valueOf(fileSystemViewStorageConfig.getString(FILESYSTEM_VIEW_STORAGE_TYPE));
      FileSystemViewStorageType.valueOf(fileSystemViewStorageConfig.getString(FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE));
      ValidationUtils.checkArgument(fileSystemViewStorageConfig.getInt(FILESYSTEM_VIEW_REMOTE_PORT) > 0);
      return fileSystemViewStorageConfig;
    }
  }

}
