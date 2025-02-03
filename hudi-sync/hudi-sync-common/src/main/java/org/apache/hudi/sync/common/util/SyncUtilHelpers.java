/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetaSyncException;
import org.apache.hudi.sync.common.HoodieSyncTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;

/**
 * Helper class for syncing Hudi commit data with external metastores.
 */
public class SyncUtilHelpers {
  private static final Logger LOG = LoggerFactory.getLogger(SyncUtilHelpers.class);

  // Locks for each table (base path) to avoid concurrent modification of the same underneath meta storage.
  // Meta store such as Hive may encounter {@code ConcurrentModificationException} for #alter_table.
  private static final ConcurrentHashMap<String, Lock> TABLE_LOCKS = new ConcurrentHashMap<>();

  /**
   * Create an instance of an implementation of {@link HoodieSyncTool} that will sync all the relevant meta information
   * with an external metastore such as Hive etc. to ensure Hoodie tables can be queried or read via external systems.
   *
   * @param syncToolClassName Class name of the {@link HoodieSyncTool} implementation.
   * @param props             property map.
   * @param hadoopConfig      Hadoop confs.
   * @param fs                Filesystem used.
   * @param targetBasePath    The target base path that contains the hoodie table.
   * @param baseFileFormat    The file format used by the hoodie table (defaults to PARQUET).
   */
  public static void runHoodieMetaSync(String syncToolClassName,
                                       TypedProperties props,
                                       Configuration hadoopConfig,
                                       FileSystem fs,
                                       String targetBasePath,
                                       String baseFileFormat) {
    runHoodieMetaSync(syncToolClassName, props, hadoopConfig, fs, targetBasePath, baseFileFormat, Option.empty());
  }

  /**
   * Create an instance of an implementation of {@link HoodieSyncTool} that will sync all the relevant meta information
   * with an external metastore such as Hive etc. to ensure Hoodie tables can be queried or read via external systems.
   *
   * @param syncToolClassName Class name of the {@link HoodieSyncTool} implementation.
   * @param props             property map.
   * @param hadoopConfig      Hadoop confs.
   * @param fs                Filesystem used.
   * @param targetBasePath    The target base path that contains the hoodie table.
   * @param baseFileFormat    The file format used by the hoodie table (defaults to PARQUET).
   * @param metaClient        An optional {@link HoodieTableMetaClient} to use when running the sync.
   *                          If the option is empty, a client will be created in the sync tools if one is required by the sync.
   */
  public static void runHoodieMetaSync(String syncToolClassName,
                                       TypedProperties props,
                                       Configuration hadoopConfig,
                                       FileSystem fs,
                                       String targetBasePath,
                                       String baseFileFormat,
                                       Option<HoodieTableMetaClient> metaClient) {
    if (targetBasePath == null) {
      throw new IllegalArgumentException("Target base path must not be null");
    }

    // Get or create a lock for the specific table
    Lock tableLock = TABLE_LOCKS.computeIfAbsent(targetBasePath, k -> new ReentrantLock());
    tableLock.lock();
    try {
      try (HoodieSyncTool syncTool = instantiateMetaSyncTool(syncToolClassName, props, hadoopConfig, fs, targetBasePath, baseFileFormat, metaClient)) {
        syncTool.syncHoodieTable();
      } catch (Throwable e) {
        throw new HoodieMetaSyncException("Could not sync using the meta sync class " + syncToolClassName, e);
      }
    } finally {
      tableLock.unlock();
    }
  }

  public static HoodieSyncTool instantiateMetaSyncTool(String syncToolClassName,
                                                       TypedProperties props,
                                                       Configuration hadoopConfig,
                                                       FileSystem fs,
                                                       String targetBasePath,
                                                       String baseFileFormat,
                                                       Option<HoodieTableMetaClient> metaClient) {
    TypedProperties properties = new TypedProperties();
    properties.putAll(props);
    properties.put(BASE_PATH.key(), targetBasePath);
    properties.put(META_SYNC_BASE_PATH.key(), targetBasePath);
    properties.put(META_SYNC_BASE_FILE_FORMAT.key(), baseFileFormat);
    if (properties.containsKey(META_SYNC_TABLE_NAME.key())) {
      String tableName = properties.getString(META_SYNC_TABLE_NAME.key());
      if (!tableName.equals(tableName.toLowerCase())) {
        LOG.warn(
            "Table name \"{}\" contains capital letters. Your metastore may automatically convert this to lower case and can cause table not found errors during subsequent syncs.", tableName);
      }
    }

    if (ReflectionUtils.hasConstructor(syncToolClassName, new Class<?>[] {Properties.class, Configuration.class, Option.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(syncToolClassName,
          new Class<?>[] {Properties.class, Configuration.class, Option.class},
          properties, hadoopConfig, metaClient));
    } else if (ReflectionUtils.hasConstructor(syncToolClassName,
        new Class<?>[] {Properties.class, Configuration.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(syncToolClassName,
          new Class<?>[] {Properties.class, Configuration.class},
          properties, hadoopConfig));
    } else if (ReflectionUtils.hasConstructor(syncToolClassName,
        new Class<?>[] {Properties.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(syncToolClassName,
          new Class<?>[] {Properties.class},
          properties));
    } else if (ReflectionUtils.hasConstructor(syncToolClassName,
        new Class<?>[] {TypedProperties.class, Configuration.class, FileSystem.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(syncToolClassName,
          new Class<?>[] {TypedProperties.class, Configuration.class, FileSystem.class},
          properties, hadoopConfig, fs));
    } else if (ReflectionUtils.hasConstructor(syncToolClassName,
        new Class<?>[] {Properties.class, FileSystem.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(syncToolClassName,
          new Class<?>[] {Properties.class, FileSystem.class},
          properties, fs));
    } else {
      throw new HoodieException("Could not load meta sync class " + syncToolClassName
          + ": no valid constructor found.");
    }
  }

  public static HoodieException getHoodieMetaSyncException(Map<String, HoodieException> failedMetaSyncs) {
    if (failedMetaSyncs.size() == 1) {
      return failedMetaSyncs.values().stream().findFirst().get();
    }
    StringBuilder sb = new StringBuilder();
    sb.append("MetaSyncs failed: {");
    sb.append(String.join(",", failedMetaSyncs.keySet()));
    sb.append("}\n");
    for (String impl : failedMetaSyncs.keySet()) {
      sb.append(failedMetaSyncs.get(impl).getMessage());
      sb.append("\n");
    }
    return new HoodieMetaSyncException(sb.toString(), failedMetaSyncs);
  }

  static int getNumberOfLocks() {
    return TABLE_LOCKS.size();
  }
}
