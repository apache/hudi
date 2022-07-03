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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.sync.common.HoodieSyncTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.Properties;

/**
 * Helper class for syncing Hudi commit data with external metastores.
 */
public class SyncUtilHelpers {

  /**
   * Create an instance of an implementation of {@link HoodieSyncTool} that will sync all the relevant meta information
   * with an external metastore such as Hive etc. to ensure Hoodie tables can be queried or read via external systems.
   *
   * @param metaSyncFQCN   The class that implements the sync of the metadata.
   * @param props          property map.
   * @param hadoopConfig   Hadoop confs.
   * @param fs             Filesystem used.
   * @param targetBasePath The target base path that contains the hoodie table.
   * @param baseFileFormat The file format used by the hoodie table (defaults to PARQUET).
   */
  public static void runHoodieMetaSync(String metaSyncFQCN,
                                       TypedProperties props,
                                       Configuration hadoopConfig,
                                       FileSystem fs,
                                       String targetBasePath,
                                       String baseFileFormat) {
    try {
      instantiateMetaSyncTool(metaSyncFQCN, props, hadoopConfig, fs, targetBasePath, baseFileFormat).syncHoodieTable();
    } catch (Throwable e) {
      throw new HoodieException("Could not sync using the meta sync class " + metaSyncFQCN, e);
    }
  }

  static HoodieSyncTool instantiateMetaSyncTool(String metaSyncFQCN,
                                                TypedProperties props,
                                                Configuration hadoopConfig,
                                                FileSystem fs,
                                                String targetBasePath,
                                                String baseFileFormat) {
    TypedProperties properties = new TypedProperties();
    properties.putAll(props);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), targetBasePath);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.key(), baseFileFormat);

    if (ReflectionUtils.hasConstructor(metaSyncFQCN,
        new Class<?>[] {Properties.class, Configuration.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(metaSyncFQCN,
          new Class<?>[] {Properties.class, Configuration.class},
          properties, hadoopConfig));
    } else if (ReflectionUtils.hasConstructor(metaSyncFQCN,
        new Class<?>[] {Properties.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(metaSyncFQCN,
          new Class<?>[] {Properties.class},
          properties));
    } else if (ReflectionUtils.hasConstructor(metaSyncFQCN,
        new Class<?>[] {TypedProperties.class, Configuration.class, FileSystem.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(metaSyncFQCN,
          new Class<?>[] {TypedProperties.class, Configuration.class, FileSystem.class},
          properties, hadoopConfig, fs));
    } else if (ReflectionUtils.hasConstructor(metaSyncFQCN,
        new Class<?>[] {Properties.class, FileSystem.class})) {
      return ((HoodieSyncTool) ReflectionUtils.loadClass(metaSyncFQCN,
          new Class<?>[] {Properties.class, FileSystem.class},
          properties, fs));
    } else {
      throw new HoodieException("Could not load meta sync class " + metaSyncFQCN
          + ": no valid constructor found.");
    }
  }
}
