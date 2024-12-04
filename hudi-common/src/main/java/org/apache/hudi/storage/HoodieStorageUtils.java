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

package org.apache.hudi.storage;

import static org.apache.hudi.common.util.ConfigUtils.fetchConfigs;

import java.io.IOException;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.util.StorageIOUtils;
import org.apache.hudi.storage.strategy.DefaultStorageStrategy;
import org.apache.hudi.storage.strategy.StorageStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

public class HoodieStorageUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieStorageUtils.class);
  public static final String DEFAULT_URI = "file:///";

  // TODO: Remove this method and force to pass storage strategy in or use default strategy
  public static HoodieStorage getStorage(StorageConfiguration<?> conf) {
    return getStorage(DEFAULT_URI, conf, new DefaultStorageStrategy());
  }

  // TODO: Remove this method and force to pass storage strategy in or use default strategy
  public static HoodieStorage getStorage(String basePath, StorageConfiguration<?> conf) {
    return getStorage(new StoragePath(basePath), conf);
  }

  // TODO: Remove this method and force to pass storage strategy in or use default strategy
  public static HoodieStorage getStorage(StoragePath path, StorageConfiguration<?> conf) {
    String storageClass = conf.getString(HoodieStorageConfig.HOODIE_STORAGE_CLASS.key())
        .orElse(HoodieStorageConfig.HOODIE_STORAGE_CLASS.defaultValue());
    try {
      return (HoodieStorage) ReflectionUtils.loadClass(
          storageClass,
          new Class<?>[] {StoragePath.class, StorageConfiguration.class, StorageStrategy.class},
          path, conf, new DefaultStorageStrategy());
    } catch (Exception e) {
      throw new HoodieException("Unable to create " + storageClass, e);
    }
  }

  public static HoodieStorage getStorage(StorageConfiguration<?> conf, StorageStrategy storageStrategy) {
    return getStorage(DEFAULT_URI, conf, storageStrategy);
  }

  public static HoodieStorage getStorage(String basePath, StorageConfiguration<?> conf, StorageStrategy storageStrategy) {
    return getStorage(new StoragePath(basePath), conf, storageStrategy);
  }

  public static HoodieStorage getStorage(StoragePath path, StorageConfiguration<?> conf, StorageStrategy storageStrategy) {
    String storageClass = conf.getString(HoodieStorageConfig.HOODIE_STORAGE_CLASS.key())
        .orElse(HoodieStorageConfig.HOODIE_STORAGE_CLASS.defaultValue());
    try {
      return (HoodieStorage) ReflectionUtils.loadClass(
          storageClass,
          new Class<?>[] {StoragePath.class, StorageConfiguration.class, StorageStrategy.class},
          path, conf, storageStrategy);
    } catch (Exception e) {
      throw new HoodieException("Unable to create " + storageClass, e);
    }
  }

  public static StorageStrategy getStorageStrategy(HoodieStorage storage, String basePath) {
    TypedProperties props;
    try {
      props = fetchConfigs(storage, new StoragePath(basePath,
              HoodieTableMetaClient.METAFOLDER_NAME), HoodieTableConfig.HOODIE_PROPERTIES_FILE, HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP,
          HoodieTableConfig.MAX_READ_RETRIES, HoodieTableConfig.READ_RETRY_DELAY_MSEC);
    } catch (IOException ioe) {
      throw new HoodieException("Failed to fetch table config");
    }

    return getStorageStrategy(props);
  }

  public static StorageStrategy getStorageStrategy(TypedProperties props) {
    String storageStrategyClass = props.getString(HoodieStorageConfig.STORAGE_STRATEGY_CLASS.key(), null);
    String basePath = props.getString(HoodieCommonConfig.BASE_PATH.key(), null);
    String tableName = props.getString(HoodieCommonConfig.BASE_PATH.key(), null);
    String storagePrefix = props.getString(HoodieCommonConfig.BASE_PATH.key(), null);
    if (isNullOrEmpty(storageStrategyClass)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("hoodie.storage.strategy.class is not set in the props, falling back to default strategy");
      }
      storageStrategyClass = HoodieStorageConfig.STORAGE_STRATEGY_CLASS.defaultValue();
    }

    if (isNullOrEmpty(basePath) || isNullOrEmpty(tableName) || isNullOrEmpty(storagePrefix)) {
      // TODO: Should make sure we always pass required configs in
      if (LOG.isDebugEnabled()) {
        LOG.debug("Important configs missing for initializing storage strategy, falling back to use a dummy strategy");
      }
      return new DefaultStorageStrategy();
    }

    return StorageIOUtils.createStorageStrategy(
        storageStrategyClass,
        basePath,
        tableName,
        storagePrefix);
  }
}
