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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;

public class HoodieStorageUtils {
  public static final String DEFAULT_URI = "file:///";

  public static HoodieStorage getStorage(StorageConfiguration<?> conf) {
    return getStorage(DEFAULT_URI, conf);
  }

  public static HoodieStorage getStorage(String basePath, StorageConfiguration<?> conf) {
    return getStorage(new StoragePath(basePath), conf);
  }

  public static HoodieStorage getStorage(StoragePath path, StorageConfiguration<?> conf) {
    String storageClass = conf.getString(HoodieStorageConfig.HOODIE_STORAGE_CLASS.key())
        .orElse(HoodieStorageConfig.HOODIE_STORAGE_CLASS.defaultValue());
    try {
      return (HoodieStorage) ReflectionUtils.loadClass(
          storageClass, new Class<?>[] {StoragePath.class, StorageConfiguration.class}, path, conf);
    } catch (Exception e) {
      throw new HoodieException("Unable to create " + storageClass, e);
    }
  }

  /**
   * Creates a {@link HoodieStorage} instance using custom constructor parameters.
   * This method uses reflection to instantiate the storage class with provided parameter types.
   *
   * @param conf       storage configuration containing the storage class name
   * @param paramTypes array of parameter types for the constructor
   * @param params     array of parameter values for the constructor
   * @return {@link HoodieStorage} instance
   * @throws HoodieException if unable to create the storage instance
   */
  public static HoodieStorage getStorage(StorageConfiguration<?> conf,
                                         Class<?>[] paramTypes,
                                         Object... params) {
    String storageClass = conf.getString(HoodieStorageConfig.HOODIE_STORAGE_CLASS.key())
        .orElse(HoodieStorageConfig.HOODIE_STORAGE_CLASS.defaultValue());
    try {
      return (HoodieStorage) ReflectionUtils.loadClass(storageClass, paramTypes, params);
    } catch (Exception e) {
      throw new HoodieException("Unable to create " + storageClass
          + " with constructor parameters: " + java.util.Arrays.toString(paramTypes), e);
    }
  }
}
