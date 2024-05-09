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

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.util.ReflectionUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HoodieStorageUtils {
  public static final String HUDI_HADOOP_STORAGE = "org.apache.hudi.storage.hadoop.HoodieHadoopStorage";
  public static final String HADOOP_STORAGE_CONF = "org.apache.hudi.storage.hadoop.HadoopStorageConfiguration";
  public static final String DEFAULT_URI = "file:///";

  public static HoodieStorage getStorage(StorageConfiguration<?> conf) {
    return getStorage(DEFAULT_URI, conf);
  }

  public static HoodieStorage getStorage(FileSystem fs) {
    return (HoodieStorage) ReflectionUtils.loadClass(HUDI_HADOOP_STORAGE, new Class<?>[] {FileSystem.class}, fs);
  }

  public static HoodieStorage getStorage(String basePath, StorageConfiguration<?> conf) {
    return (HoodieStorage) ReflectionUtils.loadClass(HUDI_HADOOP_STORAGE, new Class<?>[] {String.class, StorageConfiguration.class}, basePath, conf);
  }

  public static HoodieStorage getStorage(String basePath, Configuration conf) {
    return (HoodieStorage) ReflectionUtils.loadClass(HUDI_HADOOP_STORAGE, new Class<?>[] {String.class, Configuration.class}, basePath, conf);
  }

  public static HoodieStorage getStorage(StoragePath path, StorageConfiguration<?> conf) {
    return (HoodieStorage) ReflectionUtils.loadClass(HUDI_HADOOP_STORAGE, new Class<?>[] {StoragePath.class, StorageConfiguration.class}, path, conf);
  }

  public static HoodieStorage getStorage(StoragePath path,
                                         StorageConfiguration<?> conf,
                                         boolean enableRetry,
                                         long maxRetryIntervalMs,
                                         int maxRetryNumbers,
                                         long initialRetryIntervalMs,
                                         String retryExceptions,
                                         ConsistencyGuard consistencyGuard) {
    return (HoodieStorage) ReflectionUtils.loadClass(HUDI_HADOOP_STORAGE,
        new Class<?>[] {StoragePath.class, StorageConfiguration.class, boolean.class, long.class, int.class, long.class,
            String.class, ConsistencyGuard.class},
        path, conf, enableRetry, maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptions,
        consistencyGuard);
  }

  public static HoodieStorage getRawStorage(HoodieStorage storage) {
    return (HoodieStorage) ReflectionUtils.loadClass(HUDI_HADOOP_STORAGE, new Class<?>[] {HoodieStorage.class}, storage);
  }

  public static StorageConfiguration<?> getStorageConf(Configuration conf) {
    return (StorageConfiguration<?>) ReflectionUtils.loadClass(HADOOP_STORAGE_CONF,
        new Class<?>[] {Configuration.class}, conf);
  }

  public static StorageConfiguration<Configuration> getStorageConfWithCopy(Configuration conf) {
    return (StorageConfiguration<Configuration>) getStorageConf(conf).newInstance();
  }
}
