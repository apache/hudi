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

import org.apache.hudi.common.util.ReflectionUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HoodieStorageUtils {
  public static final String DEFAULT_URI = "file:///";

  public static HoodieStorage getStorage(StorageConfiguration<?> conf) {
    return getStorage(DEFAULT_URI, conf);
  }

  public static HoodieStorage getStorage(FileSystem fs) {
    return (HoodieStorage) ReflectionUtils.loadClass("org.apache.hudi.storage.hadoop.HoodieHadoopStorage", new Class<?>[] {FileSystem.class}, fs);
  }

  public static HoodieStorage getStorage(String basePath) {
    return (HoodieStorage) ReflectionUtils.loadClass("org.apache.hudi.storage.hadoop.HoodieHadoopStorage", new Class<?>[] {String.class}, basePath);
  }

  public static HoodieStorage getStorage(String basePath, StorageConfiguration<?> conf) {
    return (HoodieStorage) ReflectionUtils.loadClass("org.apache.hudi.storage.hadoop.HoodieHadoopStorage", new Class<?>[] {String.class, StorageConfiguration.class}, basePath, conf);
  }

  public static HoodieStorage getStorage(String basePath, Configuration conf) {
    return (HoodieStorage) ReflectionUtils.loadClass("org.apache.hudi.storage.hadoop.HoodieHadoopStorage", new Class<?>[] {String.class, Configuration.class}, basePath, conf);
  }

  public static HoodieStorage getStorage(StoragePath path, StorageConfiguration<?> conf) {
    return (HoodieStorage) ReflectionUtils.loadClass("org.apache.hudi.storage.hadoop.HoodieHadoopStorage", new Class<?>[] {StoragePath.class, StorageConfiguration.class}, path, conf);
  }

  public static HoodieStorage getRawStorage(HoodieStorage storage) {
    return (HoodieStorage) ReflectionUtils.loadClass("org.apache.hudi.storage.hadoop.HoodieHadoopStorage", new Class<?>[] {HoodieStorage.class}, storage);
  }

  public static StorageConfiguration<?> getNewStorageConf() {
    return ReflectionUtils.loadClass("org.apache.hudi.storage.hadoop.HadoopStorageConfiguration.HadoopStorageConfiguration");
  }

  public static StorageConfiguration<?> getStorageConf(Configuration conf) {
    return (StorageConfiguration<?>) ReflectionUtils.loadClass("org.apache.hudi.storage.hadoop.HadoopStorageConfiguration.HadoopStorageConfiguration",
        new Class<?>[] {Configuration.class}, conf);
  }
}
