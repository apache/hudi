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

package org.apache.hudi.storage.strategy;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.storage.StorageIOUtils;
import org.apache.hudi.storage.StoragePath;

import java.util.Map;

public class ObjectStoreStorageStrategy implements StorageStrategy {
  public static final String NON_PARTITIONED_NAME = ".";
  private static final int HASH_SEED = 0x0e43cd7a;

  String basePath;
  String tableName;
  String storagePrefix;

  public ObjectStoreStorageStrategy(String basePath, Map<String, String> props) {
    this.basePath = basePath;
    this.tableName = props.get(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
    this.storagePrefix = props.getOrDefault(HoodieStorageConfig.HOODIE_STORAGE_PREFIX.key(), basePath);

    ValidationUtils.checkArgument(StringUtils.nonEmpty(this.basePath));
    ValidationUtils.checkArgument(StringUtils.nonEmpty(this.tableName));
    ValidationUtils.checkArgument(StringUtils.nonEmpty(this.storagePrefix));
  }

  @Override
  public StoragePath getStorageLocation(String partitionPath, String fileId) {
    boolean nonPartitioned = StringUtils.isNullOrEmpty(partitionPath) || NON_PARTITIONED_NAME.equals(partitionPath);
    return new StoragePath(String.format("%s/%08x/%s/%s",
        storagePrefix,
        nonPartitioned ? hash(fileId) : hash(partitionPath + fileId),
        tableName,
        nonPartitioned ? "" : partitionPath));
  }

  @Override
  public StoragePath getStorageLocation(StoragePath filePath) {
    return getStorageLocation(
        StorageIOUtils.getRelativePartitionPath(new StoragePath(basePath), filePath),
        StorageIOUtils.getFileIdFromFilePath(filePath));
  }

  @Override
  public String getBasePath() {
    return basePath;
  }

  @Override
  public String getStoragePrefix() {
    return storagePrefix;
  }

  protected int hash(String str) {
    return HashID.getXXHash32(str, HASH_SEED);
  }
}
