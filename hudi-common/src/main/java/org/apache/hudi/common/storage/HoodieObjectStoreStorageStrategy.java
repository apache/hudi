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

package org.apache.hudi.common.storage;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.hash.HashID;

import java.util.Map;

public class HoodieObjectStoreStorageStrategy implements HoodieStorageStrategy {
  private static final int HASH_SEED = 0x0e43cd7a;

  private String tableName;
  private String storagePath;
  private String basePath;

  public HoodieObjectStoreStorageStrategy(String basePath, Map<String, String> properties) {
    this.basePath = HoodieWrapperFileSystem.getPathStrWithoutScheme(basePath);
    this.storagePath = properties.get(HoodieTableConfig.HOODIE_STORAGE_PATH_KEY);
    this.tableName = properties.get(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);

    ValidationUtils.checkArgument(StringUtils.nonEmpty(this.basePath));
    ValidationUtils.checkArgument(StringUtils.nonEmpty(this.storagePath));
    ValidationUtils.checkArgument(StringUtils.nonEmpty(this.tableName));
  }

  public String storageLocation(String partitionPath, String fileId) {
    boolean nonPartitioned = StringUtils.isNullOrEmpty(partitionPath) || NON_PARTITIONED_NAME.equals(partitionPath);

    return String.format("%s/%08x/%s/%s",
        storagePath,
        nonPartitioned ? hash(fileId) : hash(partitionPath + fileId),
        tableName,
        nonPartitioned ? "" : FSUtils.stripLeadingSlash(partitionPath));
  }

  // TODO: hash function should be configurable
  protected int hash(String str) {
    return HashID.getXXHash32(str, HASH_SEED);
  }

  public String getBasePath() {
    return basePath;
  }

  public String getStoragePath() {
    return storagePath;
  }
}
