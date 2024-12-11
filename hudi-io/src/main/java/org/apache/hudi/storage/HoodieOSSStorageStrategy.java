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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HoodieOSSStorageStrategy implements HoodieStorageStrategy {

  public static final String FILE_ID_KEY = "hoodie_file_id";
  public static final String TABLE_BASE_PATH = "hoodie_table_base_path";
  public static final String TABLE_NAME = "hoodie_table_name";
  public static final String TABLE_STORAGE_PATH = "hoodie_storage_path";
  private final String hoodieStoragePath;
  private final String basePath;
  public HoodieOSSStorageStrategy(StorageConfiguration<?> storageConf) {
    this.hoodieStoragePath = "";
    this.basePath = "";
  }

  @Override
  public StoragePath storageLocation(String path, Map<String, String> configMap) {
    String fileID = configMap.get(FILE_ID_KEY);
    String basePath = configMap.get(TABLE_BASE_PATH);
    String storagePath = configMap.get(TABLE_STORAGE_PATH);
    int hash = (path + fileID).hashCode() & Integer.MAX_VALUE;
    String urlString = "/tmp/bucketA/" + hoodieStoragePath + "/" + hash + "/" + basePath + "/" + path;
    return new StoragePath(urlString);
  }

  @Override
  public Set<StoragePath> getAllLocations(String partitionPath, Map<String, String> configMap) {
    HashSet<StoragePath> res = new HashSet<>();
    res.add(new StoragePath(basePath, partitionPath));
    return res;
  }

  @Override
  public StoragePath getRelativePath(String path, Map<String, String> configMap) {
    String tableName = configMap.get(TABLE_NAME);
    String[] res = path.split(tableName);
    return new StoragePath(res[res.length - 1].replace("/", ""));
  }
}
