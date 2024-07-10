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

import org.apache.hudi.storage.StoragePath;

import java.util.Map;

public class DefaultStorageStrategy implements StorageStrategy {
  String basePath;
  String storagePrefix;

  public DefaultStorageStrategy(String basePath, Map<String, String> props) {
    // basePath == storagePrefix with DefaultStorageStrategy
    this.basePath = basePath;
    this.storagePrefix = basePath;
  }

  public DefaultStorageStrategy(String basePath) {
    this.basePath = basePath;
    this.storagePrefix = basePath;
  }

  public DefaultStorageStrategy() {
    // AVOID USING THIS CONSTRUCTOR IF POSSIBLE
    // can't use @VisibleForTesting for scala test classes like ColumnStatsIndexHelper
    // TODO: add a TestStrategy for test-use only
    // TODO: Revisit this for some usages where basePath is not accessible
  }

  @Override
  public StoragePath getStorageLocation(String partitionPath, String fileId) {
    return new StoragePath(String.format("%s/%s", storagePrefix, partitionPath));
  }

  @Override
  public StoragePath getStorageLocation(StoragePath filePath) {
    return filePath;
  }

  @Override
  public String getBasePath() {
    return basePath;
  }

  @Override
  public String getStoragePrefix() {
    return storagePrefix;
  }

}
