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

package org.apache.hudi.io;

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;

import java.io.IOException;

import static org.apache.hudi.io.HoodieReadHandle.createNewFileReader;

/**
 * Extract range information for a given file slice.
 */
public class HoodieRangeInfoHandle {
  private final StorageConfiguration<?> storageConfig;
  private final HoodieWriteConfig writeConfig;

  public HoodieRangeInfoHandle(HoodieWriteConfig config, StorageConfiguration<?> storageConfig) {
    this.writeConfig = config;
    this.storageConfig = storageConfig;
  }

  public String[] getMinMaxKeys(HoodieBaseFile baseFile) throws IOException {
    HoodieStorage hoodieStorage = HoodieStorageUtils.getStorage(baseFile.getStoragePath(), storageConfig);
    try (HoodieFileReader reader = createNewFileReader(hoodieStorage, writeConfig, baseFile)) {
      return reader.readMinMaxRecordKeys();
    }
  }

}
