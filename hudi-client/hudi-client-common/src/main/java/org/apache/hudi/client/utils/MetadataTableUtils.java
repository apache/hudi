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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.config.HoodieWriteConfig;

public class MetadataTableUtils {

  /**
   * Whether to use batch lookup for listing the latest base files in metadata table.
   * <p>
   * Note that metadata table has to be enabled, and the storage type of the file system view
   * cannot be EMBEDDED_KV_STORE or SPILLABLE_DISK (these two types are not integrated with
   * metadata table, see HUDI-5612).
   *
   * @param config Write configs.
   * @return {@code true} if using batch lookup; {@code false} otherwise.
   */
  public static boolean shouldUseBatchLookup(HoodieWriteConfig config) {
    FileSystemViewStorageType storageType =
        config.getClientSpecifiedViewStorageConfig().getStorageType();
    return config.getMetadataConfig().enabled()
        && !FileSystemViewStorageType.EMBEDDED_KV_STORE.equals(storageType)
        && !FileSystemViewStorageType.SPILLABLE_DISK.equals(storageType);
  }
}
