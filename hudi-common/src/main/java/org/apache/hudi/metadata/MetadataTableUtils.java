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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MetadataTableUtils {

  /**
   * Whether to use batch lookup for listing the latest base files in metadata table.
   * <p>
   * Note that metadata table has to be enabled, and the storage type of the file system view
   * cannot be EMBEDDED_KV_STORE or SPILLABLE_DISK (these two types are not integrated with
   * metadata table, see HUDI-5612).
   *
   * @param metadataConfig
   * @param fsViewStorageConfig
   * @return {@code true} if using batch lookup; {@code false} otherwise.
   */
  public static boolean shouldUseBatchLookup(HoodieMetadataConfig metadataConfig,
                                             FileSystemViewStorageConfig fsViewStorageConfig) {
    FileSystemViewStorageType storageType = fsViewStorageConfig.getStorageType();
    return metadataConfig.enabled()
        && !FileSystemViewStorageType.EMBEDDED_KV_STORE.equals(storageType)
        && !FileSystemViewStorageType.SPILLABLE_DISK.equals(storageType);
  }

  /**
   * Delete the metadata table from physical storage. This operation also updates
   * `hoodie.properties` for the data table accordingly.
   * <p>
   * Note: it takes in data table's {@link HoodieTableMetaClient} as it's safe to assume that
   * metadata table resides in the same file system as the data table does and its base path
   * can be retrieved by {@link HoodieTableMetadata#getMetadataTableBasePath}.
   *
   * @param dataTableMetaClient data table's meta client.
   * @return the {@link Path} of the deleted metadata table.
   */
  public static Path deleteMetadataTable(HoodieTableMetaClient dataTableMetaClient) throws IOException {
    Path metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(dataTableMetaClient.getBasePathV2());
    dataTableMetaClient.getFs().delete(metadataTableBasePath, true);
    dataTableMetaClient.getTableConfig().clearMetadataPartitions(dataTableMetaClient);
    return metadataTableBasePath;
  }
}
