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

package org.apache.hudi.utilities.health;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;

/**
 * Whether a metadata table exists for the data table.
 *
 * <p>Shared by the metadata-table checks so they skip for the same reasons in the same words, and
 * so neither depends on the other's internals.
 */
final class MetadataTablePresence {

  private MetadataTablePresence() {
  }

  /** Empty when the metadata table is present; otherwise the reason to skip, for the operator. */
  static Option<String> reasonAbsent(HoodieTableMetaClient metaClient) throws IOException {
    if (!metaClient.getTableConfig().isMetadataTableAvailable()) {
      return Option.of("Table has no metadata table (its FILES partition has not been initialized)");
    }
    StoragePath metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
    if (!metaClient.getStorage().exists(metadataTablePath)) {
      return Option.of("Metadata table is registered in hoodie.properties but " + metadataTablePath
          + " does not exist on storage; the next write will re-initialize it");
    }
    return Option.empty();
  }
}
