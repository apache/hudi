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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.table.HoodieTable;

import java.util.Collections;
import java.util.Map;

public class EightToNineUpgradeHandler implements UpgradeHandler {

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config,
                                             HoodieEngineContext context,
                                             String instantTime,
                                             SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);

    // If auto upgrade is disabled, set writer version to 8 and return
    if (!config.autoUpgrade()) {
      config.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
      return Collections.emptyMap();
    }
    HoodieTableMetaClient metaClient = table.getMetaClient();

    // Populate missing index versions indexes
    Option<HoodieIndexMetadata> indexMetadataOpt = metaClient.getIndexMetadata();
    if (indexMetadataOpt.isPresent()) {
      populateIndexVersionIfMissing(indexMetadataOpt);

      // Write the updated index metadata back to storage
      HoodieTableMetaClient.writeIndexMetadataToStorage(
          metaClient.getStorage(),
          metaClient.getIndexDefinitionPath(),
          indexMetadataOpt.get(),
          metaClient.getTableConfig().getTableVersion());
    }
    return Collections.emptyMap();
  }

  /**
   * Populates missing version attributes in index definitions based on table version.
   *
   * @param indexDefOption optional index metadata containing index definitions
   */
  static void populateIndexVersionIfMissing(Option<HoodieIndexMetadata> indexDefOption) {
    indexDefOption.ifPresent(idxDefs ->
        idxDefs.getIndexDefinitions().replaceAll((indexName, idxDef) -> {
          if (idxDef.getVersion() == null) {
            return idxDef.toBuilder().withVersion(HoodieIndexVersion.V1).build();
          } else {
            return idxDef;
          }
        }));
  }
}
