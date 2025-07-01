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
import org.apache.hudi.common.util.IndexVersionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Collections;
import java.util.Map;

public class EightToNineUpgradeHandler implements UpgradeHandler {

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context,
                                             String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();

    // Populate missing index versions indexes
    Option<HoodieIndexMetadata> indexMetadataOpt = metaClient.getIndexMetadata();
    if (indexMetadataOpt.isPresent()) {
      IndexVersionUtils.populateIndexVersionIfMissing(metaClient.getTableConfig().getTableVersion(), indexMetadataOpt);

      // Write the updated index metadata back to storage
      HoodieTableMetaClient.writeIndexMetadataToStorage(
          metaClient.getStorage(),
          metaClient.getIndexDefinitionPath(),
          indexMetadataOpt.get());
    }
    
    return Collections.emptyMap();
  }
}
