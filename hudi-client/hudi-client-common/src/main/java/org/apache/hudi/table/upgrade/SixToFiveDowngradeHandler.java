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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.table.HoodieTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT;

/**
 * Downgrade handle to assist in downgrading hoodie table from version 6 to 5.
 * To ensure compatibility, we need recreate the compaction requested file to
 * .aux folder.
 * Since version 6 includes a new schema field for metadata table(MDT),
 * the MDT needs to be deleted during downgrade to avoid column drop error.
 * Also log block version was upgraded in version 6, therefore full compaction needs
 * to be completed during downgrade to avoid both read and future compaction failures.
 */
public class SixToFiveDowngradeHandler implements DowngradeHandler {

  @Override
  public UpgradeDowngrade.TableConfigChangeSet downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);

    // Since version 6 includes a new schema field for metadata table(MDT), the MDT needs to be deleted during downgrade to avoid column drop error.
    HoodieTableMetadataUtil.deleteMetadataTable(config.getBasePath(), context);
    UpgradeDowngradeUtils.syncCompactionRequestedFileToAuxiliaryFolder(table);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(table.getMetaClient());
    Map<ConfigProperty, String> updatedTableProps = new HashMap<>();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    Option.ofNullable(tableConfig.getString(TABLE_METADATA_PARTITIONS))
        .ifPresent(v -> updatedTableProps.put(TABLE_METADATA_PARTITIONS, v));
    Option.ofNullable(tableConfig.getString(TABLE_METADATA_PARTITIONS_INFLIGHT))
        .ifPresent(v -> updatedTableProps.put(TABLE_METADATA_PARTITIONS_INFLIGHT, v));
    return new UpgradeDowngrade.TableConfigChangeSet(updatedTableProps, Collections.emptySet());
  }

}
