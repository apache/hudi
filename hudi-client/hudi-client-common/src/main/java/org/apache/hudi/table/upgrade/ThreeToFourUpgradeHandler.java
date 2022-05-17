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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.MetadataPartitionType;

import java.util.Hashtable;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_CHECKSUM;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.metadataPartitionExists;

/**
 * UpgradeHandler to assist in upgrading {@link org.apache.hudi.table.HoodieTable} from version 3 to 4.
 */
public class ThreeToFourUpgradeHandler implements UpgradeHandler {

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    Map<ConfigProperty, String> tablePropsToAdd = new Hashtable<>();
    tablePropsToAdd.put(TABLE_CHECKSUM, String.valueOf(HoodieTableConfig.generateChecksum(config.getProps())));
    // if metadata is enabled and files partition exist then update TABLE_METADATA_INDEX_COMPLETED
    // schema for the files partition is same between the two versions
    if (config.isMetadataTableEnabled() && metadataPartitionExists(config.getBasePath(), context, MetadataPartitionType.FILES)) {
      tablePropsToAdd.put(TABLE_METADATA_PARTITIONS, MetadataPartitionType.FILES.getPartitionPath());
    }
    return tablePropsToAdd;
  }
}
