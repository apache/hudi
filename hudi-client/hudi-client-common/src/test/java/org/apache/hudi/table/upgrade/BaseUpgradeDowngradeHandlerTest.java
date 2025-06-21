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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
abstract class BaseUpgradeDowngradeHandlerTest {
  @Mock
  protected HoodieWriteConfig config;
  @Mock
  protected HoodieEngineContext context;
  @Mock
  protected HoodieTable table;
  @Mock
  protected HoodieTableMetaClient metaClient;
  @Mock
  protected HoodieTableConfig tableConfig;
  @Mock
  protected SupportsUpgradeDowngrade upgradeDowngradeHelper;

  protected void setupMocks() {
    when(upgradeDowngradeHelper.getTable(config, context)).thenReturn(table);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
  }

  protected Set<String> createMetadataPartitions(boolean includeSecondaryIndex) {
    Set<String> partitions = new LinkedHashSet<>(Arrays.asList(
        "files",
        "column_stats"
    ));
    
    if (includeSecondaryIndex) {
      partitions.addAll(Arrays.asList(
          PARTITION_NAME_SECONDARY_INDEX_PREFIX + "idx1",
          PARTITION_NAME_SECONDARY_INDEX_PREFIX + "idx2"
      ));
    }
    
    return partitions;
  }
}
