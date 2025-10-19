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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestUpgradeDowngradeConcurrencyControl {

  @Test
  void testConcurrencyModeForMDTDowngradeFromVersion9() throws Exception {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);

    when(table.isMetadataTable()).thenReturn(true);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(tableConfig.getTimelineTimezone()).thenReturn(HoodieTimelineTimeZone.UTC);

    TypedProperties props = new TypedProperties();
    props.put(HoodieWriteConfig.BASE_PATH.key(), "/tmp/test-mdt-table");
    props.put("hoodie.table.name", "test_mdt_table");
    props.put(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name());
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getProps()).thenReturn(props);
    when(config.shouldRollbackUsingMarkers()).thenReturn(true);

    AtomicReference<HoodieWriteConfig> capturedConfig = new AtomicReference<>();

    SupportsUpgradeDowngrade upgradeDowngradeHelper = new SupportsUpgradeDowngrade() {
      @Override
      public HoodieTable getTable(HoodieWriteConfig config, HoodieEngineContext context) {
        return table;
      }

      @Override
      public String getPartitionColumns(HoodieWriteConfig config) {
        return "";
      }

      @Override
      public BaseHoodieWriteClient getWriteClient(HoodieWriteConfig config, HoodieEngineContext context) {
        capturedConfig.set(config);
        BaseHoodieWriteClient mockClient = mock(BaseHoodieWriteClient.class);
        when(mockClient.rollbackFailedWrites(any())).thenReturn(true);
        return mockClient;
      }
    };

    UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        table, context, config, upgradeDowngradeHelper, false, HoodieTableVersion.NINE);

    HoodieWriteConfig rollbackConfig = capturedConfig.get();
    assertEquals(WriteConcurrencyMode.SINGLE_WRITER.name(),
        rollbackConfig.getWriteConcurrencyMode().name(),
        "WRITE_CONCURRENCY_MODE should be SINGLE_WRITER for MDT when downgrading from version 9");
  }
}