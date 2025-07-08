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
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.DEBEZIUM_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_PROPERTIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestEightToNineUpgradeHandler {
  @Test
  void testUpgrade() {
    EightToNineUpgradeHandler handler = new EightToNineUpgradeHandler();

    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    SupportsUpgradeDowngrade upgradeDowngradeHelper = mock(SupportsUpgradeDowngrade.class);
    when(upgradeDowngradeHelper.getTable(any(), any())).thenReturn(table);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);

    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);

    // When `AWSDmsAvroPayload` is the payload class.
    when(tableConfig.getPayloadClass()).thenReturn(AWSDmsAvroPayload.class.getName());
    Map<ConfigProperty, String> propertiesToAdd = handler.upgrade(
        config, context, "anyInstant", upgradeDowngradeHelper);
    assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_PROPERTIES));
    assertEquals(
        DELETE_KEY + "=Op," + DELETE_MARKER + "=D",
        propertiesToAdd.get(PARTIAL_UPDATE_PROPERTIES));

    // When `PostgresDebeziumAvroPayload` is the payload class.
    when(tableConfig.getPayloadClass()).thenReturn(PostgresDebeziumAvroPayload.class.getName());
    propertiesToAdd = handler.upgrade(
        config, context, "anyInstant", upgradeDowngradeHelper);
    assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_PROPERTIES));
    assertEquals(
        PARTIAL_UPDATE_CUSTOM_MARKER + "=" + DEBEZIUM_UNAVAILABLE_VALUE,
        propertiesToAdd.get(PARTIAL_UPDATE_PROPERTIES));
  }
}
