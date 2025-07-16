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
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.DEBEZIUM_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.MERGE_PROPERTIES;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.common.table.PartialUpdateMode.IGNORE_MARKERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestEightToNineUpgradeHandler {
  @Test
  void testUpgradeWithAWSDmsAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        any(), any(), any(), any(), anyBoolean(), any()))
               .thenAnswer(invocation -> null);
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
      when(config.autoUpgrade()).thenReturn(true);
      when(tableConfig.getPayloadClass()).thenReturn(AWSDmsAvroPayload.class.getName());
      Map<ConfigProperty, String> propertiesToAdd = handler.upgrade(
          config, context, "anyInstant", upgradeDowngradeHelper);
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertEquals(
          DELETE_KEY + "=Op," + DELETE_MARKER + "=D",
          propertiesToAdd.get(MERGE_PROPERTIES));
      assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_STRATEGY_ID));
      assertEquals(
          COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
          propertiesToAdd.get(RECORD_MERGE_STRATEGY_ID));
      assertFalse(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
    }
  }

  @Test
  void testUpgradeWithPostgresDebeziumAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        any(), any(), any(), any(), anyBoolean(), any()))
               .thenAnswer(invocation -> null);
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
      when(config.autoUpgrade()).thenReturn(true);
      when(tableConfig.getPayloadClass()).thenReturn(PostgresDebeziumAvroPayload.class.getName());
      Map<ConfigProperty, String> propertiesToAdd = handler.upgrade(
          config, context, "anyInstant", upgradeDowngradeHelper);
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertEquals(
          PARTIAL_UPDATE_CUSTOM_MARKER + "=" + DEBEZIUM_UNAVAILABLE_VALUE,
          propertiesToAdd.get(MERGE_PROPERTIES));
      assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_STRATEGY_ID));
      assertEquals(
          EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
          propertiesToAdd.get(RECORD_MERGE_STRATEGY_ID));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          IGNORE_MARKERS.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
    }
  }

  @Test
  void testUpgradeWithPartialUpdateAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        any(), any(), any(), any(), anyBoolean(), any()))
               .thenAnswer(invocation -> null);
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
      when(config.autoUpgrade()).thenReturn(true);
      when(tableConfig.getPayloadClass()).thenReturn(PartialUpdateAvroPayload.class.getName());
      Map<ConfigProperty, String> propertiesToAdd = handler.upgrade(
          config, context, "anyInstant", upgradeDowngradeHelper);
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertTrue(StringUtils.isNullOrEmpty(propertiesToAdd.get(MERGE_PROPERTIES)));
      assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_STRATEGY_ID));
      assertEquals(
          EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
          propertiesToAdd.get(RECORD_MERGE_STRATEGY_ID));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          PartialUpdateMode.IGNORE_DEFAULTS.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
    }
  }

  @Test
  void testUpgradeWithOverwriteNonDefaultsWithLatestAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        any(), any(), any(), any(), anyBoolean(), any()))
               .thenAnswer(invocation -> null);
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
      when(config.autoUpgrade()).thenReturn(true);
      when(tableConfig.getPayloadClass()).thenReturn(OverwriteNonDefaultsWithLatestAvroPayload.class.getName());
      Map<ConfigProperty, String> propertiesToAdd = handler.upgrade(
          config, context, "anyInstant", upgradeDowngradeHelper);
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertTrue(StringUtils.isNullOrEmpty(propertiesToAdd.get(MERGE_PROPERTIES)));
      assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_STRATEGY_ID));
      assertEquals(
          COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
          propertiesToAdd.get(RECORD_MERGE_STRATEGY_ID));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          PartialUpdateMode.IGNORE_DEFAULTS.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
    }
  }

  @Test
  void testUpgradeWhenAutoUpgradeIsFalse() {
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
    when(config.autoUpgrade()).thenReturn(false);
    // Payload class can be any, e.g., AWSDmsAvroPayload
    when(tableConfig.getPayloadClass()).thenReturn(AWSDmsAvroPayload.class.getName());
    Map<ConfigProperty, String> propertiesToAdd = handler.upgrade(
        config, context, "anyInstant", upgradeDowngradeHelper);
    assertTrue(
        propertiesToAdd.isEmpty(),
        "Expected no properties to be added when autoUpgrade is false");
  }
}
