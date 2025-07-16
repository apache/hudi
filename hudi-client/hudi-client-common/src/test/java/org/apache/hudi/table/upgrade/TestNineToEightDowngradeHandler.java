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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.MERGE_PROPERTIES;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.MockedStatic;

class TestNineToEightDowngradeHandler {
  private final NineToEightDowngradeHandler handler = new NineToEightDowngradeHandler();
  private final HoodieWriteConfig config = mock(HoodieWriteConfig.class);
  private final HoodieEngineContext context = mock(HoodieEngineContext.class);
  private final HoodieTable table = mock(HoodieTable.class);
  private HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
  private HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
  private SupportsUpgradeDowngrade upgradeDowngradeHelper = mock(SupportsUpgradeDowngrade.class);

  @BeforeEach
  public void setUp() {
    when(upgradeDowngradeHelper.getTable(any(), any())).thenReturn(table);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
  }

  @Test
  void testDowngradeForAWSDmsAvroPayload() {
    try (MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(AWSDmsAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      assertEquals(2, propertiesToChange.getRight().size());
      assertEquals(MERGE_PROPERTIES, propertiesToChange.getRight().get(0));
      assertEquals(PARTIAL_UPDATE_MODE, propertiesToChange.getRight().get(1));
      assertEquals(1, propertiesToChange.getLeft().size());
      assertEquals(
          PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          propertiesToChange.getLeft().get(RECORD_MERGE_STRATEGY_ID));
    }
  }

  @Test
  void testDowngradeForOverwriteNonDefaultsWithLatestAvroPayload() {
    try (MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(OverwriteNonDefaultsWithLatestAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      assertEquals(2, propertiesToChange.getRight().size());
      assertEquals(MERGE_PROPERTIES, propertiesToChange.getRight().get(0));
      assertEquals(PARTIAL_UPDATE_MODE, propertiesToChange.getRight().get(1));
      assertEquals(1, propertiesToChange.getLeft().size());
      assertEquals(
          PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          propertiesToChange.getLeft().get(RECORD_MERGE_STRATEGY_ID));
    }
  }

  @Test
  void testDowngradeForPartialUpdateAvroPayload() {
    try (MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(PartialUpdateAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      assertEquals(2, propertiesToChange.getRight().size());
      assertEquals(MERGE_PROPERTIES, propertiesToChange.getRight().get(0));
      assertEquals(PARTIAL_UPDATE_MODE, propertiesToChange.getRight().get(1));
      assertEquals(1, propertiesToChange.getLeft().size());
      assertEquals(
          PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          propertiesToChange.getLeft().get(RECORD_MERGE_STRATEGY_ID));
    }
  }

  @Test
  void testDowngradeForPostgresDebeziumAvroPayload() {
    try (MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(PostgresDebeziumAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      assertEquals(2, propertiesToChange.getRight().size());
      assertEquals(MERGE_PROPERTIES, propertiesToChange.getRight().get(0));
      assertEquals(PARTIAL_UPDATE_MODE, propertiesToChange.getRight().get(1));
      assertEquals(1, propertiesToChange.getLeft().size());
      assertEquals(
          PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          propertiesToChange.getLeft().get(RECORD_MERGE_STRATEGY_ID));
    }
  }

  @Test
  void testDowngradeForOtherPayloadClass() {
    try (MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn("NonExistentPayloadClass");
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      assertEquals(2, propertiesToChange.getRight().size());
      assertEquals(MERGE_PROPERTIES, propertiesToChange.getRight().get(0));
      assertEquals(PARTIAL_UPDATE_MODE, propertiesToChange.getRight().get(1));
      assertEquals(0, propertiesToChange.getLeft().size());
    }
  }
}
