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
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.RecordMergeMode.COMMIT_TIME_ORDERING;
import static org.apache.hudi.common.config.RecordMergeMode.EVENT_TIME_ORDERING;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.DEBEZIUM_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.MERGE_PROPERTIES;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.PartialUpdateMode.IGNORE_MARKERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestEightToNineUpgradeHandler {
  private final EightToNineUpgradeHandler handler = new EightToNineUpgradeHandler();
  private final HoodieEngineContext context = mock(HoodieEngineContext.class);
  private final HoodieTable table = mock(HoodieTable.class);
  private final HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
  private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
  private final SupportsUpgradeDowngrade upgradeDowngradeHelper =
      mock(SupportsUpgradeDowngrade.class);
  private final HoodieWriteConfig config = mock(HoodieWriteConfig.class);

  @BeforeEach
  public void setUp() {
    when(upgradeDowngradeHelper.getTable(any(), any())).thenReturn(table);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(config.autoUpgrade()).thenReturn(true);
  }

  @Test
  void testDefaultHoodieRecordPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
              any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(DefaultHoodieRecordPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToHandle =
          handler.upgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      Map<ConfigProperty, String> propertiesToAdd = propertiesToHandle.getLeft();
      List<ConfigProperty> propertiesToRemove = propertiesToHandle.getRight();
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertTrue(StringUtils.isNullOrEmpty(
          propertiesToAdd.get(MERGE_PROPERTIES)));
      assertFalse(propertiesToAdd.containsKey(RECORD_MERGE_MODE));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          PartialUpdateMode.NONE.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
      assertPayloadClassChange(
          propertiesToAdd, propertiesToRemove, DefaultHoodieRecordPayload.class.getName());
    }
  }

  @Test
  void testOverwriteWithLatestAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
              any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(
          OverwriteWithLatestAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToHandle =
          handler.upgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      Map<ConfigProperty, String> propertiesToAdd = propertiesToHandle.getLeft();
      List<ConfigProperty> propertiesToRemove = propertiesToHandle.getRight();
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertTrue(StringUtils.isNullOrEmpty(
          propertiesToAdd.get(MERGE_PROPERTIES)));
      assertFalse(propertiesToAdd.containsKey(RECORD_MERGE_MODE));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          PartialUpdateMode.NONE.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
      assertPayloadClassChange(
          propertiesToAdd, propertiesToRemove, OverwriteWithLatestAvroPayload.class.getName());
    }
  }

  @Test
  void testUpgradeWithAWSDmsAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        any(), any(), any(), any(), anyBoolean(), any()))
               .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(AWSDmsAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToHandle =
          handler.upgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      Map<ConfigProperty, String> propertiesToAdd = propertiesToHandle.getLeft();
      List<ConfigProperty> propertiesToRemove = propertiesToHandle.getRight();
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertEquals(
          DELETE_KEY + "=Op," + DELETE_MARKER + "=D",
          propertiesToAdd.get(MERGE_PROPERTIES));
      assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_MODE));
      assertEquals(
          COMMIT_TIME_ORDERING.name(),
          propertiesToAdd.get(RECORD_MERGE_MODE));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          PartialUpdateMode.NONE.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
      assertPayloadClassChange(
          propertiesToAdd, propertiesToRemove, AWSDmsAvroPayload.class.getName());
    }
  }

  @Test
  void testUpgradeWithPostgresDebeziumAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        any(), any(), any(), any(), anyBoolean(), any()))
               .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(PostgresDebeziumAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToHandle =
          handler.upgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      Map<ConfigProperty, String> propertiesToAdd = propertiesToHandle.getLeft();
      List<ConfigProperty> propertiesToRemove = propertiesToHandle.getRight();
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertEquals(
          PARTIAL_UPDATE_CUSTOM_MARKER + "=" + DEBEZIUM_UNAVAILABLE_VALUE,
          propertiesToAdd.get(MERGE_PROPERTIES));
      assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_MODE));
      assertEquals(
          EVENT_TIME_ORDERING.name(),
          propertiesToAdd.get(RECORD_MERGE_MODE));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          IGNORE_MARKERS.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
      assertPayloadClassChange(
          propertiesToAdd,
          propertiesToRemove,
          PostgresDebeziumAvroPayload.class.getName());
    }
  }

  @Test
  void testUpgradeWithPartialUpdateAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        any(), any(), any(), any(), anyBoolean(), any()))
               .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(PartialUpdateAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToHandle =
          handler.upgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      Map<ConfigProperty, String> propertiesToAdd = propertiesToHandle.getLeft();
      List<ConfigProperty> propertiesToRemove = propertiesToHandle.getRight();
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertTrue(StringUtils.isNullOrEmpty(propertiesToAdd.get(MERGE_PROPERTIES)));
      assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_MODE));
      assertEquals(
          EVENT_TIME_ORDERING.name(),
          propertiesToAdd.get(RECORD_MERGE_MODE));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          PartialUpdateMode.IGNORE_DEFAULTS.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
      assertPayloadClassChange(
          propertiesToAdd,
          propertiesToRemove,
          PartialUpdateAvroPayload.class.getName());
    }
  }

  @Test
  void testUpgradeWithOverwriteNonDefaultsWithLatestAvroPayload() {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
        any(), any(), any(), any(), anyBoolean(), any()))
               .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(
          OverwriteNonDefaultsWithLatestAvroPayload.class.getName());
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToHandle =
          handler.upgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      Map<ConfigProperty, String> propertiesToAdd = propertiesToHandle.getLeft();
      List<ConfigProperty> propertiesToRemove = propertiesToHandle.getRight();
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      assertTrue(StringUtils.isNullOrEmpty(propertiesToAdd.get(MERGE_PROPERTIES)));
      assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_MODE));
      assertEquals(
          COMMIT_TIME_ORDERING.name(),
          propertiesToAdd.get(RECORD_MERGE_MODE));
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(
          PartialUpdateMode.IGNORE_DEFAULTS.name(),
          propertiesToAdd.get(PARTIAL_UPDATE_MODE));
      assertPayloadClassChange(
          propertiesToAdd,
          propertiesToRemove,
          OverwriteNonDefaultsWithLatestAvroPayload.class.getName());
    }
  }

  @Test
  void testUpgradeWhenAutoUpgradeIsFalse() {
    when(config.autoUpgrade()).thenReturn(false);
    // Payload class can be any, e.g., AWSDmsAvroPayload
    when(tableConfig.getPayloadClass()).thenReturn(AWSDmsAvroPayload.class.getName());
    Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToHandle =
        handler.upgrade(config, context, "anyInstant", upgradeDowngradeHelper);
    Map<ConfigProperty, String> propertiesToAdd = propertiesToHandle.getLeft();
    List<ConfigProperty> propertiesToRemove = propertiesToHandle.getRight();
    assertTrue(
        propertiesToAdd.isEmpty(),
        "Expected no properties to be added when autoUpgrade is false");
    assertTrue(
        propertiesToRemove.isEmpty(),
        "Expected no properties to be removed when autoUpgrade is false");
  }

  private void assertPayloadClassChange(Map<ConfigProperty, String> propertiesToAdd,
                                        List<ConfigProperty> propertiesToRemove,
                                        String payloadClass) {
    assertEquals(1, propertiesToRemove.size());
    assertEquals(PAYLOAD_CLASS_NAME, propertiesToRemove.get(0));
    assertTrue(propertiesToAdd.containsKey(LEGACY_PAYLOAD_CLASS_NAME));
    assertEquals(
        payloadClass,
        propertiesToAdd.get(LEGACY_PAYLOAD_CLASS_NAME));
  }
}
