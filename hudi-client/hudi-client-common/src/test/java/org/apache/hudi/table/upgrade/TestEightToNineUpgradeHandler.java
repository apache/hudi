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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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

  static Stream<Arguments> payloadClassTestCases() {
    return Stream.of(
        // DefaultHoodieRecordPayload
        Arguments.of(
            DefaultHoodieRecordPayload.class.getName(),
            "", // mergeProperties
            null, // recordMergeMode (should not be present)
            PartialUpdateMode.NONE.name(), // partialUpdateMode
            "DefaultHoodieRecordPayload"
        ),
        // OverwriteWithLatestAvroPayload
        Arguments.of(
            OverwriteWithLatestAvroPayload.class.getName(),
            "", // mergeProperties
            null, // recordMergeMode (should not be present)
            PartialUpdateMode.NONE.name(), // partialUpdateMode
            "OverwriteWithLatestAvroPayload"
        ),
        // AWSDmsAvroPayload
        Arguments.of(
            AWSDmsAvroPayload.class.getName(),
            DELETE_KEY + "=Op," + DELETE_MARKER + "=D", // mergeProperties
            COMMIT_TIME_ORDERING.name(), // recordMergeMode
            PartialUpdateMode.NONE.name(), // partialUpdateMode
            "AWSDmsAvroPayload"
        ),
        // PostgresDebeziumAvroPayload
        Arguments.of(
            PostgresDebeziumAvroPayload.class.getName(),
            PARTIAL_UPDATE_CUSTOM_MARKER + "=" + DEBEZIUM_UNAVAILABLE_VALUE, // mergeProperties
            EVENT_TIME_ORDERING.name(), // recordMergeMode
            IGNORE_MARKERS.name(), // partialUpdateMode
            "PostgresDebeziumAvroPayload"
        ),
        // PartialUpdateAvroPayload
        Arguments.of(
            PartialUpdateAvroPayload.class.getName(),
            "", // mergeProperties
            EVENT_TIME_ORDERING.name(), // recordMergeMode
            PartialUpdateMode.IGNORE_DEFAULTS.name(), // partialUpdateMode
            "PartialUpdateAvroPayload"
        ),
        // MySqlDebeziumAvroPayload
        Arguments.of(
            MySqlDebeziumAvroPayload.class.getName(),
            "", // mergeProperties
            EVENT_TIME_ORDERING.name(), // recordMergeMode
            PartialUpdateMode.NONE.name(), // partialUpdateMode
            "MySqlDebeziumAvroPayload"
        ),
        // OverwriteNonDefaultsWithLatestAvroPayload
        Arguments.of(
            OverwriteNonDefaultsWithLatestAvroPayload.class.getName(),
            "", // mergeProperties
            COMMIT_TIME_ORDERING.name(), // recordMergeMode
            PartialUpdateMode.IGNORE_DEFAULTS.name(), // partialUpdateMode
            "OverwriteNonDefaultsWithLatestAvroPayload"
        )
    );
  }

  @ParameterizedTest(name = "testUpgradeWith{4}")
  @MethodSource("payloadClassTestCases")
  void testUpgradeWithPayloadClass(String payloadClassName, String expectedMergeProperties,
                                   String expectedRecordMergeMode, String expectedPartialUpdateMode,
                                   String testName) {
    try (org.mockito.MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
              any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getPayloadClass()).thenReturn(payloadClassName);
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToHandle =
          handler.upgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      Map<ConfigProperty, String> propertiesToAdd = propertiesToHandle.getLeft();
      List<ConfigProperty> propertiesToRemove = propertiesToHandle.getRight();
      // Assert merge properties
      assertTrue(propertiesToAdd.containsKey(MERGE_PROPERTIES));
      if (StringUtils.isNullOrEmpty(expectedMergeProperties)) {
        assertTrue(StringUtils.isNullOrEmpty(propertiesToAdd.get(MERGE_PROPERTIES)));
      } else {
        assertEquals(expectedMergeProperties, propertiesToAdd.get(MERGE_PROPERTIES));
      }
      // Assert record merge mode
      if (expectedRecordMergeMode == null) {
        assertFalse(propertiesToAdd.containsKey(RECORD_MERGE_MODE));
      } else {
        assertTrue(propertiesToAdd.containsKey(RECORD_MERGE_MODE));
        assertEquals(expectedRecordMergeMode, propertiesToAdd.get(RECORD_MERGE_MODE));
      }
      // Assert partial update mode
      assertTrue(propertiesToAdd.containsKey(PARTIAL_UPDATE_MODE));
      assertEquals(expectedPartialUpdateMode, propertiesToAdd.get(PARTIAL_UPDATE_MODE));
      // Assert payload class change
      assertPayloadClassChange(propertiesToAdd, propertiesToRemove, payloadClassName);
    }
  }

  @Test
  void testUpgradeWhenAutoUpgradeIsFalse() {
    when(config.autoUpgrade()).thenReturn(false);
    // Payload class can be any, e.g., AWSDmsAvroPayload
    when(config.getProps()).thenReturn(new TypedProperties());
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
