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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.ORDERING_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    when(tableConfig.getOrderingFieldsStr()).thenReturn(Option.empty());
  }

  static Stream<Arguments> payloadClassTestCases() {
    return Stream.of(
        // AWSDmsAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            AWSDmsAvroPayload.class.getName(),
            4, // propertiesToRemove size
            LEGACY_PAYLOAD_CLASS_NAME.key() + "," + PARTIAL_UPDATE_MODE.key() + "," + RECORD_MERGE_PROPERTY_PREFIX + DELETE_KEY + ","
                + RECORD_MERGE_PROPERTY_PREFIX + DELETE_MARKER,
            3, // propertiesToAdd size
            true, // hasRecordMergeMode
            true, // hasRecordMergeStrategyId
            "AWSDmsAvroPayload"
        ),
        // OverwriteNonDefaultsWithLatestAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            OverwriteNonDefaultsWithLatestAvroPayload.class.getName(),
            2,
            LEGACY_PAYLOAD_CLASS_NAME.key() + "," + PARTIAL_UPDATE_MODE.key(),
            3,
            true,
            true,
            "OverwriteNonDefaultsWithLatestAvroPayload"
        ),
        // PartialUpdateAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            PartialUpdateAvroPayload.class.getName(),
            2,
            LEGACY_PAYLOAD_CLASS_NAME.key() + "," + PARTIAL_UPDATE_MODE.key(),
            3,
            true,
            true,
            "PartialUpdateAvroPayload"
        ),
        // MySqlDebeziumAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            MySqlDebeziumAvroPayload.class.getName(),
            5,
            LEGACY_PAYLOAD_CLASS_NAME.key() + "," + ORDERING_FIELDS.key() + "," + PARTIAL_UPDATE_MODE.key() + "," + RECORD_MERGE_PROPERTY_PREFIX + DELETE_KEY + ","
                + RECORD_MERGE_PROPERTY_PREFIX + DELETE_MARKER,
            4,
            true,
            true,
            "MySqlDebeziumAvroPayload"
        ),
        // PostgresDebeziumAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            PostgresDebeziumAvroPayload.class.getName(),
            5,
            LEGACY_PAYLOAD_CLASS_NAME.key() + "," + PARTIAL_UPDATE_MODE.key() + "," + RECORD_MERGE_PROPERTY_PREFIX + PARTIAL_UPDATE_UNAVAILABLE_VALUE + ","
                + RECORD_MERGE_PROPERTY_PREFIX + DELETE_KEY + "," + RECORD_MERGE_PROPERTY_PREFIX + DELETE_MARKER,
            3,
            true,
            true,
            "PostgresDebeziumAvroPayload"
        ),
        // OverwriteWithLatestAvroPayload - only requires PAYLOAD_CLASS_NAME
        Arguments.of(
            OverwriteWithLatestAvroPayload.class.getName(),
            2,
            LEGACY_PAYLOAD_CLASS_NAME.key() + "," + PARTIAL_UPDATE_MODE.key(),
            1,
            false,
            false,
            "OverwriteWithLatestAvroPayload"
        ),
        // DefaultHoodieRecordPayload - only requires PAYLOAD_CLASS_NAME
        Arguments.of(
            DefaultHoodieRecordPayload.class.getName(),
            2,
            LEGACY_PAYLOAD_CLASS_NAME.key() + "," + PARTIAL_UPDATE_MODE.key(),
            1,
            false,
            false,
            "DefaultHoodieRecordPayload"
        ),
        // EventTimeAvroPayload - only requires PAYLOAD_CLASS_NAME
        Arguments.of(
            EventTimeAvroPayload.class.getName(),
            2,
            LEGACY_PAYLOAD_CLASS_NAME.key() + "," + PARTIAL_UPDATE_MODE.key(),
            1,
            false,
            false,
            "EventTimeAvroPayload"
        )
    );
  }

  @ParameterizedTest(name = "testDowngradeFor{6}")
  @MethodSource("payloadClassTestCases")
  void testDowngradeForPayloadClass(String payloadClassName, int expectedPropertiesToRemoveSize, String expectedPropsToRemove,
                                    int expectedPropertiesToAddSize, boolean hasRecordMergeMode,
                                    boolean hasRecordMergeStrategyId, String testName) {
    try (MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getLegacyPayloadClass()).thenReturn(payloadClassName);
      when(tableConfig.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
      UpgradeDowngrade.TableConfigChangeSet propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      // Assert properties to remove
      assertEquals(expectedPropertiesToRemoveSize, propertiesToChange.propertiesToDelete().size());
      if (expectedPropertiesToAddSize > 0) {
        List<String> propsToRemove = Arrays.asList(expectedPropsToRemove.split(","));
        Collections.sort(propsToRemove);
        List<String> actualPropsRemoved = propertiesToChange.propertiesToDelete().stream().map(ConfigProperty::key).sorted().collect(Collectors.toList());
        assertEquals(propsToRemove, actualPropsRemoved);
      }
      assertTrue(propertiesToChange.propertiesToDelete().contains(PARTIAL_UPDATE_MODE));
      assertTrue(propertiesToChange.propertiesToDelete().contains(LEGACY_PAYLOAD_CLASS_NAME));
      // Assert properties to add
      assertEquals(expectedPropertiesToAddSize, propertiesToChange.propertiesToUpdate().size());
      // Assert payload class is always set
      assertEquals(payloadClassName, propertiesToChange.propertiesToUpdate().get(PAYLOAD_CLASS_NAME));
      // Assert record merge mode if required
      if (hasRecordMergeMode) {
        assertEquals(RecordMergeMode.CUSTOM.name(),
            propertiesToChange.propertiesToUpdate().get(RECORD_MERGE_MODE));
      }
      // Assert record merge strategy ID if required
      if (hasRecordMergeStrategyId) {
        assertEquals(PAYLOAD_BASED_MERGE_STRATEGY_UUID,
            propertiesToChange.propertiesToUpdate().get(RECORD_MERGE_STRATEGY_ID));
      }
      if (payloadClassName.equals(MySqlDebeziumAvroPayload.class.getName())) {
        assertTrue(propertiesToChange.propertiesToUpdate().containsKey(HoodieTableConfig.PRECOMBINE_FIELD));
        assertEquals(propertiesToChange.propertiesToUpdate().get(HoodieTableConfig.PRECOMBINE_FIELD), DebeziumConstants.ADDED_SEQ_COL_NAME);
      }
    }
  }

  @Test
  void testDowngradeForOtherPayloadClass() {
    try (MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getLegacyPayloadClass()).thenReturn("NonExistentPayloadClass");
      when(tableConfig.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
      UpgradeDowngrade.TableConfigChangeSet propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      assertEquals(1, propertiesToChange.propertiesToDelete().size());
      assertTrue(propertiesToChange.propertiesToDelete().contains(PARTIAL_UPDATE_MODE));
      assertEquals(0, propertiesToChange.propertiesToUpdate().size());
    }
  }
}
