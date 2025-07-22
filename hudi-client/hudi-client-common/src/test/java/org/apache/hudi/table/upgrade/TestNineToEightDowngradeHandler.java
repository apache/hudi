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
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
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

import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.MERGE_PROPERTIES;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
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

  static Stream<Arguments> payloadClassTestCases() {
    return Stream.of(
        // AWSDmsAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            AWSDmsAvroPayload.class.getName(),
            3, // propertiesToRemove size
            3, // propertiesToAdd size
            true, // hasRecordMergeMode
            true, // hasRecordMergeStrategyId
            "AWSDmsAvroPayload"
        ),
        // OverwriteNonDefaultsWithLatestAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            OverwriteNonDefaultsWithLatestAvroPayload.class.getName(),
            3, // propertiesToRemove size
            3, // propertiesToAdd size
            true, // hasRecordMergeMode
            true, // hasRecordMergeStrategyId
            "OverwriteNonDefaultsWithLatestAvroPayload"
        ),
        // PartialUpdateAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            PartialUpdateAvroPayload.class.getName(),
            3, // propertiesToRemove size
            3, // propertiesToAdd size
            true, // hasRecordMergeMode
            true, // hasRecordMergeStrategyId
            "PartialUpdateAvroPayload"
        ),
        // MySqlDebeziumAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            MySqlDebeziumAvroPayload.class.getName(),
            3, // propertiesToRemove size
            3, // propertiesToAdd size
            true, // hasRecordMergeMode
            true, // hasRecordMergeStrategyId
            "MySqlDebeziumAvroPayload"
        ),
        // PostgresDebeziumAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            PostgresDebeziumAvroPayload.class.getName(),
            3, // propertiesToRemove size
            3, // propertiesToAdd size
            true, // hasRecordMergeMode
            true, // hasRecordMergeStrategyId
            "PostgresDebeziumAvroPayload"
        ),
        // OverwriteWithLatestAvroPayload - only requires PAYLOAD_CLASS_NAME
        Arguments.of(
            OverwriteWithLatestAvroPayload.class.getName(),
            3, // propertiesToRemove size
            1, // propertiesToAdd size
            false, // hasRecordMergeMode
            false, // hasRecordMergeStrategyId
            "OverwriteWithLatestAvroPayload"
        ),
        // DefaultHoodieRecordPayload - only requires PAYLOAD_CLASS_NAME
        Arguments.of(
            DefaultHoodieRecordPayload.class.getName(),
            3, // propertiesToRemove size
            1, // propertiesToAdd size
            false, // hasRecordMergeMode
            false, // hasRecordMergeStrategyId
            "DefaultHoodieRecordPayload"
        )
    );
  }

  @ParameterizedTest(name = "testDowngradeFor{5}")
  @MethodSource("payloadClassTestCases")
  void testDowngradeForPayloadClass(String payloadClassName, int expectedPropertiesToRemoveSize,
                                    int expectedPropertiesToAddSize, boolean hasRecordMergeMode,
                                    boolean hasRecordMergeStrategyId, String testName) {
    try (MockedStatic<UpgradeDowngradeUtils> utilities =
             org.mockito.Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      utilities.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(invocation -> null);
      when(tableConfig.getLegacyPayloadClass()).thenReturn(payloadClassName);
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      // Assert properties to remove
      assertEquals(expectedPropertiesToRemoveSize, propertiesToChange.getRight().size());
      assertEquals(MERGE_PROPERTIES, propertiesToChange.getRight().get(0));
      assertEquals(PARTIAL_UPDATE_MODE, propertiesToChange.getRight().get(1));
      assertEquals(LEGACY_PAYLOAD_CLASS_NAME, propertiesToChange.getRight().get(2));
      // Assert properties to add
      assertEquals(expectedPropertiesToAddSize, propertiesToChange.getLeft().size());
      // Assert payload class is always set
      assertEquals(payloadClassName, propertiesToChange.getLeft().get(PAYLOAD_CLASS_NAME));
      // Assert record merge mode if required
      if (hasRecordMergeMode) {
        assertEquals(RecordMergeMode.CUSTOM.name(),
            propertiesToChange.getLeft().get(RECORD_MERGE_MODE));
      }
      // Assert record merge strategy ID if required
      if (hasRecordMergeStrategyId) {
        assertEquals(PAYLOAD_BASED_MERGE_STRATEGY_UUID,
            propertiesToChange.getLeft().get(RECORD_MERGE_STRATEGY_ID));
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
      Pair<Map<ConfigProperty, String>, List<ConfigProperty>> propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      assertEquals(2, propertiesToChange.getRight().size());
      assertEquals(MERGE_PROPERTIES, propertiesToChange.getRight().get(0));
      assertEquals(PARTIAL_UPDATE_MODE, propertiesToChange.getRight().get(1));
      assertEquals(0, propertiesToChange.getLeft().size());
    }
  }
}
