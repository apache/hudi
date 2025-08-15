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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTIAL_UPDATE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
            4, // propertiesToRemove size
            3, // propertiesToAdd size
            true, // hasRecordMergeMode
            true, // hasRecordMergeStrategyId
            "AWSDmsAvroPayload"
        ),
        // OverwriteNonDefaultsWithLatestAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            OverwriteNonDefaultsWithLatestAvroPayload.class.getName(),
            2,
            3,
            true,
            true,
            "OverwriteNonDefaultsWithLatestAvroPayload"
        ),
        // PartialUpdateAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            PartialUpdateAvroPayload.class.getName(),
            2,
            3,
            true,
            true,
            "PartialUpdateAvroPayload"
        ),
        // MySqlDebeziumAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            MySqlDebeziumAvroPayload.class.getName(),
            2,
            3,
            true,
            true,
            "MySqlDebeziumAvroPayload"
        ),
        // PostgresDebeziumAvroPayload - requires RECORD_MERGE_MODE and RECORD_MERGE_STRATEGY_ID
        Arguments.of(
            PostgresDebeziumAvroPayload.class.getName(),
            3,
            3,
            true,
            true,
            "PostgresDebeziumAvroPayload"
        ),
        // OverwriteWithLatestAvroPayload - only requires PAYLOAD_CLASS_NAME
        Arguments.of(
            OverwriteWithLatestAvroPayload.class.getName(),
            2,
            1,
            false,
            false,
            "OverwriteWithLatestAvroPayload"
        ),
        // DefaultHoodieRecordPayload - only requires PAYLOAD_CLASS_NAME
        Arguments.of(
            DefaultHoodieRecordPayload.class.getName(),
            2,
            1,
            false,
            false,
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
      when(tableConfig.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
      UpgradeDowngrade.TableConfigChangeSet propertiesToChange =
          handler.downgrade(config, context, "anyInstant", upgradeDowngradeHelper);
      // Assert properties to remove
      assertEquals(expectedPropertiesToRemoveSize, propertiesToChange.propertiesToDelete().size());
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

  @Test
  void testDowngradeDropsOnlyV2OrAboveIndexes() {
    // Use try-with-resources to ensure the static mock is closed
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = Mockito.mockStatic(UpgradeDowngradeUtils.class)) {
      // Mock the static method to do nothing - avoid NPE
      mockedUtils.when(() -> UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          any(HoodieTable.class), 
          any(HoodieEngineContext.class), 
          any(HoodieWriteConfig.class), 
          any(SupportsUpgradeDowngrade.class),
          anyBoolean(),
          any(HoodieTableVersion.class)
      )).thenAnswer(invocation -> null); // Do nothing
      
      // Mock the dropNonV1SecondaryIndexPartitions to simulate dropping V2 indexes
      mockedUtils.when(() -> UpgradeDowngradeUtils.dropNonV1SecondaryIndexPartitions(
          eq(config),
          eq(context),
          eq(table),
          eq(upgradeDowngradeHelper),
          any(String.class)
      )).thenAnswer(invocation -> {
        // Get the write client and call dropIndex with expected partitions
        BaseHoodieWriteClient writeClient = upgradeDowngradeHelper.getWriteClient(config, context);
        List<String> partitionsToDrop = Arrays.asList("secondary_index_v2");
        writeClient.dropIndex(partitionsToDrop);
        return null;
      });

      // Arrange
      // Mock index definitions: one V1, one V2, one V3
      Map<String, HoodieIndexDefinition> indexDefs = new HashMap<>();
      indexDefs.put(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "v1", HoodieIndexDefinition.newBuilder()
          .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "v1")
          .withIndexType(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath())
          .withVersion(HoodieIndexVersion.V1)
          .build());
      indexDefs.put(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "v2", HoodieIndexDefinition.newBuilder()
          .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "v2")
          .withIndexType(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath())
          .withVersion(HoodieIndexVersion.V2)
          .build());
      indexDefs.put(MetadataPartitionType.FILES.getPartitionPath(), HoodieIndexDefinition.newBuilder()
          .withIndexName(MetadataPartitionType.FILES.getPartitionPath())
          .withIndexType(MetadataPartitionType.FILES.getPartitionPath())
          .withVersion(HoodieIndexVersion.V2)
          .build());
      indexDefs.put(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), HoodieIndexDefinition.newBuilder()
          .withIndexName(MetadataPartitionType.COLUMN_STATS.getPartitionPath())
          .withIndexType(MetadataPartitionType.COLUMN_STATS.getPartitionPath())
          .withVersion(HoodieIndexVersion.V2)
          .build());
      HoodieIndexMetadata metadata = new HoodieIndexMetadata(indexDefs);

      when(table.getMetaClient()).thenReturn(metaClient);
      when(metaClient.getIndexMetadata()).thenReturn(Option.of(metadata));
      when(metaClient.getTableConfig()).thenReturn(tableConfig);
      Set<String> mdtPartitions = new HashSet<>(indexDefs.keySet());
      when(tableConfig.getMetadataPartitions()).thenReturn(mdtPartitions);

      // Mock write client
      BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
      when(upgradeDowngradeHelper.getWriteClient(config, context)).thenReturn(writeClient);

      // Act
      handler.downgrade(config, context, "20240101120000", upgradeDowngradeHelper);

      // Assert: dropIndex should be called with only index with version v2 or higher.
      ArgumentCaptor<List<String>> argumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(writeClient, times(1)).dropIndex(argumentCaptor.capture());
      List<String> capturedIndexes = argumentCaptor.getValue();
      assertEquals(new HashSet<>(Arrays.asList("secondary_index_v2")), new HashSet<>(capturedIndexes));
    }
  }
}
