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

import org.apache.hudi.client.timeline.LSMTimelineWriter;
import org.apache.hudi.client.timeline.LegacyArchivedMetaEntryReader;
import org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.INITIAL_VERSION;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestSevenToEightUpgradeHandler {
  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieTableConfig tableConfig;

  private SevenToEightUpgradeHandler upgradeHandler;

  @BeforeEach
  void setUp() {
    upgradeHandler = new SevenToEightUpgradeHandler();
  }

  @Test
  void testPropertyUpgrade() {
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();

    // Simulate config values for key generator and partition path
    when(config.getString(anyString())).thenAnswer(i -> {
      Object arg0 = i.getArguments()[0];
      if (arg0.equals(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())) {
        return "partition_field";
      } else if (arg0.equals(HoodieWriteConfig.RECORD_MERGE_MODE.key())) {
        return RecordMergeMode.EVENT_TIME_ORDERING.name();
      } else {
        return null;
      }
    });
    when(tableConfig.getKeyGeneratorClassName()).thenReturn("org.apache.hudi.keygen.CustomKeyGenerator");

    // Upgrade properties
    SevenToEightUpgradeHandler.upgradePartitionFields(config, tableConfig, tablePropsToAdd);
    assertEquals("partition_field", tablePropsToAdd.get(PARTITION_FIELDS));

    SevenToEightUpgradeHandler.setInitialVersion(tableConfig, tablePropsToAdd);
    assertEquals("6", tablePropsToAdd.get(INITIAL_VERSION));

    // Mock record merge mode configuration for merging behavior
    when(tableConfig.contains(isA(ConfigProperty.class))).thenAnswer(i -> i.getArguments()[0].equals(PAYLOAD_CLASS_NAME));
    when(tableConfig.getPayloadClass()).thenReturn(OverwriteWithLatestAvroPayload.class.getName());
    when(tableConfig.getOrderingFieldsStr()).thenReturn(Option.empty());
    SevenToEightUpgradeHandler.upgradeMergeMode(tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(RECORD_MERGE_MODE));
    assertNotNull(tablePropsToAdd.get(RECORD_MERGE_MODE));

    // Simulate bootstrap index type upgrade
    when(tableConfig.getBooleanOrDefault(BOOTSTRAP_INDEX_ENABLE)).thenReturn(true);
    when(tableConfig.contains(BOOTSTRAP_INDEX_CLASS_NAME)).thenReturn(true);
    when(tableConfig.getString(BOOTSTRAP_INDEX_CLASS_NAME)).thenReturn(HFileBootstrapIndex.class.getName());
    SevenToEightUpgradeHandler.upgradeBootstrapIndexType(tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_CLASS_NAME));
    assertTrue(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_TYPE));

    // Simulate key generator type upgrade
    SevenToEightUpgradeHandler.upgradeKeyGeneratorType(tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(KEY_GENERATOR_CLASS_NAME));
    assertTrue(tablePropsToAdd.containsKey(KEY_GENERATOR_TYPE));
  }

  @ParameterizedTest
  @CsvSource({
      // hard coding all merge strategy Ids since older version of hudi has different variable name to represent the merge strategy id.
      "com.example.CustomPayload, , CUSTOM, " + "00000000-0000-0000-0000-000000000000" + ", com.example.CustomPayload",
      "com.example.CustomPayload, preCombineFieldValue, CUSTOM, " + "00000000-0000-0000-0000-000000000000" + ", com.example.CustomPayload",
      "org.apache.hudi.metadata.HoodieMetadataPayload, , CUSTOM, " + "00000000-0000-0000-0000-000000000000" + ", org.apache.hudi.metadata.HoodieMetadataPayload",
      "org.apache.hudi.metadata.HoodieMetadataPayload, preCombineFieldValue, CUSTOM, " + "00000000-0000-0000-0000-000000000000" + ", org.apache.hudi.metadata.HoodieMetadataPayload",
      "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload, , COMMIT_TIME_ORDERING, " + "ce9acb64-bde0-424c-9b91-f6ebba25356d"
          + ", org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      "org.apache.hudi.common.model.DefaultHoodieRecordPayload, , EVENT_TIME_ORDERING, " + "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5"
          + ", org.apache.hudi.common.model.DefaultHoodieRecordPayload",
      ", preCombineFieldValue, EVENT_TIME_ORDERING, " + "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5" + ", org.apache.hudi.common.model.DefaultHoodieRecordPayload",
      "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload, preCombineFieldValue, EVENT_TIME_ORDERING,"
          + "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5" + ", org.apache.hudi.common.model.DefaultHoodieRecordPayload",
      ", preCombineFieldValue, EVENT_TIME_ORDERING, " + "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5" + ", org.apache.hudi.common.model.DefaultHoodieRecordPayload",
      "org.apache.hudi.common.model.DefaultHoodieRecordPayload, preCombineFieldValue, EVENT_TIME_ORDERING,"
          + "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5" + ", org.apache.hudi.common.model.DefaultHoodieRecordPayload",
      ", , COMMIT_TIME_ORDERING, " + "ce9acb64-bde0-424c-9b91-f6ebba25356d" + ", org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
  })
  void testUpgradeMergeMode(String payloadClass, String preCombineField, String expectedMergeMode, String expectedStrategy, String expectedPayloadClass) {
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();

    when(tableConfig.getPayloadClass()).thenReturn(payloadClass);
    when(tableConfig.getOrderingFieldsStr()).thenReturn(Option.ofNullable(preCombineField));

    SevenToEightUpgradeHandler.upgradeMergeMode(tableConfig, tablePropsToAdd);

    assertEquals(expectedMergeMode, tablePropsToAdd.get(HoodieTableConfig.RECORD_MERGE_MODE));
    assertEquals(expectedStrategy, tablePropsToAdd.get(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID));
    if (expectedPayloadClass != null) {
      assertEquals(expectedPayloadClass, tablePropsToAdd.get(HoodieTableConfig.PAYLOAD_CLASS_NAME));
    } else {
      assertTrue(!tablePropsToAdd.containsKey(HoodieTableConfig.PAYLOAD_CLASS_NAME));
    }
  }

  @Test
  void testUpgradeToLSMTimelineSingleBatch() throws Exception {
    // A single batch large enough to hold all actions should result in exactly one write() call,
    // proving the migration batch size config (not the regular archival batch size) drives batching.
    LSMTimelineWriter writer = runMigration(500, 4);
    verify(writer, times(1)).write(any(), any(), any());
    verify(writer, never()).compactAndClean(any());
  }

  @Test
  void testUpgradeToLSMTimelineBatchesByMigrationBatchSize() throws Exception {
    // With more actions than the migration batch size, the in-loop batching branch must fire:
    // 4 actions with a batch size of 2 -> [2, 2] -> 2 write() calls. This pins that the configured
    // migration batch size (not just "all actions in one batch") actually governs batching.
    LSMTimelineWriter writer = runMigration(2, 4);
    verify(writer, times(2)).write(any(), any(), any());
    verify(writer, never()).compactAndClean(any());
  }

  /**
   * Runs {@link SevenToEightUpgradeHandler#upgradeToLSMTimeline} with the given migration batch size
   * over the given number of archived actions, and returns the (mocked) LSM timeline writer for verification.
   */
  private LSMTimelineWriter runMigration(int migrationBatchSize, int totalActions) {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getTimelineLayoutVersion()).thenReturn(Option.of(TimelineLayoutVersion.LAYOUT_VERSION_1));
    when(metaClient.getMetaPath()).thenReturn(new StoragePath("/tmp/.hoodie"));
    when(config.getMigrationCommitArchivalBatchSize()).thenReturn(migrationBatchSize);
    // The regular archival batch size must not be consulted during migration.
    lenient().when(config.getCommitArchivalBatchSize()).thenReturn(1);

    List<ActiveAction> actions = new ArrayList<>();
    for (int i = 0; i < totalActions; i++) {
      actions.add(mock(ActiveAction.class));
    }

    LSMTimelineWriter writer = mock(LSMTimelineWriter.class);
    try (MockedStatic<LSMTimelineWriter> mockedWriterStatic = mockStatic(LSMTimelineWriter.class);
         MockedConstruction<LegacyArchivedMetaEntryReader> mockedReader = Mockito.mockConstruction(
             LegacyArchivedMetaEntryReader.class,
             (readerMock, ctx) -> when(readerMock.getActiveActionsIterator())
                 .thenReturn(ClosableIterator.wrap(actions.iterator())))) {
      mockedWriterStatic.when(() -> LSMTimelineWriter.getInstance(
          any(HoodieWriteConfig.class), any(HoodieTable.class), any(Option.class))).thenReturn(writer);

      SevenToEightUpgradeHandler.upgradeToLSMTimeline(table, config);
    }
    return writer;
  }

  private static Map<ConfigProperty, String> createMap(Object... keyValues) {
    Map<ConfigProperty, String> map = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put((ConfigProperty) keyValues[i], (String) keyValues[i + 1]);
    }
    return map;
  }
}
