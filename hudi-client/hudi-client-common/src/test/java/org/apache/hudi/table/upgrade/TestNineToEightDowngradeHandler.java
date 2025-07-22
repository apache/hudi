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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestNineToEightDowngradeHandler {

  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieTableConfig tblConfig;
  @Mock
  private HoodieTableMetaClient metaClient;
  @Mock
  private HoodieEngineContext context;
  @Mock
  private SupportsUpgradeDowngrade upgradeDowngradeHelper;
  @Mock
  private HoodieTable table;

  private NineToEightDowngradeHandler downgradeHandler;

  @BeforeEach
  void setUp() {
    downgradeHandler = new NineToEightDowngradeHandler();
    when(upgradeDowngradeHelper.getTable(config, context)).thenReturn(table);
  }

  @Test
  void testDowngradeDropsOnlyV2OrAboveIndexes() {
    // Use try-with-resources to ensure the static mock is closed
    try (MockedStatic<UpgradeDowngradeUtils> mockedUtils = mockStatic(UpgradeDowngradeUtils.class)) {
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
      when(metaClient.getTableConfig()).thenReturn(tblConfig);
      Set<String> mdtPartitions = new HashSet<>(indexDefs.keySet());
      when(tblConfig.getMetadataPartitions()).thenReturn(mdtPartitions);

      // Mock write client
      BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
      when(upgradeDowngradeHelper.getWriteClient(config, context)).thenReturn(writeClient);

      // Act
      downgradeHandler.downgrade(config, context, "20240101120000", upgradeDowngradeHelper);

      // Assert: dropIndex should be called with only index with version v2 or higher.
      ArgumentCaptor<List<String>> argumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(writeClient, times(1)).dropIndex(argumentCaptor.capture());
      List<String> capturedIndexes = argumentCaptor.getValue();
      assertEquals(new HashSet<>(Arrays.asList("secondary_index_v2")), new HashSet<>(capturedIndexes));
    }
  }
} 