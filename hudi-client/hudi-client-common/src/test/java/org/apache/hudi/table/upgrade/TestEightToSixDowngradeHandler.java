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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.INITIAL_VERSION;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestEightToSixDowngradeHandler {
  @TempDir
  private File baseDir;

  private static final List<String> SAMPLE_METADATA_PATHS = Arrays.asList(
      "func_index_random",
      "secondary_index_random",
      "partition_stats",
      FILES.getPartitionPath(),
      COLUMN_STATS.getPartitionPath());
  @Mock
  HoodieTableMetaClient metaClient;
  @Mock
  HoodieEngineContext context;
  @Mock
  HoodieWriteConfig config;
  @Mock
  SupportsUpgradeDowngrade upgradeDowngradeHelper;

  private EightToSixDowngradeHandler downgradeHandler;

  @BeforeEach
  void setUp() {
    downgradeHandler = new EightToSixDowngradeHandler();
  }

  @Test
  void testDeleteMetadataPartition() {
    try (MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtils = mockStatic(HoodieTableMetadataUtil.class)) {
      List<String> leftPartitionPaths =
          EightToSixDowngradeHandler.deleteMetadataPartition(context, metaClient, SAMPLE_METADATA_PATHS);

      mockedMetadataUtils.verify(
          () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(
              metaClient, context, "func_index_random", true),
          times(1));
      mockedMetadataUtils.verify(
          () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(
              metaClient, context, "secondary_index_random", true),
          times(1));
      mockedMetadataUtils.verify(
          () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(
              metaClient, context, "partition_stats", true),
          times(1));

      assertArrayEquals(new String[] {"files", "column_stats"}, leftPartitionPaths.toArray());
    }
  }

  @Test
  void testDowngradeMetadataPartitions() {
    String baseTablePath = baseDir.toString();
    HoodieStorage hoodieStorage = HoodieStorageUtils.getStorage(getDefaultStorageConf());
    StoragePath basePath = new StoragePath(baseTablePath);
    when(metaClient.getBasePath()).thenReturn(basePath);

    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    try (MockedStatic<FSUtils> mockedFSUtils = mockStatic(FSUtils.class);
         MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtils = mockStatic(HoodieTableMetadataUtil.class)) {
      StoragePath mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
      mockedFSUtils
          .when(() -> FSUtils.getAllPartitionPaths(context, hoodieStorage, mdtBasePath, false))
          .thenReturn(SAMPLE_METADATA_PATHS);

      EightToSixDowngradeHandler.downgradeMetadataPartitions(context, hoodieStorage, metaClient, tablePropsToAdd);

      assertTrue(tablePropsToAdd.containsKey(TABLE_METADATA_PARTITIONS));
      assertEquals("files,column_stats", tablePropsToAdd.get(TABLE_METADATA_PARTITIONS));
    }
  }

  @Test
  void testTimelineDowngrade() {
    List<HoodieInstant> instants = Arrays.asList(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, CLUSTERING_ACTION, "20211012123000"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, CLUSTERING_ACTION, "20211012123001")
    );
    when(metaClient.getActiveTimeline().getInstants()).thenReturn(instants);
    when(config.getBasePath()).thenReturn(baseDir.getAbsolutePath());

    try (MockedStatic<UpgradeDowngradeUtils> upgradeDowngradeUtilsMock = mockStatic(UpgradeDowngradeUtils.class)) {
      downgradeHandler.downgrade(config, context, "20211012123000", upgradeDowngradeHelper);

      upgradeDowngradeUtilsMock.verify(() -> UpgradeDowngradeUtils.runCompaction(any(), any(), any(), any()), times(1));
      upgradeDowngradeUtilsMock.verify(() -> UpgradeDowngradeUtils.syncCompactionRequestedFileToAuxiliaryFolder(any()), times(1));
      upgradeDowngradeUtilsMock.verify(() -> UpgradeDowngradeUtils.downgradeFromLSMTimeline(any(), any(), any()), times(1));
      upgradeDowngradeUtilsMock.verify(() -> UpgradeDowngradeUtils.downgradeActiveTimelineInstant(any(), any(), any(), any(), any(), any()), times(2));
    }
  }

  @Test
  void testPropertyDowngrade() {
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();

    when(config.getBasePath()).thenReturn(baseDir.getAbsolutePath());
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(config.getString(anyString())).thenReturn("partition_field");
    when(tableConfig.getPartitionFieldProp()).thenReturn("partition_field");
    when(tableConfig.getKeyGeneratorClassName()).thenReturn("CustomKeyGenerator");

    downgradeHandler.downgradePartitionFields(config, context, upgradeDowngradeHelper, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(PARTITION_FIELDS));
    assertEquals("partition_field", tablePropsToAdd.get(PARTITION_FIELDS));

    downgradeHandler.unsetInitialVersion(config, tableConfig, tablePropsToAdd);
    assertFalse(tableConfig.getProps().containsKey(INITIAL_VERSION.key()));

    downgradeHandler.unsetRecordMergeMode(config, tableConfig, tablePropsToAdd);
    assertFalse(tableConfig.getProps().containsKey(RECORD_MERGE_MODE.key()));

    downgradeHandler.downgradeBootstrapIndexType(config, tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_CLASS_NAME));

    downgradeHandler.downgradeKeyGeneratorType(config, tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(KEY_GENERATOR_CLASS_NAME));
  }
}
