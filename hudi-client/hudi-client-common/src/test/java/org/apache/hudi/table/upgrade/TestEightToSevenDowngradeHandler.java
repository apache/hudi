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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapIndexType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.INITIAL_VERSION;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestEightToSevenDowngradeHandler {
  @TempDir
  private File baseDir;

  private static final List<String> SAMPLE_METADATA_PATHS = Arrays.asList(
      "expr_index_random",
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

  private EightToSevenDowngradeHandler downgradeHandler;

  @BeforeEach
  void setUp() {
    downgradeHandler = new EightToSevenDowngradeHandler();
  }

  @Test
  void testDeleteMetadataPartition() {
    try (MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtils = mockStatic(HoodieTableMetadataUtil.class)) {
      List<String> leftPartitionPaths =
          EightToSevenDowngradeHandler.deleteMetadataPartition(context, metaClient, SAMPLE_METADATA_PATHS);

      mockedMetadataUtils.verify(
          () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(
              metaClient, context, "expr_index_random", true),
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

    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getBasePath()).thenReturn(basePath);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getStorage()).thenReturn(hoodieStorage);

    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    try (MockedStatic<FSUtils> mockedFSUtils = mockStatic(FSUtils.class);
         MockedStatic<HoodieTableMetaClient> mockedStaticMetaClient = mockStatic(HoodieTableMetaClient.class)) {
      StoragePath mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());

      // Mock FSUtils.getAllPartitionPaths to return SAMPLE_METADATA_PATHS
      mockedFSUtils
          .when(() -> FSUtils.getAllPartitionPaths(context, metaClient, false))
          .thenReturn(SAMPLE_METADATA_PATHS);

      // Mock HoodieTableMetaClient.builder() to return a builder that returns a mock metaClient
      HoodieTableMetaClient.Builder mockBuilder = mock(HoodieTableMetaClient.Builder.class);
      when(mockBuilder.setBasePath(mdtBasePath.toUri().toString())).thenReturn(mockBuilder);
      when(mockBuilder.setConf(hoodieStorage.getConf())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(metaClient);
      mockedStaticMetaClient.when(HoodieTableMetaClient::builder).thenReturn(mockBuilder);

      // Mock FSUtils.isTableExists to return true
      mockedFSUtils
          .when(() -> FSUtils.isTableExists(mdtBasePath.toString(), hoodieStorage))
          .thenReturn(true);

      EightToSevenDowngradeHandler.downgradeMetadataPartitions(context, hoodieStorage, metaClient, tablePropsToAdd);

      assertTrue(tablePropsToAdd.containsKey(TABLE_METADATA_PARTITIONS));
      assertEquals("files,column_stats", tablePropsToAdd.get(TABLE_METADATA_PARTITIONS));
    }
  }

  @Test
  void testPropertyDowngrade() {
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    Properties existingTableProps = new Properties();
    existingTableProps.put(INITIAL_VERSION.key(), HoodieTableVersion.SIX.name());
    existingTableProps.put(RECORD_MERGE_MODE.key(), RecordMergeMode.EVENT_TIME_ORDERING.name());
    existingTableProps.put(BOOTSTRAP_INDEX_TYPE.key(), BootstrapIndexType.HFILE.name());
    existingTableProps.put(KEY_GENERATOR_TYPE.key(), KeyGeneratorType.CUSTOM.name());
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    when(tableConfig.getProps()).thenReturn(TypedProperties.copy(existingTableProps));
    when(config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())).thenReturn("partition_field");
    when(tableConfig.getPartitionFieldProp()).thenReturn("partition_field");
    when(tableConfig.getKeyGeneratorClassName()).thenReturn("org.apache.hudi.keygen.CustomKeyGenerator");
    EightToSevenDowngradeHandler.downgradePartitionFields(config, tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(PARTITION_FIELDS));
    assertEquals("partition_field", tablePropsToAdd.get(PARTITION_FIELDS));
    EightToSevenDowngradeHandler.unsetInitialVersion(tableConfig, tablePropsToAdd);
    assertFalse(tableConfig.getProps().containsKey(INITIAL_VERSION.key()));
    List<ConfigProperty> propsToRemove = EightToSevenDowngradeHandler.unsetRecordMergeMode(config, tableConfig, tablePropsToAdd);
    assertTrue(propsToRemove.contains(RECORD_MERGE_MODE));
    assertTrue(tablePropsToAdd.containsKey(PAYLOAD_CLASS_NAME));
    EightToSevenDowngradeHandler.downgradeBootstrapIndexType(tableConfig, tablePropsToAdd);
    assertFalse(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_TYPE));
    assertFalse(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_CLASS_NAME));
    EightToSevenDowngradeHandler.downgradeKeyGeneratorType(tableConfig, tablePropsToAdd);
    assertFalse(tablePropsToAdd.containsKey(KEY_GENERATOR_TYPE));
    assertFalse(tablePropsToAdd.containsKey(KEY_GENERATOR_CLASS_NAME));
  }

  @ParameterizedTest
  @CsvSource({
      "com.example.CustomPayload, CUSTOM, com.example.CustomPayload",
      ", CUSTOM, ",
      "org.apache.hudi.metadata.HoodieMetadataPayload, CUSTOM, org.apache.hudi.metadata.HoodieMetadataPayload",
      "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload, COMMIT_TIME_ORDERING, org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      "org.apache.hudi.common.model.DefaultHoodieRecordPayload, EVENT_TIME_ORDERING, org.apache.hudi.common.model.DefaultHoodieRecordPayload",
      ", EVENT_TIME_ORDERING, org.apache.hudi.common.model.DefaultHoodieRecordPayload",
      ", COMMIT_TIME_ORDERING, org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
  })
  void testUnsetRecordMergeMode(String payloadClass, String recordMergeMode, String expectedPayloadClass) {
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();

    when(tableConfig.getPayloadClass()).thenReturn(payloadClass);
    if (StringUtils.isNullOrEmpty(payloadClass)) {
      when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.valueOf(recordMergeMode));
    }

    if (!StringUtils.isNullOrEmpty(recordMergeMode) && recordMergeMode.equals("CUSTOM") && StringUtils.isNullOrEmpty(payloadClass)) {
      assertThrows(HoodieUpgradeDowngradeException.class, () -> EightToSevenDowngradeHandler.unsetRecordMergeMode(config, tableConfig, tablePropsToAdd));
    } else {
      List<ConfigProperty> propsToRemove = EightToSevenDowngradeHandler.unsetRecordMergeMode(config, tableConfig, tablePropsToAdd);
      assertTrue(propsToRemove.stream().anyMatch(cfg -> cfg.key().equals(RECORD_MERGE_MODE.key())));
      assertTrue(!tablePropsToAdd.containsKey(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID));

      if (!StringUtils.isNullOrEmpty(payloadClass)) {
        assertFalse(tablePropsToAdd.containsKey(HoodieTableConfig.PAYLOAD_CLASS_NAME));
      } else {
        assertEquals(expectedPayloadClass, tablePropsToAdd.get(HoodieTableConfig.PAYLOAD_CLASS_NAME));
      }
    }
  }
}
