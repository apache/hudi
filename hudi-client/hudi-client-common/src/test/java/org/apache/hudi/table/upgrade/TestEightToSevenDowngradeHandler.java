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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.metadata.MetadataPartitionType.FUNCTIONAL_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

class TestEightToSevenDowngradeHandler {
  @Test
  void testDeleteMetadataPartition() {
    HoodieTableMetaClient mdtMetaClient = mock(HoodieTableMetaClient.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);

    List<String> metadataPartitionPaths = Arrays.asList(
        FUNCTIONAL_INDEX.getPartitionPath(),
        SECONDARY_INDEX.getPartitionPath(),
        PARTITION_STATS.getPartitionPath(),
        "random_path1",
        "random_path2");
    try (MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtils = mockStatic(HoodieTableMetadataUtil.class)) {
      List<String> leftPartitionPaths = EightToSevenDowngradeHandler.deleteMetadataPartition(context, mdtMetaClient, metadataPartitionPaths);

      mockedMetadataUtils.verify(
          () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(mdtMetaClient, context, FUNCTIONAL_INDEX.getPartitionPath(), true),
          times(1));
      mockedMetadataUtils.verify(
          () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(mdtMetaClient, context, SECONDARY_INDEX.getPartitionPath(), true),
          times(1));
      mockedMetadataUtils.verify(
          () -> HoodieTableMetadataUtil.deleteMetadataTablePartition(mdtMetaClient, context, PARTITION_STATS.getPartitionPath(), true),
          times(1));

      assertArrayEquals(new String[]{"random_path1", "random_path2"}, leftPartitionPaths.toArray());
    }
  }

  @Test
  void testDowngradeMetadataPartitions() {
    HoodieStorage hoodieStorage = new HoodieHadoopStorage("any_path", new Configuration(false));
    StoragePath basePath = new StoragePath("file:///base_path/.hoodie/metadata/.hoodie");
    HoodieTableMetaClient mdtMetaClient = mock(HoodieTableMetaClient.class);
    when(mdtMetaClient.getBasePath()).thenReturn(basePath);
    HoodieEngineContext context = mock(HoodieEngineContext.class);

    List<String> metadataPartitionPaths = Arrays.asList(
        FUNCTIONAL_INDEX.getPartitionPath(),
        SECONDARY_INDEX.getPartitionPath(),
        PARTITION_STATS.getPartitionPath(),
        "random_path1",
        "random_path2");
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    try (MockedStatic<FSUtils> mockedFSUtils = mockStatic(FSUtils.class);
         MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtils = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedFSUtils
          .when(() -> FSUtils.getAllPartitionPaths(context, hoodieStorage, basePath, true))
          .thenReturn(metadataPartitionPaths);

      EightToSevenDowngradeHandler.downgradeMetadataPartitions(context, hoodieStorage, mdtMetaClient, tablePropsToAdd);

      assertTrue(tablePropsToAdd.containsKey(TABLE_METADATA_PARTITIONS));
      assertEquals("random_path1,random_path2", tablePropsToAdd.get(TABLE_METADATA_PARTITIONS));
    }
  }
}
