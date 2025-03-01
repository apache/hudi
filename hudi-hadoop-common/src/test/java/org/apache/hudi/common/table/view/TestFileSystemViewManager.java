/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.apache.hudi.common.table.view.FileSystemViewStorageType.REMOTE_FIRST;
import static org.apache.hudi.common.table.view.FileSystemViewStorageType.REMOTE_ONLY;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestFileSystemViewManager extends HoodieCommonTestHarness {

  @BeforeEach
  public void setup() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath().toString();
    refreshFsView();
  }

  @AfterEach
  public void tearDown() {
    cleanMetaClient();
  }

  @Test
  void testSecondaryViewSupplier() throws Exception {
    HoodieCommonConfig config = HoodieCommonConfig.newBuilder().build();
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getStorageConf());
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieTableMetadata metadata = new HoodieBackedTableMetadata(engineContext, metaClient.getStorage(), metadataConfig, basePath, true)) {
      FileSystemViewManager.SecondaryViewCreator secondaryViewCreator = new FileSystemViewManager.SecondaryViewCreator(getViewConfig(FileSystemViewStorageType.MEMORY),
          metaClient, TimelineUtils.getVisibleTimelineForFsView(metaClient), config, true, unused -> metadata);
      Assertions.assertTrue(secondaryViewCreator.apply(engineContext) instanceof HoodieTableFileSystemView);
      FileSystemViewManager.SecondaryViewCreator spillableSupplier = new FileSystemViewManager.SecondaryViewCreator(getViewConfig(FileSystemViewStorageType.SPILLABLE_DISK),
          metaClient, TimelineUtils.getVisibleTimelineForFsView(metaClient), config, true, unused -> metadata);
      Assertions.assertTrue(spillableSupplier.apply(engineContext) instanceof SpillableMapBasedFileSystemView);
      FileSystemViewManager.SecondaryViewCreator embeddedSupplier = new FileSystemViewManager.SecondaryViewCreator(getViewConfig(FileSystemViewStorageType.EMBEDDED_KV_STORE),
          metaClient, TimelineUtils.getVisibleTimelineForFsView(metaClient), config, true, unused -> metadata);
      Assertions.assertTrue(embeddedSupplier.apply(engineContext) instanceof RocksDbBasedFileSystemView);
      assertThrows(IllegalArgumentException.class, () -> new FileSystemViewManager.SecondaryViewCreator(getViewConfig(REMOTE_FIRST),
          metaClient, TimelineUtils.getVisibleTimelineForFsView(metaClient), config, true, unused -> metadata).apply(engineContext));
    }
  }

  @ParameterizedTest
  @EnumSource(FileSystemViewStorageType.class)
  void testCreateViewManager(FileSystemViewStorageType storageType) throws Exception {
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getStorageConf());
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    FileSystemViewStorageConfig fileSystemViewStorageConfig = FileSystemViewStorageConfig.newBuilder()
        .withStorageType(storageType)
        .withRemoteInitTimeline(true)
        .build();
    FileSystemViewManager viewManager = FileSystemViewManager.createViewManager(engineContext, metadataConfig, fileSystemViewStorageConfig, commonConfig);
    FileSystemViewManager viewManagerWithMetadata = FileSystemViewManager.createViewManagerWithTableMetadata(engineContext, metadataConfig, fileSystemViewStorageConfig, commonConfig);
    if (storageType == REMOTE_ONLY || storageType == REMOTE_FIRST) {
      assertThrows(HoodieRemoteException.class, () -> viewManager.getFileSystemView(metaClient));
      assertThrows(HoodieRemoteException.class, () -> viewManagerWithMetadata.getFileSystemView(metaClient));
      assertThrows(HoodieRemoteException.class, () -> viewManager.getFileSystemView(metaClient, metaClient.getActiveTimeline()));
      assertThrows(HoodieRemoteException.class, () -> viewManagerWithMetadata.getFileSystemView(metaClient, metaClient.getActiveTimeline()));
    } else {
      viewManager.getFileSystemView(metaClient);
      viewManagerWithMetadata.getFileSystemView(metaClient);
      viewManager.getFileSystemView(metaClient, metaClient.getActiveTimeline());
      viewManagerWithMetadata.getFileSystemView(metaClient, metaClient.getActiveTimeline());
    }
    viewManager.close();
    viewManagerWithMetadata.close();
  }

  private FileSystemViewStorageConfig getViewConfig(FileSystemViewStorageType type) {
    return FileSystemViewStorageConfig.newBuilder()
        .withSecondaryStorageType(type)
        .build();
  }
}