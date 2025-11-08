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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestWriteMarkersFactory extends HoodieCommonTestHarness {
  private static final String NON_HDFS_BASE_PATH = "/tmp/dir";
  private static final String HDFS_BASE_PATH = "hdfs://localhost/dir";
  private final HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
  private final HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
  private final HoodieStorage storage = mock(HoodieStorage.class);
  private final HoodieWrapperFileSystem fileSystem = mock(HoodieWrapperFileSystem.class);
  private final HoodieEngineContext context = mock(HoodieEngineContext.class);
  private final HoodieTable table = mock(HoodieTable.class);

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  public static Stream<Arguments> configParams() {
    Object[][] data = new Object[][] {
        {NON_HDFS_BASE_PATH, true}, {HDFS_BASE_PATH, false},
        {NON_HDFS_BASE_PATH, true}, {HDFS_BASE_PATH, false},
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("configParams")
  public void testDirectMarkers(String basePath, boolean isTimelineServerEnabled) {
    testWriteMarkersFactory(
        MarkerType.DIRECT, basePath, HoodieTableVersion.current(),
        isTimelineServerEnabled, DirectWriteMarkers.class);
    testWriteMarkersFactory(
        MarkerType.DIRECT, basePath, HoodieTableVersion.SIX,
        isTimelineServerEnabled, DirectWriteMarkersV1.class);
  }

  @Test
  public void testTimelineServerBasedMarkersWithTimelineServerEnabled() {
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, NON_HDFS_BASE_PATH,
        HoodieTableVersion.current(), true, TimelineServerBasedWriteMarkers.class);
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, NON_HDFS_BASE_PATH,
        HoodieTableVersion.SIX, true, TimelineServerBasedWriteMarkersV1.class);
  }

  @Test
  public void testTimelineServerBasedMarkersWithTimelineServerDisabled() {
    // Fallback to direct markers should happen
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, NON_HDFS_BASE_PATH,
        HoodieTableVersion.current(), false, DirectWriteMarkers.class);
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, NON_HDFS_BASE_PATH,
        HoodieTableVersion.SIX, false, DirectWriteMarkersV1.class);
  }

  @Test
  public void testTimelineServerBasedMarkersWithHDFS() {
    // Fallback to direct markers should happen
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, HDFS_BASE_PATH,
        HoodieTableVersion.current(), true, DirectWriteMarkers.class);
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, HDFS_BASE_PATH,
        HoodieTableVersion.SIX, true, DirectWriteMarkersV1.class);
  }

  @Test
  public void testTimelineServerBasedMarkersWithRemoteViewStorageType() {
    // Fallback to direct markers should happen
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, NON_HDFS_BASE_PATH,
        HoodieTableVersion.current(), false, true, TimelineServerBasedWriteMarkers.class);
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, NON_HDFS_BASE_PATH,
        HoodieTableVersion.SIX, false, true, TimelineServerBasedWriteMarkersV1.class);
  }

  private void testWriteMarkersFactory(
      MarkerType markerTypeConfig, String basePath, HoodieTableVersion tableVersion,
      boolean isTimelineServerEnabled, Class<?> expectedWriteMarkersClass) {
    testWriteMarkersFactory(markerTypeConfig, basePath, tableVersion, isTimelineServerEnabled, false, expectedWriteMarkersClass);
  }

  private void testWriteMarkersFactory(
      MarkerType markerTypeConfig, String basePath, HoodieTableVersion tableVersion,
      boolean isTimelineServerEnabled, boolean isRemoteViewStorageType, Class<?> expectedWriteMarkersClass) {
    String instantTime = "001";
    when(table.getConfig()).thenReturn(writeConfig);
    when(writeConfig.isEmbeddedTimelineServerEnabled())
        .thenReturn(isTimelineServerEnabled);
    when(writeConfig.isRemoteViewStorageType()).thenReturn(isRemoteViewStorageType);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getStorage()).thenReturn(storage);
    when(storage.getFileSystem()).thenReturn(fileSystem);
    when(metaClient.getBasePath()).thenReturn(new StoragePath(basePath));
    when(metaClient.getMarkerFolderPath(any())).thenReturn(basePath + ".hoodie/.temp");
    when(table.getContext()).thenReturn(context);
    StorageConfiguration storageConfToReturn = getDefaultStorageConf();
    when(context.getStorageConf()).thenReturn(storageConfToReturn);
    when(writeConfig.getViewStorageConfig())
        .thenReturn(FileSystemViewStorageConfig.newBuilder().build());
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    assertEquals(expectedWriteMarkersClass,
        WriteMarkersFactory.get(markerTypeConfig, table, instantTime).getClass());
  }
}
