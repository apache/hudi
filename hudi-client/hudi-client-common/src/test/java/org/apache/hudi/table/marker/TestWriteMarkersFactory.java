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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public class TestWriteMarkersFactory extends HoodieCommonTestHarness {
  private static final String NON_HDFS_BASE_PATH = "/tmp/dir";
  private static final String HDFS_BASE_PATH = "hdfs://localhost/dir";
  private final HoodieWriteConfig writeConfig = Mockito.mock(HoodieWriteConfig.class);
  private final HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
  private final HoodieWrapperFileSystem fileSystem = Mockito.mock(HoodieWrapperFileSystem.class);
  private final HoodieEngineContext context = Mockito.mock(HoodieEngineContext.class);
  private final HoodieTable table = Mockito.mock(HoodieTable.class);

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
        MarkerType.DIRECT, basePath, isTimelineServerEnabled, DirectWriteMarkers.class);
  }

  @Test
  public void testTimelineServerBasedMarkersWithTimelineServerEnabled() {
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, NON_HDFS_BASE_PATH, true,
        TimelineServerBasedWriteMarkers.class);
  }

  @Test
  public void testTimelineServerBasedMarkersWithTimelineServerDisabled() {
    // Fallback to direct markers should happen
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, NON_HDFS_BASE_PATH, false,
        DirectWriteMarkers.class);
  }

  @Test
  public void testTimelineServerBasedMarkersWithHDFS() {
    // Fallback to direct markers should happen
    testWriteMarkersFactory(
        MarkerType.TIMELINE_SERVER_BASED, HDFS_BASE_PATH, true,
        DirectWriteMarkers.class);
  }

  private void testWriteMarkersFactory(
      MarkerType markerTypeConfig, String basePath, boolean isTimelineServerEnabled,
      Class<?> expectedWriteMarkersClass) {
    String instantTime = "001";
    Mockito.when(table.getConfig()).thenReturn(writeConfig);
    Mockito.when(writeConfig.isEmbeddedTimelineServerEnabled())
        .thenReturn(isTimelineServerEnabled);
    Mockito.when(table.getMetaClient()).thenReturn(metaClient);
    Mockito.when(metaClient.getFs()).thenReturn(fileSystem);
    Mockito.when(metaClient.getBasePath()).thenReturn(basePath);
    Mockito.when(metaClient.getMarkerFolderPath(any())).thenReturn(basePath + ".hoodie/.temp");
    Mockito.when(table.getContext()).thenReturn(context);
    Mockito.when(context.getHadoopConf()).thenReturn(new SerializableConfiguration(new Configuration()));
    Mockito.when(writeConfig.getViewStorageConfig())
        .thenReturn(FileSystemViewStorageConfig.newBuilder().build());
    assertEquals(expectedWriteMarkersClass,
        WriteMarkersFactory.get(markerTypeConfig, table, instantTime).getClass());
  }
}
