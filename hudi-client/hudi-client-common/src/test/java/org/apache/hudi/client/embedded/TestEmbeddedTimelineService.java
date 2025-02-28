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

package org.apache.hudi.client.embedded;

import org.apache.hudi.client.embedded.EmbeddedTimelineService.TimelineServiceIdentifier;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.timeline.service.TimelineService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * These tests are mainly focused on testing the creation and reuse of the embedded timeline server.
 */
public class TestEmbeddedTimelineService extends HoodieCommonTestHarness {

  @BeforeEach
  void setUp() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath().toString();
  }

  @Test
  public void embeddedTimelineServiceReused() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table1").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(123);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteTimelineClientRetry(true)
            .build())
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    // do not mock the create method since that should never be called
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    assertSame(service1, service2);

    // Test client properties are not overridden
    assertFalse(service1.getRemoteFileSystemViewConfig(writeConfig1).isRemoteTimelineClientRetryEnabled());
    assertTrue(service1.getRemoteFileSystemViewConfig(writeConfig2).isRemoteTimelineClientRetryEnabled());

    // test shutdown happens after the last path is removed
    service1.stopForBasePath(writeConfig2.getBasePath());
    verify(mockService, never()).close();
    verify(mockService, times(1)).unregisterBasePath(writeConfig2.getBasePath());

    service2.stopForBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).unregisterBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).close();
  }

  @Test
  public void embeddedTimelineServiceCreatedForDifferentMetadataConfig() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table1").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(321);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(false)
            .build())
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService2 = Mockito.mock(TimelineService.class);
    when(mockCreator2.create(any(), any(), any())).thenReturn(mockService2);
    when(mockService2.startService()).thenReturn(456);
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    assertNotSame(service1, service2);

    // test shutdown happens immediately since each server has only one path associated with it
    service1.stopForBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).close();

    service2.stopForBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).close();
  }

  @Test
  public void embeddedTimelineServerNotReusedIfReuseDisabled() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table1").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(789);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(false)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService2 = Mockito.mock(TimelineService.class);
    when(mockCreator2.create(any(), any(), any())).thenReturn(mockService2);
    when(mockService2.startService()).thenReturn(987);
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    assertNotSame(service1, service2);

    // test shutdown happens immediately since each server has only one path associated with it
    service1.stopForBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).unregisterBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).close();

    service2.stopForBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).unregisterBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).close();
  }

  @Test
  public void embeddedTimelineServerIsNotReusedAfterStopped() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table1").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(555);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    service1.stopForBasePath(writeConfig1.getBasePath());

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService2 = Mockito.mock(TimelineService.class);
    when(mockCreator2.create(any(), any(), any())).thenReturn(mockService2);
    when(mockService2.startService()).thenReturn(111);
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    // a new service will be started since the original was shutdown already
    assertNotSame(service1, service2);

    // test shutdown happens immediately since each server has only one path associated with it
    service1.stopForBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).unregisterBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).close();

    service2.stopForBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).unregisterBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).close();
  }

  @Test
  void testMultipleTimelineServersWithDifferentPorts() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "temp");
    properties.setProperty(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withProperties(properties)
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true);
    HoodieWriteConfig writeConfig1 = writeConfigBuilder.withEmbeddedTimelineServerPort(8010).build();
    HoodieWriteConfig writeConfig2 = writeConfigBuilder.withEmbeddedTimelineServerPort(8020).build();
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    // Start two services and having assert they have different views for same table.
    HoodieTableMetaClient.newTableBuilder().fromProperties(writeConfig1.getProps()).initTable(getDefaultStorageConf(), basePath);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, "localhost", writeConfig1);
    service1.getViewManager().getFileSystemView(basePath);
    // Write commits to basePath.
    String partitionPath = "partition1";
    writeInstantToTimeline(basePath, partitionPath);
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, "localhost", writeConfig2);
    // Assert service1 is still behind.
    assertTrue(service1.getViewManager().getFileSystemView(basePath).getTimeline().getInstants().isEmpty());
    assertEquals(0, service1.getViewManager().getFileSystemView(basePath).getAllBaseFiles(partitionPath).count());
    // Assert service1 is still latest.
    assertEquals(1, service2.getViewManager().getFileSystemView(basePath).getTimeline().getInstants().size());
    assertEquals(1, service2.getViewManager().getFileSystemView(basePath).getAllBaseFiles(partitionPath).count());
    // Assert services are independent of each other.
    service1.stopForBasePath(basePath);
    assertEquals(1, service2.getViewManager().getFileSystemView(basePath).getAllBaseFiles(partitionPath).count());
    assertNotEquals(service1.hashCode(), service2.hashCode());
  }

  @ParameterizedTest
  @ValueSource(strings = {"hostAddrNull", "hostAddr", "port", "markerType", "isMetadataEnabled", "isEarlyConflictDetectionEnable", "isTimelineServerBasedInstantStateEnabled"})
  void testEquals(String equalityField) {
    String hostAddr = "localhost";
    int port = 9090;
    MarkerType markerType = MarkerType.DIRECT;
    boolean isMetadataEnabled = true;
    boolean isEarlyConflictDetectionEnable = true;
    TimelineServiceIdentifier firstTimelineId = new TimelineServiceIdentifier(
        hostAddr, port, markerType, isMetadataEnabled, isEarlyConflictDetectionEnable);
    switch (equalityField) {
      case "hostAddrNull":
        hostAddr = null;
        break;
      case "hostAddr":
        hostAddr = "onehouse.ai";
        break;
      case "port":
        port = 9091;
        break;
      case "markerType":
        markerType = MarkerType.TIMELINE_SERVER_BASED;
        break;
      case "isMetadataEnabled":
        isMetadataEnabled = false;
        break;
      case "isEarlyConflictDetectionEnable":
        isEarlyConflictDetectionEnable = false;
        break;
      default:
        throw new IllegalArgumentException("Invalid parameterized test");
    }
    TimelineServiceIdentifier secondTimelineId = new TimelineServiceIdentifier(
        hostAddr, port, markerType, isMetadataEnabled, isEarlyConflictDetectionEnable);
    assertNotEquals(firstTimelineId, secondTimelineId);
    assertNotEquals(firstTimelineId.hashCode(), secondTimelineId.hashCode());
  }

  private void writeInstantToTimeline(String basePath, String partitionPath) throws IOException {
    // Write data to a single partition.
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();
    String instantTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(instantTime1,"1-0-1", fileId, HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension());
    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, inflight.getAction(), inflight.requestedTime(), InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    commitTimeline.createNewInstant(requested);
    commitTimeline.transitionRequestedToInflight(requested, Option.empty());
    commitTimeline.saveAsComplete(inflight, Option.empty());
  }
}