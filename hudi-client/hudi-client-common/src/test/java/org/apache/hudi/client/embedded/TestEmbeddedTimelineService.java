package org.apache.hudi.client.embedded;
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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.timeline.service.TimelineService;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * These tests are mainly focused on testing the creation and reuse of the embedded timeline server.
 */
public class TestEmbeddedTimelineService extends HoodieCommonTestHarness {

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
    when(mockCreator.create(any(), any(), any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(123);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    // do not mock the create method since that should never be called
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    assertSame(service1, service2);

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
    when(mockCreator.create(any(), any(), any(), any(), any())).thenReturn(mockService);
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
    when(mockCreator2.create(any(), any(), any(), any(), any())).thenReturn(mockService2);
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
    when(mockCreator.create(any(), any(), any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(789);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(false)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService2 = Mockito.mock(TimelineService.class);
    when(mockCreator2.create(any(), any(), any(), any(), any())).thenReturn(mockService2);
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
    when(mockCreator.create(any(), any(), any(), any(), any())).thenReturn(mockService);
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
    when(mockCreator2.create(any(), any(), any(), any(), any())).thenReturn(mockService2);
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
}