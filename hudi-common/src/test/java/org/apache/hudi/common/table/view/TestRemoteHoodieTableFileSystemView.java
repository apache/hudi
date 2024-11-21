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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.timeline.TimelineServiceClient;
import org.apache.hudi.timeline.TimelineServiceClientBase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.CLOSE_TABLE;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.GET_TIMELINE_HASH;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.INIT_TIMELINE;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.REFRESH_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestRemoteHoodieTableFileSystemView extends TestHoodieTableFileSystemView {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());

  @Mock
  TimelineServiceClient timelineServiceClient;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void initialiseTimelineInRemoteView(boolean enableRemoteInitTimeline) throws IOException {
    // Write data to a single partition.
    String partitionPath = "partition1";
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();
    String instantTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(instantTime1,"1-0-1", fileId);
    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    saveAsComplete(commitTimeline, instant1, Option.empty());
    metaClient.reloadActiveTimeline();

    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder()
        .withRemoteInitTimeline(enableRemoteInitTimeline)
        .build();
    when(timelineServiceClient.makeRequest(any())).thenReturn(new TimelineServiceClientBase.Response((OBJECT_MAPPER.writeValueAsString("timeline-hash"))));
    when(timelineServiceClient.makeRequest(any())).thenReturn(new TimelineServiceClientBase.Response(OBJECT_MAPPER.writeValueAsString("true")));
    RemoteHoodieTableFileSystemView view = new RemoteHoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), timelineServiceClient, config);
    String expectedBody = OBJECT_MAPPER.writeValueAsString(TimelineDTO.fromTimeline(metaClient.getActiveTimeline()));
    if (enableRemoteInitTimeline) {
      verify(timelineServiceClient, times(1)).makeRequest(argThat(
          request -> request.getMethod().equals(TimelineServiceClientBase.RequestMethod.GET) && request.getPath().equals(GET_TIMELINE_HASH)));
      verify(timelineServiceClient, times(1)).makeRequest(argThat(request -> request.getMethod().equals(TimelineServiceClientBase.RequestMethod.POST)
          && request.getPath().equals(INIT_TIMELINE)
          && request.getBody().equals(expectedBody))
      );
    }

    when(timelineServiceClient.makeRequest(any())).thenThrow(new IOException("Failed to connect to server"));
    assertThrows(HoodieRemoteException.class, () -> view.initialiseTimelineInRemoteView(metaClient.getActiveTimeline()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void syncRemoteView(boolean enableRemoteInitTimeline) throws IOException {
    // Write data to a single partition.
    String partitionPath = "partition1";
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();
    String instantTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(instantTime1,"1-0-1", fileId);
    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    saveAsComplete(commitTimeline, instant1, Option.empty());
    metaClient.reloadActiveTimeline();

    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder()
        .withRemoteInitTimeline(enableRemoteInitTimeline)
        .build();

    // Mock calls made during initialization and sync while capturing request args for validation
    ArgumentCaptor<TimelineServiceClientBase.Request> captor = ArgumentCaptor.forClass(TimelineServiceClientBase.Request.class);
    if (enableRemoteInitTimeline) {
      // expect that initialization flow is called twice
      when(timelineServiceClient.makeRequest(captor.capture()))
          .thenReturn(new TimelineServiceClientBase.Response((OBJECT_MAPPER.writeValueAsString("timeline-hash"))))
          .thenReturn(new TimelineServiceClientBase.Response(OBJECT_MAPPER.writeValueAsString("true")))
          .thenReturn(new TimelineServiceClientBase.Response((OBJECT_MAPPER.writeValueAsString("timeline-hash-2"))))
          .thenReturn(new TimelineServiceClientBase.Response(OBJECT_MAPPER.writeValueAsString("true")));
    } else {
      // otherwise, only one call is made to clear the current state in the server
      when(timelineServiceClient.makeRequest(captor.capture())).thenReturn(new TimelineServiceClientBase.Response(OBJECT_MAPPER.writeValueAsString("true")));
    }
    RemoteHoodieTableFileSystemView view = new RemoteHoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), timelineServiceClient, config);
    String instantTime2 = "2";
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime2);
    saveAsComplete(commitTimeline, instant2, Option.empty());
    view.sync();
    List<TimelineServiceClientBase.Request> requests = captor.getAllValues().stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (enableRemoteInitTimeline) {
      assertEquals(4, requests.size());
      assertEquals(GET_TIMELINE_HASH, requests.get(2).getPath());
      assertEquals(INIT_TIMELINE, requests.get(3).getPath());
      // assert latest timeline is passed to server
      metaClient.reloadActiveTimeline();
      assertEquals(OBJECT_MAPPER.writeValueAsString(TimelineDTO.fromTimeline(metaClient.getActiveTimeline())), requests.get(3).getBody());
    } else {
      assertEquals(1, requests.size());
      TimelineServiceClientBase.Request refreshRequest = requests.get(0);
      assertEquals(TimelineServiceClientBase.RequestMethod.POST, refreshRequest.getMethod());
      assertEquals(REFRESH_TABLE, refreshRequest.getPath());
    }
  }

  @Test
  void closeRemoteView() throws Exception {
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder().build();
    RemoteHoodieTableFileSystemView view = new RemoteHoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), timelineServiceClient, config);
    when(timelineServiceClient.makeRequest(any())).thenReturn(new TimelineServiceClientBase.Response(OBJECT_MAPPER.writeValueAsString("true")));
    view.close();
    // calling a second time should not make a second request
    view.close();
    // verify request parameters
    verify(timelineServiceClient).makeRequest(argThat(request -> request.getMethod().equals(TimelineServiceClientBase.RequestMethod.POST) && request.getPath().equals(CLOSE_TABLE)));
  }

  @Test
  void remoteLoadPartitions() throws Exception {
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder().build();
    RemoteHoodieTableFileSystemView view = new RemoteHoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), timelineServiceClient, config);

    when(timelineServiceClient.makeRequest(any())).thenReturn(new TimelineServiceClientBase.Response(OBJECT_MAPPER.writeValueAsString("true")));
    List<String> partitionPath = Collections.singletonList("partition-path");
    view.loadPartitions(partitionPath);

    ArgumentCaptor<TimelineServiceClientBase.Request> argCaptor = ArgumentCaptor.forClass(TimelineServiceClientBase.Request.class);
    verify(timelineServiceClient, times(1)).makeRequest(argCaptor.capture());
    TimelineServiceClientBase.Request request = argCaptor.getValue();
    assertFalse(request.getBody().isEmpty());
    assertEquals(OBJECT_MAPPER.writeValueAsString(partitionPath), request.getBody());
  }
}