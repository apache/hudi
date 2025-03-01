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

package org.apache.hudi.timeline.service;

import org.apache.http.client.HttpResponseException;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.timeline.TimelineServiceClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.BASEPATH_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.INIT_TIMELINE_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LAST_INSTANT_TS;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LOAD_PARTITIONS_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.REFRESH_TABLE_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.TIMELINE_HASH;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.apache.hudi.timeline.TimelineServiceClientBase.RequestMethod.POST;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRequestHandler extends HoodieCommonTestHarness {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());

  private TimelineService server = null;
  private TimelineServiceClient timelineServiceClient;

  @BeforeEach
  void setUp() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath().toString();
    FileSystemViewStorageConfig sConf =
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());

    try {
      if (server != null) {
        server.close();
      }
      TimelineServiceTestHarness.Builder builder = TimelineServiceTestHarness.newBuilder();
      server = builder.build(getStorageConf().unwrap(),
          TimelineService.Config.builder().serverPort(0).enableMarkerRequests(true).build(),
          FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, sConf, commonConfig));
      server.startService();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(server.getServerPort())
        .withRemoteInitTimeline(true)
        .withRemoteTimelineClientTimeoutSecs(60);
    timelineServiceClient = new TimelineServiceClient(builder.build());
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  @Test
  void initTimelineAPI() throws IOException {
    Map<String, String> queryParameters = new HashMap<>();
    queryParameters.put(BASEPATH_PARAM, basePath);
    metaClient.getActiveTimeline().lastInstant().ifPresent(instant -> queryParameters.put(LAST_INSTANT_TS, instant.requestedTime()));
    queryParameters.put(TIMELINE_HASH, metaClient.getActiveTimeline().getTimelineHash());
    String body = OBJECT_MAPPER.writeValueAsString(TimelineDTO.fromTimeline(metaClient.getActiveTimeline()));
    boolean content = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, INIT_TIMELINE_URL).addQueryParams(queryParameters).setBody(body).build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertTrue(content);
  }

  @Test
  void refreshTableAPI() throws IOException {
    Map<String, String> queryParameters = new HashMap<>();
    queryParameters.put(BASEPATH_PARAM, basePath);
    metaClient.getActiveTimeline().lastInstant().ifPresent(instant -> queryParameters.put(LAST_INSTANT_TS, instant.requestedTime()));
    queryParameters.put(TIMELINE_HASH, metaClient.getActiveTimeline().getTimelineHash());
    boolean content = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, REFRESH_TABLE_URL).addQueryParams(queryParameters).build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertTrue(content);
  }

  @Test
  void loadPartitionAPI() throws IOException {
    String[] partitionPaths = {"partition-path"};
    Map<String, String> queryParameters = new HashMap<>();
    queryParameters.put(BASEPATH_PARAM, basePath);

    boolean content = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, LOAD_PARTITIONS_URL).addQueryParams(queryParameters)
                .setBody(OBJECT_MAPPER.writeValueAsString(partitionPaths)).build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertTrue(content);

    // Test that if a request is made with no partitionPaths in body, then an exception is thrown.
    assertThrows(HttpResponseException.class, () -> timelineServiceClient.makeRequest(
        TimelineServiceClient.Request.newBuilder(POST, LOAD_PARTITIONS_URL).addQueryParams(queryParameters).build()));
  }
}
