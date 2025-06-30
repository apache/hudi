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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.timeline.TimelineServiceClient;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_STORAGE_CLASS;
import static org.apache.hudi.common.table.marker.MarkerOperation.CREATE_MARKER_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_DIR_PATH_PARAM;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_NAME_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.BASEPATH_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LAST_INSTANT_TS;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.REFRESH_TABLE_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.TIMELINE_HASH;
import static org.apache.hudi.timeline.TimelineServiceClientBase.RequestMethod.POST;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRequestHandler extends HoodieCommonTestHarness {

  private static final String DEFAULT_FILE_SCHEME = "file:/";

  private TimelineService server = null;
  private TimelineServiceClient timelineServiceClient;

  @BeforeEach
  void setUp() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath().toString();
    Configuration configuration = new Configuration();
    configuration.set(HOODIE_STORAGE_CLASS.key(), MockHoodieHadoopStorage.class.getName());
    FileSystemViewStorageConfig sConf =
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(new HadoopStorageConfiguration(configuration));

    try {
      if (server != null) {
        server.close();
      }
      TimelineServiceTestHarness.Builder builder = TimelineServiceTestHarness.newBuilder();
      server = builder.build(configuration,
          TimelineService.Config.builder().serverPort(0).enableMarkerRequests(true).build(),
          FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, sConf, commonConfig));
      server.startService();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(server.getServerPort())
        .withRemoteTimelineClientTimeoutSecs(60);
    timelineServiceClient = new TimelineServiceClient(builder.build());
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  @Test
  void testRefreshTableAPIWithDifferentSchemes() throws IOException {
    assertRefreshTable(tempDir.resolve("base-path-1").toUri().toString(), "test1:/");
    assertRefreshTable(tempDir.resolve("base-path-2").toUri().toString(), "test2:/");
  }

  private void assertRefreshTable(String basePath, String scheme) throws IOException {
    Map<String, String> queryParameters = new HashMap<>();
    queryParameters.put(BASEPATH_PARAM, getPathWithReplacedSchema(basePath, scheme));
    metaClient.getActiveTimeline().lastInstant().ifPresent(instant -> queryParameters.put(LAST_INSTANT_TS, instant.requestedTime()));
    queryParameters.put(TIMELINE_HASH, metaClient.getActiveTimeline().getTimelineHash());
    boolean content = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, REFRESH_TABLE_URL).addQueryParams(queryParameters).build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertTrue(content);
  }

  @Test
  void testCreateMarkerAPIWithDifferentSchemes() throws IOException {
    assertMarkerCreation(tempDir.resolve("base-path-1").toUri().toString(), "test1:/");
    assertMarkerCreation(tempDir.resolve("base-path-2").toUri().toString(), "test2:/");
  }

  private void assertMarkerCreation(String basePath, String schema) throws IOException {
    Map<String, String> queryParameters = new HashMap<>();
    String basePathScheme = getPathWithReplacedSchema(basePath, schema);
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, getTableType());
    String markerDir = getPathWithReplacedSchema(metaClient.getMarkerFolderPath("101"), schema);

    queryParameters.put(BASEPATH_PARAM, basePathScheme);
    queryParameters.put(MARKER_DIR_PATH_PARAM, markerDir);
    queryParameters.put(MARKER_NAME_PARAM, "marker-file-1");

    boolean content = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, CREATE_MARKER_URL)
                .addQueryParams(queryParameters)
                .build())
        .getDecodedContent(new TypeReference<Boolean>() {});

    assertTrue(content);
  }

  private String getPathWithReplacedSchema(String path, String schemaToUse) {
    if (path.startsWith(DEFAULT_FILE_SCHEME)) {
      return path.replace(DEFAULT_FILE_SCHEME, schemaToUse);
    } else if (path.startsWith(String.valueOf(StoragePath.SEPARATOR_CHAR))) {
      return schemaToUse + StoragePath.SEPARATOR_CHAR + path;
    }
    throw new IllegalArgumentException("Invalid file provided");
  }
}
