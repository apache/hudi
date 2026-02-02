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
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_REQUEST_ID_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.BASEPATH_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LAST_INSTANT_TS;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.REFRESH_TABLE_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.TIMELINE_HASH;
import static org.apache.hudi.timeline.TimelineServiceClientBase.RequestMethod.POST;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
          TimelineService.Config.builder()
              .serverPort(0)
              .enableMarkerRequests(true)
              .build(),
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

  @Test
  void testMarkerIdempotency() throws IOException, InterruptedException {
    String basePath = tempDir.resolve("base-path-idempotency").toUri().toString();
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, getTableType());
    String markerDir = metaClient.getMarkerFolderPath("102");
    String markerName = "partition1/file1.parquet.marker.CREATE";
    String requestId1 = java.util.UUID.randomUUID().toString();
    String requestId2 = java.util.UUID.randomUUID().toString();

    Map<String, String> queryParameters = new HashMap<>();
    queryParameters.put(BASEPATH_PARAM, basePath);
    queryParameters.put(MARKER_DIR_PATH_PARAM, markerDir);
    queryParameters.put(MARKER_NAME_PARAM, markerName);
    queryParameters.put(MARKER_REQUEST_ID_PARAM, requestId1);

    // First request
    boolean result1 = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, CREATE_MARKER_URL)
                .addQueryParams(queryParameters)
                .build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertTrue(result1, "First marker creation should succeed");

    // Give server time to process
    Thread.sleep(500);

    // Retry with same requestId (simulating timeout + retry) should succeed (idempotent)
    boolean result2 = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, CREATE_MARKER_URL)
                .addQueryParams(queryParameters)
                .build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertTrue(result2, "Retry with same requestId should succeed (idempotent)");

    // Retry with different requestId (distinct logical request) should fail
    queryParameters.put(MARKER_REQUEST_ID_PARAM, requestId2);
    boolean result3 = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, CREATE_MARKER_URL)
                .addQueryParams(queryParameters)
                .build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertFalse(result3, "Retry with different requestId should fail");
  }

  @Test
  void testMarkerBackwardCompatibilityNullExistingRequestId() throws IOException, InterruptedException {
    String basePath = tempDir.resolve("base-path-backward-compat").toUri().toString();
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, getTableType());
    String markerDir = metaClient.getMarkerFolderPath("104");
    String markerName = "partition1/file1.parquet.marker.CREATE";
    String newRequestId = java.util.UUID.randomUUID().toString();

    // First request: no requestId (legacy client) → marker stored with null requestId
    Map<String, String> queryParametersLegacy = new HashMap<>();
    queryParametersLegacy.put(BASEPATH_PARAM, basePath);
    queryParametersLegacy.put(MARKER_DIR_PATH_PARAM, markerDir);
    queryParametersLegacy.put(MARKER_NAME_PARAM, markerName);
    // Omit MARKER_REQUEST_ID_PARAM

    boolean result1 = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, CREATE_MARKER_URL)
                .addQueryParams(queryParametersLegacy)
                .build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertTrue(result1, "First marker creation (no requestId) should succeed");

    // Give server time to process
    Thread.sleep(500);

    // Second request: same marker with non-null requestId → should succeed (backward compat)
    Map<String, String> queryParametersWithRequestId = new HashMap<>(queryParametersLegacy);
    queryParametersWithRequestId.put(MARKER_REQUEST_ID_PARAM, newRequestId);

    boolean result2 = timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(POST, CREATE_MARKER_URL)
                .addQueryParams(queryParametersWithRequestId)
                .build())
        .getDecodedContent(new TypeReference<Boolean>() {});
    assertTrue(result2, "Request with non-null requestId when marker has null should succeed (backward compat)");
  }

  private void assertMarkerCreation(String basePath, String schema) throws IOException {
    Map<String, String> queryParameters = new HashMap<>();
    String basePathScheme = getPathWithReplacedSchema(basePath, schema);
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, getTableType());
    String markerDir = getPathWithReplacedSchema(metaClient.getMarkerFolderPath("101"), schema);

    queryParameters.put(BASEPATH_PARAM, basePathScheme);
    queryParameters.put(MARKER_DIR_PATH_PARAM, markerDir);
    queryParameters.put(MARKER_NAME_PARAM, "marker-file-1");
    // Add requestId for idempotency
    queryParameters.put(MARKER_REQUEST_ID_PARAM, java.util.UUID.randomUUID().toString());

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
