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
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineServiceClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_STORAGE_CLASS;
import static org.apache.hudi.common.table.marker.MarkerOperation.CREATE_MARKER_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_DIR_PATH_PARAM;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_NAME_PARAM;
import static org.apache.hudi.common.table.timeline.TimelineServiceClientBase.RequestMethod.GET;
import static org.apache.hudi.common.table.timeline.TimelineServiceClientBase.RequestMethod.POST;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.BASEPATH_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LAST_INSTANT_TS;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LATEST_PARTITION_DATA_FILES_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.PARTITION_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.REFRESH_TABLE_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.TIMELINE_HASH;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

  /**
   * Async cleans that complete after a writer loaded its timeline leave the server timeline ahead of the
   * (stale) timeline sent by the writer's executors only by trailing cleans. The server is not behind those
   * clients, so it must neither sync its view for them (which made every request do a full timeline reload)
   * nor reject their requests.
   */
  @Test
  void testNoViewSyncWhenServerIsAheadOnlyByCleans() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("001");
    // Timeline loaded by the writer before the async cleans complete; shipped to all the executors.
    HoodieTimeline staleClientTimeline = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    // The server loads its view while the client and server timelines agree, so no sync is needed.
    assertEquals(0, getLatestDataFilesWithSyncCount(staleClientTimeline));

    // Two async cleans complete while the write is in progress.
    testTable.addClean("002");
    HoodieTimeline clientTimelineWithFirstClean = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    testTable.addClean("003");
    // A client with the latest timeline makes the server sync once; the server timeline now ends with both cleans.
    HoodieTimeline latestClientTimeline = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    assertEquals(1, getLatestDataFilesWithSyncCount(latestClientTimeline));

    // Executors keep sending the stale timeline. None of these requests should sync the view or be rejected.
    int numRequests = 32;
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      long syncsBefore = getViewSyncCount();
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < numRequests; i++) {
        futures.add(executor.submit(() -> getLatestDataFiles(staleClientTimeline)));
      }
      for (Future<?> future : futures) {
        future.get(60, TimeUnit.SECONDS);
      }
      assertEquals(0, getViewSyncCount() - syncsBefore);
    } finally {
      executor.shutdownNow();
    }

    // A client that has seen only some of the trailing cleans is not ahead of the server either.
    assertEquals(0, getLatestDataFilesWithSyncCount(clientTimelineWithFirstClean));
    assertEquals(0, getLatestDataFilesWithSyncCount(latestClientTimeline));
  }

  /**
   * If the server timeline is ahead of the client by anything other than cleans, e.g. a commit that a restore
   * has since rolled back, the server must sync its view.
   */
  @Test
  void testViewSyncedWhenServerIsAheadByNonCleanInstant() throws Exception {
    // The server loads its view of the empty table.
    assertEquals(0, getLatestDataFilesWithSyncCount(metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants()));

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("001");
    HoodieTimeline clientTimelineBeforeCommit = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    assertEquals(1, getLatestDataFilesWithSyncCount(clientTimelineBeforeCommit));

    testTable.addCommit("002");
    HoodieTimeline clientTimelineWithCommit = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    assertEquals(1, getLatestDataFilesWithSyncCount(clientTimelineWithCommit));

    // Roll back commit 002 the way a restore does: the client timeline goes back to what it was before 002,
    // while the server view still has 002.
    metaClient.getActiveTimeline().deleteInstantFileIfExists(clientTimelineWithCommit.lastInstant().get());
    HoodieTimeline clientTimelineAfterRestore = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    assertEquals(clientTimelineBeforeCommit.getTimelineHash(), clientTimelineAfterRestore.getTimelineHash());
    assertEquals(1, getLatestDataFilesWithSyncCount(clientTimelineAfterRestore));
  }

  private long getLatestDataFilesWithSyncCount(HoodieTimeline clientTimeline) throws IOException {
    long syncsBefore = getViewSyncCount();
    getLatestDataFiles(clientTimeline);
    return getViewSyncCount() - syncsBefore;
  }

  private Void getLatestDataFiles(HoodieTimeline clientTimeline) throws IOException {
    Map<String, String> queryParameters = new HashMap<>();
    queryParameters.put(BASEPATH_PARAM, basePath);
    queryParameters.put(PARTITION_PARAM, "");
    clientTimeline.lastInstant().ifPresent(instant -> queryParameters.put(LAST_INSTANT_TS, instant.requestedTime()));
    queryParameters.put(TIMELINE_HASH, clientTimeline.getTimelineHash());
    // Fails if the server rejects the request, e.g. because it considers its view to be behind the client's.
    timelineServiceClient.makeRequest(
        TimelineServiceClient.Request.newBuilder(GET, LATEST_PARTITION_DATA_FILES_URL).addQueryParams(queryParameters).build());
    return null;
  }

  private static long getViewSyncCount() {
    return Registry.getRegistry("TimelineService").getAllCounts().getOrDefault("VIEW_SYNC", 0L);
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
