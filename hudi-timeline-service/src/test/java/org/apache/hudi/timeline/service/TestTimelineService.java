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

import org.apache.hudi.common.table.marker.MarkerOperation;
import org.apache.hudi.common.table.timeline.TimelineServiceClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.StorageConfiguration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestTimelineService {

  @Test
  void createServerUsesRandomPortIfAnotherTimelineServiceRunningOnSamePort() throws Exception {
    TimelineService timelineService = null;
    TimelineService secondTimelineService = null;
    try {
      StorageConfiguration<Configuration> conf = HoodieTestUtils.getDefaultStorageConf();
      int originalServerPort = 8888;
      TimelineService.Config config = TimelineService.Config.builder()
          .enableMarkerRequests(true)
          .serverPort(originalServerPort)
          .build();
      FileSystemViewManager viewManager = mock(FileSystemViewManager.class);
      timelineService = new TimelineService(conf, config, viewManager);
      assertEquals(originalServerPort, timelineService.startService());
      // Create second service with the same configs
      secondTimelineService = new TimelineService(conf, config, viewManager);
      assertNotEquals(originalServerPort, secondTimelineService.startService());
    } finally {
      if (timelineService != null) {
        timelineService.close();
      }
      if (secondTimelineService != null) {
        secondTimelineService.close();
      }
    }
  }

  @Test
  void createServerUsesRandomPortIfProvidedPortInUse() throws Exception {
    TimelineService timelineService = null;
    HttpServer server = null;
    try {
      int originalServerPort = 8888;
      server = HttpServer.create(new InetSocketAddress(originalServerPort), 0);
      server.createContext("/", new MyHandler());
      server.setExecutor(null);
      server.start();

      TimelineService.Config config = TimelineService.Config.builder()
          .enableMarkerRequests(true)
          .serverPort(originalServerPort)
          .build();
      FileSystemViewManager viewManager = mock(FileSystemViewManager.class);
      StorageConfiguration<Configuration> conf = HoodieTestUtils.getDefaultStorageConf();
      timelineService = new TimelineService(conf, config, viewManager);
      assertNotEquals(originalServerPort, timelineService.startService());
    } finally {
      if (timelineService != null) {
        timelineService.close();
      }
      if (server != null) {
        server.stop(0);
      }
    }
  }

  /**
   * A listener bound to the loopback address answers requests sent there even if the service binds the same
   * port on the wildcard address, so the service has to end up on a port where the advertised host reaches it.
   */
  @Test
  void createServerMovesOffPortAnsweredByHttpListenerAtAdvertisedHost(@TempDir Path markerDir) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.start();
    try {
      assertServesMarkerRequestsOffPort(server.getAddress().getPort(), markerDir);
    } finally {
      server.stop(0);
    }
  }

  @Test
  void createServerMovesOffPortHeldBySilentListenerAtAdvertisedHost(@TempDir Path markerDir) throws Exception {
    try (ServerSocket silentListener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      assertServesMarkerRequestsOffPort(silentListener.getLocalPort(), markerDir);
    }
  }

  private static void assertServesMarkerRequestsOffPort(int takenPort, Path markerDir) throws IOException {
    String advertisedHost = InetAddress.getLoopbackAddress().getHostAddress();
    TimelineService.Config config = TimelineService.Config.builder()
        .enableMarkerRequests(true)
        .serverPort(takenPort)
        .build();
    TimelineService timelineService =
        new TimelineService(HoodieTestUtils.getDefaultStorageConf(), config, mock(FileSystemViewManager.class));
    try {
      int servicePort = timelineService.startService(advertisedHost);
      TimelineServiceClient client = new TimelineServiceClient(FileSystemViewStorageConfig.newBuilder()
          .withRemoteServerHost(advertisedHost)
          .withRemoteServerPort(servicePort)
          .withRemoteTimelineClientTimeoutSecs(10)
          .build());
      boolean created = client.makeRequest(
              TimelineServiceClient.Request.newBuilder(TimelineServiceClient.RequestMethod.POST, MarkerOperation.CREATE_MARKER_URL)
                  .addQueryParam(MarkerOperation.MARKER_DIR_PATH_PARAM, markerDir.toString())
                  .addQueryParam(MarkerOperation.MARKER_NAME_PARAM, "file1.parquet.marker.CREATE")
                  .build())
          .getDecodedContent(new TypeReference<Boolean>() {});
      assertTrue(created);
      assertNotEquals(takenPort, servicePort);
    } finally {
      timelineService.close();
    }
  }

  static class MyHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange t) throws IOException {
      String response = "Hello World!";
      t.sendResponseHeaders(200, response.length());
      try (OutputStream os = t.getResponseBody()) {
        os.write(response.getBytes());
      }
    }
  }
}
