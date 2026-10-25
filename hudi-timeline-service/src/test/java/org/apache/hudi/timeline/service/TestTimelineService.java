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

import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.StorageConfiguration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
