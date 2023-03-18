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

package org.apache.hudi.client;

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestSparkRDDWriteClient extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  public void testWriteClientAndTableServiceClientWithTimelineServer(
      boolean enableEmbeddedTimelineServer, boolean passInTimelineServer) throws IOException {
    HoodieTableMetaClient metaClient =
        getHoodieMetaClient(hadoopConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withPath(metaClient.getBasePathV2().toString())
        .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineServer)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(incrementTimelineServicePortToUse()).build())
        .build();

    SparkRDDWriteClient writeClient;
    if (passInTimelineServer) {
      EmbeddedTimelineService timelineService =
          new EmbeddedTimelineService(context(), null, writeConfig);
      timelineService.startServer();
      writeConfig.setViewStorageConfig(timelineService.getRemoteFileSystemViewConfig());
      writeClient = new SparkRDDWriteClient(context(), writeConfig, Option.of(timelineService));
      // Both the write client and the table service client should use the same passed-in
      // timeline server instance.
      assertEquals(timelineService, writeClient.getTimelineServer().get());
      assertEquals(timelineService, writeClient.getTableServiceClient().getTimelineServer().get());
      // Write config should not be changed
      assertEquals(writeConfig, writeClient.getConfig());
      timelineService.stop();
    } else {
      writeClient = new SparkRDDWriteClient(context(), writeConfig);
      // Only one timeline server should be instantiated, and the same timeline server
      // should be used by both the write client and the table service client.
      assertEquals(
          writeClient.getTimelineServer(),
          writeClient.getTableServiceClient().getTimelineServer());
      if (!enableEmbeddedTimelineServer) {
        assertFalse(writeClient.getTimelineServer().isPresent());
      }
    }
    writeClient.close();
  }
}
