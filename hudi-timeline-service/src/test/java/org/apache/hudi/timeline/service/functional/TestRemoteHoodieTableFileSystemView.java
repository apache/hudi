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

package org.apache.hudi.timeline.service.functional;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TestHoodieTableFileSystemView;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

/**
 * Bring up a remote Timeline Server and run all test-cases of TestHoodieTableFileSystemView against it.
 */
public class TestRemoteHoodieTableFileSystemView extends TestHoodieTableFileSystemView {

  private static final Logger LOG = LogManager.getLogger(TestRemoteHoodieTableFileSystemView.class);

  private TimelineService server;
  private RemoteHoodieTableFileSystemView view;

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) {
    FileSystemViewStorageConfig sConf =
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());

    try {
      server = new TimelineService(localEngineContext, new Configuration(),
          TimelineService.Config.builder().serverPort(0).build(), FileSystem.get(new Configuration()),
          FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, sConf, commonConfig));
      server.startService();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    LOG.info("Connecting to Timeline Server :" + server.getServerPort());
    view = new RemoteHoodieTableFileSystemView("localhost", server.getServerPort(), metaClient);
    return view;
  }

  @Test
  public void testRemoteHoodieTableFileSystemViewWithRetry() {
    // Service is available.
    view.getLatestBaseFiles();
    // Shut down the service.
    server.close();
    try {
      // Immediately fails and throws a connection refused exception.
      view.getLatestBaseFiles();
    } catch (HoodieRemoteException e) {
      assert e.getMessage().contains("Connection refused (Connection refused)");
    }
    // Enable API request retry for remote file system view.
    view =  new RemoteHoodieTableFileSystemView(metaClient, FileSystemViewStorageConfig
            .newBuilder()
            .withRemoteServerHost("localhost")
            .withRemoteServerPort(server.getServerPort())
            .withRemoteTimelineClientRetry(true)
            .withRemoteTimelineClientMaxRetryNumbers(4)
            .build());
    try {
      view.getLatestBaseFiles();
    } catch (HoodieRemoteException e) {
      assert e.getMessage().equalsIgnoreCase("Still failed to Sending request after retried 4 times.");
    }
  }
}
