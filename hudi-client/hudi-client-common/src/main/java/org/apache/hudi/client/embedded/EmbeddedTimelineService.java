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

package org.apache.hudi.client.embedded;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.NetworkUtils;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Timeline Service that runs as part of write client.
 */
public class EmbeddedTimelineService {

  private static final Logger LOG = LogManager.getLogger(EmbeddedTimelineService.class);

  private int serverPort;
  private int preferredPort;
  private String hostAddr;
  private HoodieEngineContext context;
  private final SerializableConfiguration hadoopConf;
  private final FileSystemViewStorageConfig config;
  private final HoodieMetadataConfig metadataConfig;
  private final String basePath;

  private final int numThreads;
  private final boolean shouldCompressOutput;
  private final boolean useAsync;
  private transient FileSystemViewManager viewManager;
  private transient TimelineService server;

  public EmbeddedTimelineService(HoodieEngineContext context, String embeddedTimelineServiceHostAddr, int embeddedTimelineServerPort,
                                 HoodieMetadataConfig metadataConfig, FileSystemViewStorageConfig config, String basePath,
                                 int numThreads, boolean compressOutput, boolean useAsync) {
    setHostAddr(embeddedTimelineServiceHostAddr);
    this.context = context;
    this.config = config;
    this.basePath = basePath;
    this.metadataConfig = metadataConfig;
    this.hadoopConf = context.getHadoopConf();
    this.viewManager = createViewManager();
    this.preferredPort = embeddedTimelineServerPort;
    this.numThreads = numThreads;
    this.shouldCompressOutput = compressOutput;
    this.useAsync = useAsync;
  }

  private FileSystemViewManager createViewManager() {
    // Using passed-in configs to build view storage configs
    FileSystemViewStorageConfig.Builder builder =
        FileSystemViewStorageConfig.newBuilder().fromProperties(config.getProps());
    FileSystemViewStorageType storageType = builder.build().getStorageType();
    if (storageType.equals(FileSystemViewStorageType.REMOTE_ONLY)
        || storageType.equals(FileSystemViewStorageType.REMOTE_FIRST)) {
      // Reset to default if set to Remote
      builder.withStorageType(FileSystemViewStorageType.MEMORY);
    }
    return FileSystemViewManager.createViewManager(context, metadataConfig, builder.build(), basePath);
  }

  public void startServer() throws IOException {
    server = new TimelineService(preferredPort, viewManager, hadoopConf.newCopy(), numThreads, shouldCompressOutput, useAsync);
    serverPort = server.startService();
    LOG.info("Started embedded timeline server at " + hostAddr + ":" + serverPort);
  }

  private void setHostAddr(String embeddedTimelineServiceHostAddr) {
    if (embeddedTimelineServiceHostAddr != null) {
      LOG.info("Overriding hostIp to (" + embeddedTimelineServiceHostAddr + ") found in spark-conf. It was " + this.hostAddr);
      this.hostAddr = embeddedTimelineServiceHostAddr;
    } else {
      LOG.warn("Unable to find driver bind address from spark config");
      this.hostAddr = NetworkUtils.getHostname();
    }
  }

  /**
   * Retrieves proper view storage configs for remote clients to access this service.
   */
  public FileSystemViewStorageConfig getRemoteFileSystemViewConfig() {
    FileSystemViewStorageType viewStorageType = config.shouldEnableBackupForRemoteFileSystemView()
            ? FileSystemViewStorageType.REMOTE_FIRST : FileSystemViewStorageType.REMOTE_ONLY;
    return FileSystemViewStorageConfig.newBuilder().withStorageType(viewStorageType)
        .withRemoteServerHost(hostAddr).withRemoteServerPort(serverPort).build();
  }

  public FileSystemViewManager getViewManager() {
    return viewManager;
  }

  public void stop() {
    if (null != server) {
      LOG.info("Closing Timeline server");
      this.server.close();
      this.server = null;
      this.viewManager = null;
      LOG.info("Closed Timeline server");
    }
  }
}
