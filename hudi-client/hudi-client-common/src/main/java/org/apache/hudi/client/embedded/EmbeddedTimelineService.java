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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.NetworkUtils;
import org.apache.hudi.config.HoodieWriteConfig;
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
  private String hostAddr;
  private HoodieEngineContext context;
  private final SerializableConfiguration hadoopConf;
  private final HoodieWriteConfig writeConfig;
  private final String basePath;

  private transient FileSystemViewManager viewManager;
  private transient TimelineService server;

  public EmbeddedTimelineService(HoodieEngineContext context, String embeddedTimelineServiceHostAddr, HoodieWriteConfig writeConfig) {
    setHostAddr(embeddedTimelineServiceHostAddr);
    this.context = context;
    this.writeConfig = writeConfig;
    this.basePath = writeConfig.getBasePath();
    this.hadoopConf = context.getHadoopConf();
    this.viewManager = createViewManager();
  }

  private FileSystemViewManager createViewManager() {
    // Using passed-in configs to build view storage configs
    FileSystemViewStorageConfig.Builder builder =
        FileSystemViewStorageConfig.newBuilder().fromProperties(writeConfig.getClientSpecifiedViewStorageConfig().getProps());
    FileSystemViewStorageType storageType = builder.build().getStorageType();
    if (storageType.equals(FileSystemViewStorageType.REMOTE_ONLY)
        || storageType.equals(FileSystemViewStorageType.REMOTE_FIRST)) {
      // Reset to default if set to Remote
      builder.withStorageType(FileSystemViewStorageType.MEMORY);
    }
    return FileSystemViewManager.createViewManager(context, writeConfig.getMetadataConfig(), builder.build(), writeConfig.getCommonConfig(), basePath);
  }

  public void startServer() throws IOException {
    TimelineService.Config.Builder timelineServiceConfBuilder = TimelineService.Config.builder()
        .serverPort(Integer.parseInt(writeConfig.getStringOrDefault(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_PORT_NUM)))
        .numThreads(Integer.parseInt(writeConfig.getStringOrDefault(HoodieWriteConfig.EMBEDDED_TIMELINE_NUM_SERVER_THREADS)))
        .compress(Boolean.parseBoolean(writeConfig.getStringOrDefault(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_COMPRESS_ENABLE)))
        .async(Boolean.parseBoolean(writeConfig.getStringOrDefault(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_USE_ASYNC_ENABLE)));
    // Only passing marker-related write configs to timeline server
    // if timeline-server-based markers are used.
    if (writeConfig.getMarkersType() == MarkerType.TIMELINE_SERVER_BASED) {
      timelineServiceConfBuilder
          .enableMarkerRequests(true)
          .markerBatchNumThreads(writeConfig.getInt(HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_NUM_THREADS))
          .markerBatchIntervalMs(writeConfig.getLong(HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS))
          .markerParallelism(writeConfig.getInt(HoodieWriteConfig.MARKERS_DELETE_PARALLELISM_VALUE));
    }

    server = new TimelineService(context, hadoopConf.newCopy(), timelineServiceConfBuilder.build(),
        FSUtils.getFs(basePath, hadoopConf.newCopy()), viewManager);
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
    FileSystemViewStorageType viewStorageType = writeConfig.getClientSpecifiedViewStorageConfig()
        .shouldEnableBackupForRemoteFileSystemView()
        ? FileSystemViewStorageType.REMOTE_FIRST : FileSystemViewStorageType.REMOTE_ONLY;
    return FileSystemViewStorageConfig.newBuilder()
        .withStorageType(viewStorageType)
        .withRemoteServerHost(hostAddr)
        .withRemoteServerPort(serverPort)
        .withRemoteTimelineClientTimeoutSecs(writeConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineClientTimeoutSecs())
        .withRemoteTimelineClientRetry(writeConfig.getClientSpecifiedViewStorageConfig().isRemoteTimelineClientRetryEnabled())
        .withRemoteTimelineClientMaxRetryNumbers(writeConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineClientMaxRetryNumbers())
        .withRemoteTimelineInitialRetryIntervalMs(writeConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineInitialRetryIntervalMs())
        .withRemoteTimelineClientMaxRetryIntervalMs(writeConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineClientMaxRetryIntervalMs())
        .withRemoteTimelineClientRetryExceptions(writeConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineClientRetryExceptions())
        .build();
  }

  public FileSystemViewManager getViewManager() {
    return viewManager;
  }

  public boolean canReuseFor(String basePath) {
    return this.server != null
        && this.viewManager != null
        && this.basePath.equals(basePath);
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
