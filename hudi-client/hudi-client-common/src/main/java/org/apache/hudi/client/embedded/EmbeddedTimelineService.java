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
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.NetworkUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Timeline Service that runs as part of write client.
 */
public class EmbeddedTimelineService {
  // lock used when starting/stopping/modifying embedded services
  private static final Object SERVICE_LOCK = new Object();

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedTimelineService.class);
  private static final AtomicInteger NUM_SERVERS_RUNNING = new AtomicInteger(0);
  // Map of port to existing timeline service running on that port
  private static final Map<Integer, EmbeddedTimelineService> RUNNING_SERVICES = new HashMap<>();
  private static final Registry METRICS_REGISTRY = Registry.getRegistry("TimelineService");
  private static final String NUM_EMBEDDED_TIMELINE_SERVERS = "numEmbeddedTimelineServers";
  private int serverPort;
  private String hostAddr;
  private final HoodieEngineContext context;
  private final SerializableConfiguration hadoopConf;
  private final HoodieWriteConfig writeConfig;
  private TimelineService.Config serviceConfig;
  private final Set<String> basePaths; // the set of base paths using this EmbeddedTimelineService

  private transient FileSystemViewManager viewManager;
  private transient TimelineService server;

  private EmbeddedTimelineService(HoodieEngineContext context, String embeddedTimelineServiceHostAddr, HoodieWriteConfig writeConfig) {
    setHostAddr(embeddedTimelineServiceHostAddr);
    this.context = context;
    this.writeConfig = writeConfig;
    this.basePaths = new HashSet<>();
    this.basePaths.add(writeConfig.getBasePath());
    this.hadoopConf = context.getHadoopConf();
    this.viewManager = createViewManager();
  }

  /**
   * Returns an existing embedded timeline service if one is running for the given configuration and reuse is enabled, or starts a new one.
   * @param context The {@link HoodieEngineContext} for the client
   * @param embeddedTimelineServiceHostAddr The host address to use for the service (nullable)
   * @param writeConfig The {@link HoodieWriteConfig} for the client
   * @return A running {@link EmbeddedTimelineService}
   * @throws IOException if an error occurs while starting the service
   */
  public static EmbeddedTimelineService getOrStartEmbeddedTimelineService(HoodieEngineContext context, String embeddedTimelineServiceHostAddr, HoodieWriteConfig writeConfig) throws IOException {
    return getOrStartEmbeddedTimelineService(context, embeddedTimelineServiceHostAddr, writeConfig, TimelineService::new);
  }

  static EmbeddedTimelineService getOrStartEmbeddedTimelineService(HoodieEngineContext context, String embeddedTimelineServiceHostAddr, HoodieWriteConfig writeConfig,
                                                                   TimelineServiceCreator timelineServiceCreator) throws IOException {
    // if reuse is enabled, check if any existing instances are compatible
    if (writeConfig.isEmbeddedTimelineServerReuseEnabled()) {
      synchronized (SERVICE_LOCK) {
        for (EmbeddedTimelineService service : RUNNING_SERVICES.values()) {
          if (service.canReuseFor(writeConfig, embeddedTimelineServiceHostAddr)) {
            service.addBasePath(writeConfig.getBasePath());
            LOG.info("Reusing existing embedded timeline server with configuration: " + service.serviceConfig);
            return service;
          }
        }
        // if no compatible instance is found, create a new one
        EmbeddedTimelineService service = createAndStartService(context, embeddedTimelineServiceHostAddr, writeConfig, timelineServiceCreator);
        RUNNING_SERVICES.put(service.serverPort, service);
        return service;
      }
    }
    // if not, create a new instance. If reuse is not enabled, there is no need to add it to RUNNING_SERVICES
    return createAndStartService(context, embeddedTimelineServiceHostAddr, writeConfig, timelineServiceCreator);
  }

  private static EmbeddedTimelineService createAndStartService(HoodieEngineContext context, String embeddedTimelineServiceHostAddr, HoodieWriteConfig writeConfig,
                                                               TimelineServiceCreator timelineServiceCreator) throws IOException {
    EmbeddedTimelineService service = new EmbeddedTimelineService(context, embeddedTimelineServiceHostAddr, writeConfig);
    service.startServer(timelineServiceCreator);
    METRICS_REGISTRY.set(NUM_EMBEDDED_TIMELINE_SERVERS, NUM_SERVERS_RUNNING.incrementAndGet());
    return service;
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
    return FileSystemViewManager.createViewManagerWithTableMetadata(context, writeConfig.getMetadataConfig(), builder.build(), writeConfig.getCommonConfig());
  }

  private void startServer(TimelineServiceCreator timelineServiceCreator) throws IOException {
    TimelineService.Config.Builder timelineServiceConfBuilder = TimelineService.Config.builder()
        .serverPort(writeConfig.getEmbeddedTimelineServerPort())
        .numThreads(writeConfig.getEmbeddedTimelineServerThreads())
        .compress(writeConfig.getEmbeddedTimelineServerCompressOutput())
        .async(writeConfig.getEmbeddedTimelineServerUseAsync());
    // Only passing marker-related write configs to timeline server
    // if timeline-server-based markers are used.
    if (writeConfig.getMarkersType() == MarkerType.TIMELINE_SERVER_BASED) {
      timelineServiceConfBuilder
          .enableMarkerRequests(true)
          .markerBatchNumThreads(writeConfig.getMarkersTimelineServerBasedBatchNumThreads())
          .markerBatchIntervalMs(writeConfig.getMarkersTimelineServerBasedBatchIntervalMs())
          .markerParallelism(writeConfig.getMarkersDeleteParallelism());
    }

    if (writeConfig.isEarlyConflictDetectionEnable()) {
      timelineServiceConfBuilder.earlyConflictDetectionEnable(true)
          .earlyConflictDetectionStrategy(writeConfig.getEarlyConflictDetectionStrategyClassName())
          .earlyConflictDetectionCheckCommitConflict(writeConfig.earlyConflictDetectionCheckCommitConflict())
          .asyncConflictDetectorInitialDelayMs(writeConfig.getAsyncConflictDetectorInitialDelayMs())
          .asyncConflictDetectorPeriodMs(writeConfig.getAsyncConflictDetectorPeriodMs())
          .earlyConflictDetectionMaxAllowableHeartbeatIntervalInMs(
              writeConfig.getHoodieClientHeartbeatIntervalInMs()
                  * writeConfig.getHoodieClientHeartbeatTolerableMisses());
    }

    if (writeConfig.isTimelineServerBasedInstantStateEnabled()) {
      timelineServiceConfBuilder
          .instantStateForceRefreshRequestNumber(writeConfig.getTimelineServerBasedInstantStateForceRefreshRequestNumber())
          .enableInstantStateRequests(true);
    }

    this.serviceConfig = timelineServiceConfBuilder.build();

    server = timelineServiceCreator.create(context, hadoopConf.newCopy(), serviceConfig,
        FSUtils.getFs(writeConfig.getBasePath(), hadoopConf.newCopy()), createViewManager());
    serverPort = server.startService();
    LOG.info("Started embedded timeline server at " + hostAddr + ":" + serverPort);
  }

  @FunctionalInterface
  interface TimelineServiceCreator {
    TimelineService create(HoodieEngineContext context, Configuration hadoopConf, TimelineService.Config timelineServerConf,
                           FileSystem fileSystem, FileSystemViewManager globalFileSystemViewManager) throws IOException;
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

  /**
   * Adds a new base path to the set that are managed by this instance.
   * @param basePath the new base path to add
   */
  private void addBasePath(String basePath) {
    basePaths.add(basePath);
  }

  private boolean canReuseFor(HoodieWriteConfig newWriteConfig, String newHostAddr) {
    if (server == null || viewManager == null) {
      return false; // service is not running
    }
    if (basePaths.contains(newWriteConfig.getBasePath())) {
      return true; // already running for this base path
    }
    if (newHostAddr != null && !newHostAddr.equals(this.hostAddr)) {
      return false; // different host address
    }
    if (writeConfig.getMarkersType() != newWriteConfig.getMarkersType()) {
      return false; // different marker type
    }
    return metadataConfigsAreEquivalent(writeConfig.getMetadataConfig().getProps(), newWriteConfig.getMetadataConfig().getProps());
  }

  private boolean metadataConfigsAreEquivalent(Properties properties1, Properties properties2) {
    Set<Object> metadataConfigs = new HashSet<>(properties1.keySet());
    metadataConfigs.addAll(properties2.keySet());
    return metadataConfigs.stream()
        .filter(key -> ((String) key).startsWith(HoodieMetadataConfig.METADATA_PREFIX))
        .allMatch(key -> {
          String value1 = properties1.getProperty((String) key, "");
          String value2 = properties2.getProperty((String) key, "");
          return value1.equals(value2);
        });

  }

  /**
   * Stops the embedded timeline service for the given base path. If a timeline service is managing multiple tables, it will only be shutdown once all tables have been stopped.
   * @param basePath For the table to stop the service for
   */
  public void stopForBasePath(String basePath) {
    synchronized (SERVICE_LOCK) {
      basePaths.remove(basePath);
      if (basePaths.isEmpty()) {
        RUNNING_SERVICES.remove(serverPort);
      }
    }
    if (this.server != null) {
      this.server.unregisterBasePath(basePath);
    }
    // continue rest of shutdown outside of the synchronized block to avoid excess blocking
    if (basePaths.isEmpty() && null != server) {
      LOG.info("Closing Timeline server");
      this.server.close();
      METRICS_REGISTRY.set(NUM_EMBEDDED_TIMELINE_SERVERS, NUM_SERVERS_RUNNING.decrementAndGet());
      this.server = null;
      this.viewManager = null;
      LOG.info("Closed Timeline server");
    }
  }
}