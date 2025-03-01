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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.NetworkUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.timeline.service.TimelineService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
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
  // Map of TimelineServiceIdentifier to existing timeline service running
  private static final Map<TimelineServiceIdentifier, EmbeddedTimelineService> RUNNING_SERVICES = new HashMap<>();
  private static final Registry METRICS_REGISTRY = Registry.getRegistry("TimelineService");
  private static final String NUM_EMBEDDED_TIMELINE_SERVERS = "numEmbeddedTimelineServers";
  private int serverPort;
  private String hostAddr;
  private final HoodieEngineContext context;
  private final StorageConfiguration<?> storageConf;
  private final HoodieWriteConfig writeConfig;
  private TimelineService.Config serviceConfig;
  private final TimelineServiceIdentifier timelineServiceIdentifier;
  private final Set<String> basePaths; // the set of base paths using this EmbeddedTimelineService

  private transient FileSystemViewManager viewManager;
  private transient TimelineService server;

  private EmbeddedTimelineService(HoodieEngineContext context, String embeddedTimelineServiceHostAddr, HoodieWriteConfig writeConfig,
                                  TimelineServiceIdentifier timelineServiceIdentifier) {
    setHostAddr(embeddedTimelineServiceHostAddr);
    this.context = context;
    this.writeConfig = writeConfig;
    this.timelineServiceIdentifier = timelineServiceIdentifier;
    this.basePaths = new HashSet<>();
    this.basePaths.add(writeConfig.getBasePath());
    this.storageConf = context.getStorageConf();
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
    TimelineServiceIdentifier timelineServiceIdentifier = getTimelineServiceIdentifier(embeddedTimelineServiceHostAddr, writeConfig);
    // if reuse is enabled, check if any existing instances are compatible
    if (writeConfig.isEmbeddedTimelineServerReuseEnabled()) {
      synchronized (SERVICE_LOCK) {
        if (RUNNING_SERVICES.containsKey(timelineServiceIdentifier)) {
          RUNNING_SERVICES.get(timelineServiceIdentifier).addBasePath(writeConfig.getBasePath());
          LOG.info("Reusing existing embedded timeline server with configuration: " + RUNNING_SERVICES.get(timelineServiceIdentifier).serviceConfig);
          return RUNNING_SERVICES.get(timelineServiceIdentifier);
        }
        // if no compatible instance is found, create a new one
        EmbeddedTimelineService service = createAndStartService(context, embeddedTimelineServiceHostAddr, writeConfig,
            timelineServiceCreator, timelineServiceIdentifier);
        RUNNING_SERVICES.put(timelineServiceIdentifier, service);
        return service;
      }
    }
    // if not, create a new instance. If reuse is not enabled, there is no need to add it to RUNNING_SERVICES
    return createAndStartService(context, embeddedTimelineServiceHostAddr, writeConfig, timelineServiceCreator, timelineServiceIdentifier);
  }

  private static EmbeddedTimelineService createAndStartService(HoodieEngineContext context, String embeddedTimelineServiceHostAddr, HoodieWriteConfig writeConfig,
                                                               TimelineServiceCreator timelineServiceCreator,
                                                               TimelineServiceIdentifier timelineServiceIdentifier) throws IOException {
    EmbeddedTimelineService service = new EmbeddedTimelineService(context, embeddedTimelineServiceHostAddr, writeConfig, timelineServiceIdentifier);
    service.startServer(timelineServiceCreator);
    METRICS_REGISTRY.set(NUM_EMBEDDED_TIMELINE_SERVERS, NUM_SERVERS_RUNNING.incrementAndGet());
    return service;
  }

  public static void shutdownAllTimelineServers() {
    RUNNING_SERVICES.entrySet().forEach(entry -> {
      LOG.info("Closing Timeline server");
      entry.getValue().server.close();
      METRICS_REGISTRY.set(NUM_EMBEDDED_TIMELINE_SERVERS, NUM_SERVERS_RUNNING.decrementAndGet());
      LOG.info("Closed Timeline server");
    });
    RUNNING_SERVICES.clear();
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

    if (writeConfig.isUsingRemotePartitioner()) {
      timelineServiceConfBuilder.enableRemotePartitioner(true);
    }

    this.serviceConfig = timelineServiceConfBuilder.build();

    server = timelineServiceCreator.create(storageConf.newInstance(), serviceConfig, viewManager);
    serverPort = server.startService();
    LOG.info("Started embedded timeline server at {}:{}", hostAddr, serverPort);
  }

  @FunctionalInterface
  interface TimelineServiceCreator {
    TimelineService create(StorageConfiguration<?> storageConf, TimelineService.Config timelineServerConf,
                           FileSystemViewManager globalFileSystemViewManager) throws IOException;
  }

  private void setHostAddr(String embeddedTimelineServiceHostAddr) {
    if (embeddedTimelineServiceHostAddr != null) {
      LOG.info("Overriding hostIp to ({}) found in write conf. It was {}", embeddedTimelineServiceHostAddr, this.hostAddr);
      this.hostAddr = embeddedTimelineServiceHostAddr;
    } else {
      LOG.warn("Unable to find driver bind address from write config, use current host name");
      this.hostAddr = NetworkUtils.getHostname();
    }
  }

  /**
   * Retrieves proper view storage configs for remote clients to access this service.
   */
  public FileSystemViewStorageConfig getRemoteFileSystemViewConfig(HoodieWriteConfig clientWriteConfig) {
    FileSystemViewStorageType viewStorageType = clientWriteConfig.getClientSpecifiedViewStorageConfig()
        .shouldEnableBackupForRemoteFileSystemView()
        ? FileSystemViewStorageType.REMOTE_FIRST : FileSystemViewStorageType.REMOTE_ONLY;
    return FileSystemViewStorageConfig.newBuilder()
        .withStorageType(viewStorageType)
        .withRemoteServerHost(hostAddr)
        .withRemoteServerPort(serverPort)
        .withRemoteTimelineClientTimeoutSecs(clientWriteConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineClientTimeoutSecs())
        .withRemoteTimelineClientRetry(clientWriteConfig.getClientSpecifiedViewStorageConfig().isRemoteTimelineClientRetryEnabled())
        .withRemoteTimelineClientMaxRetryNumbers(clientWriteConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineClientMaxRetryNumbers())
        .withRemoteTimelineInitialRetryIntervalMs(clientWriteConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineInitialRetryIntervalMs())
        .withRemoteTimelineClientMaxRetryIntervalMs(clientWriteConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineClientMaxRetryIntervalMs())
        .withRemoteTimelineClientRetryExceptions(clientWriteConfig.getClientSpecifiedViewStorageConfig().getRemoteTimelineClientRetryExceptions())
        .withRemoteInitTimeline(writeConfig.getClientSpecifiedViewStorageConfig().isRemoteInitEnabled())
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

  /**
   * Stops the embedded timeline service for the given base path. If a timeline service is managing multiple tables, it will only be shutdown once all tables have been stopped.
   * @param basePath For the table to stop the service for
   */
  public void stopForBasePath(String basePath) {
    synchronized (SERVICE_LOCK) {
      basePaths.remove(basePath);
      if (basePaths.isEmpty()) {
        RUNNING_SERVICES.remove(timelineServiceIdentifier);
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

  private static TimelineServiceIdentifier getTimelineServiceIdentifier(String hostAddr, HoodieWriteConfig writeConfig) {
    return new TimelineServiceIdentifier(hostAddr, writeConfig.getMarkersType(), writeConfig.isMetadataTableEnabled(),
        writeConfig.isEarlyConflictDetectionEnable());
  }

  static class TimelineServiceIdentifier {
    private final String hostAddr;
    private final MarkerType markerType;
    private final boolean isMetadataEnabled;
    private final boolean isEarlyConflictDetectionEnable;

    public TimelineServiceIdentifier(String hostAddr,
                                     MarkerType markerType,
                                     boolean isMetadataEnabled,
                                     boolean isEarlyConflictDetectionEnable) {
      this.hostAddr = hostAddr;
      this.markerType = markerType;
      this.isMetadataEnabled = isMetadataEnabled;
      this.isEarlyConflictDetectionEnable = isEarlyConflictDetectionEnable;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimelineServiceIdentifier)) {
        return false;
      }
      TimelineServiceIdentifier that = (TimelineServiceIdentifier) o;
      if (this.hostAddr != null && that.hostAddr != null) {
        return isMetadataEnabled == that.isMetadataEnabled && isEarlyConflictDetectionEnable == that.isEarlyConflictDetectionEnable
            && hostAddr.equals(that.hostAddr) && markerType == that.markerType;
      } else {
        return (hostAddr == null && that.hostAddr == null);
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(hostAddr, markerType, isMetadataEnabled, isEarlyConflictDetectionEnable);
    }
  }
}