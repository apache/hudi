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

package org.apache.hudi.client;

import org.apache.hudi.callback.HoodieClientInitCallback;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Abstract class taking care of holding common member variables (FileSystem, SparkContext, HoodieConfigs) Also, manages
 * embedded timeline-server if enabled.
 */
public abstract class BaseHoodieClient implements Serializable, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseHoodieClient.class);

  private static final long serialVersionUID = 1L;
  protected final transient HoodieStorage storage;
  protected final transient HoodieEngineContext context;
  protected final transient StorageConfiguration<?> storageConf;
  protected final transient HoodieMetrics metrics;
  protected final HoodieWriteConfig config;
  protected final String basePath;
  protected final HoodieHeartbeatClient heartbeatClient;
  protected final TransactionManager txnManager;
  protected final TimeGenerator timeGenerator;

  /**
   * Timeline Server has the same lifetime as that of Client. Any operations done on the same timeline service will be
   * able to take advantage of the cached file-system view. New completed actions will be synced automatically in an
   * incremental fashion.
   */
  private transient Option<EmbeddedTimelineService> timelineServer;
  private final boolean shouldStopTimelineServer;

  protected BaseHoodieClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, Option.empty());
  }

  protected BaseHoodieClient(HoodieEngineContext context, HoodieWriteConfig clientConfig,
                             Option<EmbeddedTimelineService> timelineServer) {
    this(context, clientConfig, timelineServer, buildTransactionManager(context, clientConfig), buildTimeGenerator(context, clientConfig));
  }

  private static TimeGenerator buildTimeGenerator(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    return TimeGenerators.getTimeGenerator(clientConfig.getTimeGeneratorConfig(), context.getStorageConf());
  }

  private static TransactionManager buildTransactionManager(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    return new TransactionManager(clientConfig, HoodieStorageUtils.getStorage(clientConfig.getBasePath(), context.getStorageConf()));
  }

  @VisibleForTesting
  BaseHoodieClient(HoodieEngineContext context, HoodieWriteConfig clientConfig,
                   Option<EmbeddedTimelineService> timelineServer, TransactionManager transactionManager, TimeGenerator timeGenerator) {
    this.storageConf = context.getStorageConf();
    this.storage = HoodieStorageUtils.getStorage(clientConfig.getBasePath(), storageConf);
    this.context = context;
    this.basePath = clientConfig.getBasePath();
    this.config = clientConfig;
    this.timelineServer = timelineServer;
    shouldStopTimelineServer = !timelineServer.isPresent();
    this.heartbeatClient = new HoodieHeartbeatClient(storage, this.basePath,
        clientConfig.getHoodieClientHeartbeatIntervalInMs(),
        clientConfig.getHoodieClientHeartbeatTolerableMisses());
    this.metrics = new HoodieMetrics(config, storage);
    this.txnManager = transactionManager;
    this.timeGenerator = timeGenerator;
    startEmbeddedServerView();
    initWrapperFSMetrics();
    runClientInitCallbacks();
  }

  /**
   * Releases any resources used by the client.
   */
  @Override
  public void close() {
    stopEmbeddedServerView(true);
    this.context.setJobStatus("", "");
    this.heartbeatClient.close();
    this.txnManager.close();
  }

  private synchronized void stopEmbeddedServerView(boolean resetViewStorageConfig) {
    if (timelineServer.isPresent() && shouldStopTimelineServer) {
      // Stop only if owner
      LOG.info("Stopping Timeline service !!");
      timelineServer.get().stopForBasePath(basePath);
    }

    timelineServer = Option.empty();
    // Reset Storage Config to Client specified config
    if (resetViewStorageConfig) {
      config.resetViewStorageConfig();
    }
  }

  private synchronized void startEmbeddedServerView() {
    if (config.isEmbeddedTimelineServerEnabled()) {
      if (!timelineServer.isPresent()) {
        // Run Embedded Timeline Server
        try {
          timelineServer = Option.of(EmbeddedTimelineServerHelper.createEmbeddedTimelineService(context, config));
        } catch (IOException e) {
          LOG.warn("Unable to start timeline service. Proceeding as if embedded server is disabled", e);
          stopEmbeddedServerView(false);
        }
      } else {
        LOG.debug("Timeline Server already running. Not restarting the service");
      }
    } else {
      LOG.info("Embedded Timeline Server is disabled. Not starting timeline service");
    }
  }

  private void runClientInitCallbacks() {
    String callbackClassNames = config.getClientInitCallbackClassNames();
    if (StringUtils.isNullOrEmpty(callbackClassNames)) {
      return;
    }
    Arrays.stream(callbackClassNames.split(",")).forEach(callbackClass -> {
      Object callback = ReflectionUtils.loadClass(callbackClass);
      if (!(callback instanceof HoodieClientInitCallback)) {
        throw new HoodieException(callbackClass + " is not a subclass of "
            + HoodieClientInitCallback.class.getName());
      }
      ((HoodieClientInitCallback) callback).call(this);
    });
  }

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieEngineContext getEngineContext() {
    return context;
  }

  protected void initWrapperFSMetrics() {
    // no-op.
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(config.getBasePath())
        .setLoadActiveTimelineOnLoad(loadActiveTimelineOnLoad)
        .setConsistencyGuardConfig(config.getConsistencyGuardConfig())
        .setTimeGeneratorConfig(config.getTimeGeneratorConfig())
        .setFileSystemRetryConfig(config.getFileSystemRetryConfig())
        .setMetaserverConfig(config.getProps()).build();
  }

  /**
   * Returns next instant time in the correct format.
   *
   * @param shouldLock Whether to lock the context to get the instant time.
   */
  public String createNewInstantTime(boolean shouldLock) {
    return TimelineUtils.generateInstantTime(shouldLock, timeGenerator);
  }

  public Option<EmbeddedTimelineService> getTimelineServer() {
    return timelineServer;
  }

  public HoodieHeartbeatClient getHeartbeatClient() {
    return heartbeatClient;
  }

  /**
   * Resolve write conflicts before commit.
   *
   * @param table A hoodie table instance created after transaction starts so that the latest commits and files are captured.
   * @param metadata Current committing instant's metadata
   * @param pendingInflightAndRequestedInstants Pending instants on the timeline
   *
   * @see {@link BaseHoodieWriteClient#preCommit}
   * @see {@link BaseHoodieTableServiceClient#preCommit}
   */
  protected void resolveWriteConflict(HoodieTable table, HoodieCommitMetadata metadata, Set<String> pendingInflightAndRequestedInstants) {
    Timer.Context conflictResolutionTimer = metrics.getConflictResolutionCtx();
    try {
      TransactionUtils.resolveWriteConflictIfAny(table, this.txnManager.getCurrentTransactionOwner(),
          Option.of(metadata), config, txnManager.getLastCompletedTransactionOwner(), true, pendingInflightAndRequestedInstants);
      metrics.emitConflictResolutionSuccessful();
    } catch (HoodieWriteConflictException e) {
      metrics.emitConflictResolutionFailed();
      throw e;
    } finally {
      if (conflictResolutionTimer != null) {
        conflictResolutionTimer.stop();
      }
    }
  }

  /**
   * Finalize Write operation.
   *
   * @param table HoodieTable
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieTable table, String instantTime, List<HoodieWriteStat> stats) {
    try {
      final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
      table.finalizeWrite(context, instantTime, stats);
      if (finalizeCtx != null) {
        Option<Long> durationInMs = Option.of(metrics.getDurationInMs(finalizeCtx.stop()));
        durationInMs.ifPresent(duration -> {
          LOG.info("Finalize write elapsed time (milliseconds): {}", duration);
          metrics.updateFinalizeWriteMetrics(duration, stats.size());
        });
      }
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  /**
   * Write the HoodieCommitMetadata to metadata table if available.
   *
   * @param table         {@link HoodieTable} of interest.
   * @param instantTime   instant time of the commit.
   * @param metadata      instance of {@link HoodieCommitMetadata}.
   */
  protected void writeTableMetadata(HoodieTable table, String instantTime, HoodieCommitMetadata metadata) {
    context.setJobStatus(this.getClass().getSimpleName(), "Committing to metadata table: " + config.getTableName());
    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(instantTime);
    if (metadataWriterOpt.isPresent()) {
      try (HoodieTableMetadataWriter metadataWriter = metadataWriterOpt.get()) {
        metadataWriter.update(metadata, instantTime);
      } catch (Exception e) {
        if (e instanceof HoodieException) {
          throw (HoodieException) e;
        } else {
          throw new HoodieException("Failed to update metadata", e);
        }
      }
    }
  }

  /**
   * Updates the cols being indexed with column stats. This is for tracking purpose so that queries can leverage col stats
   * from MDT only for indexed columns.
   * @param metaClient instance of {@link HoodieTableMetaClient} of interest.
   * @param columnsToIndex list of columns to index.
   */
  protected abstract void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex);

  protected void executeUsingTxnManager(Option<HoodieInstant> ownerInstant, Runnable r) {
    this.txnManager.beginStateChange(ownerInstant, Option.empty());
    try {
      r.run();
    } finally {
      this.txnManager.endStateChange(ownerInstant);
    }
  }

  public TransactionManager getTransactionManager() {
    return txnManager;
  }

  protected boolean isStreamingWriteToMetadataEnabled(HoodieTable table) {
    return config.isMetadataTableEnabled()
        && config.isMetadataStreamingWritesEnabled(table.getMetaClient().getTableConfig().getTableVersion());
  }
}
