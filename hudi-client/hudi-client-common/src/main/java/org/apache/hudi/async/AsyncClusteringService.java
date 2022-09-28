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

package org.apache.hudi.async;

import org.apache.hudi.client.BaseClusterer;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Async clustering service that runs in a separate thread.
 * Currently, only one clustering thread is allowed to run at any time.
 */
public abstract class AsyncClusteringService extends HoodieAsyncTableService {

  public static final String CLUSTERING_POOL_NAME = "hoodiecluster";
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(AsyncClusteringService.class);
  private final int maxConcurrentClustering;
  protected transient HoodieEngineContext context;
  // will be reinstantiated if write config is updated by external caller.
  private BaseClusterer clusteringClient;

  public AsyncClusteringService(HoodieEngineContext context, HoodieWriteConfig writeConfig, Option<EmbeddedTimelineService> embeddedTimelineService) {
    this(context, writeConfig, embeddedTimelineService, false);
  }

  public AsyncClusteringService(HoodieEngineContext context, HoodieWriteConfig writeConfig, Option<EmbeddedTimelineService> embeddedTimelineService, boolean runInDaemonMode) {
    super(writeConfig, embeddedTimelineService, runInDaemonMode);
    this.maxConcurrentClustering = 1;
    this.context = context;
  }

  protected abstract BaseClusterer createClusteringClient();

  /**
   * Start clustering service.
   */
  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    ExecutorService executor = Executors.newFixedThreadPool(maxConcurrentClustering,
        new CustomizedThreadFactory("async_clustering_thread", isRunInDaemonMode()));
    return Pair.of(CompletableFuture.allOf(IntStream.range(0, maxConcurrentClustering).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
      try {
        // Set Compactor Pool Name for allowing users to prioritize compaction
        LOG.info("Setting pool name for clustering to " + CLUSTERING_POOL_NAME);
        context.setProperty(EngineProperty.CLUSTERING_POOL_NAME, CLUSTERING_POOL_NAME);
        while (!isShutdownRequested()) {
          final HoodieInstant instant = fetchNextAsyncServiceInstant();
          if (null != instant) {
            LOG.info("Starting clustering for instant " + instant);
            synchronized (writeConfigUpdateLock) {
              // re-instantiate only for first time or if write config is updated externally
              if (clusteringClient == null || isWriteConfigUpdated.get()) {
                if (clusteringClient != null) {
                  clusteringClient.close();
                }
                clusteringClient = createClusteringClient();
                isWriteConfigUpdated.set(false);
              }
            }
            clusteringClient.cluster(instant);
            LOG.info("Finished clustering for instant " + instant);
            clusteringClient.close();
            clusteringClient = null;
          }
        }
        LOG.info("Clustering executor shutting down properly");
      } catch (InterruptedException ie) {
        hasError = true;
        LOG.warn("Clustering executor got interrupted exception! Stopping", ie);
      } catch (IOException e) {
        hasError = true;
        LOG.error("Clustering executor failed due to IOException", e);
        throw new HoodieIOException(e.getMessage(), e);
      } catch (Exception e) {
        hasError = true;
        LOG.error("Clustering executor failed", e);
        throw e;
      }
      return true;
    }, executor)).toArray(CompletableFuture[]::new)), executor);
  }
}
