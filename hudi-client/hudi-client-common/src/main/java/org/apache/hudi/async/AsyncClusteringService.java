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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Async clustering service that runs in a separate thread.
 * Currently, only one clustering thread is allowed to run at any time.
 */
@Slf4j
public abstract class AsyncClusteringService extends HoodieAsyncTableService {

  public static final String CLUSTERING_POOL_NAME = "hoodiecluster";
  private static final long serialVersionUID = 1L;
  private static final long WRITE_CLIENT_POLL_MS = 1000;
  private final int maxConcurrentClustering;
  protected transient HoodieEngineContext context;
  private transient BaseClusterer clusteringClient;

  public AsyncClusteringService(HoodieEngineContext context, BaseHoodieWriteClient writeClient) {
    this(context, writeClient, false);
  }

  public AsyncClusteringService(HoodieEngineContext context, BaseHoodieWriteClient writeClient, boolean runInDaemonMode) {
    super(writeClient.getConfig(), runInDaemonMode);
    this.clusteringClient = createClusteringClient(writeClient);
    this.maxConcurrentClustering = 1;
    this.context = context;
  }

  protected abstract BaseClusterer createClusteringClient(BaseHoodieWriteClient client);

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
        log.info("Setting pool name for clustering to {}", CLUSTERING_POOL_NAME);
        context.setProperty(EngineProperty.CLUSTERING_POOL_NAME, CLUSTERING_POOL_NAME);
        while (!isShutdownRequested()) {
          final String instant = fetchNextAsyncServiceInstant();
          if (null != instant) {
            log.info("Starting clustering for instant {}", instant);
            BaseHoodieWriteClient runClient = clusteringClient.clusteringClient;
            try {
              clusteringClient.cluster(instant);
            } catch (Exception e) {
              if (!isRetryableFailure(runClient, e)) {
                throw e;
              }
              log.warn("Clustering for instant {} failed with a write client that was replaced, retrying it with the new write client",
                  instant, e);
              awaitWriteClientReplaced(runClient);
              if (isPendingClustering(instant)) {
                enqueuePendingAsyncServiceInstant(instant);
              } else {
                log.info("Clustering for instant {} is no longer pending, not retrying it", instant);
              }
              continue;
            }
            log.info("Finished clustering for instant {}", instant);
          }
        }
        log.info("Clustering executor shutting down properly");
      } catch (InterruptedException ie) {
        hasError = true;
        log.warn("Clustering executor got interrupted exception! Stopping", ie);
      } catch (IOException e) {
        hasError = true;
        log.error("Clustering executor failed due to IOException", e);
        throw new HoodieIOException(e.getMessage(), e);
      } catch (Exception e) {
        hasError = true;
        log.error("Clustering executor failed", e);
        throw e;
      }
      return true;
    }, executor)).toArray(CompletableFuture[]::new)), executor);
  }

  /**
   * Switches the client used by later clustering runs. Does not wait for a clustering that is running: it can run
   * for a long time, and the writer calling this must not be blocked until it completes.
   */
  public synchronized void updateWriteClient(BaseHoodieWriteClient writeClient) {
    this.clusteringClient.updateWriteClient(writeClient);
    notifyAll();
  }

  /**
   * Whether a clustering run that failed with the given exception is retried. The run used {@code runClient}, the
   * client of the service when the run started. By default no failure is retried and the service stops with it.
   */
  protected boolean isRetryableFailure(BaseHoodieWriteClient runClient, Exception failure) {
    return false;
  }

  /**
   * Whether the clustering instant is still pending, i.e. a retry of it has work to do.
   */
  protected boolean isPendingClustering(String instantTime) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance())
        .setBasePath(clusteringClient.clusteringClient.getConfig().getBasePath())
        .build();
    return metaClient.getActiveTimeline().filterPendingClusteringTimeline().containsInstant(instantTime);
  }

  /**
   * Waits until the writer has switched this service away from the given client.
   */
  private synchronized void awaitWriteClientReplaced(BaseHoodieWriteClient client) throws InterruptedException {
    while (!isShutdownRequested() && clusteringClient.clusteringClient == client) {
      wait(WRITE_CLIENT_POLL_MS);
    }
  }
}
