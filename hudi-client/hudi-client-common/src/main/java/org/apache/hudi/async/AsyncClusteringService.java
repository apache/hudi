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

import org.apache.hudi.client.AbstractClusteringClient;
import org.apache.hudi.client.AbstractHoodieWriteClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
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
public abstract class AsyncClusteringService extends HoodieAsyncService {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(AsyncClusteringService.class);

  private final int maxConcurrentClustering;
  private transient AbstractClusteringClient clusteringClient;

  public AsyncClusteringService(AbstractHoodieWriteClient writeClient) {
    this(writeClient, false);
  }

  public AsyncClusteringService(AbstractHoodieWriteClient writeClient, boolean runInDaemonMode) {
    super(runInDaemonMode);
    this.clusteringClient = createClusteringClient(writeClient);
    this.maxConcurrentClustering = 1;
  }

  protected abstract AbstractClusteringClient createClusteringClient(AbstractHoodieWriteClient client);

  /**
   * Start clustering service.
   */
  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    ExecutorService executor = Executors.newFixedThreadPool(maxConcurrentClustering,
        r -> {
          Thread t = new Thread(r, "async_clustering_thread");
          t.setDaemon(isRunInDaemonMode());
          return t;
        });

    return Pair.of(CompletableFuture.allOf(IntStream.range(0, maxConcurrentClustering).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
      try {
        while (!isShutdownRequested()) {
          final HoodieInstant instant = fetchNextAsyncServiceInstant();
          if (null != instant) {
            LOG.info("Starting clustering for instant " + instant);
            clusteringClient.cluster(instant);
            LOG.info("Finished clustering for instant " + instant);
          }
        }
        LOG.info("Clustering executor shutting down properly");
      } catch (InterruptedException ie) {
        LOG.warn("Clustering executor got interrupted exception! Stopping", ie);
      } catch (IOException e) {
        LOG.error("Clustering executor failed", e);
        throw new HoodieIOException(e.getMessage(), e);
      }
      return true;
    }, executor)).toArray(CompletableFuture[]::new)), executor);
  }

  /**
   * Update the write client to be used for clustering.
   */
  public synchronized void updateWriteClient(AbstractHoodieWriteClient writeClient) {
    this.clusteringClient.updateWriteClient(writeClient);
  }
}
