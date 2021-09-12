/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.async;

import org.apache.hudi.client.AbstractCompactor;
import org.apache.hudi.client.AbstractHoodieWriteClient;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
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
 * Async Compactor Service that runs in separate thread. Currently, only one compactor is allowed to run at any time.
 */
public abstract class AsyncCompactService extends HoodieAsyncService {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(AsyncCompactService.class);

  /**
   * This is the job pool used by async compaction.
   */
  public static final String COMPACT_POOL_NAME = "hoodiecompact";

  private final int maxConcurrentCompaction;
  private transient AbstractCompactor compactor;
  protected transient HoodieEngineContext context;

  public AsyncCompactService(HoodieEngineContext context, AbstractHoodieWriteClient client) {
    this(context, client, false);
  }

  public AsyncCompactService(HoodieEngineContext context, AbstractHoodieWriteClient client, boolean runInDaemonMode) {
    super(runInDaemonMode);
    this.context = context;
    this.compactor = createCompactor(client);
    this.maxConcurrentCompaction = 1;
  }

  protected abstract AbstractCompactor createCompactor(AbstractHoodieWriteClient client);

  /**
   * Start Compaction Service.
   */
  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    ExecutorService executor = Executors.newFixedThreadPool(maxConcurrentCompaction,
        r -> {
        Thread t = new Thread(r, "async_compact_thread");
        t.setDaemon(isRunInDaemonMode());
        return t;
      });
    return Pair.of(CompletableFuture.allOf(IntStream.range(0, maxConcurrentCompaction).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
      try {
        // Set Compactor Pool Name for allowing users to prioritize compaction
        LOG.info("Setting pool name for compaction to " + COMPACT_POOL_NAME);
        context.setProperty(EngineProperty.COMPACTION_POOL_NAME, COMPACT_POOL_NAME);

        while (!isShutdownRequested()) {
          final HoodieInstant instant = fetchNextAsyncServiceInstant();

          if (null != instant) {
            LOG.info("Starting Compaction for instant " + instant);
            compactor.compact(instant);
            LOG.info("Finished Compaction for instant " + instant);
          }
        }
        LOG.info("Compactor shutting down properly!!");
      } catch (InterruptedException ie) {
        LOG.warn("Compactor executor thread got interrupted exception. Stopping", ie);
      } catch (IOException e) {
        LOG.error("Compactor executor failed", e);
        throw new HoodieIOException(e.getMessage(), e);
      }
      return true;
    }, executor)).toArray(CompletableFuture[]::new)), executor);
  }


  /**
   * Check whether compactor thread needs to be stopped.
   * @return
   */
  protected boolean shouldStopCompactor() {
    return false;
  }

  public synchronized void updateWriteClient(AbstractHoodieWriteClient writeClient) {
    this.compactor.updateWriteClient(writeClient);
  }
}
