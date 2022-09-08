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

import org.apache.hudi.client.BaseCompactor;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CustomizedThreadFactory;
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
 * Async Compactor Service that runs in separate thread. Currently, only one compactor is allowed to run at any time.
 */
public abstract class AsyncCompactService extends HoodieAsyncTableService {

  /**
   * This is the job pool used by async compaction.
   */
  public static final String COMPACT_POOL_NAME = "hoodiecompact";
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(AsyncCompactService.class);
  private final int maxConcurrentCompaction;
  private final Object writeConfigUpdateLock = new Object();
  protected transient HoodieEngineContext context;
  protected HoodieWriteConfig writeConfig;

  public AsyncCompactService(HoodieEngineContext context, HoodieWriteConfig writeConfig) {
    this(context, writeConfig, false);
  }

  public AsyncCompactService(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean runInDaemonMode) {
    super(writeConfig, runInDaemonMode);
    this.context = context;
    this.maxConcurrentCompaction = 1;
  }

  protected abstract BaseCompactor createCompactor();

  /**
   * Start Compaction Service.
   */
  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    ExecutorService executor = Executors.newFixedThreadPool(maxConcurrentCompaction,
        new CustomizedThreadFactory("async_compact_thread", isRunInDaemonMode()));
    return Pair.of(CompletableFuture.allOf(IntStream.range(0, maxConcurrentCompaction).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
      BaseCompactor compactor = null;
      try {
        // Set Compactor Pool Name for allowing users to prioritize compaction
        LOG.info("Setting pool name for compaction to " + COMPACT_POOL_NAME);
        context.setProperty(EngineProperty.COMPACTION_POOL_NAME, COMPACT_POOL_NAME);

        while (!isShutdownRequested()) {
          final HoodieInstant instant = fetchNextAsyncServiceInstant();

          if (null != instant) {
            LOG.info("Starting Compaction for instant " + instant);
            synchronized (writeConfigUpdateLock) {
              compactor = createCompactor();
            }
            compactor.compact(instant);
            LOG.info("Finished Compaction for instant " + instant);
            compactor.close();
            compactor = null;
          }
        }
        LOG.info("Compactor shutting down properly!!");
      } catch (InterruptedException ie) {
        hasError = true;
        LOG.warn("Compactor executor thread got interrupted exception. Stopping", ie);
      } catch (IOException e) {
        hasError = true;
        LOG.error("Compactor executor failed due to IOException", e);
        throw new HoodieIOException(e.getMessage(), e);
      } catch (Exception e) {
        hasError = true;
        LOG.error("Compactor executor failed", e);
        throw e;
      } finally {
        if (compactor != null) {
          compactor.close();
        }
      }
      return true;
    }, executor)).toArray(CompletableFuture[]::new)), executor);
  }
}
