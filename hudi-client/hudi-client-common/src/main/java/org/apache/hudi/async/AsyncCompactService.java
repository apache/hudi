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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.heartbeat.HoodieHeartbeatUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Async Compactor Service that runs in separate thread. Currently, only one compactor is allowed to run at any time.
 */
@Slf4j
public abstract class AsyncCompactService extends HoodieAsyncTableService {

  /**
   * This is the job pool used by async compaction.
   */
  public static final String COMPACT_POOL_NAME = "hoodiecompact";
  private static final long serialVersionUID = 1L;
  private final int maxConcurrentCompaction;
  protected transient HoodieEngineContext context;
  private final transient BaseCompactor compactor;

  public AsyncCompactService(HoodieEngineContext context, BaseHoodieWriteClient client) {
    this(context, client, false);
  }

  public AsyncCompactService(HoodieEngineContext context, BaseHoodieWriteClient client, boolean runInDaemonMode) {
    super(client.getConfig(), runInDaemonMode);
    this.context = context;
    this.compactor = createCompactor(client);
    this.maxConcurrentCompaction = 1;
  }

  protected abstract BaseCompactor createCompactor(BaseHoodieWriteClient client);

  /**
   * Start Compaction Service.
   */
  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    ExecutorService executor = Executors.newFixedThreadPool(maxConcurrentCompaction,
        new CustomizedThreadFactory("async_compact_thread", isRunInDaemonMode()));
    return Pair.of(CompletableFuture.allOf(IntStream.range(0, maxConcurrentCompaction).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
      try {
        // Set Compactor Pool Name for allowing users to prioritize compaction
        log.info("Setting pool name for compaction to {}", COMPACT_POOL_NAME);
        context.setProperty(EngineProperty.COMPACTION_POOL_NAME, COMPACT_POOL_NAME);

        while (!isShutdownRequested()) {
          final String instantTime = fetchNextAsyncServiceInstant();

          if (null != instantTime) {
            log.info("Starting Compaction for instant {}", instantTime);
            if (runWithClientHandoff("Compaction", instantTime, () -> compactor.compactionClient, compactor::compact)) {
              log.info("Finished Compaction for instant {}", instantTime);
            }
          }
        }
        log.info("Compactor shutting down properly!!");
      } catch (InterruptedException ie) {
        hasError = true;
        log.warn("Compactor executor thread got interrupted exception. Stopping", ie);
      } catch (IOException e) {
        hasError = true;
        log.error("Compactor executor failed due to IOException", e);
        throw new HoodieIOException(e.getMessage(), e);
      } catch (Exception e) {
        hasError = true;
        log.error("Compactor executor failed", e);
        throw e;
      }
      return true;
    }, executor)).toArray(CompletableFuture[]::new)), executor);
  }

  /**
   * Check whether compactor thread needs to be stopped.
   *
   * @return
   */
  protected boolean shouldStopCompactor() {
    return false;
  }

  /**
   * Whether the compaction instant is still pending, i.e. a retry of it has work to do.
   */
  @Override
  protected boolean isPendingInstant(BaseHoodieWriteClient client, String instantTime) {
    return isPendingCompaction(instantTime);
  }

  protected boolean isPendingCompaction(String instantTime) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance())
        .setBasePath(compactor.compactionClient.getConfig().getBasePath())
        .build();
    return metaClient.getActiveTimeline().filterPendingCompactionTimeline().containsInstant(instantTime);
  }

  /**
   * With multi-writer concurrency, a compaction run holds a heartbeat on its instant, and a new run refuses to start
   * while that heartbeat is fresh (it may belong to a concurrent writer). A run that failed because its client was
   * closed leaves its heartbeat behind: closing a client stops its heartbeats without deleting their files, and the
   * failed run can no longer delete it either. So wait for the heartbeat to expire before retrying. Without knowing
   * whose heartbeat it is, this is safe either way: an orphaned heartbeat expires within the heartbeat timeout, and a
   * heartbeat a concurrent writer keeps refreshing is waited out, after which the instant is no longer pending.
   */
  @Override
  protected void prepareRetry(BaseHoodieWriteClient retryClient, String instantTime) throws IOException, InterruptedException {
    HoodieWriteConfig config = retryClient.getConfig();
    if (config.getWriteConcurrencyMode().supportsMultiWriter()) {
      awaitHeartbeatExpired(config, instantTime);
    }
  }

  private void awaitHeartbeatExpired(HoodieWriteConfig config, String instantTime) throws IOException, InterruptedException {
    // same expiry as HoodieHeartbeatClient#isHeartbeatExpired, which compact() checks
    long heartbeatIntervalMs = config.getHoodieClientHeartbeatIntervalInMs();
    long maxHeartbeatIntervalMs = heartbeatIntervalMs * config.getHoodieClientHeartbeatTolerableMisses();
    HoodieStorage storage = getStorage(config);
    while (!isShutdownRequested()) {
      long lastHeartbeatTime = HoodieHeartbeatUtils.getLastHeartbeatTime(storage, config.getBasePath(), instantTime);
      long remainingMs = lastHeartbeatTime + maxHeartbeatIntervalMs - System.currentTimeMillis();
      if (remainingMs < 0) {
        return;
      }
      log.info("Waiting up to {} ms for the heartbeat of compaction instant {} to expire before retrying it", remainingMs, instantTime);
      // re-read at least once per heartbeat interval, in case a concurrent writer is refreshing it
      Thread.sleep(Math.min(remainingMs + 1, heartbeatIntervalMs));
    }
  }

  protected HoodieStorage getStorage(HoodieWriteConfig config) {
    return HoodieStorageUtils.getStorage(config.getBasePath(), context.getStorageConf());
  }

  /**
   * Switches the client used by later compaction runs. Does not wait for a compaction that is running: it can run
   * for a long time, and the writer calling this must not be blocked until it completes.
   */
  public synchronized void updateWriteClient(BaseHoodieWriteClient writeClient) {
    this.compactor.updateWriteClient(writeClient);
    notifyAll();
  }
}
