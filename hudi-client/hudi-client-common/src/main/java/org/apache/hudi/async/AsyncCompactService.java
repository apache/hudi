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
import org.apache.hudi.exception.HoodieCompactionAlreadyRunningException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
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
        recordFailure(ie);
        log.warn("Compactor executor thread got interrupted exception. Stopping", ie);
      } catch (IOException e) {
        recordFailure(e);
        log.error("Compactor executor failed due to IOException", e);
        throw new HoodieIOException(e.getMessage(), e);
      } catch (Exception e) {
        recordFailure(e);
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
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance())
        .setBasePath(client.getConfig().getBasePath())
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

  /**
   * Also true while a concurrent writer holds a fresh heartbeat on the instant, i.e. runs it: compact() would refuse
   * to run it. Only used before execution, when this attempt has not started its own heartbeat.
   */
  @Override
  protected boolean isTakenOverOrCompleted(BaseHoodieWriteClient client, String instantTime) throws IOException {
    HoodieWriteConfig config = client.getConfig();
    return super.isTakenOverOrCompleted(client, instantTime)
        || (config.getWriteConcurrencyMode().supportsMultiWriter() && heartbeatRemainingMs(config, getStorage(config), instantTime) >= 0);
  }

  @Override
  protected boolean canSkipFailedRetry(BaseHoodieWriteClient client, String instantTime, Exception failure) {
    return failure instanceof HoodieCompactionAlreadyRunningException
        || super.canSkipFailedRetry(client, instantTime, failure);
  }

  private void awaitHeartbeatExpired(HoodieWriteConfig config, String instantTime) throws IOException, InterruptedException {
    long heartbeatIntervalMs = config.getHoodieClientHeartbeatIntervalInMs();
    HoodieStorage storage = getStorage(config);
    while (!isShutdownRequested()) {
      long remainingMs = heartbeatRemainingMs(config, storage, instantTime);
      if (remainingMs < 0) {
        return;
      }
      log.info("Waiting up to {} ms for the heartbeat of compaction instant {} to expire before retrying it", remainingMs, instantTime);
      // re-read at least once per heartbeat interval, in case a concurrent writer is refreshing it; shutdown ends the wait
      waitUnlessShutdown(Math.min(remainingMs + 1, heartbeatIntervalMs));
    }
  }

  /**
   * Time until the heartbeat of the instant expires, negative if it has expired or doesn't exist. Uses the same expiry
   * as HoodieHeartbeatClient#isHeartbeatExpired, which compact() checks.
   */
  private long heartbeatRemainingMs(HoodieWriteConfig config, HoodieStorage storage, String instantTime) throws IOException {
    long maxHeartbeatIntervalMs = config.getHoodieClientHeartbeatIntervalInMs() * config.getHoodieClientHeartbeatTolerableMisses();
    long lastHeartbeatTime;
    try {
      lastHeartbeatTime = HoodieHeartbeatUtils.getLastHeartbeatTime(storage, config.getBasePath(), instantTime);
    } catch (FileNotFoundException e) {
      // deleted between its existence check and its read, e.g. by a concurrent writer completing the instant: a
      // missing heartbeat is an expired one, as when the existence check already misses it
      log.info("Heartbeat of compaction instant {} was deleted while reading it", instantTime);
      return -1;
    }
    return lastHeartbeatTime + maxHeartbeatIntervalMs - System.currentTimeMillis();
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
