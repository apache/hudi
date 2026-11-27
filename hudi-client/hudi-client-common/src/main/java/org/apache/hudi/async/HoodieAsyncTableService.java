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
import org.apache.hudi.client.RunsTableService;
import org.apache.hudi.config.HoodieWriteConfig;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class HoodieAsyncTableService extends HoodieAsyncService implements RunsTableService {

  protected HoodieWriteConfig writeConfig;

  // instants re-enqueued by runWithClientHandoff for a retry and not yet dequeued again
  private final transient Set<String> retriedInstants = ConcurrentHashMap.newKeySet();

  protected HoodieAsyncTableService(HoodieWriteConfig writeConfig, boolean runInDaemonMode) {
    super(runInDaemonMode);
    this.writeConfig = writeConfig;
  }

  @Override
  public void start(Function<Boolean, Boolean> onShutdownCallback) {
    if (!tableServicesEnabled(writeConfig)) {
      return;
    }
    super.start(onShutdownCallback);
  }

  /**
   * Runs a table service instant with one write client.
   */
  @FunctionalInterface
  protected interface WriteClientRun {
    void run(BaseHoodieWriteClient client, String instantTime) throws IOException;
  }

  /**
   * Runs the table service for the instant with the write client the service has when the run starts. That same
   * client runs the instant and is passed to {@link #isRetryableFailure}, so a failure is never attributed to a
   * different client than the one it happened on. On a retryable failure, waits until the writer has switched the
   * service to another client, lets the service prepare the retry, and re-enqueues the instant if it is still pending.
   *
   * <p>Other instants run before a re-enqueued instant comes up again, and meanwhile a concurrent writer may complete
   * it or start running it. So when it comes up, it is skipped if that happened (see {@link #isTakenOverOrCompleted}),
   * and a failure of the retry is checked for the same, since that may also happen between the check and the run.
   *
   * @param action        name of the table service, for logging
   * @param currentClient the write client the service currently runs with
   * @param run           runs the instant with the given client
   * @return whether the run completed; false if it failed and was handed back for a retry or dropped
   */
  protected boolean runWithClientHandoff(String action, String instantTime, Supplier<BaseHoodieWriteClient> currentClient,
                                         WriteClientRun run) throws IOException, InterruptedException {
    BaseHoodieWriteClient runClient = currentClient.get();
    boolean isRetry = retriedInstants.remove(instantTime);
    if (isRetry && isTakenOverOrCompleted(runClient, instantTime)) {
      log.info("{} for instant {} is completed or run by another writer since it failed, not retrying it", action, instantTime);
      return false;
    }
    try {
      run.run(runClient, instantTime);
      return true;
    } catch (Exception e) {
      if (!isRetryableFailure(runClient, e)) {
        if (isRetry && canSkipFailedRetrySafely(runClient, instantTime, e)) {
          log.warn("Retry of {} for instant {} failed, and the instant was completed or taken over by another writer meanwhile, "
              + "not retrying it again", action, instantTime, e);
          return false;
        }
        throw e;
      }
      log.warn("{} for instant {} failed with a write client that was replaced, retrying it with the new write client",
          action, instantTime, e);
      awaitWriteClientReplaced(runClient, currentClient);
      if (isShutdownRequested()) {
        return false;
      }
      BaseHoodieWriteClient retryClient = currentClient.get();
      // checked before preparing the retry too: preparing it may wait, which an instant completed meanwhile doesn't need
      if (isPendingInstant(retryClient, instantTime)) {
        prepareRetry(retryClient, instantTime);
        if (isShutdownRequested()) {
          return false;
        }
        // Revalidate when dequeued, since other instants may run before this retry.
        retriedInstants.add(instantTime);
        enqueuePendingAsyncServiceInstant(instantTime);
        return false;
      }
      log.info("{} for instant {} is no longer pending, not retrying it", action, instantTime);
      return false;
    }
  }

  /**
   * Whether a run that failed with the given exception is retried. The run used {@code runClient}, the client of the
   * service when the run started. By default no failure is retried and the service stops with it.
   */
  protected boolean isRetryableFailure(BaseHoodieWriteClient runClient, Exception failure) {
    return false;
  }

  /**
   * Whether the instant is still pending, i.e. a retry of it has work to do. Only consulted for retryable failures.
   */
  protected boolean isPendingInstant(BaseHoodieWriteClient client, String instantTime) {
    return true;
  }

  /**
   * Whether a concurrent writer completed the instant or is running it, so a retry of it has nothing to do. By default,
   * whether the instant is no longer pending.
   */
  protected boolean isTakenOverOrCompleted(BaseHoodieWriteClient client, String instantTime) throws IOException {
    return !isPendingInstant(client, instantTime);
  }

  /**
   * Whether a failed retry can be skipped. A heartbeat after execution may belong to the failed run itself, so
   * unlike the pre-execution check, this must not infer takeover from heartbeat freshness alone.
   */
  protected boolean canSkipFailedRetry(BaseHoodieWriteClient client, String instantTime, Exception failure) {
    return !isPendingInstant(client, instantTime);
  }

  private boolean canSkipFailedRetrySafely(BaseHoodieWriteClient client, String instantTime, Exception failure) {
    try {
      return canSkipFailedRetry(client, instantTime, failure);
    } catch (Exception checkFailure) {
      failure.addSuppressed(checkFailure);
      return false;
    }
  }

  /**
   * Prepares the retry of a failed run of the instant with {@code retryClient}, before it is re-enqueued.
   */
  protected void prepareRetry(BaseHoodieWriteClient retryClient, String instantTime) throws IOException, InterruptedException {
  }

  /**
   * Waits up to the given time, returning early when shutdown is requested (see {@link #onShutdownRequested}) or the
   * writer switches this service to another client. Callers re-check what they wait for after it returns.
   */
  protected synchronized void waitUnlessShutdown(long millis) throws InterruptedException {
    if (!isShutdownRequested()) {
      wait(millis);
    }
  }

  @Override
  protected synchronized void onShutdownRequested() {
    notifyAll();
  }

  /**
   * Waits until the writer has switched this service away from the given client. Services switch clients while
   * holding this monitor and notify on it, and {@link #onShutdownRequested} notifies on it too.
   */
  private synchronized void awaitWriteClientReplaced(BaseHoodieWriteClient client, Supplier<BaseHoodieWriteClient> currentClient)
      throws InterruptedException {
    while (!isShutdownRequested() && currentClient.get() == client) {
      wait();
    }
  }
}
