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
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class HoodieAsyncTableService extends HoodieAsyncService implements RunsTableService {

  private static final long WRITE_CLIENT_POLL_MS = 1000;

  protected HoodieWriteConfig writeConfig;

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
   * @param action        name of the table service, for logging
   * @param currentClient the write client the service currently runs with
   * @param run           runs the instant with the given client
   * @return whether the run completed; false if it failed and was handed back for a retry or dropped
   */
  protected boolean runWithClientHandoff(String action, String instantTime, Supplier<BaseHoodieWriteClient> currentClient,
                                         WriteClientRun run) throws IOException, InterruptedException {
    BaseHoodieWriteClient runClient = currentClient.get();
    try {
      run.run(runClient, instantTime);
      return true;
    } catch (Exception e) {
      if (!isRetryableFailure(runClient, e)) {
        throw e;
      }
      log.warn("{} for instant {} failed with a write client that was replaced, retrying it with the new write client",
          action, instantTime, e);
      awaitWriteClientReplaced(runClient, currentClient);
      if (isShutdownRequested()) {
        return false;
      }
      BaseHoodieWriteClient retryClient = currentClient.get();
      prepareRetry(retryClient, instantTime);
      if (!isShutdownRequested() && isPendingInstant(retryClient, instantTime)) {
        enqueuePendingAsyncServiceInstant(instantTime);
      } else {
        log.info("{} for instant {} is no longer pending, not retrying it", action, instantTime);
      }
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
   * Prepares the retry of a failed run of the instant with {@code retryClient}, before it is re-enqueued.
   */
  protected void prepareRetry(BaseHoodieWriteClient retryClient, String instantTime) throws IOException, InterruptedException {
  }

  /**
   * Waits until the writer has switched this service away from the given client. Services notify on this monitor
   * when they switch clients.
   */
  private synchronized void awaitWriteClientReplaced(BaseHoodieWriteClient client, Supplier<BaseHoodieWriteClient> currentClient)
      throws InterruptedException {
    while (!isShutdownRequested() && currentClient.get() == client) {
      wait(WRITE_CLIENT_POLL_MS);
    }
  }
}
