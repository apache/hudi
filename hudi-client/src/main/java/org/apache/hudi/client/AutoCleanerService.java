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

import org.apache.hudi.async.AbstractAsyncService;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Auto Clean service running concurrently with write operation.
 */
class AutoCleanerService extends AbstractAsyncService {

  private static final Logger LOG = LogManager.getLogger(AutoCleanerService.class);

  private final HoodieWriteClient writeClient;
  private final String cleanInstant;
  private final transient ExecutorService executor = Executors.newFixedThreadPool(1);

  protected AutoCleanerService(HoodieWriteClient writeClient, String cleanInstant) {
    this.writeClient = writeClient;
    this.cleanInstant = cleanInstant;
  }

  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    return Pair.of(CompletableFuture.supplyAsync(() -> {
      writeClient.clean(cleanInstant);
      return true;
    }), executor);
  }

  /**
   * Run auto cleaner in parallel if enabled.
   * @param instantTime
   */
  public static AutoCleanerService spawnAutoCleanerIfEnabled(HoodieWriteClient writeClient,
      String instantTime) {
    AutoCleanerService autoCleanerService = null;
    if (writeClient.getConfig().isAutoClean() && writeClient.getConfig().isRunParallelAutoClean()) {
      // Call clean to cleanup if there is anything to cleanup after the commit,
      LOG.info("Auto cleaning is enabled. Running cleaner now in parallel with write operation");
      autoCleanerService = new AutoCleanerService(writeClient, instantTime);
      autoCleanerService.start(null);
    } else {
      LOG.info("Auto cleaning is not enabled. Not running cleaner now");
    }
    return autoCleanerService;
  }

  /**
   * Wait for auto cleaner to finish if enabled.
   */
  public static void waitForAutoCleanerToFinish(AutoCleanerService autoCleanerService) {
    if (autoCleanerService != null) {
      LOG.info("Waiting for auto cleaner to finish");
      try {
        autoCleanerService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      }
    }
  }

  /**
   * Shutdown auto cleaner if running.
   */
  public static void shutdownAutoCleaner(AutoCleanerService autoCleanerService) {
    if (autoCleanerService != null) {
      LOG.info("Shutting down auto cleaner");
      autoCleanerService.shutdown(true);
    }
  }
}
