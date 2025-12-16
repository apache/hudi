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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Async clean service to run concurrently with write operation.
 */
@Slf4j
public class AsyncCleanerService extends HoodieAsyncTableService {

  private final BaseHoodieWriteClient writeClient;
  private final transient ExecutorService executor = Executors.newSingleThreadExecutor();

  protected AsyncCleanerService(BaseHoodieWriteClient writeClient) {
    super(writeClient.getConfig());
    this.writeClient = writeClient;
  }

  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    return Pair.of(CompletableFuture.supplyAsync(() -> {
      writeClient.clean();
      return true;
    }, executor), executor);
  }

  public static AsyncCleanerService startAsyncCleaningIfEnabled(BaseHoodieWriteClient writeClient) {
    HoodieWriteConfig config = writeClient.getConfig();
    if (!config.isAutoClean() || !config.isAsyncClean()) {
      log.info("The HoodieWriteClient is not configured to auto & async clean. Async clean service will not start.");
      return null;
    }
    AsyncCleanerService asyncCleanerService = new AsyncCleanerService(writeClient);
    asyncCleanerService.start(null);
    return asyncCleanerService;
  }

  public static void waitForCompletion(AsyncCleanerService asyncCleanerService) {
    if (asyncCleanerService != null) {
      log.info("Waiting for async clean service to finish");
      try {
        asyncCleanerService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException("Error waiting for async clean service to finish", e);
      }
    }
  }

  public static void forceShutdown(AsyncCleanerService asyncCleanerService) {
    if (asyncCleanerService != null) {
      log.info("Shutting down async clean service...");
      asyncCleanerService.shutdown(true);
    }
  }
}
