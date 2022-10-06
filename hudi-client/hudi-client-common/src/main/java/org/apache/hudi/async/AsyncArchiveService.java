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
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Async archive service to run concurrently with write operation.
 */
public class AsyncArchiveService extends HoodieAsyncTableService {

  private static final Logger LOG = LogManager.getLogger(AsyncArchiveService.class);

  private final BaseHoodieWriteClient writeClient;
  private final transient ExecutorService executor = Executors.newSingleThreadExecutor();

  protected AsyncArchiveService(BaseHoodieWriteClient writeClient) {
    super(writeClient.getConfig());
    this.writeClient = writeClient;
  }

  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    LOG.info("Starting async archive service...");
    return Pair.of(CompletableFuture.supplyAsync(() -> {
      writeClient.archive();
      return true;
    }, executor), executor);
  }

  public static AsyncArchiveService startAsyncArchiveIfEnabled(BaseHoodieWriteClient writeClient) {
    HoodieWriteConfig config = writeClient.getConfig();
    if (!(boolean) config.getBoolean(HoodieArchivalConfig.AUTO_ARCHIVE) || !(boolean) config.getBoolean(HoodieArchivalConfig.ASYNC_ARCHIVE)) {
      LOG.info("The HoodieWriteClient is not configured to auto & async archive. Async archive service will not start.");
      return null;
    }
    AsyncArchiveService asyncArchiveService = new AsyncArchiveService(writeClient);
    asyncArchiveService.start(null);
    return asyncArchiveService;
  }

  public static void waitForCompletion(AsyncArchiveService asyncArchiveService) {
    if (asyncArchiveService != null) {
      LOG.info("Waiting for async archive service to finish");
      try {
        asyncArchiveService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException("Error waiting for async archive service to finish", e);
      }
    }
  }

  public static void forceShutdown(AsyncArchiveService asyncArchiveService) {
    if (asyncArchiveService != null) {
      LOG.info("Shutting down async archive service...");
      asyncArchiveService.shutdown(true);
    }
  }
}
