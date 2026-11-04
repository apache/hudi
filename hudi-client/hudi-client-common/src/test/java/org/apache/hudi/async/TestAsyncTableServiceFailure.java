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

package org.apache.hudi.async;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(30)
class TestAsyncTableServiceFailure {

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void failureCauseIsPublishedBeforeFutureCompletes(boolean clustering, boolean ioFailure) throws Exception {
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    BaseHoodieWriteClient client = mock(BaseHoodieWriteClient.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(client.getConfig()).thenReturn(config);
    when(config.areTableServicesEnabled()).thenReturn(true);
    Exception failure = ioFailure ? new IOException("original failure") : new IllegalStateException("original failure");
    CompletionGate gate = new CompletionGate();
    CountDownLatch callbackFinished = new CountDownLatch(1);
    HoodieAsyncTableService service;
    if (clustering) {
      service = new AsyncClusteringService(context, client) {
        @Override
        protected BaseClusterer createClusteringClient(BaseHoodieWriteClient writeClient) {
          return new BaseClusterer(writeClient) {
            @Override
            public void cluster(String instantTime) throws IOException {
              throwFailure(failure);
            }
          };
        }

        @Override
        protected Pair<CompletableFuture, ExecutorService> startService() {
          return gate.delayCompletion(super.startService());
        }
      };
    } else {
      service = new AsyncCompactService(context, client) {
        @Override
        protected BaseCompactor createCompactor(BaseHoodieWriteClient writeClient) {
          return new BaseCompactor(writeClient) {
            @Override
            public void compact(String instantTime) throws IOException {
              throwFailure(failure);
            }
          };
        }

        @Override
        protected Pair<CompletableFuture, ExecutorService> startService() {
          return gate.delayCompletion(super.startService());
        }
      };
    }
    try {
      service.enqueuePendingAsyncServiceInstant("001");
      service.start(error -> {
        callbackFinished.countDown();
        return true;
      });
      await(gate.workerFinished);
      assertTrue(service.hasError());
      assertSame(failure, service.getFailure().get());
      gate.allowCompletion.countDown();
      await(callbackFinished);
      assertSame(failure, service.getFailure().get(), "completion must not replace the original cause with a wrapper");
    } finally {
      gate.allowCompletion.countDown();
      service.shutdown(true);
    }
  }

  private static void throwFailure(Exception failure) throws IOException {
    if (failure instanceof IOException) {
      throw (IOException) failure;
    }
    throw (RuntimeException) failure;
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(10, TimeUnit.SECONDS), "timed out waiting for async service");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static class CompletionGate {
    private final CountDownLatch workerFinished = new CountDownLatch(1);
    private final CountDownLatch allowCompletion = new CountDownLatch(1);

    private Pair<CompletableFuture, ExecutorService> delayCompletion(Pair<CompletableFuture, ExecutorService> service) {
      CompletableFuture<?> future = service.getLeft();
      return Pair.of(future.handleAsync((result, error) -> {
        workerFinished.countDown();
        await(allowCompletion);
        if (error != null) {
          throw new CompletionException(error);
        }
        return result;
      }, service.getRight()), service.getRight());
    }
  }
}
