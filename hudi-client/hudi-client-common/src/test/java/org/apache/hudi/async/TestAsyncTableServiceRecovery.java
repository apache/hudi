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
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(30)
class TestAsyncTableServiceRecovery extends HoodieCommonTestHarness {
  private static final String INSTANT = "20261027000000000";
  private static final String COMPLETION_TIME = "20261027000001000";

  private HoodieEngineContext context;
  private BaseHoodieWriteClient previousClient;
  private BaseHoodieWriteClient replacementClient;

  @BeforeEach
  void setUp() throws Exception {
    initMetaClient();
    context = new HoodieLocalEngineContext(metaClient.getStorageConf());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    previousClient = mock(BaseHoodieWriteClient.class);
    replacementClient = mock(BaseHoodieWriteClient.class);
    when(previousClient.getConfig()).thenReturn(config);
    when(replacementClient.getConfig()).thenReturn(config);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failureCauseIsPublishedBeforeFutureCompletes(boolean clustering) throws Exception {
    TestRun run = new TestRun();
    RuntimeException failure = new IllegalStateException("original failure");
    run.attempt = (client, instant) -> {
      throw failure;
    };
    run.delayCompletion = true;
    CountDownLatch callbackFinished = new CountDownLatch(1);
    HoodieAsyncTableService service = createService(clustering, run);
    try {
      service.enqueuePendingAsyncServiceInstant(INSTANT);
      service.start(error -> {
        callbackFinished.countDown();
        return true;
      });
      await(run.workerFinished);
      assertTrue(service.hasError());
      assertSame(failure, service.getFailure().get());
      run.allowCompletion.countDown();
      await(callbackFinished);
      assertSame(failure, service.getFailure().get(), "completion must not replace the original cause with a wrapper");
    } finally {
      run.allowCompletion.countDown();
      service.shutdown(true);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void attemptKeepsItsClientAcrossSwap(boolean clustering) throws Exception {
    createPendingInstant(clustering);
    TestRun run = new TestRun();
    CountDownLatch attemptEntered = new CountDownLatch(1);
    CountDownLatch allowClientRead = new CountDownLatch(1);
    CountDownLatch retryFinished = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    List<BaseHoodieWriteClient> usedClients = new CopyOnWriteArrayList<>();
    run.beforeClientRead = () -> {
      if (attempts.incrementAndGet() == 1) {
        attemptEntered.countDown();
        await(allowClientRead);
      }
    };
    run.attempt = (client, instant) -> {
      usedClients.add(client);
      if (client == previousClient) {
        throw new IllegalStateException("closed previous client");
      }
      retryFinished.countDown();
    };
    HoodieAsyncTableService service = createService(clustering, run);
    try {
      service.enqueuePendingAsyncServiceInstant(INSTANT);
      service.start(null);
      await(attemptEntered);
      updateClient(service, replacementClient);
      allowClientRead.countDown();
      await(retryFinished);
      assertEquals(Arrays.asList(previousClient, replacementClient), usedClients);
      assertEquals(2, run.enqueued.get());
      assertFalse(service.hasError());
    } finally {
      allowClientRead.countDown();
      service.shutdown(true);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void replacementFailureIsNotRetriedAgain(boolean clustering) throws Exception {
    createPendingInstant(clustering);
    TestRun run = new TestRun();
    CountDownLatch firstAttempt = new CountDownLatch(1);
    CountDownLatch swapped = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    RuntimeException replacementFailure = new IllegalStateException("replacement failed");
    run.attempt = (client, instant) -> {
      attempts.incrementAndGet();
      if (client == previousClient) {
        firstAttempt.countDown();
        await(swapped);
        throw new IllegalStateException("closed previous client");
      }
      throw replacementFailure;
    };
    HoodieAsyncTableService service = createService(clustering, run);
    try {
      service.enqueuePendingAsyncServiceInstant(INSTANT);
      service.start(null);
      await(firstAttempt);
      updateClient(service, replacementClient);
      swapped.countDown();
      assertThrows(ExecutionException.class, service::waitForShutdown);
      assertSame(replacementFailure, service.getFailure().get());
      assertEquals(2, attempts.get());
      assertEquals(2, run.enqueued.get());
    } finally {
      swapped.countDown();
      service.shutdown(true);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void invalidInstantFailureStopsAfterRetry(boolean clustering, boolean completed) throws Exception {
    TestRun run = new TestRun();
    CountDownLatch firstAttempt = new CountDownLatch(1);
    CountDownLatch swapped = new CountDownLatch(1);
    RuntimeException firstFailure = new IllegalStateException("previous client failed");
    RuntimeException retryFailure = new IllegalStateException("instant is no longer pending");
    run.attempt = (client, instant) -> {
      if (client == previousClient) {
        firstAttempt.countDown();
        await(swapped);
        throw firstFailure;
      }
      throw retryFailure;
    };
    HoodieAsyncTableService service = createService(clustering, run);
    try {
      service.enqueuePendingAsyncServiceInstant(INSTANT);
      service.start(null);
      await(firstAttempt);
      if (completed) {
        metaClient.getActiveTimeline().createCompleteInstant(metaClient.getInstantGenerator().createNewInstant(
            HoodieInstant.State.COMPLETED, clustering ? REPLACE_COMMIT_ACTION : COMMIT_ACTION, INSTANT, COMPLETION_TIME));
      }
      updateClient(service, replacementClient);
      swapped.countDown();
      assertThrows(ExecutionException.class, service::waitForShutdown);
      assertTrue(service.hasError());
      assertSame(retryFailure, service.getFailure().get());
      assertEquals(2, run.enqueued.get(), "a completed or missing instant must not be retried repeatedly");
    } finally {
      swapped.countDown();
      service.shutdown(true);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void updatingWithSameClientDoesNotPermitRetry(boolean clustering) throws Exception {
    createPendingInstant(clustering);
    TestRun run = new TestRun();
    CountDownLatch firstAttempt = new CountDownLatch(1);
    CountDownLatch updated = new CountDownLatch(1);
    RuntimeException failure = new IllegalStateException("same client failed");
    run.attempt = (client, instant) -> {
      firstAttempt.countDown();
      await(updated);
      throw failure;
    };
    HoodieAsyncTableService service = createService(clustering, run);
    try {
      service.enqueuePendingAsyncServiceInstant(INSTANT);
      service.start(null);
      await(firstAttempt);
      updateClient(service, previousClient);
      updated.countDown();
      assertThrows(ExecutionException.class, service::waitForShutdown);
      assertSame(failure, service.getFailure().get());
      assertEquals(1, run.enqueued.get());
    } finally {
      updated.countDown();
      service.shutdown(true);
    }
  }

  private void createPendingInstant(boolean clustering) {
    metaClient.getActiveTimeline().createNewInstant(metaClient.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, clustering ? CLUSTERING_ACTION : COMPACTION_ACTION, INSTANT));
  }

  private HoodieAsyncTableService createService(boolean clustering, TestRun run) {
    if (clustering) {
      return new AsyncClusteringService(context, previousClient) {
        @Override
        protected BaseClusterer createClusteringClient(BaseHoodieWriteClient client) {
          return new BaseClusterer(client) {
            @Override
            public void cluster(String instantTime) {
              run.beforeClientRead.run();
              run.attempt.accept(clusteringClient, instantTime);
            }
          };
        }

        @Override
        public void enqueuePendingAsyncServiceInstant(String instantTime) {
          run.enqueued.incrementAndGet();
          super.enqueuePendingAsyncServiceInstant(instantTime);
        }

        @Override
        protected Pair<CompletableFuture, ExecutorService> startService() {
          return run.gateCompletion(super.startService());
        }
      };
    }
    return new AsyncCompactService(context, previousClient) {
      @Override
      protected BaseCompactor createCompactor(BaseHoodieWriteClient client) {
        return new BaseCompactor(client) {
          @Override
          public void compact(String instantTime) {
            run.beforeClientRead.run();
            run.attempt.accept(compactionClient, instantTime);
          }
        };
      }

      @Override
      public void enqueuePendingAsyncServiceInstant(String instantTime) {
        run.enqueued.incrementAndGet();
        super.enqueuePendingAsyncServiceInstant(instantTime);
      }

      @Override
      protected Pair<CompletableFuture, ExecutorService> startService() {
        return run.gateCompletion(super.startService());
      }
    };
  }

  private static void updateClient(HoodieAsyncTableService service, BaseHoodieWriteClient client) {
    if (service instanceof AsyncClusteringService) {
      ((AsyncClusteringService) service).updateWriteClient(client);
    } else {
      ((AsyncCompactService) service).updateWriteClient(client);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(10, TimeUnit.SECONDS), "timed out waiting for async service");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static class TestRun {
    private final AtomicInteger enqueued = new AtomicInteger();
    private final CountDownLatch workerFinished = new CountDownLatch(1);
    private final CountDownLatch allowCompletion = new CountDownLatch(1);
    private Runnable beforeClientRead = () -> { };
    private BiConsumer<BaseHoodieWriteClient, String> attempt;
    private boolean delayCompletion;

    private Pair<CompletableFuture, ExecutorService> gateCompletion(Pair<CompletableFuture, ExecutorService> service) {
      if (!delayCompletion) {
        return service;
      }
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
