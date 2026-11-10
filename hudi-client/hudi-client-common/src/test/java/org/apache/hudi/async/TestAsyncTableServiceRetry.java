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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A run that fails is retried only when the service says the failure is retryable (by default never, e.g. for
 * HoodieStreamer): the retry waits for the writer to switch the service to a new client, and runs only if the instant
 * is still pending. A failure is always classified against the client the run actually used.
 */
@Timeout(60)
class TestAsyncTableServiceRetry {

  private static final String INSTANT = "001";

  /**
   * A compaction or clustering service under test, with the clients it runs with and the clients its failures were
   * classified against. Runs on {@code retiredClient} fail and are retryable; runs on any other client fail if
   * {@code failingClient} is that client.
   */
  private static final class Harness {
    final HoodieAsyncService service;
    final Consumer<BaseHoodieWriteClient> handoff;
    final List<BaseHoodieWriteClient> runClients = new CopyOnWriteArrayList<>();
    final List<BaseHoodieWriteClient> classifiedClients = new CopyOnWriteArrayList<>();
    final CountDownLatch firstRunFailed = new CountDownLatch(1);
    final CountDownLatch secondRun = new CountDownLatch(1);
    final CountDownLatch stopped = new CountDownLatch(1);
    final CountDownLatch inFirstRun = new CountDownLatch(1);
    final CountDownLatch releaseFirstRun = new CountDownLatch(1);
    final RuntimeException failure = new IllegalStateException("write client closed");
    final RuntimeException secondFailure = new IllegalStateException("unrelated failure");

    Harness(boolean compact, HoodieEngineContext context, BaseHoodieWriteClient oldClient, boolean retryable,
            boolean stillPending, BaseHoodieWriteClient failingClient, boolean blockFirstRun) throws IOException {
      this(compact, context, oldClient, retryable, stillPending, failingClient, blockFirstRun, null);
    }

    Harness(boolean compact, HoodieEngineContext context, BaseHoodieWriteClient oldClient, boolean retryable,
            boolean stillPending, BaseHoodieWriteClient failingClient, boolean blockFirstRun, HoodieStorage storage)
        throws IOException {
      Answer<Void> run = invocation -> {
        // the client the service passes to the run is the one it runs with
        BaseHoodieWriteClient client = invocation.getArgument(0);
        runClients.add(client);
        if (runClients.size() == 1) {
          inFirstRun.countDown();
          if (blockFirstRun) {
            assertTrue(releaseFirstRun.await(30, TimeUnit.SECONDS));
          }
          firstRunFailed.countDown();
          throw failure;
        }
        secondRun.countDown();
        if (client == failingClient) {
          throw secondFailure;
        }
        return null;
      };
      Answer<Void> update = invocation -> {
        invocation.callRealMethod();
        return null;
      };
      if (compact) {
        BaseCompactor compactor = mock(BaseCompactor.class);
        compactor.compactionClient = oldClient;
        doAnswer(run).when(compactor).compact(any(), anyString());
        doAnswer(update).when(compactor).updateWriteClient(any());
        AsyncCompactService compaction = new AsyncCompactService(context, oldClient, true) {
          @Override
          protected BaseCompactor createCompactor(BaseHoodieWriteClient client) {
            return compactor;
          }

          @Override
          protected boolean isRetryableFailure(BaseHoodieWriteClient runClient, Exception e) {
            classifiedClients.add(runClient);
            return retryable ? runClient == oldClient : super.isRetryableFailure(runClient, e);
          }

          @Override
          protected boolean isPendingCompaction(String instantTime) {
            return stillPending;
          }

          @Override
          protected HoodieStorage getStorage(HoodieWriteConfig config) {
            return storage;
          }
        };
        service = compaction;
        handoff = compaction::updateWriteClient;
      } else {
        BaseClusterer clusterer = mock(BaseClusterer.class);
        clusterer.clusteringClient = oldClient;
        doAnswer(run).when(clusterer).cluster(any(), anyString());
        doAnswer(update).when(clusterer).updateWriteClient(any());
        AsyncClusteringService clustering = new AsyncClusteringService(context, oldClient, true) {
          @Override
          protected BaseClusterer createClusteringClient(BaseHoodieWriteClient client) {
            return clusterer;
          }

          @Override
          protected boolean isRetryableFailure(BaseHoodieWriteClient runClient, Exception e) {
            classifiedClients.add(runClient);
            return retryable ? runClient == oldClient : super.isRetryableFailure(runClient, e);
          }

          @Override
          protected boolean isPendingClustering(String instantTime) {
            return stillPending;
          }
        };
        service = clustering;
        handoff = clustering::updateWriteClient;
      }
    }

    void start() {
      service.start(errored -> {
        stopped.countDown();
        return true;
      });
      service.enqueuePendingAsyncServiceInstant(INSTANT);
    }
  }

  private static HoodieWriteConfig config(WriteConcurrencyMode mode) {
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.areTableServicesEnabled()).thenReturn(true);
    when(config.getWriteConcurrencyMode()).thenReturn(mode);
    return config;
  }

  private static BaseHoodieWriteClient client(HoodieWriteConfig config) {
    BaseHoodieWriteClient client = mock(BaseHoodieWriteClient.class);
    when(client.getConfig()).thenReturn(config);
    return client;
  }

  @ParameterizedTest
  @CsvSource({
      "true,true,true", "true,true,false", "true,false,true",
      "false,true,true", "false,true,false", "false,false,true"})
  void testFailedRun(boolean compact, boolean retryable, boolean stillPending) throws Exception {
    HoodieWriteConfig config = config(WriteConcurrencyMode.SINGLE_WRITER);
    BaseHoodieWriteClient oldClient = client(config);
    BaseHoodieWriteClient newClient = client(config);
    Harness harness = new Harness(compact, mock(HoodieEngineContext.class), oldClient, retryable, stillPending, null, false);
    try {
      harness.start();
      assertTrue(harness.firstRunFailed.await(10, TimeUnit.SECONDS));
      if (!retryable) {
        // the default: the service stops with the failure
        assertTrue(harness.stopped.await(10, TimeUnit.SECONDS));
        assertTrue(harness.service.hasError());
        assertSame(harness.failure, harness.service.getFailure().get());
        return;
      }
      // the failed run is not retried with the client it failed with: the service waits for the new client
      assertFalse(harness.secondRun.await(500, TimeUnit.MILLISECONDS));
      harness.handoff.accept(newClient);
      if (stillPending) {
        assertTrue(harness.secondRun.await(10, TimeUnit.SECONDS));
        assertEquals(2, harness.runClients.size());
        assertSame(newClient, harness.runClients.get(1));
      } else {
        // the instant completed in the meantime: nothing to retry
        assertFalse(harness.secondRun.await(500, TimeUnit.MILLISECONDS));
        assertEquals(1, harness.runClients.size());
      }
      assertFalse(harness.service.hasError());
      assertEquals(1, harness.stopped.getCount(), "the service must keep running");
    } finally {
      harness.service.shutdown(true);
    }
  }

  /**
   * The writer switches clients while a run is in progress on the old one. The run keeps the client it started with,
   * and its failure is classified against that client, not the one the service has by the time the run fails.
   */
  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testFailureClassifiedAgainstTheClientOfTheRun(boolean compact) throws Exception {
    HoodieWriteConfig config = config(WriteConcurrencyMode.SINGLE_WRITER);
    BaseHoodieWriteClient oldClient = client(config);
    BaseHoodieWriteClient newClient = client(config);
    Harness harness = new Harness(compact, mock(HoodieEngineContext.class), oldClient, true, true, null, true);
    try {
      harness.start();
      assertTrue(harness.inFirstRun.await(10, TimeUnit.SECONDS));
      harness.handoff.accept(newClient);
      harness.releaseFirstRun.countDown();
      assertTrue(harness.secondRun.await(10, TimeUnit.SECONDS));
      assertSame(oldClient, harness.runClients.get(0));
      assertEquals(1, harness.classifiedClients.size());
      assertSame(harness.runClients.get(0), harness.classifiedClients.get(0));
      assertSame(newClient, harness.runClients.get(1));
      assertFalse(harness.service.hasError());
    } finally {
      harness.releaseFirstRun.countDown();
      harness.service.shutdown(true);
    }
  }

  /**
   * A retry that fails again on the new client, which was not retired, is not retried: the service stops with that
   * failure, so an unrelated error coinciding with a client switch costs at most one extra attempt.
   */
  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testRetryFailingOnNewClientStopsTheService(boolean compact) throws Exception {
    HoodieWriteConfig config = config(WriteConcurrencyMode.SINGLE_WRITER);
    BaseHoodieWriteClient oldClient = client(config);
    BaseHoodieWriteClient newClient = client(config);
    Harness harness = new Harness(compact, mock(HoodieEngineContext.class), oldClient, true, true, newClient, false);
    try {
      harness.start();
      assertTrue(harness.firstRunFailed.await(10, TimeUnit.SECONDS));
      harness.handoff.accept(newClient);
      assertTrue(harness.stopped.await(10, TimeUnit.SECONDS));
      assertTrue(harness.service.hasError());
      assertSame(harness.secondFailure, harness.service.getFailure().get());
      assertEquals(2, harness.runClients.size());
      assertSame(newClient, harness.classifiedClients.get(1));
    } finally {
      harness.service.shutdown(true);
    }
  }

  /**
   * With multi-writer concurrency, the failed compaction run leaves a fresh heartbeat on its instant (closing its
   * client stops the heartbeat without deleting the file), and compact() refuses to run an instant with a fresh
   * heartbeat. The retry waits for the heartbeat to expire instead of failing on it.
   */
  @Test
  void testCompactionRetryWaitsForHeartbeatToExpire() throws Exception {
    long heartbeatIntervalMs = 1000;
    int tolerableMisses = 2;
    String basePath = "/tmp/table";
    HoodieWriteConfig config = config(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);
    when(config.getBasePath()).thenReturn(basePath);
    when(config.getHoodieClientHeartbeatIntervalInMs()).thenReturn(heartbeatIntervalMs);
    when(config.getHoodieClientHeartbeatTolerableMisses()).thenReturn(tolerableMisses);
    // the heartbeat the failed run left behind, last refreshed now
    long lastHeartbeat = System.currentTimeMillis();
    long expiresAt = lastHeartbeat + heartbeatIntervalMs * tolerableMisses;
    StoragePath heartbeat = new StoragePath(HoodieTableMetaClient.getHeartbeatFolderPath(basePath), INSTANT);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.exists(heartbeat)).thenReturn(true);
    when(storage.getPathInfo(heartbeat)).thenReturn(new StoragePathInfo(heartbeat, 0, false, (short) 1, 0, lastHeartbeat));
    BaseHoodieWriteClient oldClient = client(config);
    BaseHoodieWriteClient newClient = client(config);
    Harness harness = new Harness(true, mock(HoodieEngineContext.class), oldClient, true, true, null, false, storage);
    try {
      harness.start();
      assertTrue(harness.firstRunFailed.await(10, TimeUnit.SECONDS));
      harness.handoff.accept(newClient);
      assertTrue(harness.secondRun.await(20, TimeUnit.SECONDS));
      assertTrue(System.currentTimeMillis() > expiresAt, "the retry must wait for the heartbeat to expire");
      assertSame(newClient, harness.runClients.get(1));
      assertFalse(harness.service.hasError());
    } finally {
      harness.service.shutdown(true);
    }
  }
}
