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
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
 * is still pending.
 */
@Timeout(60)
class TestAsyncTableServiceRetry {

  @ParameterizedTest
  @CsvSource({
      "true,true,true", "true,true,false", "true,false,true",
      "false,true,true", "false,true,false", "false,false,true"})
  void testFailedRun(boolean compact, boolean retryable, boolean stillPending) throws Exception {
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    BaseHoodieWriteClient oldClient = mock(BaseHoodieWriteClient.class);
    BaseHoodieWriteClient newClient = mock(BaseHoodieWriteClient.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(oldClient.getConfig()).thenReturn(config);
    when(config.areTableServicesEnabled()).thenReturn(true);
    AtomicReference<BaseHoodieWriteClient> currentClient = new AtomicReference<>(oldClient);
    List<BaseHoodieWriteClient> runClients = new CopyOnWriteArrayList<>();
    RuntimeException failure = new IllegalStateException("write client closed");
    CountDownLatch firstRunFailed = new CountDownLatch(1);
    CountDownLatch secondRun = new CountDownLatch(1);
    CountDownLatch stopped = new CountDownLatch(1);
    Answer<Void> run = invocation -> {
      BaseHoodieWriteClient client = currentClient.get();
      runClients.add(client);
      if (client == oldClient) {
        firstRunFailed.countDown();
        throw failure;
      }
      secondRun.countDown();
      return null;
    };
    // the services read the client a run uses from the compactor/clusterer: keep it in sync with the mock's
    Answer<Void> update = invocation -> {
      currentClient.set(invocation.getArgument(0));
      invocation.callRealMethod();
      return null;
    };
    HoodieAsyncService service;
    Consumer<BaseHoodieWriteClient> handoff;
    if (compact) {
      BaseCompactor compactor = mock(BaseCompactor.class);
      compactor.compactionClient = oldClient;
      doAnswer(run).when(compactor).compact(anyString());
      doAnswer(update).when(compactor).updateWriteClient(any());
      AsyncCompactService compaction = new AsyncCompactService(context, oldClient, true) {
        @Override
        protected BaseCompactor createCompactor(BaseHoodieWriteClient client) {
          return compactor;
        }

        @Override
        protected boolean isRetryableFailure(BaseHoodieWriteClient runClient, Exception e) {
          return retryable ? runClient == oldClient : super.isRetryableFailure(runClient, e);
        }

        @Override
        protected boolean isPendingCompaction(String instantTime) {
          return stillPending;
        }
      };
      service = compaction;
      handoff = compaction::updateWriteClient;
    } else {
      BaseClusterer clusterer = mock(BaseClusterer.class);
      clusterer.clusteringClient = oldClient;
      doAnswer(run).when(clusterer).cluster(anyString());
      doAnswer(update).when(clusterer).updateWriteClient(any());
      AsyncClusteringService clustering = new AsyncClusteringService(context, oldClient, true) {
        @Override
        protected BaseClusterer createClusteringClient(BaseHoodieWriteClient client) {
          return clusterer;
        }

        @Override
        protected boolean isRetryableFailure(BaseHoodieWriteClient runClient, Exception e) {
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

    try {
      service.start(errored -> {
        stopped.countDown();
        return true;
      });
      service.enqueuePendingAsyncServiceInstant("001");
      assertTrue(firstRunFailed.await(10, TimeUnit.SECONDS));
      if (!retryable) {
        // the default: the service stops with the failure
        assertTrue(stopped.await(10, TimeUnit.SECONDS));
        assertTrue(service.hasError());
        assertSame(failure, service.getFailure().get());
        return;
      }
      // the failed run is not retried with the client it failed with: the service waits for the new client
      assertFalse(secondRun.await(500, TimeUnit.MILLISECONDS));
      handoff.accept(newClient);
      if (stillPending) {
        assertTrue(secondRun.await(10, TimeUnit.SECONDS));
        assertEquals(2, runClients.size());
        assertSame(newClient, runClients.get(1));
      } else {
        // the instant completed in the meantime: nothing to retry
        assertFalse(secondRun.await(500, TimeUnit.MILLISECONDS));
        assertEquals(1, runClients.size());
      }
      assertFalse(service.hasError());
      assertEquals(1, stopped.getCount(), "the service must keep running");
    } finally {
      service.shutdown(true);
    }
  }
}
