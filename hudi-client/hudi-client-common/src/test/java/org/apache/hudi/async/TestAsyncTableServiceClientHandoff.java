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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestAsyncTableServiceClientHandoff {

  /**
   * A compaction or clustering run can take a long time (e.g. waiting for cluster resources), so switching the
   * client must not block the writer until the run completes. The running run keeps the client it started with,
   * and the next run uses the new client.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHandoffDoesNotWaitForActiveRun(boolean compact) throws Exception {
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    BaseHoodieWriteClient oldClient = mock(BaseHoodieWriteClient.class);
    BaseHoodieWriteClient newClient = mock(BaseHoodieWriteClient.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(oldClient.getConfig()).thenReturn(config);
    when(config.areTableServicesEnabled()).thenReturn(true);
    AtomicReference<BaseHoodieWriteClient> currentClient = new AtomicReference<>(oldClient);
    AtomicReference<BaseHoodieWriteClient> firstRunClient = new AtomicReference<>();
    AtomicReference<BaseHoodieWriteClient> secondRunClient = new AtomicReference<>();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch finishRun = new CountDownLatch(1);
    CountDownLatch firstRunDone = new CountDownLatch(1);
    CountDownLatch nextRun = new CountDownLatch(1);
    Answer<Void> run = invocation -> {
      if ("001".equals(invocation.getArgument(0))) {
        firstRunClient.set(currentClient.get());
        entered.countDown();
        assertTrue(finishRun.await(30, TimeUnit.SECONDS));
        firstRunDone.countDown();
      } else {
        secondRunClient.set(currentClient.get());
        nextRun.countDown();
      }
      return null;
    };
    Answer<Void> update = invocation -> {
      currentClient.set(invocation.getArgument(0));
      return null;
    };
    HoodieAsyncService service;
    Consumer<BaseHoodieWriteClient> handoff;
    if (compact) {
      BaseCompactor compactor = mock(BaseCompactor.class);
      doAnswer(run).when(compactor).compact(anyString());
      doAnswer(update).when(compactor).updateWriteClient(any());
      AsyncCompactService compaction = new AsyncCompactService(context, oldClient, true) {
        @Override
        protected BaseCompactor createCompactor(BaseHoodieWriteClient client) {
          return compactor;
        }
      };
      service = compaction;
      handoff = compaction::updateWriteClient;
    } else {
      BaseClusterer clusterer = mock(BaseClusterer.class);
      doAnswer(run).when(clusterer).cluster(anyString());
      doAnswer(update).when(clusterer).updateWriteClient(any());
      AsyncClusteringService clustering = new AsyncClusteringService(context, oldClient, true) {
        @Override
        protected BaseClusterer createClusteringClient(BaseHoodieWriteClient client) {
          return clusterer;
        }
      };
      service = clustering;
      handoff = clustering::updateWriteClient;
    }

    ExecutorService swapExecutor = Executors.newSingleThreadExecutor();
    try {
      service.start(errored -> true);
      service.enqueuePendingAsyncServiceInstant("001");
      assertTrue(entered.await(10, TimeUnit.SECONDS));

      // the handoff returns while the run is still active
      swapExecutor.submit(() -> handoff.accept(newClient)).get(10, TimeUnit.SECONDS);
      assertFalse(firstRunDone.await(0, TimeUnit.SECONDS), "the run must still be active after the handoff");
      assertSame(oldClient, firstRunClient.get());

      finishRun.countDown();
      assertTrue(firstRunDone.await(10, TimeUnit.SECONDS));
      service.enqueuePendingAsyncServiceInstant("002");
      assertTrue(nextRun.await(10, TimeUnit.SECONDS));
      assertSame(newClient, secondRunClient.get());
      assertFalse(service.hasError());
    } finally {
      finishRun.countDown();
      service.shutdown(true);
      swapExecutor.shutdownNow();
      assertTrue(swapExecutor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
