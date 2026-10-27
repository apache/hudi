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
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestAsyncTableServiceClientHandoff {

  @ParameterizedTest
  @CsvSource({"true,false", "false,false", "true,true", "false,true"})
  void testHandoffWaitsForActiveRun(boolean compact, boolean failRun) throws Exception {
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    BaseHoodieWriteClient oldClient = mock(BaseHoodieWriteClient.class);
    BaseHoodieWriteClient newClient = mock(BaseHoodieWriteClient.class);
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(oldClient.getConfig()).thenReturn(config);
    when(config.areTableServicesEnabled()).thenReturn(true);
    AtomicReference<BaseHoodieWriteClient> currentClient = new AtomicReference<>(oldClient);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch finishRun = new CountDownLatch(1);
    CountDownLatch nextRun = new CountDownLatch(1);
    CountDownLatch stopped = new CountDownLatch(1);
    Answer<Void> run = invocation -> {
      BaseHoodieWriteClient capturedClient = currentClient.get();
      if ("001".equals(invocation.getArgument(0))) {
        entered.countDown();
        assertTrue(finishRun.await(10, TimeUnit.SECONDS));
        assertSame(oldClient, capturedClient);
        verify(oldClient, never()).close();
        if (failRun) {
          throw new IOException("Injected async operation failure");
        }
      } else {
        assertSame(newClient, capturedClient);
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
      service.start(errored -> {
        stopped.countDown();
        return true;
      });
      service.enqueuePendingAsyncServiceInstant("001");
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      CountDownLatch swapping = new CountDownLatch(1);
      Future<?> swap = swapExecutor.submit(() -> {
        swapping.countDown();
        handoff.accept(newClient);
        oldClient.close();
      });
      assertTrue(swapping.await(10, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> swap.get(200, TimeUnit.MILLISECONDS));
      verify(oldClient, never()).close();

      finishRun.countDown();
      swap.get(10, TimeUnit.SECONDS);
      verify(oldClient).close();
      if (failRun) {
        assertTrue(stopped.await(10, TimeUnit.SECONDS));
        assertTrue(service.hasError());
      } else {
        service.enqueuePendingAsyncServiceInstant("002");
        assertTrue(nextRun.await(10, TimeUnit.SECONDS));
        // An idle service must hand back the client promptly too, without waiting for another run.
        swapExecutor.submit(() -> handoff.accept(oldClient)).get(10, TimeUnit.SECONDS);
      }
    } finally {
      finishRun.countDown();
      service.shutdown(true);
      swapExecutor.shutdownNow();
      assertTrue(swapExecutor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
