/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.WriteMetadataEvent;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link EventBuffers}.
 */
public class TestEventBuffers {

  @Test
  void testAwaitAllInstantsWaitsUntilAllPendingInstantsReset() throws Exception {
    EventBuffers eventBuffers = newBlockingEventBuffers();

    eventBuffers.initNewEventBuffer(1L, "001");
    eventBuffers.addEventToBuffer(newWriteEvent(1L, "001"));

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // two tasks block waiting on commit.
      CompletableFuture<Void> waitingFuture1 = CompletableFuture.runAsync(
          () -> eventBuffers.awaitAllInstantsToCompleteIfNecessary(2L, () -> false), executor);
      CompletableFuture<Void> waitingFuture2 = CompletableFuture.runAsync(
          () -> eventBuffers.awaitAllInstantsToCompleteIfNecessary(2L, () -> false), executor);

      Thread.sleep(100);
      assertFalse(waitingFuture1.isDone());
      assertFalse(waitingFuture2.isDone());

      eventBuffers.reset(1L);
      waitingFuture1.get(2, TimeUnit.SECONDS);
      waitingFuture2.get(2, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testAwaitBlocksOnEarlierInstantWithoutEvents() throws Exception {
    EventBuffers eventBuffers = newBlockingEventBuffers();
    // the write metadata events of the instant have not been received yet
    eventBuffers.initNewEventBuffer(1L, "001");

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      CompletableFuture<Boolean> waitingFuture = CompletableFuture.supplyAsync(
          () -> eventBuffers.awaitAllInstantsToCompleteIfNecessary(2L, () -> false), executor);
      assertThrows(TimeoutException.class, () -> waitingFuture.get(200, TimeUnit.MILLISECONDS));

      eventBuffers.reset(1L);
      assertTrue(waitingFuture.get(2, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testAwaitKeepsWaitingAfterResetOfAnotherInstant() throws Exception {
    EventBuffers eventBuffers = newBlockingEventBuffers();
    eventBuffers.initNewEventBuffer(0L, "000");
    eventBuffers.initNewEventBuffer(1L, "001");

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      CompletableFuture<Boolean> waitingFuture = CompletableFuture.supplyAsync(
          () -> eventBuffers.awaitAllInstantsToCompleteIfNecessary(2L, () -> false), executor);
      assertThrows(TimeoutException.class, () -> waitingFuture.get(200, TimeUnit.MILLISECONDS));

      // the reset of one instant signals the waiter, which must keep waiting for the other one
      eventBuffers.reset(0L);
      assertThrows(TimeoutException.class, () -> waitingFuture.get(200, TimeUnit.MILLISECONDS));

      eventBuffers.reset(1L);
      assertTrue(waitingFuture.get(2, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testAwaitIgnoresInstantsOfLaterCheckpoints() {
    EventBuffers eventBuffers = newBlockingEventBuffers();
    eventBuffers.initNewEventBuffer(2L, "002");
    eventBuffers.addEventToBuffer(newWriteEvent(2L, "002"));
    eventBuffers.initNewEventBuffer(3L, "003");

    assertTrue(eventBuffers.awaitAllInstantsToCompleteIfNecessary(2L, () -> false));
  }

  @Test
  void testAwaitStopsWhenRequestBecomesStale() throws Exception {
    EventBuffers eventBuffers = newBlockingEventBuffers();
    eventBuffers.initNewEventBuffer(1L, "001");
    AtomicBoolean stale = new AtomicBoolean(false);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      CompletableFuture<Boolean> waitingFuture = CompletableFuture.supplyAsync(
          () -> eventBuffers.awaitAllInstantsToCompleteIfNecessary(2L, stale::get), executor);
      assertThrows(TimeoutException.class, () -> waitingFuture.get(200, TimeUnit.MILLISECONDS));

      stale.set(true);
      // a failover wakes up the waiter to check its request again
      eventBuffers.unblock();
      assertFalse(waitingFuture.get(2, TimeUnit.SECONDS));
      assertNotNull(eventBuffers.getEventBuffer(1L));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testAwaitDoesNotBlockWithoutCommitGuard() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    EventBuffers eventBuffers = EventBuffers.getInstance(conf, 1);
    eventBuffers.initNewEventBuffer(1L, "001");

    assertTrue(eventBuffers.awaitAllInstantsToCompleteIfNecessary(2L, () -> false));
  }

  private static EventBuffers newBlockingEventBuffers() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 5_000L);
    return EventBuffers.getInstance(conf, 1);
  }

  private static WriteMetadataEvent newWriteEvent(long checkpointId, String instant) {
    return WriteMetadataEvent.builder()
        .taskID(0)
        .checkpointId(checkpointId)
        .instantTime(instant)
        .writeStatus(Collections.emptyList())
        .lastBatch(true)
        .build();
  }
}
