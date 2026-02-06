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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.event.WriteMetadataEvent;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link EventBuffers}.
 */
public class TestEventBuffers {

  @Test
  void testAwaitAllInstantsWaitsUntilAllPendingInstantsReset() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 5_000L);
    EventBuffers eventBuffers = EventBuffers.getInstance(conf, 1);

    eventBuffers.initNewEventBuffer(1L, "001");
    eventBuffers.addEventToBuffer(newWriteEvent(1L, "001"));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      // two tasks block waiting on commit.
      CompletableFuture<Void> waitingFuture1 = CompletableFuture.runAsync(
          eventBuffers::awaitAllInstantsToCompleteIfNecessary, executor);
      CompletableFuture<Void> waitingFuture2 = CompletableFuture.runAsync(
          eventBuffers::awaitAllInstantsToCompleteIfNecessary, executor);

      Thread.sleep(100);
      assertFalse(waitingFuture1.isDone());
      assertFalse(waitingFuture2.isDone());

      eventBuffers.reset(1L);
      Thread.sleep(100);
      assertTrue(waitingFuture1.isDone());
      assertTrue(waitingFuture2.isDone());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testAwaitPrevInstantsWaitsUntilAllPreviousCheckpointsReset() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    conf.set(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, 5_000L);
    conf.set(FlinkOptions.INDEX_WRITE_TASKS, 4);
    EventBuffers eventBuffers = EventBuffers.getInstance(conf, 1);

    eventBuffers.initNewEventBuffer(1L, "001");
    eventBuffers.initNewEventBuffer(2L, "002");
    eventBuffers.initNewEventBuffer(3L, "003");

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      // two tasks block waiting on commit.
      CompletableFuture<Void> waitingFuture1 = CompletableFuture.runAsync(
          () -> eventBuffers.awaitPrevInstantsToComplete(3L), executor);
      CompletableFuture<Void> waitingFuture2 = CompletableFuture.runAsync(
          () -> eventBuffers.awaitPrevInstantsToComplete(3L), executor);

      Thread.sleep(100);
      assertFalse(waitingFuture1.isDone());
      assertFalse(waitingFuture2.isDone());

      eventBuffers.reset(1L);
      Thread.sleep(100);
      assertFalse(waitingFuture1.isDone(), "Checkpoint 2 still pending, should continue blocking");
      assertFalse(waitingFuture2.isDone(), "Checkpoint 2 still pending, should continue blocking");

      eventBuffers.reset(2L);
      waitingFuture1.get(2, TimeUnit.SECONDS);
      assertTrue(waitingFuture1.isDone());
      waitingFuture2.get(2, TimeUnit.SECONDS);
      assertTrue(waitingFuture2.isDone());
    } finally {
      executor.shutdownNow();
    }
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
