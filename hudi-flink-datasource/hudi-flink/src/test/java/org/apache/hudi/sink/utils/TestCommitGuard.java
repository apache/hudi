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

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link CommitGuard}.
 */
public class TestCommitGuard {

  @Test
  void testBlockForRechecksPendingInstantsAfterSignal() throws Exception {
    CommitGuard guard = CommitGuard.create(5_000L);
    AtomicBoolean pending = new AtomicBoolean(true);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      CompletableFuture<Void> waitingFuture1 = CompletableFuture.runAsync(
          () -> guard.blockFor(() -> pending.get() ? List.of("001", "002") : Collections.emptyList()), executor);
      CompletableFuture<Void> waitingFuture2 = CompletableFuture.runAsync(
          () -> guard.blockFor(() -> pending.get() ? List.of("001", "002") : Collections.emptyList()), executor);

      Thread.sleep(100);
      guard.unblock();
      Thread.sleep(100);
      assertFalse(waitingFuture1.isDone(), "Should still block because pending instants are not empty");
      assertFalse(waitingFuture2.isDone(), "Should still block because pending instants are not empty");

      pending.set(false);
      guard.unblock();
      waitingFuture1.get(2, TimeUnit.SECONDS);
      assertTrue(waitingFuture1.isDone(), "Should finish when pending instants become empty");
      waitingFuture2.get(2, TimeUnit.SECONDS);
      assertTrue(waitingFuture2.isDone(), "Should finish when pending instants become empty");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testBlockForTimeoutWhenPendingInstantsRemain() {
    CommitGuard guard = CommitGuard.create(100L);
    HoodieException hoodieException = assertThrows(
        HoodieException.class,
        () -> guard.blockFor(() -> Collections.singletonList("timeout-instant")));
    assertTrue(hoodieException.getMessage().contains("timeout-instant"));
  }
}
