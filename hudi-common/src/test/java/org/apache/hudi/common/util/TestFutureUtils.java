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

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FutureUtils}.
 */
public class TestFutureUtils {

  @Test
  public void testAllOfSuccess() {
    CompletableFuture<String> f1 = CompletableFuture.completedFuture("a");
    CompletableFuture<String> f2 = CompletableFuture.completedFuture("b");
    CompletableFuture<String> f3 = CompletableFuture.completedFuture("c");

    List<String> results = FutureUtils.allOf(Arrays.asList(f1, f2, f3)).join();
    assertEquals(Arrays.asList("a", "b", "c"), results);
  }

  @Test
  public void testAllOfSingleFailurePreservesOriginalExceptionAndCancelsOthers() {
    IOException originalCause = new IOException("disk failed");

    CompletableFuture<String> f1 = new CompletableFuture<>();
    CompletableFuture<String> f2 = new CompletableFuture<>();
    CompletableFuture<String> f3 = new CompletableFuture<>();

    CompletableFuture<List<String>> result = FutureUtils.allOf(Arrays.asList(f1, f2, f3));

    f1.completeExceptionally(originalCause);

    CompletionException thrown = assertThrows(CompletionException.class, result::join);
    assertEquals(originalCause, thrown.getCause(),
        "The original IOException should be preserved, not masked by CancellationException");
    assertTrue(f2.isCancelled(), "f2 should be cancelled after f1 fails");
    assertTrue(f3.isCancelled(), "f3 should be cancelled after f1 fails");
  }

  /**
   * Ensure that the original exception is preserved even when the futures are completed in a concurrent manner.
   */
  @Test
  public void testAllOfPreservesOriginalExceptionUnderConcurrency() throws Exception {
    IOException originalCause = new IOException("disk failed");
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      CountDownLatch allStarted = new CountDownLatch(3);

      CompletableFuture<String> f1 = new CompletableFuture<>();
      CompletableFuture<String> f2 = new CompletableFuture<>();
      CompletableFuture<String> f3 = new CompletableFuture<>();

      executor.submit(() -> {
        allStarted.countDown();
        try {
          allStarted.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        f1.completeExceptionally(originalCause);
        return null;
      });
      executor.submit(() -> {
        allStarted.countDown();
        try {
          allStarted.await(5, TimeUnit.SECONDS);
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        f2.complete("b");
        return null;
      });
      executor.submit(() -> {
        allStarted.countDown();
        try {
          allStarted.await(5, TimeUnit.SECONDS);
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        f3.complete("c");
        return null;
      });

      CompletableFuture<List<String>> result = FutureUtils.allOf(Arrays.asList(f1, f2, f3));

      CompletionException thrown = assertThrows(CompletionException.class, result::join);
      assertInstanceOf(IOException.class, thrown.getCause(),
          "original IOException must be preserved, got: " + thrown.getCause());
    } finally {
      executor.shutdownNow();
    }
  }
}
