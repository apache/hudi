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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestNonThrownExecutor {

  @Test
  void testTracksTasksThroughCompletionFailureAndRejection() throws Exception {
    CountDownLatch gate = new CountDownLatch(1);
    AtomicInteger completed = new AtomicInteger();
    AtomicInteger failures = new AtomicInteger();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    NonThrownExecutor executor = NonThrownExecutor.builder(mock(Logger.class))
        .exceptionHook((message, error) -> {
          failures.incrementAndGet();
          failure.set(error);
        })
        .waitForTasksFinish(true)
        .build();
    try {
      assertFalse(executor.hasRunningTasks());
      executor.execute(() -> {
        gate.await();
        throw new IllegalStateException("test failure");
      }, "blocked task");
      executor.execute(completed::incrementAndGet, "queued task");
      assertTrue(executor.hasRunningTasks());
    } finally {
      gate.countDown();
      executor.close();
    }
    assertEquals(1, failures.get());
    assertEquals(1, completed.get());
    assertFalse(executor.hasRunningTasks());
    executor.execute(completed::incrementAndGet, "rejected task");
    assertEquals(2, failures.get());
    assertInstanceOf(RejectedExecutionException.class, failure.get());
    assertFalse(executor.hasRunningTasks());
    executor.executeSync(completed::incrementAndGet, "rejected synchronous task");
    assertEquals(3, failures.get());
    assertInstanceOf(RejectedExecutionException.class, failure.get());
    assertFalse(executor.hasRunningTasks());

    AtomicInteger customFailures = new AtomicInteger();
    executor.execute(completed::incrementAndGet, (message, error) -> {
      assertInstanceOf(RejectedExecutionException.class, error);
      customFailures.incrementAndGet();
    }, "rejected task with custom hook");
    assertEquals(1, customFailures.get());
    assertEquals(3, failures.get());
    assertEquals(1, completed.get());
    assertFalse(executor.hasRunningTasks());
  }
}
