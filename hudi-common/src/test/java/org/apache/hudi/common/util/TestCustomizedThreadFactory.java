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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

/**
 * Tests {@link CustomizedThreadFactory}.
 */
public class TestCustomizedThreadFactory {

  @Test
  public void testThreadPrefix() throws ExecutionException, InterruptedException {
    int nThreads = 100;
    String threadNamePrefix = "consumer";
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads, new CustomizedThreadFactory(threadNamePrefix));
    for (int i = 0; i < nThreads; i++) {
      Future<Boolean> resultFuture = executorService.submit(() -> {
        LockSupport.parkNanos(10000000L);
        String name = Thread.currentThread().getName();
        return name.startsWith(threadNamePrefix);
      });
      Boolean result = resultFuture.get();
      Assertions.assertTrue(result);
    }
    executorService.shutdown();
  }

  @Test
  public void testDefaultThreadPrefix() throws ExecutionException, InterruptedException {
    int nThreads = 100;
    String defaultThreadNamePrefix = "pool-1";
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads, new CustomizedThreadFactory());
    for (int i = 0; i < nThreads; i++) {
      Future<Boolean> resultFuture = executorService.submit(() -> {
        LockSupport.parkNanos(10000000L);
        String name = Thread.currentThread().getName();
        return name.startsWith(defaultThreadNamePrefix);
      });
      Boolean result = resultFuture.get();
      Assertions.assertTrue(result);
    }
    executorService.shutdown();
  }

  @Test
  public void testDaemonThread() throws ExecutionException, InterruptedException {
    int nThreads = 100;
    String threadNamePrefix = "consumer";
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads, new CustomizedThreadFactory(threadNamePrefix, true));
    for (int i = 0; i < nThreads; i++) {
      Future<Boolean> resultFuture = executorService.submit(() -> {
        LockSupport.parkNanos(10000000L);
        String name = Thread.currentThread().getName();
        boolean daemon = Thread.currentThread().isDaemon();
        return name.startsWith(threadNamePrefix) && daemon;
      });
      Boolean result = resultFuture.get();
      Assertions.assertTrue(result);
    }
    executorService.shutdown();
  }
}
