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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.metrics;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLocalRegistry {

  @Test
  public void testAddConcurrentWithClear() throws Exception {
    LocalRegistry registry = new LocalRegistry("test");
    registry.add("counter", 1);

    CountDownLatch counterLookupStarted = new CountDownLatch(1);
    CountDownLatch clearCompleted = new CountDownLatch(1);
    registry.counters = new ClearBeforeLookupMap<>(
        registry.counters, counterLookupStarted, clearCompleted);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> addFuture = executor.submit(() -> registry.add("counter", 1));
      assertTrue(counterLookupStarted.await(10, TimeUnit.SECONDS));
      registry.clear();
      clearCompleted.countDown();

      assertDoesNotThrow(() -> addFuture.get(10, TimeUnit.SECONDS));
      assertEquals(1L, registry.getAllCounts().get("counter"));
    } finally {
      clearCompleted.countDown();
      executor.shutdownNow();
    }
  }

  private static class ClearBeforeLookupMap<K, V> extends ConcurrentHashMap<K, V> {
    private final CountDownLatch counterLookupStarted;
    private final CountDownLatch clearCompleted;

    private ClearBeforeLookupMap(
        Map<K, V> entries,
        CountDownLatch counterLookupStarted,
        CountDownLatch clearCompleted) {
      super(entries);
      this.counterLookupStarted = counterLookupStarted;
      this.clearCompleted = clearCompleted;
    }

    @Override
    public boolean containsKey(Object key) {
      return super.get(key) != null;
    }

    @Override
    public V get(Object key) {
      awaitClear();
      return super.get(key);
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      awaitClear();
      return super.computeIfAbsent(key, mappingFunction);
    }

    private void awaitClear() {
      counterLookupStarted.countDown();
      try {
        assertTrue(clearCompleted.await(10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
  }
}
