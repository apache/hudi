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

package org.apache.hudi.metrics;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Lifecycle tests for {@link DistributedRegistry} that manage their own SparkContexts, so that a SparkContext restart within the same JVM can be exercis
 */
public class TestDistributedRegistryLifecycle {

  /**
   * Both registry maps are process-wide statics, so entries created here would otherwise outlive the test bound to SparkContexts it has already stopped, a
   */
  @AfterEach
  public void removeRegistriesFromProcessWideMaps() {
    HoodieSparkEngineContext.removeMetricRegistry("", "restartRegistry");
    HoodieSparkEngineContext.removeMetricRegistry("", "concurrentRestartRegistry");
  }

  @Test
  public void testRegistryEvictedAfterSparkContextRestart() {
    String registryName = "restartRegistry";

    // First SparkContext: create and register a distributed registry.
    JavaSparkContext jsc1 = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("restart-ctx-1"));
    DistributedRegistry firstRegistry;
    try {
      HoodieSparkEngineContext context1 = new HoodieSparkEngineContext(jsc1);
      firstRegistry = (DistributedRegistry) context1.getMetricRegistry("", registryName);
      Assertions.assertTrue(firstRegistry.isRegisteredWith(jsc1));
    } finally {
      jsc1.stop();
    }

    // Second SparkContext in the same JVM: the stale registry bound to the stopped context must be
    // evicted and a fresh one, bound to the new context, returned.
    JavaSparkContext jsc2 = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("restart-ctx-2"));
    try {
      HoodieSparkEngineContext context2 = new HoodieSparkEngineContext(jsc2);
      DistributedRegistry secondRegistry = (DistributedRegistry) context2.getMetricRegistry("", registryName);
      Assertions.assertNotSame(firstRegistry, secondRegistry, "A fresh registry must be created for the new SparkContext");
      Assertions.assertTrue(secondRegistry.isRegisteredWith(jsc2));
    } finally {
      jsc2.stop();
    }
  }

  @Test
  public void testConcurrentResolveAfterSparkContextRestartYieldsOneInstance() throws Exception {
    String registryName = "concurrentRestartRegistry";

    // Seed a registry against a context that is then stopped, so the cached entry is stale and every
    // caller below takes the evict-and-recreate path -- the path where a non-atomic
    // check-evict-create lets one caller drop the registry another caller just created.
    JavaSparkContext jsc1 = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("concurrent-restart-ctx-1"));
    try {
      new HoodieSparkEngineContext(jsc1).getMetricRegistry("", registryName);
    } finally {
      jsc1.stop();
    }

    JavaSparkContext jsc2 = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("concurrent-restart-ctx-2"));
    int threadCount = 16;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    try {
      HoodieSparkEngineContext context2 = new HoodieSparkEngineContext(jsc2);
      CyclicBarrier startLine = new CyclicBarrier(threadCount);
      List<Future<Registry>> futures = new ArrayList<>();
      for (int i = 0; i < threadCount; i++) {
        futures.add(pool.submit(() -> {
          startLine.await(30, TimeUnit.SECONDS);
          return context2.getMetricRegistry("", registryName);
        }));
      }

      // Every caller must observe the identical instance: two instances would mean two live
      // accumulators for one metric name, and only the mapped one is ever reported.
      Registry resolved = futures.get(0).get(60, TimeUnit.SECONDS);
      for (Future<Registry> future : futures) {
        Assertions.assertSame(resolved, future.get(60, TimeUnit.SECONDS));
      }
      Assertions.assertTrue(((DistributedRegistry) resolved).isRegisteredWith(jsc2));
      // The shared registry map must agree with what callers were handed.
      Assertions.assertSame(resolved, Registry.REGISTRY_MAP.get(Registry.makeKey("", registryName)));
    } finally {
      pool.shutdownNow();
      jsc2.stop();
    }
  }
}
