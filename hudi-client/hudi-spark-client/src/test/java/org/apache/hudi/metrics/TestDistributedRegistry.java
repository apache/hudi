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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link DistributedRegistry}.
 */
public class TestDistributedRegistry {
  private static final String METRIC_1 = "metric1";
  private static final String METRIC_2 = "metric2";
  private static final String REGISTRY_NAME = "testDistributedRegistry";

  private static JavaSparkContext jsc;
  private static HoodieSparkEngineContext engineContext;

  @BeforeAll
  public static void setUp() {
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(TestDistributedRegistry.class.getSimpleName()));
    engineContext = new HoodieSparkEngineContext(jsc);
  }

  @AfterAll
  public static void tearDown() {
    if (jsc != null) {
      jsc.stop();
      jsc = null;
    }
  }

  @Test
  public void testRegisterAndName() {
    String registryName = REGISTRY_NAME + "_testRegisterAndName";
    Registry registry = engineContext.getMetricRegistry("", registryName);
    assertThat(registry, instanceOf(DistributedRegistry.class));

    // Then: registry name must match
    Assertions.assertEquals(registryName, registry.getName());

    // Then: there shouldn't be any metrics
    Assertions.assertTrue(registry.getAllCounts().isEmpty());
  }

  @Test
  public void testAddSetIncrementMetricsSingleThread() {
    // Given: distributed registry is created but not registered to spark context
    String registryName = REGISTRY_NAME + "_testAddSetIncrement";
    DistributedRegistry registry = new DistributedRegistry(registryName);

    // When: metrics and values are added
    registry.add(METRIC_1, 1);
    registry.add(METRIC_1, 3);
    registry.add(METRIC_2, 5);
    registry.add(METRIC_2, 7);

    // Then: there should be 2 metrics counts
    Map<String, Long> counts = registry.getAllCounts();
    Assertions.assertEquals(2, counts.size());

    // Then: metric1 should exist, value 4 is expected
    Assertions.assertTrue(counts.containsKey(METRIC_1));
    Assertions.assertEquals(4, counts.get(METRIC_1));

    // Then: metric2 should exist, value 12 is expected
    Assertions.assertTrue(counts.containsKey(METRIC_2));
    Assertions.assertEquals(12, counts.get(METRIC_2));

    // When: metric1 and 2 are incremented
    registry.increment(METRIC_1);
    registry.increment(METRIC_2);

    // Then: expected for metric1 is 5, metric2 is 13
    counts = registry.getAllCounts();
    Assertions.assertEquals(5, counts.get(METRIC_1));
    Assertions.assertEquals(13, counts.get(METRIC_2));

    // When: metric1 is set to 11 and metric2 is set to 13
    registry.set(METRIC_1, 11);
    registry.set(METRIC_2, 13);

    // Then: expected value for metric1 and 2 are 11,13 respectively
    counts = registry.getAllCounts();
    Assertions.assertEquals(11, counts.get(METRIC_1));
    Assertions.assertEquals(13, counts.get(METRIC_2));
  }

  @Test
  public void testIncrementMetricsParallel() {
    // Given: distributed registry is created and registered to spark context
    String registryName = REGISTRY_NAME + "_testIncrementParallel";
    Registry registry = engineContext.getMetricRegistry("", registryName);

    // Given: list of metrics to be added in parallel in spark executor.
    List<String> info = new ArrayList<>();

    // Given: metric 1 will have 1000 metrics
    int numMetric1 = 1000;
    for (int i = 0; i < numMetric1; i++) {
      info.add(METRIC_1);
    }

    // Given: metric 2 will have 1500 metrics
    int numMetric2 = 1500;
    for (int i = 0; i < numMetric2; i++) {
      info.add(METRIC_2);
    }

    // When: spark executors run on 2500 metrics
    engineContext.map(info, metricName -> {
      registry.increment(metricName);
      return null;
    }, 100);

    // Then: there should be two metrics
    Map<String, Long> metricCounts = registry.getAllCounts();
    Assertions.assertEquals(2, metricCounts.size());

    // Then: metric 1 should exist and have value 1000
    Assertions.assertTrue(metricCounts.containsKey(METRIC_1));
    Assertions.assertEquals(numMetric1, metricCounts.get(METRIC_1));

    // Then: metric 2 should exist and have value 1500
    Assertions.assertTrue(metricCounts.containsKey(METRIC_2));
    Assertions.assertEquals(numMetric2, metricCounts.get(METRIC_2));
  }

  @Test
  public void testAddMetricsParallel() {
    // Given: distributed registry is created and registered to spark context
    String registryName = REGISTRY_NAME + "_testAddParallel";
    Registry registry = engineContext.getMetricRegistry("", registryName);

    // Given: list of metric values to be added in parallel
    List<Long> values = new ArrayList<>();
    int numValues = 1000;
    long expectedSum = 0;
    for (int i = 0; i < numValues; i++) {
      values.add((long) i);
      expectedSum += i;
    }

    // When: spark executors add all values to metric1
    final long finalExpectedSum = expectedSum;
    engineContext.map(values, value -> {
      registry.add(METRIC_1, value);
      return null;
    }, 100);

    // Then: metric1 should have the sum of all values
    Map<String, Long> metricCounts = registry.getAllCounts();
    Assertions.assertEquals(1, metricCounts.size());
    Assertions.assertEquals(finalExpectedSum, metricCounts.get(METRIC_1));
  }

  @Test
  public void testSetOnExecutorIsIgnoredWithoutFailingTheJob() {
    // Given: a registry registered to the spark context
    String registryName = REGISTRY_NAME + "_testSetOnExecutor";
    Registry registry = engineContext.getMetricRegistry("", registryName);

    List<Integer> data = new ArrayList<>();
    data.add(1);

    // When: set() is invoked from an executor. It is non-commutative under accumulator merges, so the
    // value must not be recorded -- but the guard runs inside a task, where failing would take the write
    // down with it, so it is ignored rather than thrown.
    engineContext.map(data, value -> {
      registry.set(METRIC_1, value);
      return null;
    }, 1);

    // Then: the job succeeded and nothing was recorded.
    Assertions.assertFalse(registry.getAllCounts().containsKey(METRIC_1),
        "set() from an executor must not record a value: " + registry.getAllCounts());
  }

  @Test
  public void testReleaseOnExecutorIsIgnoredWithoutFailingTheJob() {
    // Given: a registry holding a known count
    String registryName = REGISTRY_NAME + "_testReleaseOnExecutor";
    Registry registry = engineContext.getMetricRegistry("", registryName);
    registry.add(METRIC_1, 5L);

    List<Integer> data = new ArrayList<>();
    data.add(1);

    // When: release() is invoked from an executor. Clamping and eviction are order-dependent under
    // accumulator merges, and this runs after the commit has landed, so it is ignored rather than thrown.
    engineContext.map(data, value -> {
      registry.release(Collections.singletonMap(METRIC_1, (long) value));
      return null;
    }, 1);

    // Then: the job succeeded and the count was left alone.
    Assertions.assertEquals(5L, registry.getAllCounts().get(METRIC_1),
        "release() from an executor must not mutate the counters");
  }

  @Test
  public void testSetOnDriverSucceeds() {
    // set() on the driver (no TaskContext) remains supported.
    DistributedRegistry registry = new DistributedRegistry(REGISTRY_NAME + "_testSetOnDriver");
    registry.set(METRIC_1, 42);
    Assertions.assertEquals(42, registry.getAllCounts().get(METRIC_1));
  }

  @Test
  public void testGetMetricRegistryReplacesNonDistributedRegistry() {
    // Given: a LocalRegistry already occupies the shared map under this key, as happens when a write
    // client ran earlier in the same JVM with executor metrics disabled.
    String registryName = REGISTRY_NAME + "_testLocalRegistryTakeover";
    Registry local = Registry.getRegistry(registryName);
    Assertions.assertSame(local, Registry.REGISTRY_MAP.get(Registry.makeKey("", registryName)));

    // When: the same name is resolved as a distributed registry
    Registry resolved = engineContext.getMetricRegistry("", registryName);

    // Then: the incompatible entry is replaced rather than cast-failing on the way out
    assertThat(resolved, instanceOf(DistributedRegistry.class));
    Assertions.assertSame(resolved, Registry.REGISTRY_MAP.get(Registry.makeKey("", registryName)));
  }

  @Test
  public void testReleaseEvictsCountersThatReachZero() {
    // Given: a registry drained of exactly what it holds, as the commit-boundary drain does
    DistributedRegistry registry = new DistributedRegistry(REGISTRY_NAME + "_testRelease");
    registry.add(METRIC_1, 10);
    registry.add(METRIC_2, 20);

    // When: the full contents are released
    registry.release(registry.getAllCounts(false));

    // Then: the counters are gone, not left sitting at zero. A zero-valued entry survives
    // ConcurrentHashMap.merge() and would be republished by every later drain.
    Assertions.assertTrue(registry.getAllCounts().isEmpty(),
        "released counters must be evicted, found " + registry.getAllCounts());
    Assertions.assertTrue(registry.isZero());
  }

  @Test
  public void testReleaseKeepsWhatArrivedAfterTheDrain() {
    // Given: a snapshot taken, and a straggler update folded in before the release lands
    DistributedRegistry registry = new DistributedRegistry(REGISTRY_NAME + "_testReleaseStraggler");
    registry.add(METRIC_1, 10);
    Map<String, Long> drained = registry.getAllCounts(false);
    registry.add(METRIC_1, 4);

    // When: the drained counts are released
    registry.release(drained);

    // Then: only the drained amount is subtracted - the straggler belongs to the next drain
    Assertions.assertEquals(4L, registry.getAllCounts().get(METRIC_1));
  }

  @Test
  public void testReleaseClampsAtZero() {
    // Given: a registry emptied between the drain and the release. Metrics.shutdown() scrapes every
    // registry in the process with flush=true, so an unrelated table's write finishing does exactly this.
    DistributedRegistry registry = new DistributedRegistry(REGISTRY_NAME + "_testReleaseClamp");
    registry.add(METRIC_1, 10);
    Map<String, Long> drained = registry.getAllCounts(false);
    registry.clear();

    // When: the release lands on a registry that no longer holds those counts
    registry.release(drained);

    // Then: nothing negative is left behind - a negative counter would be stamped into commit metadata
    Assertions.assertTrue(registry.getAllCounts().isEmpty(),
        "an unbounded subtraction would have left " + METRIC_1 + " negative, got " + registry.getAllCounts());
  }

  @Test
  public void testClear() {
    // Given: distributed registry with some metrics
    String registryName = REGISTRY_NAME + "_testClear";
    DistributedRegistry registry = new DistributedRegistry(registryName);
    registry.add(METRIC_1, 10);
    registry.add(METRIC_2, 20);

    // Verify metrics exist
    Assertions.assertEquals(2, registry.getAllCounts().size());

    // When: clear is called
    registry.clear();

    // Then: all metrics should be cleared
    Assertions.assertTrue(registry.getAllCounts().isEmpty());
  }

  @Test
  public void testGetAllCountsWithPrefix() {
    // Given: distributed registry with some metrics
    String registryName = REGISTRY_NAME + "_testPrefix";
    DistributedRegistry registry = new DistributedRegistry(registryName);
    registry.add(METRIC_1, 10);
    registry.add(METRIC_2, 20);

    // When: getAllCounts is called with prefix
    Map<String, Long> countsWithPrefix = registry.getAllCounts(true);

    // Then: metric names should be prefixed with registry name
    Assertions.assertEquals(2, countsWithPrefix.size());
    Assertions.assertTrue(countsWithPrefix.containsKey(registryName + "." + METRIC_1));
    Assertions.assertTrue(countsWithPrefix.containsKey(registryName + "." + METRIC_2));
    Assertions.assertEquals(10, countsWithPrefix.get(registryName + "." + METRIC_1));
    Assertions.assertEquals(20, countsWithPrefix.get(registryName + "." + METRIC_2));

    // When: getAllCounts is called without prefix
    Map<String, Long> countsWithoutPrefix = registry.getAllCounts(false);

    // Then: metric names should not be prefixed
    Assertions.assertEquals(2, countsWithoutPrefix.size());
    Assertions.assertTrue(countsWithoutPrefix.containsKey(METRIC_1));
    Assertions.assertTrue(countsWithoutPrefix.containsKey(METRIC_2));
  }
}
