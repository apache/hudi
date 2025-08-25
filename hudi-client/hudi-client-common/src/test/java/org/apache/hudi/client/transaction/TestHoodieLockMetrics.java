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

package org.apache.hudi.client.transaction;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.hudi.client.transaction.lock.metrics.HoodieLockMetrics;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.metrics.Metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class TestHoodieLockMetrics {

  @Test
  public void testMetricsHappyPath() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/gdsafsd")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(HoodieWriteConfig.newBuilder()
        .forTable("idk").withPath("/dsfasdf/asdf")
        .withMetricsConfig(metricsConfig)
        .build(), storage);

    //lock acquired
    assertDoesNotThrow(lockMetrics::startLockApiTimerContext);
    assertDoesNotThrow(lockMetrics::updateLockAcquiredMetric);
    assertDoesNotThrow(lockMetrics::updateLockHeldTimerMetrics);

    //lock not acquired
    assertDoesNotThrow(lockMetrics::startLockApiTimerContext);
    assertDoesNotThrow(lockMetrics::updateLockNotAcquiredMetric);
  }

  @Test
  public void testMetricsMisses() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/gdsafsd")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(HoodieWriteConfig.newBuilder()
        .forTable("idk").withPath("/dsfasdf/asdf")
        .withMetricsConfig(metricsConfig)
        .build(), storage);

    assertDoesNotThrow(lockMetrics::updateLockHeldTimerMetrics);
    assertDoesNotThrow(lockMetrics::updateLockNotAcquiredMetric);
    assertDoesNotThrow(lockMetrics::updateLockAcquiredMetric);
  }

  @Test
  public void testLockReleaseSuccessMetric() {
    // Test that lock release success metric is properly tracked
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .forTable("testTable").withPath("/test/path")
        .withMetricsConfig(metricsConfig)
        .build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(writeConfig, storage);

    // Get the metrics registry to verify counter values
    Metrics metrics = Metrics.getInstance(metricsConfig, storage);
    MetricRegistry registry = metrics.getRegistry();

    // Verify the lock release success counter exists
    String metricName = writeConfig.getMetricReporterMetricsNamePrefix() + "." + HoodieLockMetrics.LOCK_RELEASE_SUCCESS_COUNTER_NAME;
    Counter lockReleaseSuccessCounter = registry.getCounters().get(metricName);
    assertNotNull(lockReleaseSuccessCounter, "Lock release success counter should exist");
    
    long initialCount = lockReleaseSuccessCounter.getCount();

    // Simulate successful lock release
    lockMetrics.updateLockReleaseSuccessMetric();
    assertEquals(initialCount + 1, lockReleaseSuccessCounter.getCount(), "Lock release success counter should increment by 1");

    // Simulate multiple successful lock releases
    lockMetrics.updateLockReleaseSuccessMetric();
    lockMetrics.updateLockReleaseSuccessMetric();
    assertEquals(initialCount + 3, lockReleaseSuccessCounter.getCount(), "Lock release success counter should increment by 3 total");
  }

  @Test
  public void testLockLifecycleWithReleaseSuccess() {
    // Test complete lock lifecycle including acquisition and successful release
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .forTable("testTable").withPath("/test/path")
        .withMetricsConfig(metricsConfig)
        .build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(writeConfig, storage);

    // Get the metrics registry to verify counter values
    Metrics metrics = Metrics.getInstance(metricsConfig, storage);
    MetricRegistry registry = metrics.getRegistry();

    String acquireMetricName = writeConfig.getMetricReporterMetricsNamePrefix() + "." + HoodieLockMetrics.LOCK_ACQUIRE_SUCCESS_COUNTER_NAME;
    String releaseMetricName = writeConfig.getMetricReporterMetricsNamePrefix() + "." + HoodieLockMetrics.LOCK_RELEASE_SUCCESS_COUNTER_NAME;
    Counter lockAcquiredCounter = registry.getCounters().get(acquireMetricName);
    Counter lockReleaseSuccessCounter = registry.getCounters().get(releaseMetricName);
    
    long initialAcquireCount = lockAcquiredCounter.getCount();
    long initialReleaseCount = lockReleaseSuccessCounter.getCount();
    
    // Simulate complete lock lifecycle
    lockMetrics.startLockApiTimerContext();
    lockMetrics.updateLockAcquiredMetric();
    assertEquals(initialAcquireCount + 1, lockAcquiredCounter.getCount(), "Lock acquired counter should increment by 1");
    assertEquals(initialReleaseCount, lockReleaseSuccessCounter.getCount(), "Lock release success counter should not change yet");

    // Now release the lock successfully
    lockMetrics.updateLockReleaseSuccessMetric();
    lockMetrics.updateLockHeldTimerMetrics();
    assertEquals(initialAcquireCount + 1, lockAcquiredCounter.getCount(), "Lock acquired counter should still be incremented by 1");
    assertEquals(initialReleaseCount + 1, lockReleaseSuccessCounter.getCount(), "Lock release success counter should increment by 1");

    // Verify metrics balance for multiple cycles
    for (int i = 0; i < 5; i++) {
      lockMetrics.startLockApiTimerContext();
      lockMetrics.updateLockAcquiredMetric();
      lockMetrics.updateLockReleaseSuccessMetric();
      lockMetrics.updateLockHeldTimerMetrics();
    }
    
    assertEquals(initialAcquireCount + 6, lockAcquiredCounter.getCount(), "Lock acquired counter should increment by 6 total");
    assertEquals(initialReleaseCount + 6, lockReleaseSuccessCounter.getCount(), "Lock release success counter should increment by 6 total");
  }

  @Test
  public void testNewErrorMetrics() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(HoodieWriteConfig.newBuilder()
            .forTable("idk").withPath("/dsfasdf/asdf")
            .withMetricsConfig(metricsConfig)
            .build(), storage);

    // Test all the new error metrics methods
    assertDoesNotThrow(lockMetrics::updateLockAcquiredByOthersErrorMetric, 
        "updateLockAcquiredByOthersErrorMetric should not throw");
    assertDoesNotThrow(lockMetrics::updateLockStateUnknownMetric,
        "updateLockStateUnknownMetric should not throw");
    assertDoesNotThrow(lockMetrics::updateLockAcquirePreconditionFailureMetric,
        "updateLockAcquirePreconditionFailureMetric should not throw");
    assertDoesNotThrow(lockMetrics::updateLockProviderFatalErrorMetric,
        "updateLockProviderFatalErrorMetric should not throw");
  }

  @Test
  public void testNewErrorMetricsWithDisabledMetrics() {
    // Test that the new metrics methods work safely when metrics are disabled
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(false).build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(HoodieWriteConfig.newBuilder()
            .forTable("idk").withPath("/dsfasdf/asdf")
            .withMetricsConfig(metricsConfig)
            .build(), storage);

    // All methods should be safe to call even when metrics are disabled
    assertDoesNotThrow(lockMetrics::updateLockAcquiredByOthersErrorMetric,
        "updateLockAcquiredByOthersErrorMetric should not throw when metrics disabled");
    assertDoesNotThrow(lockMetrics::updateLockStateUnknownMetric,
        "updateLockStateUnknownMetric should not throw when metrics disabled");
    assertDoesNotThrow(lockMetrics::updateLockAcquirePreconditionFailureMetric,
        "updateLockAcquirePreconditionFailureMetric should not throw when metrics disabled");
    assertDoesNotThrow(lockMetrics::updateLockProviderFatalErrorMetric,
        "updateLockProviderFatalErrorMetric should not throw when metrics disabled");
  }

  @Test
  public void testCombinedMetricsScenario() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(HoodieWriteConfig.newBuilder()
            .forTable("idk").withPath("/dsfasdf/asdf")
            .withMetricsConfig(metricsConfig)
            .build(), storage);

    // Test a realistic scenario combining old and new metrics
    assertDoesNotThrow(() -> {
      // Start lock acquisition
      lockMetrics.startLockApiTimerContext();

      // Lock acquisition failed due to unknown state
      lockMetrics.updateLockStateUnknownMetric();
      lockMetrics.updateLockNotAcquiredMetric();

      // Retry - start another acquisition attempt
      lockMetrics.startLockApiTimerContext();

      // This time failed due to precondition failure
      lockMetrics.updateLockAcquirePreconditionFailureMetric();
      lockMetrics.updateLockNotAcquiredMetric();

      // Final attempt - lock acquired by others
      lockMetrics.startLockApiTimerContext();
      lockMetrics.updateLockAcquiredByOthersErrorMetric();
      lockMetrics.updateLockNotAcquiredMetric();

    }, "Combined metrics scenario should not throw");
  }

  @Test
  public void testLockInterruptedMetric() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
            .forTable("testTable").withPath("/test/path")
            .withMetricsConfig(metricsConfig)
            .build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(writeConfig, storage);

    // Get the metrics registry to verify counter values
    Metrics metrics = Metrics.getInstance(metricsConfig, storage);
    MetricRegistry registry = metrics.getRegistry();
    String metricName = writeConfig.getMetricReporterMetricsNamePrefix() + "." + HoodieLockMetrics.LOCK_INTERRUPTED;

    // Test that the interrupted metric can be called
    assertDoesNotThrow(lockMetrics::updateLockInterruptedMetric, 
        "updateLockInterruptedMetric should not throw");

    // Verify the counter exists and increments
    Counter interruptedCounter = registry.getCounters().get(metricName);
    assertNotNull(interruptedCounter, "Lock interrupted counter should exist");

    long initialCount = interruptedCounter.getCount();

    // Call the metric multiple times
    lockMetrics.updateLockInterruptedMetric();
    lockMetrics.updateLockInterruptedMetric();
    lockMetrics.updateLockInterruptedMetric();

    // Verify the counter incremented
    assertEquals(initialCount + 3, interruptedCounter.getCount(), 
        "Lock interrupted counter should increment by 3");
  }

  @Test
  public void testLockExpirationDeadlineGauge() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .forTable("testTable").withPath("/test/path")
        .withMetricsConfig(metricsConfig)
        .build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(writeConfig, storage);

    // Test that the method doesn't throw
    assertDoesNotThrow(() -> lockMetrics.updateLockExpirationDeadlineMetric(5000), 
        "updateLockExpirationDeadlineMetric should not throw");

    // Test multiple updates
    assertDoesNotThrow(() -> {
      lockMetrics.updateLockExpirationDeadlineMetric(5000);
      lockMetrics.updateLockExpirationDeadlineMetric(15000);
      lockMetrics.updateLockExpirationDeadlineMetric(500);
      lockMetrics.updateLockExpirationDeadlineMetric(-1000);
      lockMetrics.updateLockExpirationDeadlineMetric(1);
    }, "Multiple updateLockExpirationDeadlineMetric calls should not throw");

    // Get the metrics registry to verify gauge exists
    Metrics metrics = Metrics.getInstance(metricsConfig, storage);
    MetricRegistry registry = metrics.getRegistry();
    String metricName = writeConfig.getMetricReporterMetricsNamePrefix() + "." + HoodieLockMetrics.LOCK_EXPIRATION_DEADLINE;

    // Verify the gauge exists
    Gauge<?> deadlineGaugeRaw = registry.getGauges().get(metricName);
    assertNotNull(deadlineGaugeRaw, "Lock expiration deadline gauge should exist");

    // Test final value (should be 0 from last update)
    assertEquals(1, ((Number) deadlineGaugeRaw.getValue()).intValue(), "Final gauge value should be 1");
  }

  @Test
  public void testLockDanglingMetric() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(true).build();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .forTable("testTable").withPath("/test/path")
        .withMetricsConfig(metricsConfig)
        .build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(writeConfig, storage);

    // Get the metrics registry to verify counter values
    Metrics metrics = Metrics.getInstance(metricsConfig, storage);
    MetricRegistry registry = metrics.getRegistry();
    String metricName = writeConfig.getMetricReporterMetricsNamePrefix() + "." + HoodieLockMetrics.LOCK_DANGLING;

    // Test that the dangling metric can be called
    assertDoesNotThrow(lockMetrics::updateLockDanglingMetric, 
        "updateLockDanglingMetric should not throw");

    // Verify the counter exists and increments
    Counter danglingCounter = registry.getCounters().get(metricName);
    assertNotNull(danglingCounter, "Lock dangling counter should exist");

    long initialCount = danglingCounter.getCount();

    // Call the metric multiple times
    lockMetrics.updateLockDanglingMetric();
    lockMetrics.updateLockDanglingMetric();

    // Verify the counter incremented
    assertEquals(initialCount + 2, danglingCounter.getCount(), 
        "Lock dangling counter should increment by 2");
  }

  @Test
  public void testNewMetricsWithDisabledLocking() {
    HoodieStorage storage = mock(HoodieStorage.class);
    // Test that the new metrics methods work safely when locking metrics are disabled
    HoodieMetricsConfig metricsConfig = HoodieMetricsConfig.newBuilder().withPath("/test")
        .withReporterType(MetricsReporterType.INMEMORY.name()).withLockingMetrics(false).build();
    HoodieLockMetrics lockMetrics = new HoodieLockMetrics(HoodieWriteConfig.newBuilder()
        .forTable("testTable").withPath("/test/path")
        .withMetricsConfig(metricsConfig)
        .build(), storage);

    // All new methods should be safe to call even when locking metrics are disabled
    assertDoesNotThrow(lockMetrics::updateLockInterruptedMetric,
        "updateLockInterruptedMetric should not throw when locking metrics disabled");
    assertDoesNotThrow(() -> lockMetrics.updateLockExpirationDeadlineMetric(5000),
        "updateLockExpirationDeadlineMetric should not throw when locking metrics disabled");
    assertDoesNotThrow(lockMetrics::updateLockDanglingMetric,
        "updateLockDanglingMetric should not throw when locking metrics disabled");
  }
}
