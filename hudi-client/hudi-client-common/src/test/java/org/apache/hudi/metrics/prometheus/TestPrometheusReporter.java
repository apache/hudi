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

package org.apache.hudi.metrics.prometheus;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsPrometheusConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestPrometheusReporter {

  private static final Logger LOG = LoggerFactory.getLogger(TestPrometheusReporter.class);

  @Mock
  HoodieWriteConfig writeConfig;
  @Mock
  HoodieMetricsConfig metricsConfig;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;
  
  private static final int TEST_PORT = 19090;
  private static final int TEST_PORT_2 = 19091;
  private List<PrometheusReporter> reportersToCleanup;

  @BeforeEach
  void setUp() {
    reportersToCleanup = new ArrayList<>();
  }

  @AfterEach
  void shutdownMetrics() {
    if (metrics != null) {
      metrics.shutdown();
    }
    
    for (PrometheusReporter reporter : reportersToCleanup) {
      try {
        reporter.stop();
      } catch (Exception e) {
        LOG.debug("Exception during test cleanup: {}", e.getMessage());
      }
    }
    reportersToCleanup.clear();
    
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  public void testRegisterGauge() {
    when(writeConfig.getMetricsConfig()).thenReturn(metricsConfig);
    when(writeConfig.isMetricsOn()).thenReturn(true);
    when(metricsConfig.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS);
    when(metricsConfig.getPrometheusPort()).thenReturn(9090);
    when(metricsConfig.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
    assertDoesNotThrow(() -> {
      new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
      hoodieMetrics = new HoodieMetrics(writeConfig, HoodieTestUtils.getDefaultStorage());
      metrics = hoodieMetrics.getMetrics();
    });
  }

  @Test
  public void testMultiTableReferenceCountingBasic() {
    MetricRegistry registry1 = new MetricRegistry();
    MetricRegistry registry2 = new MetricRegistry();
    
    HoodieMetricsConfig config1 = createMockConfig(TEST_PORT);
    HoodieMetricsConfig config2 = createMockConfig(TEST_PORT);
    
    assertFalse(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(0, PrometheusReporter.getReferenceCount(TEST_PORT));
    
    PrometheusReporter reporter1 = new PrometheusReporter(config1, registry1);
    reportersToCleanup.add(reporter1);
    
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(1, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertEquals(1, PrometheusReporter.getActiveExportsCount(TEST_PORT));
    
    PrometheusReporter reporter2 = new PrometheusReporter(config2, registry2);
    reportersToCleanup.add(reporter2);
    
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(2, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertEquals(2, PrometheusReporter.getActiveExportsCount(TEST_PORT));
    
    reporter1.stop();
    
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(1, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertEquals(1, PrometheusReporter.getActiveExportsCount(TEST_PORT));
    
    reporter2.stop();
    
    assertFalse(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(0, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertEquals(0, PrometheusReporter.getActiveExportsCount(TEST_PORT));
    
    reportersToCleanup.clear();
  }

  @Test
  public void testMultiplePortsIndependent() {
    MetricRegistry registry1 = new MetricRegistry();
    MetricRegistry registry2 = new MetricRegistry();
    
    HoodieMetricsConfig config1 = createMockConfig(TEST_PORT);
    HoodieMetricsConfig config2 = createMockConfig(TEST_PORT_2);
    
    PrometheusReporter reporter1 = new PrometheusReporter(config1, registry1);
    PrometheusReporter reporter2 = new PrometheusReporter(config2, registry2);
    reportersToCleanup.add(reporter1);
    reportersToCleanup.add(reporter2);
    
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT));
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT_2));
    assertEquals(1, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertEquals(1, PrometheusReporter.getReferenceCount(TEST_PORT_2));
    
    reporter1.stop();
    
    assertFalse(PrometheusReporter.isServerRunning(TEST_PORT));
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT_2));
    assertEquals(0, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertEquals(1, PrometheusReporter.getReferenceCount(TEST_PORT_2));
    
    reporter2.stop();
    reportersToCleanup.clear();
  }

  @Test
  public void testConcurrentReporterCreation() throws Exception {
    final int numThreads = 10;
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch completeLatch = new CountDownLatch(numThreads);
    final List<PrometheusReporter> reporters = new ArrayList<>();
    
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < numThreads; i++) {
        futures.add(executor.submit(() -> {
          try {
            startLatch.await();
            MetricRegistry registry = new MetricRegistry();
            HoodieMetricsConfig config = createMockConfig(TEST_PORT);
            PrometheusReporter reporter = new PrometheusReporter(config, registry);
            synchronized (reporters) {
              reporters.add(reporter);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debug("Thread interrupted during concurrent test setup");
          } catch (Exception e) {
            LOG.debug("Expected exception during concurrent PrometheusReporter test: {}", e.getMessage());
          } finally {
            completeLatch.countDown();
          }
        }));
      }
      
      startLatch.countDown();
      
      assertTrue(completeLatch.await(10, TimeUnit.SECONDS));
      
      for (Future<?> future : futures) {
        assertDoesNotThrow(() -> future.get());
      }
      
      assertTrue(PrometheusReporter.isServerRunning(TEST_PORT));
      assertEquals(numThreads, PrometheusReporter.getReferenceCount(TEST_PORT));
      assertEquals(numThreads, PrometheusReporter.getActiveExportsCount(TEST_PORT));
      
      synchronized (reporters) {
        for (PrometheusReporter reporter : reporters) {
          reporter.stop();
        }
        reportersToCleanup.addAll(reporters);
      }
      
      assertFalse(PrometheusReporter.isServerRunning(TEST_PORT));
      assertEquals(0, PrometheusReporter.getReferenceCount(TEST_PORT));
      
    } finally {
      executor.shutdown();
      reportersToCleanup.clear();
    }
  }

  @Test
  public void testReporterLifecycle() {
    MetricRegistry registry = new MetricRegistry();
    HoodieMetricsConfig config = createMockConfig(TEST_PORT);
    
    PrometheusReporter reporter = new PrometheusReporter(config, registry);
    reportersToCleanup.add(reporter);
    
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(1, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertEquals(1, PrometheusReporter.getActiveExportsCount(TEST_PORT));
    
    reporter.stop();
    
    assertFalse(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(0, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertEquals(0, PrometheusReporter.getActiveExportsCount(TEST_PORT));
    
    reportersToCleanup.clear();
  }

  @Test
  public void testPartialFailureScenario() {
    MetricRegistry registry1 = new MetricRegistry();
    MetricRegistry registry2 = new MetricRegistry();
    MetricRegistry registry3 = new MetricRegistry();
    
    HoodieMetricsConfig config = createMockConfig(TEST_PORT);
    
    PrometheusReporter reporter1 = new PrometheusReporter(config, registry1);
    PrometheusReporter reporter2 = new PrometheusReporter(config, registry2);
    PrometheusReporter reporter3 = new PrometheusReporter(config, registry3);
    reportersToCleanup.add(reporter1);
    reportersToCleanup.add(reporter2);
    reportersToCleanup.add(reporter3);
    
    assertEquals(3, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT));
    
    reporter2.stop();
    
    assertEquals(2, PrometheusReporter.getReferenceCount(TEST_PORT));
    assertTrue(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(2, PrometheusReporter.getActiveExportsCount(TEST_PORT));
    
    reporter1.stop();
    reporter3.stop();
    
    assertFalse(PrometheusReporter.isServerRunning(TEST_PORT));
    assertEquals(0, PrometheusReporter.getReferenceCount(TEST_PORT));
    
    reportersToCleanup.clear();
  }

  private HoodieMetricsConfig createMockConfig(int port) {
    Properties props = new Properties();
    props.setProperty(HoodieMetricsPrometheusConfig.PROMETHEUS_PORT_NUM.key(), String.valueOf(port));
    return HoodieMetricsConfig.newBuilder()
        .fromProperties(props)
        .build();
  }
}
