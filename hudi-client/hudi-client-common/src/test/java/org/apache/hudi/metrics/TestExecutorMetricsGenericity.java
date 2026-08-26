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

import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.common.metrics.ExecutorMetricsContext;
import org.apache.hudi.common.metrics.LocalRegistry;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Adding a class of executor metric should cost a declaration plus two lines at the emission site.
 * These tests hold that to a measurement by collecting a group the shipping code has never heard of.
 */
public class TestExecutorMetricsGenericity {

  private static final String BASE_PATH = "file:/tmp/test_generic_metrics";

  /** A class of metric added by a hypothetical future contributor: a name, reporter naming, no config. */
  private static final ExecutorMetricGroup STORAGE_CALLS = new ExecutorMetricGroup() {
    @Override
    public String registryName() {
      return "HoodieStorageCalls";
    }

    @Override
    public String metricAction() {
      return "storage";
    }

    @Override
    public String metricQualifier() {
      return "calls";
    }

    @Override
    public boolean isEnabled(HoodieWriteConfig config) {
      return true;
    }

    @Override
    public String scopedName(String basePath) {
      return registryName() + ".test";
    }
  };

  private HoodieMetrics hoodieMetrics;

  @AfterEach
  void clearProcessWideState() {
    Registry.REGISTRY_MAP.keySet().removeIf(k ->
        k.contains(STORAGE_CALLS.registryName()) || k.contains(RecordIndexMetricNames.REGISTRY_NAME));
    if (hoodieMetrics != null && hoodieMetrics.getMetrics() != null) {
      hoodieMetrics.getMetrics().shutdown();
      hoodieMetrics = null;
    }
  }

  private static HoodieWriteConfig config() {
    return HoodieWriteConfig.newBuilder()
        .withPath(BASE_PATH)
        .forTable("generic_metrics_table")
        .withMetricsConfig(HoodieMetricsConfig.newBuilder()
            .on(true)
            .withReporterType(MetricsReporterType.INMEMORY.name())
            .build())
        .build();
  }

  private static Registry seed(HoodieWriteConfig cfg, ExecutorMetricGroup group) {
    Registry registry = new LocalRegistry(group.scopedName(cfg.getBasePath()));
    Registry.REGISTRY_MAP.put(
        Registry.makeKey(cfg.getTableName(), group.scopedName(cfg.getBasePath())), registry);
    return registry;
  }

  private Long gauge(ExecutorMetricGroup group, String metric) {
    String name = hoodieMetrics.getMetricsName(group.metricAction(), group.metricQualifier()) + "." + metric;
    com.codahale.metrics.Gauge<?> g = hoodieMetrics.getMetrics().getRegistry().getGauges().get(name);
    return g == null ? null : (Long) g.getValue();
  }

  /** Emission is the two lines the requirement names: name a registry, add to it. */
  @Test
  void undeclaredMetricGroupStillReachesTheReporter() {
    HoodieWriteConfig cfg = config();
    Registry backing = seed(cfg, STORAGE_CALLS);
    hoodieMetrics = new HoodieMetrics(cfg, HoodieTestUtils.getDefaultStorage());

    // Stand in for a task: bind the bundle, then emit by name from code that was handed nothing.
    Map<String, Registry> previous = ExecutorMetricsContext.bind(
        Collections.singletonMap(STORAGE_CALLS.registryName(), backing));
    try {
      Registry r = Registry.getRegistry("HoodieStorageCalls");
      r.add("open", 7L);
      r.increment("list");
    } finally {
      ExecutorMetricsContext.unbind(previous);
    }

    ExecutorMetrics.publishAndRelease(cfg, hoodieMetrics, Collections.singletonList(STORAGE_CALLS));

    assertEquals(7L, gauge(STORAGE_CALLS, "open"),
        "a group the enum never declared must still reach the reporter under its own naming");
    assertEquals(1L, gauge(STORAGE_CALLS, "list"));
    assertEquals(Collections.emptyMap(), backing.getAllCounts(false),
        "and it is subject to the same release semantics");
  }

  /** Two groups drained in the same commit report under their own names and do not mix. */
  @Test
  void twoGroupsInOneCommitDoNotMix() {
    HoodieWriteConfig cfg = config();
    Registry storage = seed(cfg, STORAGE_CALLS);
    Registry rli = seed(cfg, ExecutorMetricRegistry.RECORD_INDEX_LOOKUP);
    hoodieMetrics = new HoodieMetrics(cfg, HoodieTestUtils.getDefaultStorage());

    Map<String, Registry> bundle = new HashMap<>();
    bundle.put(STORAGE_CALLS.registryName(), storage);
    bundle.put(ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.registryName(), rli);

    String rliCounter = RecordIndexMetricNames.key(
        RecordIndexMetricNames.CALLER_TAG_LOCATION, RecordIndexMetricNames.KEY_COUNT);

    Map<String, Registry> previous = ExecutorMetricsContext.bind(bundle);
    try {
      Registry.getRegistry("HoodieStorageCalls").add("open", 3L);
      Registry.getRegistry(ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.registryName()).add(rliCounter, 11L);
    } finally {
      ExecutorMetricsContext.unbind(previous);
    }

    ExecutorMetrics.publishAndRelease(cfg, hoodieMetrics,
        Arrays.asList(STORAGE_CALLS, ExecutorMetricRegistry.RECORD_INDEX_LOOKUP));

    assertEquals(3L, gauge(STORAGE_CALLS, "open"));
    assertEquals(11L, gauge(ExecutorMetricRegistry.RECORD_INDEX_LOOKUP, rliCounter));
    assertNull(gauge(STORAGE_CALLS, rliCounter),
        "the record index counter must not appear under the storage group's naming");
  }
}
