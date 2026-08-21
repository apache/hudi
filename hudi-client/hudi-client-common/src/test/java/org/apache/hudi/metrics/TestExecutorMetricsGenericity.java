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

import org.apache.hudi.common.metrics.ExecutorMetricsContext;
import org.apache.hudi.common.metrics.LocalRegistry;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The claim this feature exists to support is that adding a class of executor metric costs a declaration
 * plus two lines at the emission site. These tests hold that claim to a measurement rather than an
 * argument, by collecting a group the shipping code has never heard of.
 */
public class TestExecutorMetricsGenericity {

  private static final String BASE_PATH = "file:///tmp/test_generic_metrics";

  /** A class of metric added by a hypothetical future contributor: a name, a prefix, and no config. */
  private static final ExecutorMetricGroup STORAGE_CALLS = new ExecutorMetricGroup() {
    @Override
    public String registryName() {
      return "HoodieStorageCalls";
    }

    @Override
    public String commitMetadataPrefix() {
      return "hoodie.storage.calls.";
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

  private static HoodieWriteConfig config() {
    return HoodieWriteConfig.newBuilder().withPath(BASE_PATH).forTable("generic_metrics_table").build();
  }

  private static Registry seed(HoodieWriteConfig cfg) {
    Registry registry = new LocalRegistry(STORAGE_CALLS.scopedName(cfg.getBasePath()));
    Registry.REGISTRY_MAP.put(
        Registry.makeKey(cfg.getTableName(), STORAGE_CALLS.scopedName(cfg.getBasePath())), registry);
    return registry;
  }

  /**
   * Emission is the two lines the requirement names, and nothing else: name a registry, add to it. The
   * emitting code below knows only a string.
   */
  @Test
  void undeclaredMetricGroupStillReachesCommitMetadata() {
    HoodieWriteConfig cfg = config();
    Registry backing = seed(cfg);

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

    Map<String, String> commitMetadata = new HashMap<>();
    ExecutorMetrics.DrainedCounters drained = ExecutorMetrics.snapshotIntoCommitMetadata(
        commitMetadata, cfg, Collections.singletonList(STORAGE_CALLS));

    assertEquals("7", commitMetadata.get("hoodie.storage.calls.open"),
        "a group the enum never declared must still be drained under its own prefix");
    assertEquals("1", commitMetadata.get("hoodie.storage.calls.list"));
    assertTrue(commitMetadata.keySet().stream().allMatch(k -> k.startsWith("hoodie.storage.calls.")),
        "no other group's counters leak in; got " + commitMetadata.keySet());

    // And the same release semantics apply, so the next commit reports only its own work.
    ExecutorMetrics.publishAndRelease(drained, null);
    Map<String, String> nextCommit = new HashMap<>();
    ExecutorMetrics.snapshotIntoCommitMetadata(nextCommit, cfg, Collections.singletonList(STORAGE_CALLS));
    assertTrue(nextCommit.isEmpty(), "released counters must not be stamped again; got " + nextCommit);
  }

  /**
   * The counters of two groups collected in the same commit stay separate. Name-keyed resolution could not
   * express this, which is why the binding is per task.
   */
  @Test
  void twoGroupsInOneCommitDoNotMix() {
    HoodieWriteConfig cfg = config();
    Registry storage = seed(cfg);

    Registry rli = new LocalRegistry(ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.scopedName(cfg.getBasePath()));
    Registry.REGISTRY_MAP.put(
        Registry.makeKey(cfg.getTableName(),
            ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.scopedName(cfg.getBasePath())), rli);

    Map<String, Registry> bundle = new HashMap<>();
    bundle.put(STORAGE_CALLS.registryName(), storage);
    bundle.put(ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.registryName(), rli);

    Map<String, Registry> previous = ExecutorMetricsContext.bind(bundle);
    try {
      Registry.getRegistry("HoodieStorageCalls").add("open", 3L);
      Registry.getRegistry(ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.registryName())
          .add(RecordIndexMetricNames.key(RecordIndexMetricNames.CALLER_TAG_LOCATION,
              RecordIndexMetricNames.RECORDS_LOOKED_UP), 11L);
    } finally {
      ExecutorMetricsContext.unbind(previous);
    }

    Map<String, String> commitMetadata = new HashMap<>();
    ExecutorMetrics.snapshotIntoCommitMetadata(commitMetadata, cfg,
        java.util.Arrays.asList(STORAGE_CALLS, ExecutorMetricRegistry.RECORD_INDEX_LOOKUP));

    assertEquals("3", commitMetadata.get("hoodie.storage.calls.open"));
    assertEquals("11", commitMetadata.get("hoodie.rli.lookup.tag.records_looked_up"));
    assertNull(commitMetadata.get("hoodie.storage.calls.tag.records_looked_up"),
        "the record index counter must not appear under the storage prefix");
  }
}
