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

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Commit-boundary drain for executor-collected metrics, generic over {@link ExecutorMetricRegistry}. On
 * the shared commit path, so it covers Spark DataSource, Spark SQL and DeltaStreamer alike.
 */
public class ExecutorMetrics {

  private ExecutorMetrics() {
  }

  /**
   * Snapshots into commit metadata without consuming. Split from {@link #publishAndRelease} so a commit
   * that never lands neither loses its counters nor publishes gauges for rolled-back work. An all-zero
   * registry is skipped to keep residue off the timeline; zeros are otherwise kept, since an explicit
   * {@code misses=0} is meaningful.
   */
  public static DrainedCounters snapshotIntoCommitMetadata(Map<String, String> commitMetadata,
                                                           HoodieWriteConfig config) {
    return snapshotIntoCommitMetadata(commitMetadata, config, Arrays.asList(ExecutorMetricRegistry.values()));
  }

  /** Visible for testing the collection machinery against a group it does not ship with. */
  static DrainedCounters snapshotIntoCommitMetadata(Map<String, String> commitMetadata,
                                                    HoodieWriteConfig config,
                                                    Collection<? extends ExecutorMetricGroup> groups) {
    List<Drained> drained = new ArrayList<>();
    for (ExecutorMetricGroup metricRegistry : groups) {
      if (!metricRegistry.isEnabled(config)) {
        continue;
      }
      Registry registry = Registry.REGISTRY_MAP.get(
          Registry.makeKey(config.getTableName(), metricRegistry.scopedName(config.getBasePath())));
      if (registry == null) {
        continue;
      }
      Map<String, Long> counts = new HashMap<>();
      boolean recordedSomething = false;
      for (Map.Entry<String, Long> counter : registry.getAllCounts(false).entrySet()) {
        if (counter.getValue() == null) {
          continue;
        }
        counts.put(counter.getKey(), counter.getValue());
        recordedSomething |= counter.getValue() != 0L;
      }
      if (!recordedSomething) {
        continue;
      }
      counts.forEach((name, value) ->
          commitMetadata.put(metricRegistry.commitMetadataPrefix() + name, String.valueOf(value)));
      drained.add(new Drained(metricRegistry, registry, counts));
    }
    return drained.isEmpty() ? DrainedCounters.EMPTY : new DrainedCounters(drained);
  }

  /**
   * Release subtracts what was published rather than clearing, so a straggler task's update arriving after
   * the snapshot survives. Publishing here rather than letting the reporter scrape is what lets both sinks
   * work at once: {@link Registry#getAllMetrics} consumes the registry when it scrapes.
   */
  public static void publishAndRelease(DrainedCounters counters, HoodieMetrics hoodieMetrics) {
    for (Drained drained : counters.drained) {
      publishToReporter(drained, hoodieMetrics);
      drained.registry.release(drained.counts);
    }
  }

  /** Gauges, so each commit overwrites the previous value rather than accumulating. */
  private static void publishToReporter(Drained drained, HoodieMetrics hoodieMetrics) {
    if (hoodieMetrics == null || hoodieMetrics.getMetrics() == null) {
      return;
    }
    String prefix = hoodieMetrics.getMetricsName(
        drained.metricRegistry.metricAction(), drained.metricRegistry.metricQualifier());
    hoodieMetrics.getMetrics().registerGauges(drained.counts, Option.ofNullable(prefix));
  }

  private static final class Drained {
    private final ExecutorMetricGroup metricRegistry;
    private final Registry registry;
    private final Map<String, Long> counts;

    private Drained(ExecutorMetricGroup metricRegistry, Registry registry, Map<String, Long> counts) {
      this.metricRegistry = metricRegistry;
      this.registry = registry;
      this.counts = counts;
    }
  }

  /** Pinned to instances: a {@code REGISTRY_MAP} entry can be replaced between snapshot and release. */
  public static final class DrainedCounters {

    static final DrainedCounters EMPTY = new DrainedCounters(Collections.emptyList());

    private final List<Drained> drained;

    private DrainedCounters(List<Drained> drained) {
      this.drained = drained;
    }

    public boolean isEmpty() {
      return drained.isEmpty();
    }
  }
}
