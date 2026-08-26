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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Commit-boundary drain for executor-collected metrics, generic over {@link ExecutorMetricRegistry}. On
 * the shared commit path, so it covers Spark DataSource, Spark SQL and DeltaStreamer alike.
 */
public class ExecutorMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorMetrics.class);

  private ExecutorMetrics() {
  }

  /**
   * Reports each enabled group's counters and releases them, so the next commit reports only its own
   * work. Called once the commit has landed, so a rolled-back commit neither publishes its counters nor
   * loses them: they stay in the registry for the retry.
   *
   * <p>Release subtracts what was reported rather than clearing, so a straggler task whose update lands
   * mid-publish carries into the next commit instead of being dropped. An all-zero registry is skipped.
   */
  public static void publishAndRelease(HoodieWriteConfig config, HoodieMetrics hoodieMetrics) {
    try {
      publish(config, hoodieMetrics);
    } catch (Exception e) {
      // This runs after the commit has landed. Reporting is not worth failing a completed write over.
      LOG.warn("Failed to publish executor metrics; the commit is unaffected.", e);
    }
  }

  private static void publish(HoodieWriteConfig config, HoodieMetrics hoodieMetrics) {
    for (ExecutorMetricRegistry group : ExecutorMetricRegistry.values()) {
      if (!group.isEnabled(config)) {
        continue;
      }
      Registry registry = Registry.REGISTRY_MAP.get(
          Registry.makeKey(config.getTableName(), group.scopedName(config.getBasePath())));
      Map<String, Long> counts =
          registry == null ? Collections.emptyMap() : new HashMap<>(registry.getAllCounts(false));
      if (counts.values().stream().allMatch(value -> value == 0L)) {
        // Nothing was collected. Gauges hold their last value until overwritten, so leaving them alone
        // would have a reporter re-emit the previous commit's numbers for this one. Zero them instead.
        zeroPreviouslyReported(group, hoodieMetrics);
        continue;
      }
      publishToReporter(group, counts, hoodieMetrics);
      registry.release(counts);
    }
  }

  /**
   * Resets this group's gauges to zero, for a commit that collected nothing. Only names already published
   * are touched, so a table that has never emitted stays absent rather than reporting a row of zeros.
   */
  private static void zeroPreviouslyReported(ExecutorMetricRegistry group, HoodieMetrics hoodieMetrics) {
    if (hoodieMetrics == null || hoodieMetrics.getMetrics() == null) {
      return;
    }
    String prefix = hoodieMetrics.getMetricsName(group.metricAction(), group.metricQualifier());
    if (prefix == null) {
      return;
    }
    Map<String, Long> zeroed = new HashMap<>();
    hoodieMetrics.getMetrics().getRegistry().getGauges().keySet().stream()
        .filter(name -> name.startsWith(prefix + "."))
        .forEach(name -> zeroed.put(name.substring(prefix.length() + 1), 0L));
    if (!zeroed.isEmpty()) {
      publishToReporter(group, zeroed, hoodieMetrics);
    }
  }

  /** Gauges, so each commit overwrites the previous value rather than accumulating. */
  private static void publishToReporter(ExecutorMetricRegistry group, Map<String, Long> counts,
                                        HoodieMetrics hoodieMetrics) {
    if (hoodieMetrics == null || hoodieMetrics.getMetrics() == null) {
      return;
    }
    String prefix = hoodieMetrics.getMetricsName(group.metricAction(), group.metricQualifier());
    hoodieMetrics.getMetrics().registerGauges(counts, Option.ofNullable(prefix));
  }
}
