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
import org.apache.hudi.config.HoodieWriteConfig;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Predicate;

/**
 * Every class of executor-collected metric, and the only thing a new one is added to. The driver must
 * declare it up front because an {@code AccumulatorV2} must be registered with the {@code SparkContext}
 * before a task can contribute; the bundle sent to executors and the commit drain both iterate this.
 */
public enum ExecutorMetricRegistry implements ExecutorMetricGroup {

  RECORD_INDEX_LOOKUP(
      "HoodieRecordIndexLookup",
      "rli",
      "lookup",
      HoodieWriteConfig::isRecordIndexLookupMetricsEnabled);

  private final String registryName;
  private final String metricAction;
  private final String metricQualifier;
  private final Predicate<HoodieWriteConfig> enabled;

  ExecutorMetricRegistry(String registryName, String metricAction,
                         String metricQualifier, Predicate<HoodieWriteConfig> enabled) {
    this.registryName = registryName;
    this.metricAction = metricAction;
    this.metricQualifier = metricQualifier;
    this.enabled = enabled;
  }

  /** The bare name emitting code passes to {@link Registry#getRegistry(String)}. */
  @Override
  public String registryName() {
    return registryName;
  }

  @Override
  public String metricAction() {
    return metricAction;
  }

  @Override
  public String metricQualifier() {
    return metricQualifier;
  }

  /** Gating here, rather than in the drain, is what lets a new class of metric need no new config. */
  @Override
  public boolean isEnabled(HoodieWriteConfig config) {
    return enabled.test(config);
  }

  /**
   * Driver-side {@code REGISTRY_MAP} key. Table name alone is not an identity: two tables can share one
   * and would then share a registry. Executors use the bare {@link #registryName()}.
   */
  @Override
  public String scopedName(String basePath) {
    return registryName + "." + digest(basePath);
  }

  /** 48 bits of SHA-256: collision-safe enough, and short enough to sit in a metric name. */
  private static String digest(String basePath) {
    try {
      byte[] hash = MessageDigest.getInstance("SHA-256").digest(basePath.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(12);
      for (int i = 0; i < 6; i++) {
        hex.append(String.format("%02x", hash[i]));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required of every Java platform", e);
    }
  }
}
