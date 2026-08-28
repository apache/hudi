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

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lightweight Metrics Registry to track Hudi events.
 */
public class DistributedRegistry extends AccumulatorV2<Map<String, Long>, Map<String, Long>>
    implements Registry, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedRegistry.class);

  private final String name;
  ConcurrentHashMap<String, Long> counters = new ConcurrentHashMap<>();
  /** Driver-only, to detect a SparkContext restart in the same JVM (shells, notebooks, Spark Connect). */
  private transient String registeredAppId;

  public DistributedRegistry(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  public void register(JavaSparkContext jsc) {
    if (!isRegistered()) {
      jsc.sc().register(this);
      // Only when this call actually registers: stamping unconditionally would re-brand an accumulator
      // bound to a dead context and mask the staleness this field exists to detect.
      this.registeredAppId = jsc.sc().applicationId();
    }
  }

  /** False when bound to a different (typically stopped) context, meaning it must be recreated. */
  public boolean isRegisteredWith(JavaSparkContext jsc) {
    return isRegistered() && jsc.sc().applicationId().equals(registeredAppId);
  }

  @Override
  public void clear() {
    counters.clear();
  }

  @Override
  public void increment(String name) {
    counters.merge(name,  1L, Long::sum);
  }

  @Override
  public void add(String name, long value) {
    counters.merge(name,  value, Long::sum);
  }

  @Override
  public void set(String name, long value) {
    // Last-writer-wins is neither commutative nor associative, and the driver merges executor copies in
    // an unspecified order. Driver only; executors use increment()/add().
    if (TaskContext.get() != null) {
      // Warn rather than throw: this runs inside a task, and a metrics problem must not fail a write.
      LOG.warn("DistributedRegistry.set() called from a Spark executor and ignored: it is non-commutative "
          + "under accumulator merges and would produce non-deterministic values. Use increment()/add().");
      return;
    }
    counters.merge(name,  value, (oldValue, newValue) -> newValue);
  }

  /**
   * Subtracts rather than clearing, so a concurrent merge either lands before the subtraction and is
   * released with it, or after and survives. Clamped at zero because {@code Metrics.shutdown()} can empty
   * the registry underneath a release; counters reaching zero are removed so a table that performed no
   * lookup is distinguishable from one that missed everything.
   */
  public void release(Map<String, Long> counts) {
    // Driver-only for the same reason as set(): clamping and eviction are order-dependent under merges.
    if (TaskContext.get() != null) {
      LOG.warn("DistributedRegistry.release() called from a Spark executor and ignored: clamping and eviction "
          + "are order-dependent under accumulator merges. Release at the commit boundary on the driver.");
      return;
    }
    counts.forEach((name, released) -> counters.compute(name, (key, current) -> {
      long remaining = (current == null ? 0L : current) - released;
      return remaining > 0L ? remaining : null;
    }));
  }

  /**
   * Get all Counter type metrics.
   */
  @Override
  public Map<String, Long> getAllCounts(boolean prefixWithRegistryName) {
    HashMap<String, Long> countersMap = new HashMap<>();
    counters.forEach((k, v) -> {
      String key = prefixWithRegistryName ? name + "." + k : k;
      countersMap.put(key, v);
    });
    return countersMap;
  }

  @Override
  public void add(Map<String, Long> arg) {
    arg.forEach(this::add);
  }

  @Override
  public AccumulatorV2<Map<String, Long>, Map<String, Long>> copy() {
    DistributedRegistry registry = new DistributedRegistry(name);
    counters.forEach(registry::add);
    return registry;
  }

  @Override
  public boolean isZero() {
    return counters.isEmpty();
  }

  @Override
  public void merge(AccumulatorV2<Map<String, Long>, Map<String, Long>> acc) {
    acc.value().forEach(this::add);
  }

  @Override
  public void reset() {
    counters.clear();
  }

  @Override
  public Map<String, Long> value() {
    return counters;
  }
}
