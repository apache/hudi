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

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lightweight Metrics Registry to track Hudi events.
 */
public class DistributedRegistry extends AccumulatorV2<Map<String, Long>, Map<String, Long>>
    implements Registry, Serializable {
  private final String name;
  ConcurrentHashMap<String, Long> counters = new ConcurrentHashMap<>();

  public DistributedRegistry(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  public DistributedRegistry register(JavaSparkContext jsc) {
    if (isRegistered()) {
      return this;
    }
    try {
      jsc.sc().register(this);
      return this;
    } catch (IllegalStateException e) {
      // Stale singleton: was registered to a previous SparkContext that no longer exists.
      // AccumulatorV2.metadata is non-null (so register() rejects it) but isRegistered()
      // returned false (the id is not in the current AccumulatorContext).
      // Create a fresh accumulator, register it, and swap it into the registry cache.
      DistributedRegistry fresh = new DistributedRegistry(this.name);
      fresh.counters.putAll(this.counters);
      jsc.sc().register(fresh);
      Registry.REGISTRY_MAP.forEach((key, registry) -> {
        if (registry == this) {
          Registry.REGISTRY_MAP.put(key, fresh);
        }
      });
      return fresh;
    }
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
    counters.merge(name,  value, (oldValue, newValue) -> newValue);
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
