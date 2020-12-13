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

package org.apache.hudi.common.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Lightweight Metrics Registry to track Hudi events.
 */
public class Registry {
  ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
  final String name;

  private static ConcurrentHashMap<String, Registry> registryMap = new ConcurrentHashMap<>();

  private Registry(String name) {
    this.name = name;
  }

  /**
   * Get (or create) the registry for a provided name.
   */
  public static synchronized Registry getRegistry(String registryName) {
    if (!registryMap.containsKey(registryName)) {
      registryMap.put(registryName, new Registry(registryName));
    }
    return registryMap.get(registryName);
  }

  /**
   * Get all registered metrics.
   * @param flush clean all metrics as part of this operation.
   * @param prefixWithRegistryName prefix each metric name with the registry name.
   * @return
   */
  public static synchronized Map<String, Long> getAllMetrics(boolean flush, boolean prefixWithRegistryName) {
    HashMap<String, Long> allMetrics = new HashMap<>();
    registryMap.forEach((registryName, registry) -> {
      allMetrics.putAll(registry.getAllCounts(prefixWithRegistryName));
      if (flush) {
        registry.clear();
      }
    });
    return allMetrics;
  }

  public void clear() {
    counters.clear();
  }

  public void increment(String name) {
    getCounter(name).increment();
  }

  public void add(String name, long value) {
    getCounter(name).add(value);
  }

  private synchronized Counter getCounter(String name) {
    if (!counters.containsKey(name)) {
      counters.put(name, new Counter());
    }
    return counters.get(name);
  }

  /**
   * Get all Counter type metrics.
   */
  public Map<String, Long> getAllCounts() {
    return getAllCounts(false);
  }

  /**
   * Get all Counter type metrics.
   */
  public Map<String, Long> getAllCounts(boolean prefixWithRegistryName) {
    HashMap<String, Long> countersMap = new HashMap<>();
    counters.forEach((k, v) -> {
      String key = prefixWithRegistryName ? name + "." + k : k;
      countersMap.put(key, v.getValue());
    });
    return countersMap;
  }

}