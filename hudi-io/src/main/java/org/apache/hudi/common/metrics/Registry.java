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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Interface which defines a lightweight Metrics Registry to track Hudi metrics.
 *
 * Registries can be used to track related metrics under a common name - e.g. metrics for Metadata Table.
 */
public interface Registry extends Serializable {
  Logger LOG = LoggerFactory.getLogger(Registry.class);

  /**
   * Separator used for compound registry keys (tableName + registryName).
   */
  String KEY_SEPARATOR = "::";

  /**
   * Map of all registries that have been created.
   * The key is a compound of table name and registry name in format "tableName::registryName".
   * For non-table specific registries (i.e. common for all tables) the tableName would be empty.
   */
  ConcurrentHashMap<String, Registry> REGISTRY_MAP = new ConcurrentHashMap<>();

  /**
   * Creates a compound key from tableName and registryName.
   */
  static String makeKey(String tableName, String registryName) {
    return tableName + KEY_SEPARATOR + registryName;
  }

  /**
   * Extracts the registry name from a compound key.
   */
  static String getRegistryNameFromKey(String key) {
    int separatorIndex = key.lastIndexOf(KEY_SEPARATOR);
    return separatorIndex >= 0 ? key.substring(separatorIndex + KEY_SEPARATOR.length()) : key;
  }

  /**
   * Resolves against the running task first, then the process-wide map. Inside a task an unbound name
   * yields a {@link NoOpRegistry}, since a {@code LocalRegistry} there collects counters nothing merges
   * home. Off a task the behaviour is unchanged.
   *
   * @param registryName Name of the registry
   */
  static Registry getRegistry(String registryName) {
    if (ExecutorMetricsContext.isBound()) {
      Registry bound = ExecutorMetricsContext.lookup(registryName);
      if (bound != null) {
        return bound;
      }
      LOG.debug("No registry named {} is bound to this task; metrics emitted here are discarded", registryName);
      return NoOpRegistry.INSTANCE;
    }
    return getRegistryOfClass("", registryName, LocalRegistry.class.getName());
  }

  /**
   * Get (or create) the registry for a provided name and given class.
   * This is for backward compatibility with existing code.
   *
   * @param registryName Name of the registry.
   * @param clazz The fully qualified name of the registry class to create.
   */
  static Registry getRegistry(String registryName, String clazz) {
    return getRegistryOfClass("", registryName, clazz);
  }

  /**
   * Get (or create) the registry for a provided table and given class.
   *
   * @param tableName Name of the table (empty string for singleton/process-wide registries).
   * @param registryName Name of the registry.
   * @param clazz The fully qualified name of the registry class to create.
   */
  static Registry getRegistryOfClass(String tableName, String registryName, String clazz) {
    String key = makeKey(tableName, registryName);
    Registry registry = REGISTRY_MAP.computeIfAbsent(key, k -> {
      String registryFullName = tableName.isEmpty() ? registryName : tableName + "." + registryName;
      Registry r = (Registry) ReflectionUtils.loadClass(clazz, registryFullName);
      LOG.info("Created a new registry {}", r);
      return r;
    });

    if (!registry.getClass().getName().equals(clazz)) {
      LOG.error("Registry with name {} already exists with a different class {} than the requested class {}", registryName, registry.getClass().getName(), clazz);
    }
    return registry;
  }

  /**
   * Get all registered metrics.
   *
   * @param flush clear all metrics after this operation.
   * @param prefixWithRegistryName prefix each metric name with the registry name.
   * @return {@link Map} of metrics name and value
   */
  static Map<String, Long> getAllMetrics(boolean flush, boolean prefixWithRegistryName) {
    return getAllMetrics(flush, prefixWithRegistryName, Option.empty());
  }

  /**
   * Get all registered metrics.
   *
   * If a Registry did not have a prefix in its name, the commonPrefix is pre-pended to its name.
   *
   * @param flush clear all metrics after this operation.
   * @param prefixWithRegistryName prefix each metric name with the registry name.
   * @param commonPrefix prefix to use if the registry name does not have a prefix itself.
   * @return {@link Map} of metrics name and value
   */
  static Map<String, Long> getAllMetrics(boolean flush, boolean prefixWithRegistryName, Option<String> commonPrefix) {
    synchronized (Registry.class) {
      HashMap<String, Long> allMetrics = new HashMap<>();
      REGISTRY_MAP.forEach((key, registry) -> {
        final String registryName = getRegistryNameFromKey(key);
        final String prefix = (prefixWithRegistryName && commonPrefix.isPresent() && !registryName.contains("."))
            ? commonPrefix.get() + "." : "";
        registry.getAllCounts(prefixWithRegistryName).forEach((metricKey, value) -> allMetrics.put(prefix + metricKey, value));
        if (flush) {
          registry.clear();
        }
      });
      return allMetrics;
    }
  }

  /**
   * Set all registries if they are not already registered.
   */
  static void setRegistries(Collection<Registry> registries) {
    for (Registry registry : registries) {
      REGISTRY_MAP.putIfAbsent(makeKey("", registry.getName()), registry);
    }
  }

  /**
   * Returns the name of this registry.
   */
  String getName();

  /**
   * Clear all metrics.
   */
  void clear();

  /**
   * Increment the metric.
   *
   * @param name Name of the metric to increment.
   */
  void increment(String name);

  /**
   * Add value to the metric.
   *
   * @param name Name of the metric.
   * @param value The value to add to the metrics.
   */
  void add(String name, long value);

  /**
   * Set the value to the metric.
   *
   * If the metric does not exist, it is added. If the metrics already exists, its value is replaced with the
   * provided value.
   *
   * @param name Name of the metric.
   * @param value The value to set for the metrics.
   */
  void set(String name, long value);

  /**
   * Subtract a set of counts previously read out of this registry, clamping every counter at zero.
   *
   * Used to hand a batch of counters over to a consumer that owns them from then on -- the commit-boundary
   * drain for the record index lookup counters -- without discarding whatever arrived after they were read.
   *
   * Clamping is what distinguishes this from {@code add(name, -value)}. The registry can be emptied
   * underneath a caller by an unrelated destructive scrape ({@link #getAllMetrics(boolean, boolean)} with
   * {@code flush=true} clears every registry in the process), and an unbounded subtraction would then leave
   * negative counters behind for good.
   *
   * The default is a best-effort read-modify-write. Implementations able to do this atomically should
   * override it, and should drop counters that reach zero rather than leaving them at zero, so a registry
   * nobody is writing to reads as empty.
   *
   * @param counts the counts to release, as returned by {@link #getAllCounts(boolean)}.
   */
  default void release(Map<String, Long> counts) {
    Map<String, Long> current = getAllCounts(false);
    counts.forEach((name, released) -> set(name, Math.max(0L, current.getOrDefault(name, 0L) - released)));
  }

  /**
   * Get all Counter type metrics.
   */
  default Map<String, Long> getAllCounts() {
    return getAllCounts(false);
  }

  /**
   * Get all Counter type metrics.
   *
   * @param prefixWithRegistryName If true, the names of all metrics are prefixed with name of this registry.
   */
  Map<String, Long> getAllCounts(boolean prefixWithRegistryName);
}
