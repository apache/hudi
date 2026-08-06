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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Metadata contract for requesting vector-index maintenance through a Hudi indexing action.
 *
 * <p>Generation ordinals are deliberately absent. The maintenance executor allocates generations
 * while holding the transaction lock; a scheduler must not preallocate concurrency-sensitive state.
 */
public final class VectorIndexMaintenanceMetadata {

  public static final String OPERATION_KEY = "hoodie.vector.index.maintenance.operation";
  public static final String TRIGGER_KEY = "hoodie.vector.index.maintenance.trigger";
  public static final String AFFECTED_CLUSTERS_KEY = "hoodie.vector.index.maintenance.affected.clusters";

  private VectorIndexMaintenanceMetadata() {
  }

  public static Map<String, String> create(
      Operation operation,
      Trigger trigger,
      Collection<Integer> affectedClusterIds) {
    if (operation == null) {
      throw new IllegalArgumentException("Vector maintenance operation must not be null");
    }
    if (trigger == null) {
      throw new IllegalArgumentException("Vector maintenance trigger must not be null");
    }

    Set<Integer> clusterIds = normalizeClusterIds(affectedClusterIds);
    validate(operation, trigger, clusterIds);

    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(OPERATION_KEY, operation.name());
    metadata.put(TRIGGER_KEY, trigger.name());
    if (!clusterIds.isEmpty()) {
      metadata.put(AFFECTED_CLUSTERS_KEY, clusterIds.stream()
          .map(String::valueOf)
          .collect(Collectors.joining(",")));
    }
    return Collections.unmodifiableMap(metadata);
  }

  public static boolean hasMaintenanceOperation(Map<String, String> metadata) {
    return metadata != null && metadata.containsKey(OPERATION_KEY);
  }

  public static Operation operation(Map<String, String> metadata) {
    return enumValue(metadata, OPERATION_KEY, Operation.class);
  }

  public static Trigger trigger(Map<String, String> metadata) {
    return enumValue(metadata, TRIGGER_KEY, Trigger.class);
  }

  public static Set<Integer> affectedClusterIds(Map<String, String> metadata) {
    String encoded = requiredMetadata(metadata, AFFECTED_CLUSTERS_KEY, false);
    if (StringUtils.isNullOrEmpty(encoded)) {
      return Collections.emptySet();
    }
    try {
      TreeSet<Integer> clusterIds = new TreeSet<>();
      for (String value : encoded.split(",", -1)) {
        int clusterId = Integer.parseInt(value);
        if (clusterId < 0) {
          throw new IllegalArgumentException("Affected vector cluster IDs must be non-negative");
        }
        clusterIds.add(clusterId);
      }
      return Collections.unmodifiableSet(clusterIds);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid affected vector cluster IDs: " + encoded, e);
    }
  }

  public static void validate(Map<String, String> metadata) {
    validate(operation(metadata), trigger(metadata), affectedClusterIds(metadata));
  }

  private static Set<Integer> normalizeClusterIds(Collection<Integer> affectedClusterIds) {
    if (affectedClusterIds == null || affectedClusterIds.isEmpty()) {
      return Collections.emptySet();
    }
    if (affectedClusterIds.stream().anyMatch(clusterId -> clusterId == null || clusterId < 0)) {
      throw new IllegalArgumentException("Affected vector cluster IDs must be non-negative");
    }
    return Collections.unmodifiableSet(new TreeSet<>(affectedClusterIds));
  }

  private static void validate(Operation operation, Trigger trigger, Set<Integer> clusterIds) {
    boolean validTrigger;
    switch (operation) {
      case REBUILD:
        validTrigger = trigger == Trigger.MANUAL
            || trigger == Trigger.ROUTING_DRIFT
            || trigger == Trigger.PERIODIC;
        break;
      case SPLIT:
        validTrigger = trigger == Trigger.MANUAL || trigger == Trigger.SPLIT_LIMIT;
        if (clusterIds.size() != 1) {
          throw new IllegalArgumentException("Vector split maintenance requires exactly one affected cluster");
        }
        break;
      case MERGE:
        validTrigger = trigger == Trigger.MANUAL || trigger == Trigger.MERGE_FLOOR;
        if (clusterIds.size() < 2) {
          throw new IllegalArgumentException("Vector merge maintenance requires at least two affected clusters");
        }
        break;
      case COMPACT:
        validTrigger = trigger == Trigger.MANUAL || trigger == Trigger.DELTA_PRESSURE;
        break;
      default:
        throw new IllegalArgumentException("Unsupported vector maintenance operation: " + operation);
    }
    if (!validTrigger) {
      throw new IllegalArgumentException(
          "Vector maintenance trigger " + trigger + " is invalid for operation " + operation);
    }
  }

  private static <T extends Enum<T>> T enumValue(
      Map<String, String> metadata,
      String key,
      Class<T> enumClass) {
    String value = requiredMetadata(metadata, key, true);
    try {
      return Enum.valueOf(enumClass, value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid vector maintenance metadata " + key + ": " + value, e);
    }
  }

  private static String requiredMetadata(
      Map<String, String> metadata,
      String key,
      boolean required) {
    if (metadata == null) {
      throw new IllegalArgumentException("Vector maintenance metadata must not be null");
    }
    String value = metadata.get(key);
    if (required && StringUtils.isNullOrEmpty(value)) {
      throw new IllegalArgumentException("Missing vector maintenance metadata: " + key);
    }
    return value;
  }

  public enum Operation {
    REBUILD,
    SPLIT,
    MERGE,
    COMPACT
  }

  public enum Trigger {
    MANUAL,
    SPLIT_LIMIT,
    MERGE_FLOOR,
    ROUTING_DRIFT,
    DELTA_PRESSURE,
    PERIODIC
  }
}
