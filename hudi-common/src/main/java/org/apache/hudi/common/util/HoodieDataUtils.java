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

package org.apache.hudi.common.util;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Utility class for HoodieData operations.
 */
public class HoodieDataUtils {
  /**
   * Collects results of the pair data into a {@link Map<K, V>}
   *
   * If there are multiple pairs sharing the same key, the resulting map will end up with nondeterministically
   * pick one pair from them.
   *
   * This is a terminal operation
   *
   * @param pairData the HoodiePairData to collect
   * @param <K> type of the key
   * @param <V> type of the value
   * @return a Map containing the de-duplicated key-value pairs
   */
  public static <K, V> Map<K, V> dedupeAndCollectAsMap(HoodiePairData<K, V> pairData) {
    // Map each pair to (Option<Pair.key>, V) to handle null keys uniformly
    // If there are multiple entries sharing the same key, use the incoming one
    return pairData.mapToPair(pair -> 
        Pair.of(
            Option.ofNullable(pair.getKey()), 
            pair.getValue()
        ))
        .reduceByKey((existing, incoming) -> incoming, pairData.deduceNumPartitions())
        .collectAsList()
        .stream()
        .collect(HashMap::new,
            (map, pair) -> {
              K key = pair.getKey().orElse(null);
              map.put(key, pair.getValue());
            },
            HashMap::putAll);
  }

  /**
   * Collects results of the pair data into a {@link Map<K, Set<V>>} where values with the same key
   * are grouped into a set.
   *
   * @param pairData Hoodie Pair Data to be collected
   * @param <K> type of the key
   * @param <V> type of the value
   * @return a Map containing keys mapped to sets of values
   */
  public static <K, V> Map<K, Set<V>> collectPairDataAsMap(HoodiePairData<K, V> pairData) {
    // Map each pair to (Option<Pair.key>, V) to handle null keys uniformly
    // If there are multiple entries sharing the same key, combine them into a set
    return pairData.mapToPair(pair -> 
        Pair.of(
            Option.ofNullable(pair.getKey()), 
            Collections.singleton(pair.getValue())
        ))
        .reduceByKey((set1, set2) -> {
          Set<V> combined = new HashSet<>(set1);
          combined.addAll(set2);
          return combined;
        }, pairData.deduceNumPartitions())
        .collectAsList()
        .stream()
        .collect(HashMap::new,
            (map, pair) -> {
              K key = pair.getKey().orElse(null);
              map.put(key, pair.getValue());
            },
            HashMap::putAll);
  }

  /**
   * Executes a function with HoodieData and ensures cleanup after use
   *
   * @param hoodieData The HoodieData to use
   * @param f          Function that processes the HoodieData
   * @param <T>        Type parameter of HoodieData
   * @param <R>        Return type of the function
   * @return Result of the function
   */
  public static <T, R> R withHoodieDataCleanUp(HoodieData<T> hoodieData, Function<HoodieData<T>, R> f) {
    try {
      return f.apply(hoodieData);
    } finally {
      hoodieData.unpersistWithDependencies();
    }
  }

  /**
   * Executes a function with HoodiePairData and ensures cleanup after use
   *
   * @param hoodiePairData The HoodiePairData to use
   * @param f              Function that processes the HoodiePairData
   * @param <K>            Key type parameter of HoodiePairData
   * @param <V>            Value type parameter of HoodiePairData
   * @param <R>            Return type of the function
   * @return Result of the function
   */
  public static <K, V, R> R withHoodieDataCleanUp(HoodiePairData<K, V> hoodiePairData, Function<HoodiePairData<K, V>, R> f) {
    try {
      return f.apply(hoodiePairData);
    } finally {
      hoodiePairData.unpersistWithDependencies();
    }
  }

  /**
   * Executes a function with HoodieData and ensures cleanup only on exception.
   *
   * It calls unpersistWithDependencies which internally close all resources it depends on.
   * For example, for spark rdd-based API implementation, it cleans up all the upstream cached rdd.
   *
   * @param hoodieData The HoodieData to use
   * @param f          Function that processes the HoodieData
   * @param <T>        Type parameter of HoodieData
   * @param <R>        Return type of the function
   * @return Result of the function
   */
  public static <T, R> R withHoodieDataCleanUpOnException(HoodieData<T> hoodieData, Function<HoodieData<T>, R> f) {
    try {
      return f.apply(hoodieData);
    } catch (Exception e) {
      hoodieData.unpersistWithDependencies();
      throw e;
    }
  }

  /**
   * Executes a function with HoodiePairData and ensures cleanup only on exception
   *
   * @param hoodiePairData The HoodiePairData to use
   * @param f              Function that processes the HoodiePairData
   * @param <K>            Key type parameter of HoodiePairData
   * @param <V>            Value type parameter of HoodiePairData
   * @param <R>            Return type of the function
   * @return Result of the function
   */
  public static <K, V, R> R withHoodieDataCleanUpOnException(HoodiePairData<K, V> hoodiePairData, Function<HoodiePairData<K, V>, R> f) {
    try {
      return f.apply(hoodiePairData);
    } catch (Exception e) {
      hoodiePairData.unpersistWithDependencies();
      throw e;
    }
  }
}