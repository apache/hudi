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

import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    // Deduplicate locally before shuffling to reduce data movement
    // If there are multiple entries sharing the same key, use the incoming one
    // Filter out null keys before reduceByKey as it cannot handle null keys
    pairData.persist("MEMORY_AND_DISK_SER");
    Map<K, V> res;
    try {
      // Reduce by key could not handle null for some of the engines like spark. Handle non-null key first.
      res = pairData.filter((key, value) -> key != null)
          .reduceByKey((existing, incoming) -> incoming, pairData.deduceNumPartitions())
          .collectAsList()
          .stream()
          .collect(HashMap::new,
              (map, pair) -> map.put(pair.getKey(), pair.getValue()),
              HashMap::putAll);
      // Get values for the null key. Add a random one to the result map.
      List<V> valuesForNullKey = pairData.filter((key, value) -> key == null).values().distinct().collectAsList();
      if (!valuesForNullKey.isEmpty()) {
        res.put(null, valuesForNullKey.get(0));
      }
    } finally {
      pairData.unpersist();
    }
    return res;
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
  public static <K, V> Map<K, Set<V>> dedupeAndCollectAsMapOfSet(HoodiePairData<K, V> pairData) {
    // Deduplicate locally before shuffling to reduce data movement
    // If there are multiple entries sharing the same key, use the incoming one
    // Filter out null keys before reduceByKey as it cannot handle null keys
    pairData.persist("MEMORY_AND_DISK_SER");
    Map<K, Set<V>> res;
    try {
      // Reduce by key could not handle null for some of the engines like spark. Handle non-null key first.
      res = pairData.filter((key, value) -> key != null)
          .mapToPair(pair -> Pair.of(pair.getKey(), Collections.singleton(pair.getValue())))
          .reduceByKey((set1, set2) -> {
            Set<V> combined = new HashSet<>(set1);
            combined.addAll(set2);
            return combined;
          }, pairData.deduceNumPartitions())
          .collectAsList()
          .stream()
          .collect(HashMap::new,
              (map, pair) -> map.put(pair.getKey(), pair.getValue()),
              HashMap::putAll);
      // Get values for the null key. Add a random one to the result map.
      List<V> valuesForNullKey = pairData.filter((key, value) -> key == null).values().distinct().collectAsList();
      if (!valuesForNullKey.isEmpty()) {
        res.put(null, new HashSet<>(valuesForNullKey));
      }
    } finally {
      pairData.unpersist();
    }
    return res;
  }
}