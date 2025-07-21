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

import java.util.Map;
import java.util.stream.Collectors;

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
    return pairData.reduceByKey((existing, incoming) -> incoming, pairData.deduceNumPartitions())
            .collectAsList()
            .stream()
            .collect(Collectors.toMap(
                    Pair::getKey,
                    Pair::getValue
            ));
  }
} 