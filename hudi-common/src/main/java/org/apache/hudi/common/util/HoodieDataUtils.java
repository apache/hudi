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

import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for HoodieData operations.
 */
public class HoodieDataUtils {

  /**
   * Creates a {@link HoodieListPairData} from a {@link Map} with eager execution semantic.
   * Each key-value pair in the map becomes a single pair in the resulting data structure.
   *
   * @param data the input map
   * @param <K>  type of the key
   * @param <V>  type of the value
   * @return a new {@link HoodieListPairData} instance
   */
  public static <K, V> HoodieListPairData<K, V> eagerMapKV(Map<K, V> data) {
    return HoodieListPairData.eager(
        data.entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Collections.singletonList(entry.getValue())
            ))
    );
  }

  /**
   * Collects results of the underlying collection into a {@link Map<K, V>}
   * If there are multiple pairs sharing the same key, the resulting map uses the incoming value (overwrites existing).
   *
   * This is a terminal operation
   *
   * @param pairData the HoodiePairData to convert
   * @param <K> type of the key
   * @param <V> type of the value
   * @return a Map containing the key-value pairs with overwrite strategy for duplicates
   */
  public static <K, V> Map<K, V> collectAsMapWithOverwriteStrategy(HoodiePairData<K, V> pairData) {
    // If there are multiple entries sharing the same key, use the incoming one
    return pairData.collectAsList()
        .stream()
        .collect(Collectors.toMap(
            Pair::getKey,
            Pair::getValue,
            (existing, incoming) -> incoming
        ));
  }
} 