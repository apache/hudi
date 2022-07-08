/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.data;

import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An abstraction for pairs of key in type K and value in type V to store the reference
 * and do transformation.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public abstract class HoodiePairData<K, V> implements Serializable {
  /**
   * @return the collection of pairs.
   */
  public abstract Object get();

  /**
   * Caches the data.
   *
   * @param cacheConfig config value for caching.
   */
  public abstract void persist(String cacheConfig);

  /**
   * Removes the cached data.
   */
  public abstract void unpersist();

  /**
   * @return all keys in {@link HoodieData}.
   */
  public abstract HoodieData<K> keys();

  /**
   * @return all values in {@link HoodieData}.
   */
  public abstract HoodieData<V> values();

  /**
   * @return the number of pairs.
   */
  public abstract long count();

  /**
   * @return the number of pairs per key in a {@link Map}.
   */
  public abstract Map<K, Long> countByKey();

  /**
   * TODO java-doc
   */
  public abstract HoodiePairData<K, Iterable<V>> groupByKey();

  public abstract HoodiePairData<K, V> reduceByKey(SerializableBiFunction<V, V, V> func, int parallelism);

  /**
   * @param func serializable map function.
   * @param <O>  output object type.
   * @return {@link HoodieData<O>} containing the result. Actual execution may be deferred.
   */
  public abstract <O> HoodieData<O> map(SerializableFunction<Pair<K, V>, O> func);

  /**
   * @param mapToPairFunc serializable map function to generate another pair.
   * @param <L>           new key type.
   * @param <W>           new value type.
   * @return {@link HoodiePairData<L, W>} containing the result. Actual execution may be deferred.
   */
  public abstract <L, W> HoodiePairData<L, W> mapToPair(
      SerializablePairFunction<Pair<K, V>, L, W> mapToPairFunc);

  /**
   * Performs a left outer join of this and other. For each element (k, v) in this,
   * the resulting HoodiePairData will either contain all pairs (k, (v, Some(w))) for w in other,
   * or the pair (k, (v, None)) if no elements in other have key k.
   *
   * @param other the other {@link HoodiePairData}
   * @param <W>   value type of the other {@link HoodiePairData}
   * @return {@link HoodiePairData<K, Pair<V, Option<W>>>} containing the left outer join result.
   * Actual execution may be deferred.
   */
  public abstract <W> HoodiePairData<K, Pair<V, Option<W>>> leftOuterJoin(HoodiePairData<K, W> other);

  /**
   * Collects results of the underlying collection into a {@link List<Pair<K, V>>}
   *
   * This is a terminal operation
   */
  public abstract List<Pair<K, V>> collectAsList();
}
