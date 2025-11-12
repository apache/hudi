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
import org.apache.hudi.common.function.SerializablePairPredicate;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An abstraction for pairs of key in type K and value in type V to store the reference
 * and do transformation.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public interface HoodiePairData<K, V> extends Serializable {
  /**
   * @return the collection of pairs.
   */
  Object get();

  /**
   * Persists the data (if applicable)
   *
   * @param cacheConfig config value for caching.
   */
  void persist(String cacheConfig);

  /**
   * Un-persists the data (if applicable)
   */
  void unpersist();

  /**
   * Un-persists this data and all its upstream dependencies recursively.
   * This method traverses the lineage graph and unpersists any cached RDDs
   * that this data depends on.
   */
  void unpersistWithDependencies();

  /**
   * Returns a {@link HoodieData} holding the key from every corresponding pair
   */
  HoodieData<K> keys();

  /**
   * Returns a {@link HoodieData} holding the value from every corresponding pair
   */
  HoodieData<V> values();

  /**
   * Returns number of held pairs
   */
  long count();

  /**
   * Counts the number of pairs grouping them by key
   */
  Map<K, Long> countByKey();

  /**
   * Groups the values for each key in the dataset into a single sequence
   */
  HoodiePairData<K, Iterable<V>> groupByKey();

  /**
   * Reduces original sequence by de-duplicating the pairs w/ the same key, using provided
   * binary operator {@code combiner}. Returns an instance of {@link HoodiePairData} holding
   * the "de-duplicated" pairs, ie only pairs with unique keys.
   *
   * @param combiner method to combine values of the pairs with the same key
   * @param parallelism target parallelism (if applicable)
   */
  HoodiePairData<K, V> reduceByKey(SerializableBiFunction<V, V, V> combiner, int parallelism);

  /**
   * Maps key-value pairs of this {@link HoodiePairData} container leveraging provided mapper
   *
   * NOTE: That this returns {@link HoodieData} and not {@link HoodiePairData}
   */
  <O> HoodieData<O> map(SerializableFunction<Pair<K, V>, O> func);

  /**
   * Maps values of this {@link HoodiePairData} container leveraging provided mapper
   */
  <W> HoodiePairData<K, W> mapValues(SerializableFunction<V, W> func);

  /**
   * Maps values of this {@link HoodiePairData} container leveraging provided mapper
   */
  <W> HoodiePairData<K, W> flatMapValues(SerializableFunction<V, Iterator<W>> func);

  /**
   * @param mapToPairFunc serializable map function to generate another pair.
   * @param <L>           new key type.
   * @param <W>           new value type.
   * @return containing the result. Actual execution may be deferred.
   */
  <L, W> HoodiePairData<L, W> mapToPair(
      SerializablePairFunction<Pair<K, V>, L, W> mapToPairFunc);

  /**
   * Performs a left outer join of this dataset against {@code other}.
   *
   * For each element (k, v) in this, the resulting {@link HoodiePairData} will either contain all
   * pairs {@code (k, (v, Some(w)))} for every {@code w} in the {@code other}, or the pair {@code (k, (v, None))}
   * if no elements in {@code other} have the pair w/ a key {@code k}
   *
   * @param other the other {@link HoodiePairData}
   * @param <W>   value type of the other {@link HoodiePairData}
   * @return containing the result of the left outer join
   */
  <W> HoodiePairData<K, Pair<V, Option<W>>> leftOuterJoin(HoodiePairData<K, W> other);

  /**
   * Performs a union of this dataset with the provided dataset
   */
  HoodiePairData<K, V> union(HoodiePairData<K, V> other);

  /**
   * Filters key-value pairs of this {@link HoodiePairData} container based on provided predicate
   *
   * @param filter predicate function that takes a key and value as input and returns true if the pair should be kept
   * @return filtered {@link HoodiePairData} containing only pairs that satisfy the predicate
   */
  HoodiePairData<K, V> filter(SerializablePairPredicate<K, V> filter);

  /**
   * Performs an inner join of this dataset against {@code other}.
   *
   * For each element (k, v) in this, the resulting {@link HoodiePairData} will contain all
   * pairs {@code (k, (v, Some(w)))} for every {@code w} in the {@code other},
   *
   * @param other the other {@link HoodiePairData}
   * @param <W>   value type of the other {@link HoodiePairData}
   * @return containing the result of the left outer join
   */
  <W> HoodiePairData<K, Pair<V, W>> join(HoodiePairData<K, W> other);

  /**
   * Collects results of the underlying collection into a {@link List<Pair<K, V>>}
   *
   * This is a terminal operation
   */
  List<Pair<K, V>> collectAsList();

  /**
   * @return the deduce number of shuffle partitions
   */
  int deduceNumPartitions();
}
