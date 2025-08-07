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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * An interface abstracting a container holding a collection of objects of type {@code T}
 * allowing to perform common transformation on it.
 *
 * For all HoodieData, make sure you wrap it with either
 * - withHoodieDataCleanUp to clean it up in a finally clause.
 * - or use HoodieDataCleanupManager to track the HoodieData and clean it up only on exception.
 *
 * This abstraction provides common API implemented by
 * <ol>
 *   <li>In-memory implementation ({@code HoodieListData}, {@code HoodieListPairData}), where all objects
 *   are held in-memory by the executing process</li>
 *   <li>RDD-based implementation ({@code HoodieJavaRDD}, etc)</li>, where underlying collection is held
 *   by an RDD allowing to execute transformations using Spark engine on the cluster
 * </ol>
 *
 * All implementations provide for consistent semantic, where
 * <ul>
 *   <li>All non-terminal* operations are executed lazily (for ex, {@code map}, {@code filter}, etc)</li>
 *   <li>All terminal operations are executed eagerly, executing all previously accumulated transformations.
 *   Note that, collection could not be re-used after invoking terminal operation on it.</li>
 * </ul>
 *
 * @param <T> type of object
 */
public interface HoodieData<T> extends Serializable {

  /**
   * Get the {@link HoodieData}'s unique non-negative identifier. -1 indicates invalid id.
   */
  int getId();

  /**
   * Persists the data w/ provided {@code level} (if applicable).
   *
   * Use this method only when you call {@link #unpersist()} at some later point for the same {@link HoodieData}.
   * Otherwise, use {@link #persist(String, HoodieEngineContext, HoodieDataCacheKey)} instead for auto-unpersist
   * at the end of a client write operation.
   */
  void persist(String level);

  /**
   * Persists the data w/ provided {@code level} (if applicable), and cache the data's ids within the {@code engineContext}.
   */
  void persist(String level, HoodieEngineContext engineContext, HoodieDataCacheKey cacheKey);

  /**
   * Un-persists the data (if previously persisted)
   */
  void unpersist();

  /**
   * Un-persists this data and all its upstream dependencies recursively.
   * This method traverses the dependency graph and unpersists any cached dependencies
   * if the underlying hoodie engine has such capability.
   */
  void unpersistWithDependencies();

  /**
   * Returns whether the collection is empty.
   */
  boolean isEmpty();

  /**
   * Returns number of objects held in the collection
   * <p>
   * NOTE: This is a terminal operation
   */
  long count();

  /**
   * @return the number of data partitions in the engine-specific representation.
   */
  int getNumPartitions();

  /**
   * @return the deduce number of shuffle partitions
   */
  int deduceNumPartitions();

  /**
   * Maps every element in the collection using provided mapping {@code func}.
   * <p>
   * This is an intermediate operation
   *
   * @param func serializable map function
   * @param <O>  output object type
   * @return {@link HoodieData<O>} holding mapped elements
   */
  <O> HoodieData<O> map(SerializableFunction<T, O> func);

  /**
   * Maps every element in the collection's partition (if applicable) by applying provided
   * mapping {@code func} to every collection's partition
   *
   * This is an intermediate operation
   *
   * @param func                  serializable map function accepting {@link Iterator} of a single
   *                              partition's elements and returning a new {@link Iterator} mapping
   *                              every element of the partition into a new one
   * @param preservesPartitioning whether to preserve partitioning in the resulting collection
   * @param <O>                   output object type
   * @return {@link HoodieData<O>} holding mapped elements
   */
  <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>,
      Iterator<O>> func, boolean preservesPartitioning);

  /**
   * Maps every element in the collection into a collection of the new elements using provided
   * mapping {@code func}, subsequently flattening the result (by concatenating) into a single
   * collection
   *
   * This is an intermediate operation
   *
   * @param func serializable function mapping every element {@link T} into {@code Iterator<O>}
   * @param <O>  output object type
   * @return {@link HoodieData<O>} holding mapped elements
   */
  <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func);

  /**
   * Maps every element in the collection into a collection of the {@link Pair}s of new elements
   * using provided mapping {@code func}, subsequently flattening the result (by concatenating) into
   * a single collection
   *
   * NOTE: That this operation will convert container from {@link HoodieData} to {@link HoodiePairData}
   *
   * This is an intermediate operation
   */
  <K, V> HoodiePairData<K, V> flatMapToPair(SerializableFunction<T, Iterator<? extends Pair<K, V>>> func);

  /**
   * Maps every element in the collection using provided mapping {@code func} into a {@link Pair<K, V>}
   * of elements {@code K} and {@code V}
   * <p>
   * This is an intermediate operation
   *
   * @param func serializable map function
   * @param <K>  key type of the pair
   * @param <V>  value type of the pair
   * @return {@link HoodiePairData<K, V>} holding mapped elements
   */
  <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> func);

  /**
   * Returns new {@link HoodieData} collection holding only distinct objects of the original one
   *
   * This is a stateful intermediate operation
   */
  HoodieData<T> distinct();

  /**
   * Returns new {@link HoodieData} collection holding only distinct objects of the original one
   *
   * This is a stateful intermediate operation
   */
  HoodieData<T> distinct(int parallelism);

  /**
   * Returns new instance of {@link HoodieData} collection only containing elements matching provided
   * {@code filterFunc} (ie ones it returns true on)
   *
   * @param filterFunc filtering func either accepting or rejecting the elements
   * @return {@link HoodieData<T>} holding filtered elements
   */
  HoodieData<T> filter(SerializableFunction<T, Boolean> filterFunc);

  /**
   * Unions {@link HoodieData} with another instance of {@link HoodieData}.
   * Note that, it's only able to union same underlying collection implementations.
   *
   * This is a stateful intermediate operation
   *
   * @param other {@link HoodieData} collection
   * @return {@link HoodieData<T>} holding superset of elements of this and {@code other} collections
   */
  HoodieData<T> union(HoodieData<T> other);

  /**
   * Collects results of the underlying collection into a {@link List<T>}
   *
   * This is a terminal operation
   */
  List<T> collectAsList();

  /**
   * Re-partitions underlying collection (if applicable) making sure new {@link HoodieData} has
   * exactly {@code parallelism} partitions
   *
   * @param parallelism target number of partitions in the underlying collection
   * @return {@link HoodieData<T>} holding re-partitioned collection
   */
  HoodieData<T> repartition(int parallelism);

  default <O> HoodieData<T> distinctWithKey(SerializableFunction<T, O> keyGetter, int parallelism) {
    return mapToPair(i -> Pair.of(keyGetter.apply(i), i))
        .reduceByKey((value1, value2) -> value1, parallelism)
        .values();
  }

  /**
   * The key used in a caching map to identify a {@link HoodieData}.
   *
   * At the end of a write operation, we manually unpersist the {@link HoodieData} associated with that writer.
   * Therefore, in multi-writer scenario, we need to use both {@code basePath} and {@code instantTime} to identify {@link HoodieData}s.
   */
  class HoodieDataCacheKey implements Serializable {

    public static HoodieDataCacheKey of(String basePath, String instantTime) {
      return new HoodieDataCacheKey(new StoragePath(basePath).toString(), instantTime);
    }

    private final String basePath;
    private final String instantTime;

    private HoodieDataCacheKey(String basePath, String instantTime) {
      this.basePath = basePath;
      this.instantTime = instantTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      HoodieDataCacheKey that = (HoodieDataCacheKey) o;
      return basePath.equals(that.basePath) && instantTime.equals(that.instantTime);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, instantTime);
    }
  }
}
