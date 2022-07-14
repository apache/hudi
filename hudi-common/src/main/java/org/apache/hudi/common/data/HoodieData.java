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

import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * An interface abstracting a container holding a collection of objects of type {@code T}
 * allowing to perform common transformation on it.
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
public abstract class HoodieData<T> implements Serializable {

  /**
   * Persists the data w/ provided {@code level} (if applicable)
   */
  public abstract void persist(String level);

  /**
   * Un-persists the data (if previously persisted)
   */
  public abstract void unpersist();

  /**
   * Returns whether the collection is empty.
   */
  public abstract boolean isEmpty();

  /**
   * Returns number of objects held in the collection
   *
   * NOTE: This is a terminal operation
   */
  public abstract long count();

  /**
   * Maps every element in the collection using provided mapping {@code func}.
   *
   * This is an intermediate operation
   *
   * @param func serializable map function
   * @param <O>  output object type
   * @return {@link HoodieData<O>} holding mapped elements
   */
  public abstract <O> HoodieData<O> map(SerializableFunction<T, O> func);

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
  public abstract <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>,
      Iterator<O>> func, boolean preservesPartitioning);

  /**
   * Maps every element in the collection into a collection of the new elements (provided by
   * {@link Iterator}) using provided mapping {@code func}, subsequently flattening the result
   * (by concatenating) into a single collection
   *
   * This is an intermediate operation
   *
   * @param func serializable function mapping every element {@link T} into {@code Iterator<O>}
   * @param <O>  output object type
   * @return {@link HoodieData<O>} holding mapped elements
   */
  public abstract <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func);

  /**
   * TODO java-doc
   */
  public abstract <K, V> HoodiePairData<K, V> flatMapToPair(SerializableFunction<T, Iterator<? extends Pair<K, V>>> func);

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
  public abstract <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> func);

  /**
   * Returns new {@link HoodieData} collection holding only distinct objects of the original one
   *
   * This is a stateful intermediate operation
   */
  public abstract HoodieData<T> distinct();

  /**
   * Returns new {@link HoodieData} collection holding only distinct objects of the original one
   *
   * This is a stateful intermediate operation
   */
  public abstract HoodieData<T> distinct(int parallelism);

  /**
   * Returns new instance of {@link HoodieData} collection only containing elements matching provided
   * {@code filterFunc} (ie ones it returns true on)
   *
   * @param filterFunc filtering func either accepting or rejecting the elements
   * @return {@link HoodieData<T>} holding filtered elements
   */
  public abstract HoodieData<T> filter(SerializableFunction<T, Boolean> filterFunc);

  /**
   * Unions {@link HoodieData} with another instance of {@link HoodieData}.
   * Note that, it's only able to union same underlying collection implementations.
   *
   * This is a stateful intermediate operation
   *
   * @param other {@link HoodieData} collection
   * @return {@link HoodieData<T>} holding superset of elements of this and {@code other} collections
   */
  public abstract HoodieData<T> union(HoodieData<T> other);

  /**
   * Collects results of the underlying collection into a {@link List<T>}
   *
   * This is a terminal operation
   */
  public abstract List<T> collectAsList();

  /**
   * Re-partitions underlying collection (if applicable) making sure new {@link HoodieData} has
   * exactly {@code parallelism} partitions
   *
   * @param parallelism target number of partitions in the underlying collection
   * @return {@link HoodieData<T>} holding re-partitioned collection
   */
  public abstract HoodieData<T> repartition(int parallelism);

  public <O> HoodieData<T> distinctWithKey(SerializableFunction<T, O> keyGetter, int parallelism) {
    return mapToPair(i -> Pair.of(keyGetter.apply(i), i))
        .reduceByKey((value1, value2) -> value1, parallelism)
        .values();
  }
}
