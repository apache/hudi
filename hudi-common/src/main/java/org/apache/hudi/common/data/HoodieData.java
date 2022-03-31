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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * An abstraction for a data collection of objects in type T to store the reference
 * and do transformation.
 *
 * @param <T> type of object.
 */
public abstract class HoodieData<T> implements Serializable {
  /**
   * @return the collection of objects.
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
   * @return whether the collection is empty.
   */
  public abstract boolean isEmpty();

  /**
   * @return the number of objects.
   */
  public abstract long count();

  /**
   * @param func serializable map function.
   * @param <O>  output object type.
   * @return {@link HoodieData<O>} containing the result. Actual execution may be deferred.
   */
  public abstract <O> HoodieData<O> map(SerializableFunction<T, O> func);

  /**
   * @param func                  serializable map function by taking a partition of objects
   *                              and generating an iterator.
   * @param preservesPartitioning whether to preserve partitions in the result.
   * @param <O>                   output object type.
   * @return {@link HoodieData<O>} containing the result. Actual execution may be deferred.
   */
  public abstract <O> HoodieData<O> mapPartitions(
      SerializableFunction<Iterator<T>, Iterator<O>> func, boolean preservesPartitioning);

  /**
   * @param func serializable flatmap function.
   * @param <O>  output object type.
   * @return {@link HoodieData<O>} containing the result. Actual execution may be deferred.
   */
  public abstract <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func);

  /**
   * @param mapToPairFunc serializable map function to generate a pair.
   * @param <K>           key type of the pair.
   * @param <V>           value type of the pair.
   * @return {@link HoodiePairData<K, V>} containing the result. Actual execution may be deferred.
   */
  public abstract <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> mapToPairFunc);

  /**
   * @return distinct objects in {@link HoodieData}.
   */
  public abstract HoodieData<T> distinct();

  /**
   * Unions this {@link HoodieData} with other {@link HoodieData}.
   * @param other {@link HoodieData} of interest.
   * @return the union of two as as instance of {@link HoodieData}.
   */
  public abstract HoodieData<T> union(HoodieData<T> other);

  /**
   * @return collected results in {@link List<T>}.
   */
  public abstract List<T> collectAsList();

  /**
   * @return number of partitions of data.
   */
  public abstract int getNumPartitions();
}
