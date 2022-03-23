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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * Holds a {@link List} of objects.
 *
 * @param <T> type of object.
 */
public class HoodieList<T> extends HoodieData<T> {

  private final List<T> listData;

  private HoodieList(List<T> listData) {
    this.listData = listData;
  }

  /**
   * @param listData a {@link List} of objects in type T.
   * @param <T>      type of object.
   * @return a new instance containing the {@link List<T>} reference.
   */
  public static <T> HoodieList<T> of(List<T> listData) {
    return new HoodieList<>(listData);
  }

  /**
   * @param hoodieData {@link HoodieList <T>} instance containing the {@link List} of objects.
   * @param <T>        type of object.
   * @return the a {@link List} of objects in type T.
   */
  public static <T> List<T> getList(HoodieData<T> hoodieData) {
    return ((HoodieList<T>) hoodieData).get();
  }

  @Override
  public List<T> get() {
    return listData;
  }

  @Override
  public void persist(String cacheConfig) {
    // No OP
  }

  @Override
  public void unpersist() {
    // No OP
  }

  @Override
  public boolean isEmpty() {
    return listData.isEmpty();
  }

  @Override
  public long count() {
    return listData.size();
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
    return HoodieList.of(listData.stream().parallel()
        .map(throwingMapWrapper(func)).collect(Collectors.toList()));
  }

  @Override
  public <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>, Iterator<O>> func, boolean preservesPartitioning) {
    List<O> result = new ArrayList<>();
    throwingMapWrapper(func).apply(listData.iterator()).forEachRemaining(result::add);
    return HoodieList.of(result);
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func) {
    Function<T, Iterator<O>> throwableFunc = throwingMapWrapper(func);
    return HoodieList.of(listData.stream().flatMap(e -> {
      List<O> result = new ArrayList<>();
      Iterator<O> iterator = throwableFunc.apply(e);
      iterator.forEachRemaining(result::add);
      return result.stream();
    }).collect(Collectors.toList()));
  }

  @Override
  public <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> mapToPairFunc) {
    Map<K, List<V>> mapOfPairs = new HashMap<>();
    Function<T, Pair<K, V>> throwableMapToPairFunc = throwingMapToPairWrapper(mapToPairFunc);
    listData.forEach(data -> {
      Pair<K, V> pair = throwableMapToPairFunc.apply(data);
      List<V> list = mapOfPairs.computeIfAbsent(pair.getKey(), k -> new ArrayList<>());
      list.add(pair.getValue());
    });
    return HoodieMapPair.of(mapOfPairs);
  }

  @Override
  public HoodieData<T> distinct() {
    return HoodieList.of(new ArrayList<>(new HashSet<>(listData)));
  }

  @Override
  public HoodieData<T> distinct(int parallelism) {
    return distinct();
  }

  @Override
  public <O> HoodieData<T> distinctWithKey(SerializableFunction<T, O> keyGetter, int parallelism) {
    return mapToPair(i -> Pair.of(keyGetter.apply(i), i))
        .reduceByKey((value1, value2) -> value1, parallelism)
        .values();
  }

  @Override
  public HoodieData<T> filter(SerializableFunction<T, Boolean> filterFunc) {
    return HoodieList.of(listData
        .stream()
        .filter(i -> throwingMapWrapper(filterFunc).apply(i))
        .collect(Collectors.toList()));
  }

  @Override
  public HoodieData<T> union(HoodieData<T> other) {
    List<T> unionResult = new ArrayList<>();
    unionResult.addAll(listData);
    unionResult.addAll(other.collectAsList());
    return HoodieList.of(unionResult);
  }

  @Override
  public List<T> collectAsList() {
    return listData;
  }

  @Override
  public HoodieData<T> repartition(int parallelism) {
    // no op
    return this;
  }
}
