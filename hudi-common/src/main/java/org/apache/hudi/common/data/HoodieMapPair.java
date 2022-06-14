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

import org.apache.hudi.common.function.FunctionWrapper;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * Implementation of {@link HoodiePairData} using Java {@link Map}.
 * The pairs are organized by the key in the Map and values for the same key
 * are stored in a list as the value corresponding to the key in the Map.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class HoodieMapPair<K, V> extends HoodiePairData<K, V> {

  private final Map<K, List<V>> mapPairData;

  private HoodieMapPair(Map<K, List<V>> mapPairData) {
    this.mapPairData = mapPairData;
  }

  /**
   * @param mapPairData a {@link Map} of pairs.
   * @param <K>         type of key.
   * @param <V>         type of value.
   * @return a new instance containing the {@link Map<K, List<V>>} reference.
   */
  public static <K, V> HoodieMapPair<K, V> of(Map<K, List<V>> mapPairData) {
    return new HoodieMapPair<>(mapPairData);
  }

  /**
   * @param hoodiePairData {@link HoodieMapPair <K, V>} instance containing the {@link Map} of pairs.
   * @param <K>            type of key.
   * @param <V>            type of value.
   * @return the {@link Map} of pairs.
   */
  public static <K, V> Map<K, List<V>> getMapPair(HoodiePairData<K, V> hoodiePairData) {
    return ((HoodieMapPair<K, V>) hoodiePairData).get();
  }

  @Override
  public Map<K, List<V>> get() {
    return mapPairData;
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
  public HoodieData<K> keys() {
    return HoodieListData.of(new ArrayList<>(mapPairData.keySet()));
  }

  @Override
  public HoodieData<V> values() {
    return HoodieListData.of(
        mapPairData.values().stream().flatMap(List::stream).collect(Collectors.toList()));
  }

  @Override
  public long count() {
    return mapPairData.values().stream().map(
        list -> (long) list.size()).reduce(Long::sum).orElse(0L);
  }

  @Override
  public Map<K, Long> countByKey() {
    return mapPairData.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, entry -> (long) entry.getValue().size()));
  }

  @Override
  public HoodiePairData<K, Iterable<V>> groupByKey() {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public HoodiePairData<K, V> reduceByKey(SerializableBiFunction<V, V, V> func, int parallelism) {
    return HoodieMapPair.of(mapPairData.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> {
          Option<V> reducedValue = Option.fromJavaOptional(e.getValue().stream().reduce(func::apply));
          return reducedValue.isPresent() ? Collections.singletonList(reducedValue.get()) : Collections.emptyList();
        })));
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<Pair<K, V>, O> func) {
    Function<Pair<K, V>, O> throwableFunc = throwingMapWrapper(func);
    return HoodieListData.of(
        streamAllPairs().map(throwableFunc).collect(Collectors.toList()));
  }

  @Override
  public <L, W> HoodiePairData<L, W> mapToPair(SerializablePairFunction<Pair<K, V>, L, W> mapToPairFunc) {
    Map<L, List<W>> newMap = new HashMap<>();
    Function<Pair<K, V>, Pair<L, W>> throwableMapToPairFunc =
        FunctionWrapper.throwingMapToPairWrapper(mapToPairFunc);
    streamAllPairs().map(pair -> throwableMapToPairFunc.apply(pair)).forEach(newPair -> {
      List<W> list = newMap.computeIfAbsent(newPair.getKey(), k -> new ArrayList<>());
      list.add(newPair.getValue());
    });
    return HoodieMapPair.of(newMap);
  }

  @Override
  public <W> HoodiePairData<K, Pair<V, Option<W>>> leftOuterJoin(HoodiePairData<K, W> other) {
    Map<K, List<W>> otherMapPairData = HoodieMapPair.getMapPair(other);
    Stream<ImmutablePair<K, ImmutablePair<V, Option<List<W>>>>> pairs = streamAllPairs()
        .map(pair -> new ImmutablePair<>(pair.getKey(), new ImmutablePair<>(
            pair.getValue(), Option.ofNullable(otherMapPairData.get(pair.getKey())))));
    Map<K, List<Pair<V, Option<W>>>> resultMap = new HashMap<>();
    pairs.forEach(pair -> {
      K key = pair.getKey();
      ImmutablePair<V, Option<List<W>>> valuePair = pair.getValue();
      List<Pair<V, Option<W>>> resultList = resultMap.computeIfAbsent(key, k -> new ArrayList<>());
      if (!valuePair.getRight().isPresent()) {
        resultList.add(new ImmutablePair<>(valuePair.getLeft(), Option.empty()));
      } else {
        resultList.addAll(valuePair.getRight().get().stream().map(
            w -> new ImmutablePair<>(valuePair.getLeft(), Option.of(w))).collect(Collectors.toList()));
      }
    });
    return HoodieMapPair.of(resultMap);
  }

  private Stream<ImmutablePair<K, V>> streamAllPairs() {
    return mapPairData.entrySet().stream().flatMap(
        entry -> entry.getValue().stream().map(e -> new ImmutablePair<>(entry.getKey(), e)));
  }
}
