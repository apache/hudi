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

package org.apache.hudi.common.data;

import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * TODO java-doc
 */
public class HoodieListPairData<K, V> extends HoodiePairData<K, V> {

  private final Stream<Pair<K, V>> dataStream;

  public HoodieListPairData(List<Pair<K, V>> data) {
    this.dataStream = data.stream().parallel();
  }

  HoodieListPairData(Stream<Pair<K, V>> dataStream) {
    this.dataStream = dataStream;
  }

  @Override
  public List<Pair<K, V>> get() {
    return dataStream.collect(Collectors.toList());
  }

  @Override
  public void persist(String cacheConfig) {
    // no-op
  }

  @Override
  public void unpersist() {
    // no-op
  }

  @Override
  public HoodieData<K> keys() {
    return new HoodieListData<>(dataStream.map(Pair::getKey));
  }

  @Override
  public HoodieData<V> values() {
    return new HoodieListData<>(dataStream.map(Pair::getValue));
  }

  @Override
  public long count() {
    return dataStream.count();
  }

  @Override
  public Map<K, Long> countByKey() {
    return dataStream.reduce(
        new HashMap<>(),
        (acc, p) -> {
          acc.compute(p.getKey(), (k, count) -> count == null ? 1 : count + 1);
          return acc;
        },
        (a, b) -> CollectionUtils.combine(a, b, Long::sum)
    );
  }

  @Override
  public HoodiePairData<K, Iterable<V>> groupByKey() {
    Collector<Pair<K, V>, ?, List<V>> mappingCollector = Collectors.mapping(Pair::getValue, Collectors.toList());
    Collector<Pair<K, V>, ?, Map<K, List<V>>> groupingCollector =
        Collectors.groupingBy(Pair::getKey, mappingCollector);

    Map<K, List<V>> groupedByKey = dataStream.collect(groupingCollector);
    return new HoodieListPairData<>(groupedByKey.entrySet().stream()
        .map(e -> Pair.of(e.getKey(), e.getValue())));
  }

  @Override
  public HoodiePairData<K, V> reduceByKey(SerializableBiFunction<V, V, V> func, int parallelism) {
    HashMap<K, V> reducedMap = dataStream.reduce(
        new HashMap<>(),
        (acc, p) -> {
          acc.merge(p.getKey(), p.getValue(), func::apply);
          return acc;
        },
        (a, b) -> CollectionUtils.combine(a, b, func::apply)
    );
    return new HoodieListPairData<>(reducedMap.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())));
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<Pair<K, V>, O> func) {
    Function<Pair<K, V>, O> uncheckedMapper = throwingMapWrapper(func);
    return new HoodieListData<>(dataStream.map(uncheckedMapper));
  }

  @Override
  public <L, W> HoodiePairData<L, W> mapToPair(SerializablePairFunction<Pair<K, V>, L, W> mapToPairFunc) {
    return new HoodieListPairData<>(dataStream.map(p -> throwingMapToPairWrapper(mapToPairFunc).apply(p)));
  }

  @Override
  public <W> HoodiePairData<K, Pair<V, Option<W>>> leftOuterJoin(HoodiePairData<K, W> other) {
    // TODO
    throw new UnsupportedOperationException("");
  }

  public static <K, V> HoodieListPairData<K, V> of(List<Pair<K, V>> data) {
    return new HoodieListPairData<>(data);
  }

  public static <K, V> HoodieListPairData<K, V> of(Map<K, List<V>> data) {
    Stream<Pair<K, V>> exploded = data.entrySet().stream()
        .flatMap(e -> e.getValue().stream().map(v -> Pair.of(e.getKey(), v)));
    return new HoodieListPairData<>(exploded);
  }

  @Override
  public List<Pair<K, V>> collectAsList() {
    return dataStream.collect(Collectors.toList());
  }
}
