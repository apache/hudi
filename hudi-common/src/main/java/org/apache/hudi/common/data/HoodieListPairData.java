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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.common.util.collection.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * In-memory implementation of {@link HoodiePairData} holding internally a {@link Stream} of {@link Pair}s.
 *
 * NOTE: This is an in-memory counterpart for {@code HoodieJavaPairRDD}, and it strives to provide
 *       similar semantic as RDD container -- all intermediate (non-terminal, not de-referencing
 *       the stream like "collect", "groupBy", etc) operations are executed *lazily*.
 *       This allows to make sure that compute/memory churn is minimal since only necessary
 *       computations will ultimately be performed.
 *
 * @param <K> type of the key in the pair
 * @param <V> type of the value in the pair
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
    return dataStream.collect(Collectors.groupingBy(Pair::getKey, Collectors.counting()));
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
  public HoodiePairData<K, V> reduceByKey(SerializableBiFunction<V, V, V> combiner, int parallelism) {
    Map<K, java.util.Optional<V>> reducedMap = dataStream.collect(
        Collectors.groupingBy(
            Pair::getKey,
            HashMap::new,
            Collectors.mapping(Pair::getValue, Collectors.reducing(combiner::apply))));

    return new HoodieListPairData<>(
        reducedMap.entrySet()
            .stream()
            .map(e -> Pair.of(e.getKey(), e.getValue().orElse(null))));
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<Pair<K, V>, O> func) {
    Function<Pair<K, V>, O> uncheckedMapper = throwingMapWrapper(func);
    return new HoodieListData<>(dataStream.map(uncheckedMapper));
  }

  @Override
  public <W> HoodiePairData<K, W> mapValues(SerializableFunction<V, W> func) {
    Function<V, W> uncheckedMapper = throwingMapWrapper(func);
    return new HoodieListPairData<>(dataStream.map(p -> Pair.of(p.getKey(), uncheckedMapper.apply(p.getValue()))));
  }

  @Override
  public <W> HoodiePairData<K, W> flatMapValues(SerializableFunction<V, Iterator<W>> func) {
    Function<V, Iterator<W>> uncheckedMapper = throwingMapWrapper(func);
    return new HoodieListPairData<>(dataStream.flatMap(p -> {
      Iterator<W> mappedValuesIterator = uncheckedMapper.apply(p.getValue());
      Iterator<Pair<K, W>> mappedPairsIterator =
          new MappingIterator<>(mappedValuesIterator, w -> Pair.of(p.getKey(), w));

      return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(mappedPairsIterator, Spliterator.ORDERED), true);
    }));
  }

  @Override
  public <L, W> HoodiePairData<L, W> mapToPair(SerializablePairFunction<Pair<K, V>, L, W> mapToPairFunc) {
    return new HoodieListPairData<>(dataStream.map(p -> throwingMapToPairWrapper(mapToPairFunc).apply(p)));
  }

  @Override
  public <W> HoodiePairData<K, Pair<V, Option<W>>> leftOuterJoin(HoodiePairData<K, W> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListPairData);

    // Transform right-side container to a multi-map of [[K]] to [[List<W>]] values
    HashMap<K, List<W>> rightStreamMap = ((HoodieListPairData<K, W>) other).dataStream.collect(
        Collectors.groupingBy(
            Pair::getKey,
            HashMap::new,
            Collectors.mapping(Pair::getValue, Collectors.toList())));

    Stream<Pair<K, Pair<V, Option<W>>>> leftOuterJoined = dataStream.flatMap(pair -> {
      K key = pair.getKey();
      V leftValue = pair.getValue();
      List<W> rightValues = rightStreamMap.get(key);

      if (rightValues == null) {
        return Stream.of(Pair.of(key, Pair.of(leftValue, Option.empty())));
      } else {
        return rightValues.stream().map(rightValue ->
            Pair.of(key, Pair.of(leftValue, Option.of(rightValue))));
      }
    });

    return new HoodieListPairData<>(leftOuterJoined);
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
