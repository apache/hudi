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
import org.apache.hudi.common.function.SerializablePairPredicate;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import java.util.ArrayList;
import java.util.Collections;
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
 * {@link HoodieListData} can have either of the 2 execution semantics:
 *
 * <ol>
 *   <li>Eager: with every operation being executed right away</li>
 *   <li>Lazy: with every operation being "stacked up", with it execution postponed until
 *   "terminal" operation is invoked</li>
 * </ol>
 *
 *
 * NOTE: This is an in-memory counterpart for {@code HoodieJavaPairRDD}, and it strives to provide
 *       similar semantic as RDD container -- all intermediate (non-terminal, not de-referencing
 *       the stream like "collect", "groupBy", etc) operations are executed *lazily*.
 *       This allows to make sure that compute/memory churn is minimal since only necessary
 *       computations will ultimately be performed.
 *
 *       Please note, however, that while RDD container allows the same collection to be
 *       de-referenced more than once (ie terminal operation invoked more than once),
 *       {@link HoodieListData} allows that only when instantiated w/ an eager execution semantic.
 *
 * @param <K> type of the key in the pair
 * @param <V> type of the value in the pair
 */
public class HoodieListPairData<K, V> extends HoodieBaseListData<Pair<K, V>> implements HoodiePairData<K, V> {

  private HoodieListPairData(List<Pair<K, V>> data, boolean lazy) {
    super(data, lazy);
  }

  HoodieListPairData(Stream<Pair<K, V>> dataStream, boolean lazy) {
    super(dataStream, lazy);
  }

  @Override
  public List<Pair<K, V>> get() {
    return collectAsList();
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
  public void unpersistWithDependencies() {
    // no-op - in-memory implementation doesn't have dependencies to unpersist
  }

  @Override
  public HoodieData<K> keys() {
    return new HoodieListData<>(asStream().map(Pair::getKey), lazy);
  }

  @Override
  public HoodieData<V> values() {
    return new HoodieListData<>(asStream().map(Pair::getValue), lazy);
  }

  @Override
  public Map<K, Long> countByKey() {
    try (Stream<Pair<K, V>> stream = asStream()) {
      return stream.collect(Collectors.groupingBy(Pair::getKey, Collectors.counting()));
    }
  }

  @Override
  public HoodiePairData<K, Iterable<V>> groupByKey() {
    Collector<Pair<K, V>, ?, List<V>> mappingCollector = Collectors.mapping(Pair::getValue, Collectors.toList());
    Collector<Pair<K, V>, ?, Map<K, List<V>>> groupingCollector =
        Collectors.groupingBy(Pair::getKey, mappingCollector);

    try (Stream<Pair<K, V>> s = asStream()) {
      Map<K, List<V>> groupedByKey = s.collect(groupingCollector);
      return new HoodieListPairData<>(
          groupedByKey.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())),
          lazy
      );
    }
  }

  @Override
  public HoodiePairData<K, V> reduceByKey(SerializableBiFunction<V, V, V> combiner, int parallelism) {
    try (Stream<Pair<K, V>> stream = asStream()) {
      Map<K, java.util.Optional<V>> reducedMap = stream.collect(
          Collectors.groupingBy(
              Pair::getKey,
              HashMap::new,
              Collectors.mapping(Pair::getValue, Collectors.reducing(combiner::apply))));

      return new HoodieListPairData<>(
          reducedMap.entrySet()
              .stream()
              .map(e -> Pair.of(e.getKey(), e.getValue().orElse(null))),
          lazy
      );
    }
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<Pair<K, V>, O> func) {
    Function<Pair<K, V>, O> uncheckedMapper = throwingMapWrapper(func);
    return new HoodieListData<>(asStream().map(uncheckedMapper), lazy);
  }

  @Override
  public <W> HoodiePairData<K, W> mapValues(SerializableFunction<V, W> func) {
    Function<V, W> uncheckedMapper = throwingMapWrapper(func);
    return new HoodieListPairData<>(asStream().map(p -> Pair.of(p.getKey(), uncheckedMapper.apply(p.getValue()))), lazy);
  }

  @Override
  public <W> HoodiePairData<K, W> flatMapValues(SerializableFunction<V, Iterator<W>> func) {
    Function<V, Iterator<W>> uncheckedMapper = throwingMapWrapper(func);
    return new HoodieListPairData<>(asStream().flatMap(p -> {
      Iterator<W> mappedValuesIterator = uncheckedMapper.apply(p.getValue());
      Iterator<Pair<K, W>> mappedPairsIterator =
          new MappingIterator<>(mappedValuesIterator, w -> Pair.of(p.getKey(), w));

      return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(mappedPairsIterator, Spliterator.ORDERED), true).onClose(new IteratorCloser(mappedValuesIterator));
    }), lazy);
  }

  @Override
  public <L, W> HoodiePairData<L, W> mapToPair(SerializablePairFunction<Pair<K, V>, L, W> mapToPairFunc) {
    return new HoodieListPairData<>(asStream().map(p -> throwingMapToPairWrapper(mapToPairFunc).apply(p)), lazy);
  }

  @Override
  public <W> HoodiePairData<K, Pair<V, Option<W>>> leftOuterJoin(HoodiePairData<K, W> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListPairData);

    // Transform right-side container to a multi-map of [[K]] to [[List<W>]] values
    try (Stream<Pair<K, W>> stream = ((HoodieListPairData<K, W>) other).asStream()) {
      HashMap<K, List<W>> rightStreamMap = stream.collect(
          Collectors.groupingBy(
              Pair::getKey,
              HashMap::new,
              Collectors.mapping(Pair::getValue, Collectors.toList())));

      Stream<Pair<K, Pair<V, Option<W>>>> leftOuterJoined = asStream().flatMap(pair -> {
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

      return new HoodieListPairData<>(leftOuterJoined, lazy);
    }
  }

  @Override
  public HoodiePairData<K, V> union(HoodiePairData<K, V> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListPairData);
    Stream<Pair<K, V>> unionStream = Stream.concat(asStream(), ((HoodieListPairData<K, V>) other).asStream());
    return new HoodieListPairData<>(unionStream, lazy);
  }

  @Override
  public HoodiePairData<K, V> filter(SerializablePairPredicate<K, V> filter) {
    return new HoodieListPairData<>(
        asStream().filter(p -> {
          try {
            return filter.call(p.getKey(), p.getValue());
          } catch (Exception e) {
            throw new HoodieException(e.getMessage(), e.getCause());
          }
        }),
        lazy
    );
  }

  @Override
  public <W> HoodiePairData<K, Pair<V, W>> join(HoodiePairData<K, W> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListPairData);

    // Transform right-side container to a multi-map of [[K]] to [[List<W>]] values
    try (Stream<Pair<K, W>> stream = ((HoodieListPairData<K, W>) other).asStream()) {
      HashMap<K, List<W>> rightStreamMap = stream.collect(
          Collectors.groupingBy(
              Pair::getKey,
              HashMap::new,
              Collectors.mapping(Pair::getValue, Collectors.toList())));

      List<Pair<K, Pair<V, W>>> joinResult = new ArrayList<>();
      asStream().forEach(pair -> {
        K key = pair.getKey();
        V leftValue = pair.getValue();
        List<W> rightValues = rightStreamMap.getOrDefault(key, Collections.emptyList());

        for (W rightValue : rightValues) {
          joinResult.add(Pair.of(key, Pair.of(leftValue, rightValue)));
        }
      });

      return new HoodieListPairData<>(joinResult, lazy);
    }
  }

  @Override
  public long count() {
    return super.count();
  }

  @Override
  public List<Pair<K, V>> collectAsList() {
    return super.collectAsList();
  }

  @Override
  public int deduceNumPartitions() {
    return 1;
  }

  public static <K, V> HoodieListPairData<K, V> lazy(List<Pair<K, V>> data) {
    return new HoodieListPairData<>(data, true);
  }

  public static <K, V> HoodieListPairData<K, V> eager(List<Pair<K, V>> data) {
    return new HoodieListPairData<>(data, false);
  }

  public static <K, V> HoodieListPairData<K, V> lazy(Map<K, List<V>> data) {
    return new HoodieListPairData<>(explode(data), true);
  }

  public static <K, V> HoodieListPairData<K, V> eager(Map<K, List<V>> data) {
    return new HoodieListPairData<>(explode(data), false);
  }

  private static <K, V> Stream<Pair<K, V>> explode(Map<K, List<V>> data) {
    return data.entrySet().stream()
        .flatMap(e -> e.getValue().stream().map(v -> Pair.of(e.getKey(), v)));
  }
}
