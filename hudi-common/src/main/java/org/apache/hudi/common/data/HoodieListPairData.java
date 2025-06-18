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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * In-memory implementation of {@link HoodiePairData}, partition-aware.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class HoodieListPairData<K, V> extends HoodieBaseListData<Pair<K, V>> implements HoodiePairData<K, V> {

  // —— Constructors ——

  private HoodieListPairData(List<Pair<K, V>> data, boolean lazy) {
    super(data, lazy);
  }

  private HoodieListPairData(Stream<Pair<K, V>> stream, boolean lazy) {
    super(stream, lazy);
  }

  @SuppressWarnings("unused")
  HoodieListPairData(List<List<Pair<K, V>>> listParts, boolean lazy, int dummy) {
    super(listParts, lazy, dummy);
  }

  @SuppressWarnings("unused")
  HoodieListPairData(List<Stream<Pair<K, V>>> streamParts, boolean lazy, String dummy) {
    super(streamParts, lazy, dummy);
  }

  // —— Static factories ——

  public static <K, V> HoodieListPairData<K, V> eager(List<Pair<K, V>> data) {
    return new HoodieListPairData<>(data, false);
  }

  public static <K, V> HoodieListPairData<K, V> lazy(List<Pair<K, V>> data) {
    return new HoodieListPairData<>(data, true);
  }

  public static <K, V> HoodieListPairData<K, V> eagerPartitions(List<List<Pair<K, V>>> parts) {
    return new HoodieListPairData<>(parts, false, 0);
  }

  public static <K, V> HoodieListPairData<K, V> lazyPartitions(List<List<Pair<K, V>>> parts) {
    return new HoodieListPairData<>(parts, true, 0);
  }

  public static <K, V> HoodieListPairData<K, V> eagerPartitionsFromStreams(List<Stream<Pair<K, V>>> parts) {
    return new HoodieListPairData<>(parts, false, "");
  }

  public static <K, V> HoodieListPairData<K, V> lazyPartitionsFromStreams(List<Stream<Pair<K, V>>> parts) {
    return new HoodieListPairData<>(parts, true, "");
  }

  // —— Overrides & metadata ——

  @Override
  public List<Pair<K, V>> get() {
    return collectAsList();
  }

  @Override
  public void persist(String cacheConfig) { 
    /* no-op */
  }

  @Override
  public void unpersist() { 
    /* no-op */
  }

  @Override
  public int deduceNumPartitions() {
    return partitionCount();
  }

  // —— Pair-specific ops ——

  @Override
  public HoodieData<K> keys() {
    if (lazy) {
      List<Stream<K>> parts = partitions.asLeft().stream()
          .map(s -> s.map(Pair::getKey))
          .collect(Collectors.toList());
      return new HoodieListData<>(parts, true, "");
    } else {
      List<List<K>> parts = partitions.asRight().stream()
          .map(list -> list.stream().map(Pair::getKey).collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListData<>(parts, false, 0);
    }
  }

  @Override
  public HoodieData<V> values() {
    if (lazy) {
      List<Stream<V>> parts = partitions.asLeft().stream()
          .map(s -> s.map(Pair::getValue))
          .collect(Collectors.toList());
      return new HoodieListData<>(parts, true, "");
    } else {
      List<List<V>> parts = partitions.asRight().stream()
          .map(list -> list.stream().map(Pair::getValue).collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListData<>(parts, false, 0);
    }
  }

  @Override
  public Map<K, Long> countByKey() {
    try (Stream<Pair<K, V>> s = asStream()) {
      return s.collect(Collectors.groupingBy(Pair::getKey, Collectors.counting()));
    }
  }

  @Override
  public HoodiePairData<K, Iterable<V>> groupByKey() {
    Map<K, List<V>> grouped;
    try (Stream<Pair<K, V>> s = asStream()) {
      grouped = s.collect(Collectors.groupingBy(
          Pair::getKey,
          Collectors.mapping(Pair::getValue, Collectors.toList())));
    }
    Stream<Pair<K, Iterable<V>>> out = grouped.entrySet().stream()
        .map(e -> Pair.of(e.getKey(), (Iterable<V>) e.getValue()));
    return new HoodieListPairData<>(out, lazy);
  }

  @Override
  public HoodiePairData<K, V> reduceByKey(SerializableBiFunction<V, V, V> combiner, int parallelism) {
    // combine values by key using a BinaryOperator
    java.util.function.BinaryOperator<V> op = (v1, v2) -> {
      try {
        return combiner.apply(v1, v2);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    // collect into a map by merging values with the operator
    Map<K, V> reducedMap = asStream().collect(
        Collectors.toMap(
            Pair::getKey,
            Pair::getValue,
            op,
            HashMap::new
        )
    );
    // convert back to a stream of pairs
    Stream<Pair<K, V>> out = reducedMap.entrySet().stream()
        .map(e -> Pair.of(e.getKey(), e.getValue()));
    return new HoodieListPairData<>(out, lazy);
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<Pair<K, V>, O> func) {
    Function<Pair<K, V>, O> mapper = throwingMapWrapper(func);
    return new HoodieListData<>(asStream().map(mapper), lazy);
  }

  @Override
  public <W> HoodiePairData<K, W> mapValues(SerializableFunction<V, W> func) {
    Function<V, W> mapper = throwingMapWrapper(func);
    if (lazy) {
      List<Stream<Pair<K, W>>> parts = partitions.asLeft().stream()
          .map(s -> s.map(p -> Pair.of(p.getKey(), mapper.apply(p.getValue()))))
          .collect(Collectors.toList());
      return new HoodieListPairData<>(parts, true, "");
    } else {
      List<List<Pair<K, W>>> parts = partitions.asRight().stream()
          .map(list -> list.stream()
              .map(p -> Pair.of(p.getKey(), mapper.apply(p.getValue())))
              .collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListPairData<>(parts, false, 0);
    }
  }

  @Override
  public <W> HoodiePairData<K, W> flatMapValues(SerializableFunction<V, Iterator<W>> func) {
    Function<V, Iterator<W>> mapper = throwingMapWrapper(func);
    if (lazy) {
      List<Stream<Pair<K, W>>> parts = partitions.asLeft().stream()
          .map(s -> s.flatMap(p -> {
            Iterator<W> it = mapper.apply(p.getValue());
            MappingIterator<W, Pair<K, W>> mapIt = new MappingIterator<>(it, w -> Pair.of(p.getKey(), w));
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(mapIt, Spliterator.ORDERED), true)
                .onClose(new IteratorCloser(it));
          }))
          .collect(Collectors.toList());
      return new HoodieListPairData<>(parts, true, "");
    } else {
      List<List<Pair<K, W>>> parts = partitions.asRight().stream()
          .map(list -> list.stream().flatMap(p -> {
            Iterator<W> it = mapper.apply(p.getValue());
            List<Pair<K, W>> buf = new ArrayList<>();
            it.forEachRemaining(w -> buf.add(Pair.of(p.getKey(), w)));
            return buf.stream();
          }).collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListPairData<>(parts, false, 0);
    }
  }

  @Override
  public <L, W> HoodiePairData<L, W> mapToPair(SerializablePairFunction<Pair<K, V>, L, W> func) {
    Function<Pair<K, V>, Pair<L, W>> mapper = throwingMapToPairWrapper(func);
    return new HoodieListPairData<>(asStream().map(mapper), lazy);
  }

  @Override
  public <W> HoodiePairData<K, Pair<V, W>> join(HoodiePairData<K, W> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListPairData);
    try (Stream<Pair<K, W>> otherStream = ((HoodieListPairData<K, W>) other).asStream()) {
      Map<K, List<W>> rightMap = otherStream.collect(
          Collectors.groupingBy(
              Pair::getKey,
              HashMap::new,
              Collectors.mapping(Pair::getValue, Collectors.toList())
          )
      );
      Stream<Pair<K, Pair<V, W>>> joined = asStream().flatMap(pair -> {
        List<W> rights = rightMap.getOrDefault(pair.getKey(), Collections.emptyList());
        return rights.stream()
            .map(w -> Pair.of(pair.getKey(), Pair.of(pair.getValue(), w)));
      });
      return new HoodieListPairData<>(joined, lazy);
    }
  }

  @Override
  public <W> HoodiePairData<K, Pair<V, Option<W>>> leftOuterJoin(HoodiePairData<K, W> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListPairData);
    try (Stream<Pair<K, W>> otherStream = ((HoodieListPairData<K, W>) other).asStream()) {
      Map<K, List<W>> rightMap = otherStream.collect(
          Collectors.groupingBy(
              Pair::getKey,
              HashMap::new,
              Collectors.mapping(Pair::getValue, Collectors.toList())
          )
      );
      Stream<Pair<K, Pair<V, Option<W>>>> leftJoined = asStream().flatMap(pair -> {
        List<W> rights = rightMap.get(pair.getKey());
        if (rights == null || rights.isEmpty()) {
          return Stream.of(Pair.of(pair.getKey(), Pair.of(pair.getValue(), Option.empty())));
        } else {
          return rights.stream()
              .map(w -> Pair.of(pair.getKey(), Pair.of(pair.getValue(), Option.of(w))));
        }
      });
      return new HoodieListPairData<>(leftJoined, lazy);
    }
  }

  @Override
  public HoodiePairData<K, V> union(HoodiePairData<K, V> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListPairData);
    HoodieListPairData<K, V> o = (HoodieListPairData<K, V>) other;
    if (lazy) {
      List<Stream<Pair<K, V>>> all = new ArrayList<>(partitions.asLeft());
      all.addAll(o.partitions.asLeft());
      return new HoodieListPairData<>(all, true, "");
    } else {
      List<List<Pair<K, V>>> all = new ArrayList<>(partitions.asRight());
      all.addAll(o.partitions.asRight());
      return new HoodieListPairData<>(all, false, 0);
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

}
