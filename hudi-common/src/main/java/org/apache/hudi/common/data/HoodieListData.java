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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * In-memory implementation of {@link HoodieData} that supports partitioned data.
 *
 * @param <T> element type
 */
public class HoodieListData<T> extends HoodieBaseListData<T> implements HoodieData<T> {

  // —— Constructors —— //

  /** Single-list (one partition) */
  HoodieListData(List<T> data, boolean lazy) {
    super(data, lazy);
  }

  /** Single-stream (one partition) */
  HoodieListData(Stream<T> stream, boolean lazy) {
    super(stream, lazy);
  }

  /** Multi-list partitions */
  HoodieListData(List<List<T>> listPartitions, boolean lazy, int dummy) {
    super(listPartitions, lazy, dummy);
  }

  /** Multi-stream partitions */
  HoodieListData(List<Stream<T>> streamPartitions, boolean lazy, String dummy) {
    super(streamPartitions, lazy, dummy);
  }

  // —— Factory methods —— //

  public static <T> HoodieListData<T> eager(List<T> listData) {
    return new HoodieListData<>(listData, false);
  }

  public static <T> HoodieListData<T> lazy(List<T> listData) {
    return new HoodieListData<>(listData, true);
  }

  public static <T> HoodieListData<T> eagerPartitions(List<List<T>> parts) {
    return new HoodieListData<>(parts, false, 0);
  }

  public static <T> HoodieListData<T> lazyPartitions(List<List<T>> parts) {
    return new HoodieListData<>(parts, true, 0);
  }

  public static <T> HoodieListData<T> eagerPartitionsFromStreams(List<Stream<T>> parts) {
    return new HoodieListData<>(parts, false, "");
  }

  public static <T> HoodieListData<T> lazyPartitionsFromStreams(List<Stream<T>> parts) {
    return new HoodieListData<>(parts, true, "");
  }

  // —— Metadata & persistence —— //

  @Override
  public int getId() {
    return -1;
  }

  @Override
  public void persist(String level) { 
    /* no-op */
  }

  @Override
  public void persist(String level, HoodieEngineContext ctx, HoodieDataCacheKey key) { 
    /* no-op */
  }

  @Override
  public void unpersist() { 
    /* no-op */
  }

  @Override
  public int getNumPartitions() {
    return partitionCount();
  }

  @Override
  public int deduceNumPartitions() {
    return partitionCount();
  }

  // —— Transformations —— //

  @Override
  public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
    Function<T, O> mapper = throwingMapWrapper(func);
    if (lazy) {
      List<Stream<O>> newParts = partitions.asLeft().stream()
          .map(s -> s.map(mapper))
          .collect(Collectors.toList());
      return new HoodieListData<>(newParts, true, "");
    } else {
      List<List<O>> newParts = partitions.asRight().stream()
          .map(list -> list.stream().map(mapper).collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListData<>(newParts, false, 0);
    }
  }

  @Override
  public <O> HoodieData<O> mapPartitions(
      SerializableFunction<Iterator<T>, Iterator<O>> func,
      boolean preservesPartitioning) {
    Function<Iterator<T>, Iterator<O>> mapper = throwingMapWrapper(func);
    if (lazy) {
      List<Stream<O>> newParts = partitions.asLeft().stream()
          .map(s -> {
            Iterator<O> it = mapper.apply(s.iterator());
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), true)
                .onClose(new IteratorCloser(it));
          })
          .collect(Collectors.toList());
      return new HoodieListData<>(newParts, true, "");
    } else {
      List<List<O>> newParts = partitions.asRight().stream()
          .map(list -> {
            List<O> buf = new ArrayList<>();
            mapper.apply(list.iterator()).forEachRemaining(buf::add);
            return buf;
          })
          .collect(Collectors.toList());
      return new HoodieListData<>(newParts, false, 0);
    }
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func) {
    Function<T, Iterator<O>> mapper = throwingMapWrapper(func);
    if (lazy) {
      List<Stream<O>> newParts = partitions.asLeft().stream()
          .map(s -> s.flatMap(e -> {
            Iterator<O> it = mapper.apply(e);
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), true)
                .onClose(new IteratorCloser(it));
          }))
          .collect(Collectors.toList());
      return new HoodieListData<>(newParts, true, "");
    } else {
      List<List<O>> newParts = partitions.asRight().stream()
          .map(list -> list.stream().flatMap(e -> {
            Iterator<O> it = mapper.apply(e);
            List<O> buf = new ArrayList<>();
            it.forEachRemaining(buf::add);
            return buf.stream();
          }).collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListData<>(newParts, false, 0);
    }
  }

  @Override
  public <K, V> HoodiePairData<K, V> flatMapToPair(
      SerializableFunction<T, Iterator<? extends Pair<K, V>>> func) {
    Function<T, Iterator<? extends Pair<K, V>>> mapper = throwingMapWrapper(func);
    if (lazy) {
      Function<T, Iterator<Pair<K, V>>> narrowMapper = e -> (Iterator<Pair<K, V>>) mapper.apply(e);

      List<Stream<Pair<K, V>>> newParts = partitions
          .asLeft().stream()
          .map(s ->
              s.flatMap(elem -> {
                Iterator<Pair<K, V>> it = narrowMapper.apply(elem);
                return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED),
                        /* parallel */ true)
                    .onClose(new IteratorCloser(it));
              })
          )
          .collect(Collectors.toList());
      return new HoodieListPairData<>(newParts, true, "");
    } else {
      List<List<Pair<K, V>>> newParts = partitions.asRight().stream()
          .map(list -> list.stream().flatMap(e -> {
            Iterator<? extends Pair<K, V>> it = mapper.apply(e);
            List<Pair<K, V>> buf = new ArrayList<>();
            it.forEachRemaining(buf::add);
            return buf.stream();
          }).collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListPairData<>(newParts, false, 0);
    }
  }

  @Override
  public <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> func) {
    Function<T, Pair<K, V>> mapper = throwingMapToPairWrapper(func);
    if (lazy) {
      List<Stream<Pair<K, V>>> newParts = partitions.asLeft().stream()
          .map(s -> s.map(mapper))
          .collect(Collectors.toList());
      return new HoodieListPairData<>(newParts, true, "");
    } else {
      List<List<Pair<K, V>>> newParts = partitions.asRight().stream()
          .map(list -> list.stream().map(mapper).collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListPairData<>(newParts, false, 0);
    }
  }

  // —— Other ops —— //

  @Override
  public HoodieData<T> distinct() {
    return new HoodieListData<>(asStream().distinct(), lazy);
  }

  @Override
  public HoodieData<T> distinct(int p) {
    return distinct();
  }

  @Override
  public <O> HoodieData<T> distinctWithKey(SerializableFunction<T, O> keyFn, int p) {
    return mapToPair(i -> Pair.of(keyFn.apply(i), i)).reduceByKey((a, b) -> a, p).values();
  }

  @Override
  public HoodieData<T> filter(SerializableFunction<T, Boolean> f) {
    Predicate<T> p = throwingMapWrapper(f)::apply;
    if (lazy) {
      List<Stream<T>> newParts = partitions.asLeft().stream()
          .map(s -> s.filter(p))
          .collect(Collectors.toList());
      return new HoodieListData<>(newParts, true, "");
    } else {
      List<List<T>> newParts = partitions.asRight().stream()
          .map(list -> list.stream().filter(p).collect(Collectors.toList()))
          .collect(Collectors.toList());
      return new HoodieListData<>(newParts, false, 0);
    }
  }

  @Override
  public HoodieData<T> union(HoodieData<T> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListData);
    HoodieListData<T> o = (HoodieListData<T>) other;
    if (lazy) {
      List<Stream<T>> all = new ArrayList<>(partitions.asLeft());
      all.addAll(o.partitions.asLeft());
      return new HoodieListData<>(all, true, "");
    } else {
      List<List<T>> all = new ArrayList<>(partitions.asRight());
      all.addAll(o.partitions.asRight());
      return new HoodieListData<>(all, false, 0);
    }
  }

  @Override
  public HoodieData<T> repartition(int p) {
    return this;
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty();
  }

  @Override
  public long count() {
    return super.count();
  }

  @Override
  public List<T> collectAsList() {
    return super.collectAsList();
  }
}
