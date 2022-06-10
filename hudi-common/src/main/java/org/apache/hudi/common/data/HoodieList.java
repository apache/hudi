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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * Holds a {@link List} of objects.
 *
 * @param <T> type of object.
 *
 * TODO rename to HoodieListData
 */
public class HoodieList<T> extends HoodieData<T> {

  private final Stream<T> dataStream;

  private HoodieList(List<T> data) {
    // TODO handle parallelism properly
    this.dataStream = data.stream();
  }

  HoodieList(Stream<T> dataStream) {
    this.dataStream = dataStream;
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
    return dataStream.collect(Collectors.toList());
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
    return !dataStream.findAny().isPresent();
  }

  @Override
  public long count() {
    return dataStream.count();
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
    return new HoodieList<>(dataStream.map(throwingMapWrapper(func)));
  }

  @Override
  public <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>, Iterator<O>> func, boolean preservesPartitioning) {
    return mapPartitions(func);
  }

  @Override
  public <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>, Iterator<O>> func) {
    Function<Iterator<T>, Iterator<O>> mapper = throwingMapWrapper(func);
    return new HoodieList<>(
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                mapper.apply(dataStream.iterator()), Spliterator.ORDERED), true)
    );
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func) {
    Function<T, Iterator<O>> mapper = throwingMapWrapper(func);
    Stream<O> mappedStream = dataStream.flatMap(e ->
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(mapper.apply(e), Spliterator.ORDERED), true)
    );
    return new HoodieList<>(mappedStream);
  }

  @Override
  public <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> mapToPairFunc) {
    Function<T, Pair<K, V>> throwableMapToPairFunc = throwingMapToPairWrapper(mapToPairFunc);
    return new HoodieListPairData<>(dataStream.map(throwableMapToPairFunc));
  }

  @Override
  public HoodieData<T> distinct() {
    return new HoodieList<>(dataStream.distinct());
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
    return new HoodieList<>(dataStream.filter(r -> throwingMapWrapper(filterFunc).apply(r)));
  }

  @Override
  public HoodieData<T> union(HoodieData<T> other) {
    ValidationUtils.checkArgument(other instanceof HoodieList);
    return new HoodieList<>(Stream.concat(dataStream, ((HoodieList<T>)other).dataStream));
  }

  @Override
  public List<T> collectAsList() {
    return dataStream.collect(Collectors.toList());
  }

  @Override
  public HoodieData<T> repartition(int parallelism) {
    // no op
    return this;
  }
}
