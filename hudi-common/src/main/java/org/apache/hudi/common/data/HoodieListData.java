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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * In-memory implementation of {@link HoodieData} holding internally a {@link Stream} of objects.
 *
 * {@link HoodieListData} can have either of the 2 execution semantics:
 *
 * <ol>
 *   <li>Eager: with every operation being executed right away</li>
 *   <li>Lazy: with every operation being "stacked up", with it execution postponed until
 *   "terminal" operation is invoked</li>
 * </ol>
 *
 * NOTE: This is an in-memory counterpart for {@code HoodieJavaRDD}, and it strives to provide
 *       similar semantic as RDD container -- all intermediate (non-terminal, not de-referencing
 *       the stream like "collect", "groupBy", etc) operations are executed *lazily*.
 *       This allows to make sure that compute/memory churn is minimal since only necessary
 *       computations will ultimately be performed.
 *
 *       Please note, however, that while RDD container allows the same collection to be
 *       de-referenced more than once (ie terminal operation invoked more than once),
 *       {@link HoodieListData} allows that only when instantiated w/ an eager execution semantic.
 *
 * @param <T> type of object.
 */
public class HoodieListData<T> extends HoodieBaseListData<T> implements HoodieData<T> {

  private HoodieListData(List<T> data, boolean lazy) {
    super(data, lazy);
  }

  HoodieListData(Stream<T> dataStream, boolean lazy) {
    super(dataStream, lazy);
  }

  /**
   * Creates instance of {@link HoodieListData} bearing *eager* execution semantic
   *
   * @param listData a {@link List} of objects in type T
   * @param <T>      type of object
   * @return a new instance containing the {@link List<T>} reference
   */
  public static <T> HoodieListData<T> eager(List<T> listData) {
    return new HoodieListData<>(listData, false);
  }

  /**
   * Creates instance of {@link HoodieListData} bearing *lazy* execution semantic
   *
   * @param listData a {@link List} of objects in type T
   * @param <T>      type of object
   * @return a new instance containing the {@link List<T>} reference
   */
  public static <T> HoodieListData<T> lazy(List<T> listData) {
    return new HoodieListData<>(listData, true);
  }

  @Override
  public void persist(String level) {
    // No OP
  }

  @Override
  public void unpersist() {
    // No OP
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
    return new HoodieListData<>(asStream().map(throwingMapWrapper(func)), lazy);
  }

  @Override
  public <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>, Iterator<O>> func, boolean preservesPartitioning) {
    Function<Iterator<T>, Iterator<O>> mapper = throwingMapWrapper(func);
    return new HoodieListData<>(
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                mapper.apply(asStream().iterator()), Spliterator.ORDERED), true),
        lazy
    );
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func) {
    Function<T, Iterator<O>> mapper = throwingMapWrapper(func);
    Stream<O> mappedStream = asStream().flatMap(e ->
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(mapper.apply(e), Spliterator.ORDERED), true));
    return new HoodieListData<>(mappedStream, lazy);
  }

  @Override
  public <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> func) {
    Function<T, Pair<K, V>> throwableMapToPairFunc = throwingMapToPairWrapper(func);
    return new HoodieListPairData<>(asStream().map(throwableMapToPairFunc), lazy);
  }

  @Override
  public HoodieData<T> distinct() {
    return new HoodieListData<>(asStream().distinct(), lazy);
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
    return new HoodieListData<>(asStream().filter(r -> throwingMapWrapper(filterFunc).apply(r)), lazy);
  }

  @Override
  public HoodieData<T> union(HoodieData<T> other) {
    ValidationUtils.checkArgument(other instanceof HoodieListData);
    return new HoodieListData<>(Stream.concat(asStream(), ((HoodieListData<T>)other).asStream()), lazy);
  }

  @Override
  public HoodieData<T> repartition(int parallelism) {
    // no op
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
  public int getNumPartitions() {
    return 1;
  }

  @Override
  public List<T> collectAsList() {
    return super.collectAsList();
  }
}
