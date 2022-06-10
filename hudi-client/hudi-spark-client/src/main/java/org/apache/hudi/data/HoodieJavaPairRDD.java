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

package org.apache.hudi.data;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Implementation of {@link HoodiePairData} using Spark {@link JavaPairRDD}.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class HoodieJavaPairRDD<K, V> extends HoodiePairData<K, V> {

  private final JavaPairRDD<K, V> pairRDDData;

  private HoodieJavaPairRDD(JavaPairRDD<K, V> pairRDDData) {
    this.pairRDDData = pairRDDData;
  }

  /**
   * @param pairRDDData a {@link JavaPairRDD} of pairs.
   * @param <K>         type of key.
   * @param <V>         type of value.
   * @return a new instance containing the {@link JavaPairRDD<K, V>} reference.
   */
  public static <K, V> HoodieJavaPairRDD<K, V> of(JavaPairRDD<K, V> pairRDDData) {
    return new HoodieJavaPairRDD<>(pairRDDData);
  }

  /**
   * @param hoodiePairData {@link HoodieJavaPairRDD <K, V>} instance containing the {@link JavaPairRDD} of pairs.
   * @param <K>            type of key.
   * @param <V>            type of value.
   * @return the {@link JavaPairRDD} of pairs.
   */
  public static <K, V> JavaPairRDD<K, V> getJavaPairRDD(HoodiePairData<K, V> hoodiePairData) {
    return ((HoodieJavaPairRDD<K, V>) hoodiePairData).get();
  }

  @Override
  public JavaPairRDD<K, V> get() {
    return pairRDDData;
  }

  @Override
  public void persist(String storageLevel) {
    pairRDDData.persist(StorageLevel.fromString(storageLevel));
  }

  @Override
  public void unpersist() {
    pairRDDData.unpersist();
  }

  @Override
  public HoodieData<K> keys() {
    return HoodieJavaRDD.of(pairRDDData.keys());
  }

  @Override
  public HoodieData<V> values() {
    return HoodieJavaRDD.of(pairRDDData.values());
  }

  @Override
  public long count() {
    return pairRDDData.count();
  }

  @Override
  public Map<K, Long> countByKey() {
    return pairRDDData.countByKey();
  }

  @Override
  public HoodiePairData<K, Iterable<V>> groupByKey() {
    return new HoodieJavaPairRDD<>(pairRDDData.groupByKey());
  }

  @Override
  public HoodiePairData<K, V> reduceByKey(SerializableBiFunction<V, V, V> combiner, int parallelism) {
    return HoodieJavaPairRDD.of(pairRDDData.reduceByKey(combiner::apply, parallelism));
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<Pair<K, V>, O> func) {
    return HoodieJavaRDD.of(pairRDDData.map(
        tuple -> func.apply(new ImmutablePair<>(tuple._1, tuple._2))));
  }

  @Override
  public <L, W> HoodiePairData<L, W> mapToPair(SerializablePairFunction<Pair<K, V>, L, W> mapToPairFunc) {
    return HoodieJavaPairRDD.of(pairRDDData.mapToPair(pair -> {
      Pair<L, W> newPair = mapToPairFunc.call(new ImmutablePair<>(pair._1, pair._2));
      return new Tuple2<>(newPair.getLeft(), newPair.getRight());
    }));
  }

  @Override
  public <W> HoodiePairData<K, Pair<V, Option<W>>> leftOuterJoin(HoodiePairData<K, W> other) {
    return HoodieJavaPairRDD.of(JavaPairRDD.fromJavaRDD(
        pairRDDData.leftOuterJoin(HoodieJavaPairRDD.getJavaPairRDD(other))
            .map(tuple -> new Tuple2<>(tuple._1,
                new ImmutablePair<>(tuple._2._1, Option.ofNullable(tuple._2._2.orElse(null)))))));
  }

  @Override
  public List<Pair<K, V>> collectAsList() {
    return pairRDDData.map(t -> Pair.of(t._1, t._2)).collect();
  }
}
