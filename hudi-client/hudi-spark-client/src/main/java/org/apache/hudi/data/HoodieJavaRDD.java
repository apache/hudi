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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

/**
 * Holds a {@link JavaRDD} of objects.
 *
 * @param <T> type of object.
 */
public class HoodieJavaRDD<T> extends HoodieData<T> {

  private final JavaRDD<T> rddData;

  private HoodieJavaRDD(JavaRDD<T> rddData) {
    this.rddData = rddData;
  }

  /**
   * @param rddData a {@link JavaRDD} of objects in type T.
   * @param <T>     type of object.
   * @return a new instance containing the {@link JavaRDD<T>} reference.
   */
  public static <T> HoodieJavaRDD<T> of(JavaRDD<T> rddData) {
    return new HoodieJavaRDD<>(rddData);
  }

  /**
   * @param data        a {@link List} of objects in type T.
   * @param context     {@link HoodieSparkEngineContext} to use.
   * @param parallelism parallelism for the {@link JavaRDD<T>}.
   * @param <T>         type of object.
   * @return a new instance containing the {@link JavaRDD<T>} instance.
   */
  public static <T> HoodieJavaRDD<T> of(
      List<T> data, HoodieSparkEngineContext context, int parallelism) {
    return new HoodieJavaRDD<>(context.getJavaSparkContext().parallelize(data, parallelism));
  }

  /**
   * @param hoodieData {@link HoodieJavaRDD <T>} instance containing the {@link JavaRDD} of objects.
   * @param <T>        type of object.
   * @return the a {@link JavaRDD} of objects in type T.
   */
  public static <T> JavaRDD<T> getJavaRDD(HoodieData<T> hoodieData) {
    return ((HoodieJavaRDD<T>) hoodieData).get();
  }

  @Override
  public JavaRDD<T> get() {
    return rddData;
  }

  @Override
  public void persist(String storageLevel) {
    rddData.persist(StorageLevel.fromString(storageLevel));
  }

  @Override
  public void unpersist() {
    rddData.unpersist();
  }

  @Override
  public boolean isEmpty() {
    return rddData.isEmpty();
  }

  @Override
  public long count() {
    return rddData.count();
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
    return HoodieJavaRDD.of(rddData.map(func::apply));
  }

  @Override
  public <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>, Iterator<O>> func, boolean preservesPartitioning) {
    return HoodieJavaRDD.of(rddData.mapPartitions(func::apply, preservesPartitioning));
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func) {
    return HoodieJavaRDD.of(rddData.flatMap(e -> func.apply(e)));
  }

  @Override
  public <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> mapToPairFunc) {
    return HoodieJavaPairRDD.of(rddData.mapToPair(input -> {
      Pair<K, V> pair = mapToPairFunc.call(input);
      return new Tuple2<>(pair.getLeft(), pair.getRight());
    }));
  }

  @Override
  public HoodieData<T> distinct() {
    return HoodieJavaRDD.of(rddData.distinct());
  }

  @Override
  public HoodieData<T> union(HoodieData<T> other) {
    return HoodieJavaRDD.of(rddData.union((JavaRDD<T>) other.get()));
  }

  @Override
  public List<T> collectAsList() {
    return rddData.collect();
  }
}
