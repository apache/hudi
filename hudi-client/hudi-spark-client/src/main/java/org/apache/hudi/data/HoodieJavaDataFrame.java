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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.storage.StorageLevel;

import java.util.Iterator;
import java.util.List;

/**
 * Holds a {@link HoodieJavaDataFrame} of objects.
 *
 * @param <T> type of object.
 */
public class HoodieJavaDataFrame<T> implements HoodieData<T> {

  private final Dataset<T> dataFrame;

  private HoodieJavaDataFrame(Dataset<T> dataFrame) {
    this.dataFrame = dataFrame;
  }

  /**
   * @param dataFrame a {@link JavaRDD} of objects in type T.
   * @param <T>     type of object.
   * @return a new instance containing the {@link JavaRDD<T>} reference.
   */
  public static <T> HoodieJavaDataFrame<T> of(Dataset<T> dataFrame) {
    return new HoodieJavaDataFrame<>(dataFrame);
  }

  /**
   * @param data        a {@link List} of objects in type T.
   * @param context     {@link HoodieSparkEngineContext} to use.
   * @param parallelism parallelism for the {@link JavaRDD<T>}.
   * @param <T>         type of object.
   * @return a new instance containing the {@link JavaRDD<T>} instance.
   */
  public static <T> HoodieJavaDataFrame<T> of(
      List<T> data, HoodieSparkEngineContext context, int parallelism) {
    return null;
  }

  /**
   * @param hoodieData {@link HoodieJavaDataFrame <T>} instance containing the {@link JavaRDD} of objects.
   * @param <T>        type of object.
   * @return the a {@link JavaRDD} of objects in type T.
   */
  public static <T> Dataset<T> getDataFrame(HoodieData<T> hoodieData) {
    return ((HoodieJavaDataFrame<T>) hoodieData).dataFrame;
  }

  public static <K, V> JavaPairRDD<K, V> getJavaRDD(HoodiePairData<K, V> hoodieData) {
    return ((HoodieJavaPairRDD<K, V>) hoodieData).get();
  }

  @Override
  public int getId() {
    return (int) dataFrame.queryExecution().id();
  }

  @Override
  public void persist(String level) {
    dataFrame.persist(StorageLevel.fromString(level));
  }

  @Override
  public void persist(String level, HoodieEngineContext engineContext, HoodieDataCacheKey cacheKey) {
    engineContext.putCachedDataIds(cacheKey, this.getId());
    dataFrame.persist(StorageLevel.fromString(level));
  }

  @Override
  public void unpersist() {
    dataFrame.unpersist();
  }

  @Override
  public boolean isEmpty() {
    return dataFrame.isEmpty();
  }

  @Override
  public long count() {
    return dataFrame.count();
  }

  @Override
  public int getNumPartitions() {
    return dataFrame.rdd().getNumPartitions();
  }

  @Override
  public int deduceNumPartitions() {
    return 0;
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
    return null;
  }

  @Override
  public <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>, Iterator<O>> func, boolean preservesPartitioning) {
    return null;
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func) {
    return null;
  }

  @Override
  public <K, V> HoodiePairData<K, V> flatMapToPair(SerializableFunction<T, Iterator<? extends Pair<K, V>>> func) {
    return null;
  }

  @Override
  public <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> func) {
    return null;
  }

  @Override
  public HoodieData<T> distinct() {
    return HoodieJavaDataFrame.of(dataFrame.distinct());
  }

  @Override
  public HoodieData<T> distinct(int parallelism) {
    return HoodieJavaDataFrame.of(dataFrame.distinct());
  }

  @Override
  public HoodieData<T> filter(SerializableFunction<T, Boolean> filterFunc) {
    return null;
  }

  public HoodieData<T> filter(String filterExpression) {
    return HoodieJavaDataFrame.of(dataFrame.filter(filterExpression));
  }

  @Override
  public HoodieData<T> union(HoodieData<T> other) {
    return HoodieJavaDataFrame.of(dataFrame.union(((HoodieJavaDataFrame<T>) other).dataFrame));
  }

  @Override
  public List<T> collectAsList() {
    return dataFrame.collectAsList();
  }

  @Override
  public HoodieData<T> repartition(int parallelism) {
    return HoodieJavaDataFrame.of(dataFrame.repartition(parallelism));
  }
}
