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
import org.apache.hudi.client.utils.SparkMemoryUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.function.SerializableFunction;

import org.apache.spark.api.java.JavaRDD;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Holds a {@link JavaRDD} of objects.
 *
 * @param <T> type of object.
 */
public class HoodieJavaRDDData<T> extends HoodieData<T> {

  private final JavaRDD<T> rddData;

  private HoodieJavaRDDData(JavaRDD<T> rddData) {
    this.rddData = rddData;
  }

  /**
   * @param rddData a {@link JavaRDD} of objects in type T.
   * @param <T>     type of object.
   * @return a new instance containing the {@link JavaRDD<T>} reference.
   */
  public static <T> HoodieJavaRDDData<T> of(JavaRDD<T> rddData) {
    return new HoodieJavaRDDData<>(rddData);
  }

  /**
   * @param data        a {@link List} of objects in type T.
   * @param context     {@link HoodieSparkEngineContext} to use.
   * @param parallelism parallelism for the {@link JavaRDD<T>}.
   * @param <T>         type of object.
   * @return a new instance containing the {@link JavaRDD<T>} instance.
   */
  public static <T> HoodieJavaRDDData<T> of(
      List<T> data, HoodieSparkEngineContext context, int parallelism) {
    return new HoodieJavaRDDData<>(context.getJavaSparkContext().parallelize(data, parallelism));
  }

  /**
   * @param hoodieData {@link HoodieJavaRDDData<T>} instance containing the {@link JavaRDD} of objects.
   * @param <T>        type of object.
   * @return the a {@link JavaRDD} of objects in type T.
   */
  public static <T> JavaRDD<T> getJavaRDD(HoodieData<T> hoodieData) {
    return ((HoodieJavaRDDData<T>) hoodieData).get();
  }

  @Override
  public JavaRDD<T> get() {
    return rddData;
  }

  @Override
  public boolean isEmpty() {
    return rddData.isEmpty();
  }

  @Override
  public void persist(Properties properties) {
    rddData.persist(SparkMemoryUtils.getWriteStatusStorageLevel(properties));
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
    return HoodieJavaRDDData.of(rddData.map(func::apply));
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func) {
    return HoodieJavaRDDData.of(rddData.flatMap(func::apply));
  }

  @Override
  public List<T> collectAsList() {
    return rddData.collect();
  }
}
