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

package org.apache.hudi.client.utils;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * Util class for spark Engine.
 */
public class SparkEngineUtils {
  private static JavaSparkContext jsc;

  public static void setJsc(JavaSparkContext javaSparkContext) {
    jsc = javaSparkContext;
  }

  /**
   * Get the only SparkContext from JVM.
   */
  public static JavaSparkContext getJsc() {
    if (jsc == null) {
      jsc = new JavaSparkContext(SparkContext.getOrCreate());
    }
    return jsc;
  }

  /**
   * Parallelize map function.
   */
  public static <T, R> List<R> parallelizeMap(List<T> list, int num, Function<T, R> f) {
    return jsc.parallelize(list, num).map(f).collect();
  }

  /**
   * Parallelize flat map function.
   */
  public static <T, R> List<R> parallelizeFlatMap(List<T> list, int num, FlatMapFunction<T, R> f) {
    return jsc.parallelize(list, num).flatMap(f).collect();
  }
}
