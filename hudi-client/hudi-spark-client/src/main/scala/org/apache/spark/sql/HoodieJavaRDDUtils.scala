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

package org.apache.spark.sql

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

import java.util.Comparator
import scala.reflect.ClassTag

/**
 * Suite of utilities helping in handling [[JavaRDD]]
 */
object HoodieJavaRDDUtils {

  /**
   * [[HoodieRDDUtils.sortWithinPartitions]] counterpart transforming [[JavaRDD]]s
   */
  def sortWithinPartitions[K, V](rdd: JavaPairRDD[K, V], c: Comparator[K]): JavaPairRDD[K, V] = {
    implicit val classTagK: ClassTag[K] = fakeClassTag
    implicit val classTagV: ClassTag[V] = fakeClassTag
    JavaPairRDD.fromRDD(HoodieRDDUtils.sortWithinPartitions(rdd.rdd, c))
  }

}
