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

package org.apache.spark

import java.util.{Iterator => JIterator}
import java.lang.{Iterable => JIterable}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.{Function => JFunction}


object HoodieSpark2RDDUtils extends HoodieRDDUtils {

  def flatMapValues[K, V, W](rdd: JavaPairRDD[K, V], f: JFunction[V, JIterator[W]]): JavaPairRDD[K, W] =
    rdd.flatMapValues(new JFunction[V, JIterable[W]] {
      override def call(v: V): JIterable[W] = {
        new JIterable[W] {
          override def iterator: JIterator[W] = f.call(v)
        }
      }
    })

}
