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

import org.apache.hudi.HoodieUnsafeRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutablePair

/**
 * Suite of utilities helping in handling instances of [[HoodieUnsafeRDD]]
 */
object HoodieUnsafeCatalystUtils {

  /**
   * Unsafe version of [[RDD.map]] that avoids deserialization of [[InternalRow]] into [[Row]]
   *
   * @param rdd RDD to apply mapper to
   * @param f mapper
   * @return RDD holding mapped [[InternalRow]]s
   */
  def mapInternal(rdd: RDD[InternalRow])(f: InternalRow => InternalRow): RDD[InternalRow] =
    rdd.mapPartitionsInternal(_.map(f))

  /**
   * Creates [[DataFrame]] from the [[RDD]] of [[InternalRow]] avoiding de-/serialization penalty of
   * deserializing [[InternalRow]] to [[Row]] and back when using [[SparkSession.createDataFrame(RDD[Row], StructType)]]
   *
   * NOTE: No schema validation is performed, and it's up to the user of this API to make sure that
   *       [[InternalRow]] adheres to provided schema.
   *
   * @param spark target Spark session
   * @param rdd RDD bearing [[InternalRow]]s to create [[DataFrame]] from
   * @param structType schema of the target [[DataFrame]]
   * @return [[DataFrame]]
   */
  def createDataFrame(spark: SparkSession, rdd: RDD[InternalRow], structType: StructType): DataFrame =
    spark.internalCreateDataFrame(rdd, structType)

  /**
   * Canonical implementation of the [[RDD#collect]] for [[HoodieUnsafeRDD]], returning a properly
   * copied [[Array]] of [[InternalRow]]s
   */
  def collect(rdd: HoodieUnsafeRDD): Array[InternalRow] = {
    rdd.mapPartitionsInternal { iter =>
      // NOTE: We're leveraging [[MutablePair]] here to avoid unnecessary allocations, since
      //       a) iteration is performed lazily and b) iteration is single-threaded (w/in partition)
      val pair = new MutablePair[InternalRow, Null]()
      iter.map(row => pair.update(row.copy(), null))
    }
      .map(p => p._1)
      .collect()
  }

}
