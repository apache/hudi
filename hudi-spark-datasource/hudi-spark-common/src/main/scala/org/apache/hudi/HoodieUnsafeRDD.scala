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

package org.apache.hudi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * !!! PLEASE READ CAREFULLY !!!
 *
 * Base class for all of the custom low-overhead RDD implementations for Hudi.
 *
 * To keep memory allocation footprint as low as possible, each inheritor of this RDD base class
 *
 * <pre>
 *   1. Does NOT deserialize from [[InternalRow]] to [[Row]] (therefore only providing access to
 *   Catalyst internal representations (often mutable) of the read row)
 *
 *   2. DOES NOT COPY UNDERLYING ROW OUT OF THE BOX, meaning that
 *
 *      a) access to this RDD is NOT thread-safe
 *
 *      b) iterating over it reference to a _mutable_ underlying instance (of [[InternalRow]]) is
 *      returned, entailing that after [[Iterator#next()]] is invoked on the provided iterator,
 *      previous reference becomes **invalid**. Therefore, you will have to copy underlying mutable
 *      instance of [[InternalRow]] if you plan to access it after [[Iterator#next()]] is invoked (filling
 *      it with the next row's payload)
 *
 *      c) due to item b) above, no operation other than the iteration will produce meaningful
 *      results on it and will likely fail [1]
 * </pre>
 *
 * [1] For example, [[RDD#collect]] method on this implementation would not work correctly, as it's
 * simply using Scala's default [[Iterator#toArray]] method which will simply concat all the references onto
 * the same underlying mutable object into [[Array]]. Instead each individual [[InternalRow]] _has to be copied_,
 * before concatenating into the final output. Please refer to [[HoodieRDDUtils#collect]] for more details.
 *
 * NOTE: It enforces, for ex, that all of the RDDs implement [[compute]] method returning
 *       [[InternalRow]] to avoid superfluous ser/de
 */
trait HoodieUnsafeRDD extends RDD[InternalRow] {
  override def collect(): Array[InternalRow] =
    throw new UnsupportedOperationException(
      "This method will not function correctly, please refer to scala-doc for HoodieUnsafeRDD"
    )
}
