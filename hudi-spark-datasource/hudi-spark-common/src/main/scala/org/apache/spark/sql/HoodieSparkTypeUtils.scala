/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.{DataType, DecimalType, NumericType, StringType}

// TODO unify w/ DataTypeUtils
object HoodieSparkTypeUtils {

  /**
   * Returns whether this DecimalType is wider than `other`. If yes, it means `other`
   * can be casted into `this` safely without losing any precision or range.
   */
  def isWiderThan(one: DecimalType, another: DecimalType) =
    one.isWiderThan(another)

  /**
   * Checks whether casting expression of [[from]] [[DataType]] to [[to]] [[DataType]] will
   * preserve ordering of the elements
   */
  def isCastPreservingOrdering(from: DataType, to: DataType): Boolean =
    (from, to) match {
      // NOTE: Casting between String and Numeric types re-orders elements (for ex, "10" < "9"
      //       lexicographically). These arms must stay ahead of the numeric arm below, since
      //       Cast.canUpCast treats atomic-to-string casts as legal up-casts
      case (_: StringType, _: NumericType) => false
      case (_: NumericType, _: StringType) => false
      // NOTE: On Spark 4 StringType carries a collation (and constraint) and its equals compares
      //       both; casting to a different collation changes the sort order. On Spark 3
      //       StringType is a singleton, making this arm trivially true
      case (fromStr: StringType, toStr: StringType) => fromStr == toStr
      // NOTE: Narrowing numeric casts (for ex, bigint to int) overflow and wrap around in
      //       non-ANSI mode, breaking ordering; only up-casts are guaranteed order-preserving.
      //       This is conservative (for ex, double to float rounds monotonically but is
      //       rejected), which only costs pruning opportunity, never correctness
      case (fromNum: NumericType, toNum: NumericType) => Cast.canUpCast(fromNum, toNum)

      case _ => true
    }
}
