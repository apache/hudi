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
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, FloatType, LongType, NumericType, StringType, TimestampType}

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
      //       lexicographically)
      case (StringType, _: NumericType) => false
      case (_: NumericType, StringType) => false
      // NOTE: Cast.canUpCast rejects decimal to float/double as lossy, but rounding is monotonic
      //       and every decimal fits within float range. Spark's type coercion inserts this cast
      //       when a decimal column is compared with a float/double (for ex, `price > 99.5D`)
      case (_: DecimalType, FloatType | DoubleType) => true
      // NOTE: Narrowing numeric casts (for ex, bigint to int) overflow and wrap around in
      //       non-ANSI mode, breaking ordering; only up-casts are guaranteed order-preserving.
      //       This is conservative (for ex, double to float rounds monotonically but is
      //       rejected), which only costs pruning opportunity, never correctness
      case (fromNum: NumericType, toNum: NumericType) => Cast.canUpCast(fromNum, toNum)
      // NOTE: Timestamps cast to numbers through their epoch seconds, which only fit bigint and
      //       floating-point targets; narrower integral targets wrap around and decimals overflow
      //       into NULL in non-ANSI mode
      case (TimestampType, LongType | FloatType | DoubleType) => true
      case (TimestampType, _: NumericType) => false

      case _ => true
    }
}
