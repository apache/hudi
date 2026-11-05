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

package org.apache.spark.sql.hudi.dataskipping.rules

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step}
import org.apache.spark.sql.types.{BooleanType, DataType, DecimalType, DoubleType, FloatType, LongType, NumericType, StringType, TimestampType}

import scala.util.Try

/**
 * `Cast`.
 *
 * Shared guard: [[preservesOrder]] of the child type to the target type (rejects, for ex, string to
 * numeric, timestamp to string, narrowing numeric casts, and collation changes). The (from, to)
 * allowlist lives here, at the top of this object.
 *
 * NOTE: The trailing arm still accepts every pair not listed; making the allowlist deny-by-default
 *       (trailing `case _ => false`) is pending in #20513
 */
object CastRule extends RangeRule {

  /**
   * Checks whether casting expression of [[from]] [[DataType]] to [[to]] [[DataType]] will
   * preserve ordering of the elements
   */
  private[dataskipping] def preservesOrder(from: DataType, to: DataType): Boolean =
    (from, to) match {
      // NOTE: Casting between String and Numeric types re-orders elements (for ex, "10" < "9"
      //       lexicographically). These match by type, so that collated strings on Spark 4 are
      //       rejected as well
      case (_: StringType, _: NumericType) => false
      case (_: NumericType, _: StringType) => false
      // NOTE: Timestamps format in the session time zone, so local time repeats across a DST
      //       fall-back (for ex, 01:30 EDT is followed by 01:10 EST) and the strings re-order
      case (TimestampType, _: StringType) => false
      // NOTE: On Spark 4 StringType carries a collation, and casting to a different collation changes
      //       the sort order (for ex, "B" < "a" in UTF8_BINARY but "a" < "B" in UTF8_LCASE). Equality
      //       also compares the char/varchar length constraint, so those casts are conservatively
      //       rejected too. On Spark 3 StringType is a singleton, making this arm trivially true
      case (fromStr: StringType, toStr: StringType) => fromStr == toStr
      // NOTE: Every type casts to a string, so collated targets are rejected whatever the source
      //       (for ex, binary bounds are in byte order, which UTF8_LCASE does not follow)
      case (_, toStr: StringType) => toStr == StringType
      // NOTE: Casting to boolean maps every non-zero (or "true"-like) value to true, so it is not
      //       monotonic (for ex, min -5 and max 5 both cast to true while 0 casts to false)
      case (BooleanType, BooleanType) => true
      case (_, BooleanType) => false
      // NOTE: A collated string does not sort by its bytes, so casting it to any non-string type
      //       (for ex, binary, which compares bytes) can re-order values. String targets are
      //       handled above, so this only rejects collated sources
      case (fromStr: StringType, _) => fromStr == StringType
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

  override def steps(term: Expression): Seq[Step] = term match {
    // NOTE: Some casts (for ex, string to date in non-ANSI mode) return NULL on malformed input;
    //       `partial` is left false for now, as it has no behavioral effect yet
    case cast @ Cast(child, _, _, _) if hasDataType(child) && preservesOrder(child.dataType, cast.dataType) =>
      Seq(Step(term, child, Increasing, fileGuard = None, partial = false))

    case _ => Nil
  }

  /**
   * Expression indexes skip the resolution check, so the child can be unresolved. Some unresolved
   * nodes (for ex, an unknown function) throw on `dataType`; others are typed (for ex, `to_date`
   * before the analyzer sets its time zone) and are ranged like resolved ones, as before the split.
   */
  private def hasDataType(e: Expression): Boolean = Try(e.dataType).isSuccess
}
