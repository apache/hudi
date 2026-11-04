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
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NumericType, ShortType, StringType, TimestampNTZType, TimestampType}

import scala.util.Try

/**
 * `Cast`.
 *
 * Shared guard: [[preservesOrder]] of the child type to the target type. The (from, to) allowlist
 * lives here, at the top of this object, and is deny-by-default: a cast pair not listed has no range.
 */
object CastRule extends RangeRule {

  /**
   * Checks whether casting expression of [[from]] [[DataType]] to [[to]] [[DataType]] will
   * preserve ordering of the elements.
   *
   * Only casts known to preserve ordering are accepted; anything else is rejected. A rejection only
   * costs pruning opportunity, never correctness, and makes new types (for ex, Spark 4 collations or
   * TimeType) safe by construction. Notable rejected classes:
   *  - string to/from numeric re-orders values (for ex, "10" < "9" lexicographically)
   *  - timestamp to string formats in the session time zone, so local time repeats across a DST
   *    fall-back (for ex, 01:30 EDT is followed by 01:10 EST)
   *  - collation-changing string casts (for ex, "B" < "a" in UTF8_BINARY but "a" < "B" in UTF8_LCASE)
   *  - casts to boolean (for ex, min -5 and max 5 both cast to true while 0 casts to false)
   *  - narrowing numeric casts, which overflow and wrap around in non-ANSI mode (for ex, bigint to int)
   *  - timestamp to/from timestamp_ntz, which shifts across DST (for ex, a local time in the
   *    spring-forward gap shifts forward, so 02:30 maps after 03:00)
   */
  private[dataskipping] def preservesOrder(from: DataType, to: DataType): Boolean =
    (from, to) match {
      // NOTE: On Spark 4 StringType equality includes the collation and the char/varchar length
      //       constraint, so a collation-changing cast is not an identity (for ex, "B" < "a" in
      //       UTF8_BINARY but "a" < "B" in UTF8_LCASE). On Spark 3 StringType is a singleton
      case _ if from == to => true

      // Numeric
      // NOTE: Cast.canUpCast rejects decimal to float/double as lossy, but rounding is monotonic
      //       and every decimal fits within float range. Spark's type coercion inserts this cast
      //       when a decimal column is compared with a float/double (for ex, `price > 99.5D`)
      case (_: DecimalType, FloatType | DoubleType) => true
      // NOTE: Narrowing numeric casts (for ex, bigint to int) overflow and wrap around in
      //       non-ANSI mode, breaking ordering; only up-casts are guaranteed order-preserving.
      //       This is conservative (for ex, double to float rounds monotonically but is
      //       rejected), which only costs pruning opportunity, never correctness
      case (fromNum: NumericType, toNum: NumericType) => Cast.canUpCast(fromNum, toNum)
      // NOTE: false < true maps to 0 < 1. Decimal targets are excluded since, for ex,
      //       DECIMAL(1, 1) cannot hold 1 and overflows into NULL
      case (BooleanType, ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType) => true

      // Date and time
      // NOTE: Timestamps cast to numbers through their epoch seconds, which only fit bigint and
      //       floating-point targets; narrower integral targets wrap around and decimals overflow
      //       into NULL in non-ANSI mode
      case (TimestampType, LongType | FloatType | DoubleType) => true
      // NOTE: A date maps to the start of that day, which increases with the date
      case (DateType, TimestampType | TimestampNTZType) => true
      // NOTE: Truncating to the (local) date is non-decreasing, also across a DST fall-back since
      //       the date does not repeat
      case (TimestampType | TimestampNTZType, DateType) => true
      // NOTE: Not order-preserving in general (for ex, "2022-1-5" > "2022-01-10" as strings, but parses
      //       to an earlier date), but kept so that expression indexes on to_date/to_timestamp, which the
      //       optimizer rewrites into this cast, keep pruning. TODO #19481
      case (StringType, DateType | TimestampType | TimestampNTZType) => true

      // Binary and strings
      // NOTE: The default (UTF8_BINARY) string compares by its bytes, same as binary. Matching the
      //       StringType companion excludes collated strings on Spark 4
      case (StringType, BinaryType) => true
      case (BinaryType, StringType) => true

      case _ => false
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
