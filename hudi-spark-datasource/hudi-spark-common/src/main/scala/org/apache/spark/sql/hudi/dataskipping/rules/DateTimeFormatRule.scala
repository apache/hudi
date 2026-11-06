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

import org.apache.spark.sql.catalyst.expressions.{And, DateFormatClass, Expression, FromUnixTime, GetTimestamp, GreaterThanOrEqual, LessThan, Literal, ToUnixTimestamp, UnixTimestamp}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step, TermRange}
import org.apache.spark.sql.types.{DataType, DateType, LongType, StringType, TimestampNTZType, TimestampType}

import java.time.{LocalDate, ZoneOffset}
import java.util.concurrent.TimeUnit

import scala.util.Try

/**
 * Date/time formatting and parsing: `DateFormatClass`, `FromUnixTime`, `ToUnixTimestamp`,
 * `UnixTimestamp` and `GetTimestamp`.
 *
 * Shared guard: [[DateTimeOrdering]] (allowlisted literal format, non-LEGACY parser policy, and for a
 * time-of-day pattern a fixed-offset time zone), plus a per-file guard on the values the format or
 * parse is sound for. No parsing step without a time zone: the analyzer sets one on every resolved
 * expression.
 *
 *  - Formatting (`DateFormatClass`, `FromUnixTime`): the instant lies in years 0001 to 9999 (see
 *    [[formattableInstants]]).
 *  - Parsing a string (`ToUnixTimestamp`, `UnixTimestamp`, `GetTimestamp`): every string of the file
 *    starts with a digit (see [[digitFirstStrings]]). Parsing is `partial`: an unparseable string is
 *    NULL, or throws under ANSI or the EXCEPTION parser policy; `Step` turns a throwing bound into NULL,
 *    and a NULL bound keeps the file.
 *  - Converting a date or timestamp (the same three): increasing whatever the format, which is not
 *    used.
 *
 * NOTE: `ParseToDate` / `ParseToTimestamp` (`to_date(s, fmt)`, `to_timestamp(s, fmt)`) are not matched:
 *       they are RuntimeReplaceable, and the optimizer replaces them by `GetTimestamp` (under a cast to
 *       date for `to_date`) before filters reach data skipping
 */
object DateTimeFormatRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    case DateFormatClass(timestamp, format, timeZoneId)
      if dataTypeOf(timestamp).contains(TimestampType) && DateTimeOrdering.formatPreservesOrder(format, timeZoneId) =>
      Seq(Step(term, timestamp, Increasing, fileGuard = Some(formattableInstants), partial = false))
    case FromUnixTime(seconds, format, timeZoneId)
      if dataTypeOf(seconds).contains(LongType) && DateTimeOrdering.formatPreservesOrder(format, timeZoneId) =>
      Seq(Step(term, seconds, Increasing, fileGuard = Some(formattableSeconds), partial = false))

    // NOTE: `to_unix_timestamp` and `unix_timestamp` evaluate the same way, and take the same guards
    case ToUnixTimestamp(time, format, timeZoneId, _) =>
      parsingSteps(term, time, format, timeZoneId, zoneDependent = true)
    case UnixTimestamp(time, format, timeZoneId, _) =>
      parsingSteps(term, time, format, timeZoneId, zoneDependent = true)
    // NOTE: GetTimestamp has different constructor arities across Spark versions, so it is matched by
    //       type. Parsing into TIMESTAMP_NTZ reads the wall clock as is, so the zone does not matter
    case getTimestamp: GetTimestamp =>
      parsingSteps(term, getTimestamp.left, getTimestamp.right, getTimestamp.timeZoneId,
        zoneDependent = getTimestamp.dataType != TimestampNTZType)

    case _ => Nil
  }

  /**
   * `term` converts `input` to a timestamp, or to seconds since the epoch: a date or a timestamp
   * directly, a string by parsing it with `format`.
   */
  private def parsingSteps(term: Expression,
                           input: Expression,
                           format: Expression,
                           timeZoneId: Option[String],
                           zoneDependent: Boolean): Seq[Step] =
    if (timeZoneId.isEmpty) {
      Nil
    } else {
      dataTypeOf(input) match {
        case Some(DateType | TimestampType | TimestampNTZType) =>
          Seq(Step(term, input, Increasing, fileGuard = None, partial = false))
        // NOTE: Only the default collation: the bounds of a collated string are not its byte order
        case Some(StringType) if DateTimeOrdering.parsePreservesOrder(format, timeZoneId, zoneDependent) =>
          Seq(Step(term, input, Increasing, fileGuard = Some(digitFirstStrings), partial = true))
        case _ => Nil
      }
    }

  /**
   * Expression indexes skip the resolution check, so the input can be unresolved, and some unresolved
   * nodes throw on `dataType`. Such an input has no step.
   */
  private def dataTypeOf(e: Expression): Option[DataType] = Try(e.dataType).toOption

  // [0001-01-02, 9999-12-31) UTC: in every zone (offsets stay within a day), an instant of this window
  // formats with a 4-digit year
  private val formattableFromSeconds: Long = LocalDate.of(1, 1, 2).atStartOfDay(ZoneOffset.UTC).toEpochSecond
  private val formattableUntilSeconds: Long = LocalDate.of(9999, 12, 31).atStartOfDay(ZoneOffset.UTC).toEpochSecond

  /**
   * File guard of formatting a timestamp: the instants of the file lie in the formattable window.
   * Outside, years get a sign or a fifth digit (for ex, '+10000-01-01' and '-0001-06-01' sort
   * before '0001-01-01').
   */
  private def formattableInstants(r: TermRange): Expression =
    within(r, TimeUnit.SECONDS.toMicros(formattableFromSeconds), TimeUnit.SECONDS.toMicros(formattableUntilSeconds),
      TimestampType)

  /**
   * File guard of formatting seconds since the epoch: as [[formattableInstants]]. It also rules out
   * seconds whose conversion to microseconds overflows (for ex, Long.MaxValue formats as 1969-12-31).
   */
  private def formattableSeconds(r: TermRange): Expression =
    within(r, formattableFromSeconds, formattableUntilSeconds, LongType)

  private def within(r: TermRange, from: Long, until: Long, dataType: DataType): Expression =
    And(GreaterThanOrEqual(r.lo, Literal(from, dataType)), LessThan(r.hi, Literal(until, dataType)))

  /**
   * File guard of parsing a string: every string of the file starts with a digit, as the min and max
   * do. An allowlisted pattern starts with a 4-digit year, so this rules out the strings that parse
   * but sort apart from the others: signed years (for ex, '+10000-01-01' and '-0001-01-01' sort
   * before '0001-01-01').
   */
  private def digitFirstStrings(r: TermRange): Expression =
    And(GreaterThanOrEqual(r.lo, Literal("0")), LessThan(r.hi, Literal(":")))
}
