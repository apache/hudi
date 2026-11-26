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

import org.apache.spark.sql.catalyst.expressions.{And, Expression, GreaterThanOrEqual, LessThan, Literal, Not, Or}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.hudi.dataskipping.Step.Guard
import org.apache.spark.sql.hudi.dataskipping.TimeZones
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import java.time.Instant

/**
 * Shared guards of [[DateTimeFormatRule]], and of [[CastRule]] for a timestamp's local date: when does
 * turning a timestamp into text (formatting) or text into a timestamp (parsing) preserve ordering?
 *
 *  - Pattern: the format is a literal on the allowlist below. Every allowlisted pattern is year-first
 *    and fixed-width, so the lexicographic order of the strings is the chronological order of the
 *    values they denote, within years 0001 to 9999 (outside, the year gets a sign or a fifth digit).
 *    Parsing also refuses a pattern with a quoted letter (for ex, 'T'): the parser matches it in any
 *    case, and the cases sort apart ('2022-03-05t09:00:00' parses, and sorts after
 *    '2022-03-05T23:00:00'). Formatting always writes the letter as quoted.
 *  - Policy: `spark.sql.legacy.timeParserPolicy` is not LEGACY. LEGACY parses with SimpleDateFormat,
 *    which is lenient (for ex, '2022-3-6', '2022-03-06x' and ' 2022-03-06' all parse with
 *    'yyyy-MM-dd'), so parseable strings are no longer fixed-width.
 *  - Zone: a time-of-day pattern needs a fixed-offset session time zone, both to format and to parse.
 *    Across a fall-back, local time runs backwards, so a later instant can format before an earlier
 *    one (for ex, in America/New_York 05:30Z formats as '01:30' EDT, but the later 06:10Z as '01:10'
 *    EST). Across a spring-forward gap, a wall clock inside the gap is shifted forward past later ones
 *    (for ex, in America/Los_Angeles '2022-03-13 02:30:00' parses after '2022-03-13 03:05:00'). A
 *    date-only parse is monotone in every zone (a gap shifts a start of day by less than a day).
 *  - Zone, a timestamp's local date (formatting, and `CAST(ts AS DATE)`): the local date runs
 *    backwards where the clocks fell back across midnight (for ex, America/St_Johns went from 00:01
 *    on 2009-11-01 back to 23:01 on 2009-10-31). A file spanning such an instant takes no bounds (see
 *    [[noLocalDateReversal]]), and a zone whose current rules keep doing it takes no step (see
 *    `TimeZones.localDateReversals`).
 */
object DateTimeOrdering {

  /**
   * An allowlisted format pattern.
   *
   * @param hasTimeOfDay     it holds a field finer than the day
   * @param hasLetterLiteral it holds a quoted letter (for ex, 'T'), which parsing matches in any case
   */
  case class Pattern(hasTimeOfDay: Boolean, hasLetterLiteral: Boolean)

  // NOTE: Year-first, fixed-width patterns only. Day-first or month-first patterns (for ex,
  //       'dd-MM-yyyy', 'MM/dd/yyyy'), unpadded fields (for ex, 'yyyy-M-d'), and text fields (for ex,
  //       'EEE', 'MMM') do not sort chronologically. 'SSS' parses 1 to 3 digits, whose order as a
  //       decimal fraction matches their lexicographic order
  private val datePatterns: Set[String] = Set(
    "yyyy",
    "yyyy-MM",
    "yyyy-MM-dd",
    "yyyyMMdd",
    "yyyy/MM/dd")

  private val dateTimePatterns: Set[String] = Set(
    "yyyy-MM-dd HH",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyyMMddHHmmss",
    "yyyy/MM/dd HH:mm:ss")

  /**
   * The allowlisted pattern of a format operand. Only literals: a default format (for ex, the one of
   * `unix_timestamp(s)`) is a literal too.
   */
  def pattern(format: Expression): Option[Pattern] = format match {
    case Literal(value: UTF8String, _: StringType) =>
      val p = value.toString
      if (datePatterns.contains(p)) {
        Some(Pattern(hasTimeOfDay = false, hasLetterLiteral = letterLiteral.findFirstIn(p).isDefined))
      } else if (dateTimePatterns.contains(p)) {
        Some(Pattern(hasTimeOfDay = true, hasLetterLiteral = letterLiteral.findFirstIn(p).isDefined))
      } else {
        None
      }
    case _ => None
  }

  // A quoted literal holding a letter, for ex 'T'
  private val letterLiteral = "'[^']*[A-Za-z][^']*'".r

  /**
   * Read when the rule runs, i.e. from the session that compiles the filter, which is the one that
   * evaluates it.
   */
  def isLegacyTimeParserPolicy: Boolean =
    // NOTE: An enum conf from Spark 4.1 on, a string conf before, so it is compared as a string
    "LEGACY".equalsIgnoreCase(SQLConf.get.getConf(SQLConf.LEGACY_TIME_PARSER_POLICY).toString)

  /** Whether formatting with `format` in `timeZoneId` preserves ordering (see the object scaladoc). */
  def formatPreservesOrder(format: Expression, timeZoneId: Option[String]): Boolean =
    !isLegacyTimeParserPolicy && pattern(format).exists(isZoneSafe(_, timeZoneId))

  /**
   * Whether parsing with `format` in `timeZoneId` preserves ordering (see the object scaladoc).
   *
   * @param zoneDependent false when the result does not depend on the zone (for ex, parsing into
   *                      TIMESTAMP_NTZ), which skips the zone guard. A time zone is still required
   */
  def parsePreservesOrder(format: Expression, timeZoneId: Option[String], zoneDependent: Boolean): Boolean =
    !isLegacyTimeParserPolicy && timeZoneId.isDefined && pattern(format).exists { p =>
      !p.hasLetterLiteral && (!zoneDependent || isZoneSafe(p, timeZoneId))
    }

  // Whether the session time zone cannot reorder what `p` formats or parses: `p` has no time of day, or
  // the zone never changes its offset (an unknown zone is not fixed)
  private def isZoneSafe(p: Pattern, timeZoneId: Option[String]): Boolean =
    !p.hasTimeOfDay || timeZoneId.exists(TimeZones.isFixedOffsetZone)

  /**
   * File guard of rendering a timestamp's local date: the file's range spans none of `instants`, since only
   * a file spanning one can hold rows that render outside of [f(lo), f(hi)]. None, i.e. unguarded, without
   * instants.
   *
   * @param instants  in chronological order, as `TimeZones.localDateReversals` lists them
   * @param toLiteral an instant as a value of the rendered input: [[asTimestamp]] or [[asEpochSeconds]]
   */
  def noLocalDateReversal(instants: Seq[Instant], toLiteral: Instant => Literal): Option[Guard] =
    if (instants.isEmpty) {
      None
    } else {
      val literals = instants.map(toLiteral)
      // NOTE: A file wholly before the first instant or from the last one on spans none, which settles most files
      //       (for ex, any after 2010) in two comparisons
      Some(r => Or(
        Or(LessThan(r.hi, literals.head), GreaterThanOrEqual(r.lo, literals.last)),
        literals.map(t => Not(And(LessThan(r.lo, t), GreaterThanOrEqual(r.hi, t)))).reduce[Expression](And(_, _))))
    }

  /** An instant as a timestamp, the input of `CAST(ts AS DATE)` and `date_format`. */
  val asTimestamp: Instant => Literal = instant => Literal(DateTimeUtils.instantToMicros(instant), TimestampType)

  /** An instant as seconds since the epoch, the input of `from_unixtime`. */
  val asEpochSeconds: Instant => Literal = instant => Literal(instant.getEpochSecond, LongType)
}
