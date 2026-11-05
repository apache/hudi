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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import java.time.zone.ZoneOffsetTransition
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Shared guards of [[DateTimeFormatRule]] and [[TimeZoneShiftRule]]: when does turning a timestamp
 * into local wall-clock text (formatting) or local wall-clock text into a timestamp (parsing) preserve
 * ordering? All of the following must hold:
 *
 *  - Pattern: the format is a literal on the allowlist below. Every allowlisted pattern is year-first
 *    and fixed-width, so the lexicographic order of the strings is the chronological order of the
 *    values they denote, within years 0001 to 9999 (outside, the year gets a sign or a fifth digit).
 *  - Policy: `spark.sql.legacy.timeParserPolicy` is not LEGACY. LEGACY parses with SimpleDateFormat,
 *    which is lenient (for ex, '2022-3-6', '2022-03-06x' and ' 2022-03-06' all parse with
 *    'yyyy-MM-dd'), so parseable strings are no longer fixed-width.
 *  - Zone: the wall clock of the time zone never goes back (overlap) or jumps (gap) in a way that
 *    reorders values. Formatting needs no overlap with a time-of-day pattern (01:50 is followed by
 *    01:10 across a fall-back), and no overlap that turns the local date back with a date-only
 *    pattern (for ex, America/Goose_Bay fell back from 00:01 to 23:01 the day before). Parsing needs
 *    no gap with a time-of-day pattern (02:30 is shifted past 03:05 across a spring-forward), and
 *    nothing with a date-only pattern (a gap shifts a start of day by at most a day). A parsed time in
 *    an overlap resolves to its earlier instant, which preserves ordering.
 *
 * Zones are judged from java.time's rules, the ones Spark formats and parses with: every historical
 * transition and the recurring rules still in force. A zone that ever had DST is refused, however
 * long ago it ended (for ex, Asia/Shanghai); fixed offsets and UTC pass.
 */
object DateTimeOrdering {

  /** An allowlisted format pattern; `hasTimeOfDay` when it holds a field finer than the day. */
  case class Pattern(hasTimeOfDay: Boolean)

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
        Some(Pattern(hasTimeOfDay = false))
      } else if (dateTimePatterns.contains(p)) {
        Some(Pattern(hasTimeOfDay = true))
      } else {
        None
      }
    case _ => None
  }

  /**
   * Read when the rule runs, i.e. from the session that compiles the filter, which is the one that
   * evaluates it.
   */
  def isLegacyTimeParserPolicy: Boolean =
    "LEGACY".equalsIgnoreCase(SQLConf.get.getConf(SQLConf.LEGACY_TIME_PARSER_POLICY))

  /** Whether formatting with `format` in `timeZoneId` preserves ordering (see the object scaladoc). */
  def formatPreservesOrder(format: Expression, timeZoneId: Option[String]): Boolean =
    !isLegacyTimeParserPolicy && pattern(format).exists { p =>
      zoneTraits(timeZoneId).exists(zone => if (p.hasTimeOfDay) !zone.hasOverlap else !zone.hasDateTurningBackOverlap)
    }

  /**
   * Whether parsing with `format` in `timeZoneId` preserves ordering (see the object scaladoc).
   *
   * @param zoneDependent false when the result does not depend on the zone (for ex, parsing into
   *                      TIMESTAMP_NTZ), which skips the zone guard. A time zone is still required
   */
  def parsePreservesOrder(format: Expression, timeZoneId: Option[String], zoneDependent: Boolean): Boolean =
    !isLegacyTimeParserPolicy && pattern(format).exists { p =>
      zoneTraits(timeZoneId).exists(zone => !zoneDependent || !p.hasTimeOfDay || !zone.hasGap)
    }

  /** Whether `zone` is a literal time zone whose wall clock never goes back. */
  def isZoneWithoutOverlap(zone: Expression): Boolean = zoneTraits(literalZone(zone)).exists(!_.hasOverlap)

  /** Whether `zone` is a literal time zone whose wall clock never jumps forward. */
  def isZoneWithoutGap(zone: Expression): Boolean = zoneTraits(literalZone(zone)).exists(!_.hasGap)

  private def literalZone(zone: Expression): Option[String] = zone match {
    case Literal(value: UTF8String, _: StringType) => Some(value.toString)
    case _ => None
  }

  /**
   * The transitions a zone ever had or will have.
   *
   * @param hasDateTurningBackOverlap an overlap starts on a later local date than the instant before it
   */
  private case class ZoneTraits(hasGap: Boolean, hasOverlap: Boolean, hasDateTurningBackOverlap: Boolean)

  // NOTE: None caches an unknown zone id, which no rule accepts
  private val zoneTraitsCache = new ConcurrentHashMap[String, Option[ZoneTraits]]()

  /** None for a missing or unknown zone id. */
  private def zoneTraits(timeZoneId: Option[String]): Option[ZoneTraits] =
    timeZoneId.flatMap(id => zoneTraitsCache.computeIfAbsent(id, classify(_)))

  private def classify(timeZoneId: String): Option[ZoneTraits] =
    Try(DateTimeUtils.getZoneId(timeZoneId)).toOption.map { zoneId =>
      val rules = zoneId.getRules
      // NOTE: A recurring rule shifts at the same local time and by the same offsets every year, so one
      //       year stands for all of them
      val transitions: Seq[ZoneOffsetTransition] =
        rules.getTransitions.asScala.toSeq ++ rules.getTransitionRules.asScala.map(_.createTransition(2000))
      ZoneTraits(
        hasGap = transitions.exists(_.isGap),
        hasOverlap = transitions.exists(_.isOverlap),
        hasDateTurningBackOverlap = transitions.exists(turnsDateBack))
    }

  /**
   * Whether the local date goes back at an overlap. Just before the transition the wall clock reads
   * a moment before `dateTimeBefore`; from it on, `dateTimeAfter`. A fall-back from exactly 00:00 to
   * 23:00 does not turn the date back: the clock read 23:59:59 of the same day before.
   */
  private def turnsDateBack(t: ZoneOffsetTransition): Boolean =
    t.isOverlap && t.getDateTimeAfter.toLocalDate.isBefore(t.getDateTimeBefore.minusNanos(1).toLocalDate)
}
