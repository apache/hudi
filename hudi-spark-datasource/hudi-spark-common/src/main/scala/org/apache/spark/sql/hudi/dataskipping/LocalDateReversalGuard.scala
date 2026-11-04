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

package org.apache.spark.sql.hudi.dataskipping

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Cast, DateFormatClass, Expression, FromUnixTime, GreaterThanOrEqual, LessThan, Literal, Or, RuntimeReplaceable, TryEval}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, DateType, LongType, TimestampType}

import java.time.Instant
import java.time.zone.{ZoneOffsetTransition, ZoneRules}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Keeps the files whose [min, max] spans an instant at which the local date runs backwards, for the renderings of a
 * timestamp in a time zone (`CAST(ts AS DATE)`, `date_format` and `from_unixtime`): re-applying a date rendering to
 * the min/max stats assumes the local date never does. It does where the clocks fell back across midnight (for ex,
 * America/St_Johns went from 00:01 on 2009-11-01 back to 23:01 on 2009-10-31), and only a file spanning that instant
 * can hold rows that render outside of [f(min), f(max)].
 *
 * NOTE: Applied to the verdict of each leaf rather than as a `fileGuard` of the rendering's [[Step]]: until #19474
 *       keeps files on NULL verdicts, a guarded chain derives no range at all, which would end date pruning in every
 *       zone with such an instant (for ex, America/Phoenix, for its 1944 one)
 */
object LocalDateReversalGuard {

  /**
   * The verdict of `leaf`, also keeping the files that span a reversal of a date rendering in it. Returns None, so
   * that nothing is pruned, if the instants are not bounded (the zone's current rules keep doing it) or cannot be
   * compared with the rendered column
   */
  def apply(leaf: Expression, source: RangeSource, verdict: Expression): Option[Expression] = {
    // NOTE: A RuntimeReplaceable (for ex, to_date) renders through its replacement, which the optimizer
    //       substitutes before files are listed, but which an analyzed filter still hides
    val renderings = leaf.transform { case r: RuntimeReplaceable => r.replacement }.collect {
      case Cast(child, DateType, timeZoneId, _) if child.dataType == TimestampType => (child, timeZoneId)
      case DateFormatClass(child, _, timeZoneId) => (child, timeZoneId)
      case FromUnixTime(child, _, timeZoneId) => (child, timeZoneId)
    }
    // NOTE: Renderings of a constant (for ex, within the value compared against) have no bounds to check
    val spans: Seq[Option[Seq[Expression]]] = renderings.collect {
      case (child, timeZoneId) if child.references.nonEmpty =>
        timeZoneId.flatMap(cachedLocalDateReversals).flatMap { instants =>
          if (instants.isEmpty) {
            Some(Seq.empty)
          } else {
            for {
              toLiteral <- instantLiteral(child.dataType)
              attrRef <- child.references.collectFirst { case attrRef: AttributeReference => attrRef }
              range <- source.rangeOf(attrRef)
            } yield {
              // NOTE: Under ANSI the input can fail on a bound that the predicate itself never evaluates (for ex,
              //       an unparseable min of a string column under `f(col) >= v`, which only needs the max). Such a
              //       bound leaves the guard null, so the predicate alone decides, as it does without ANSI
              val minChild = TryEval(rebind(child, attrRef, range.lo))
              val maxChild = TryEval(rebind(child, attrRef, range.hi))
              instants.map(toLiteral).map(bound => And(LessThan(minChild, bound), GreaterThanOrEqual(maxChild, bound)))
            }
          }
        }
    }
    if (spans.contains(None)) None else Some(spans.flatten.flatten.foldLeft(verdict)(Or))
  }

  // `child` with the column replaced by one of its stats bounds
  private def rebind(child: Expression, column: AttributeReference, bound: Expression): Expression =
    child.transformDown {
      case attrRef: AttributeReference if attrRef.sameRef(column) => bound
    }

  // The rendered column holds the instant either as a timestamp or, for `from_unixtime`, as epoch seconds
  private def instantLiteral(dataType: DataType): Option[Instant => Literal] = dataType match {
    case TimestampType => Some(instant => Literal(DateTimeUtils.instantToMicros(instant), TimestampType))
    case LongType => Some(instant => Literal(instant.getEpochSecond, LongType))
    case _ => None
  }

  private val localDateReversalsByZoneId = new ConcurrentHashMap[String, Option[Seq[Instant]]]()

  // Resolves the zone the way Spark evaluates it. An unknown zone is not bounded
  private def cachedLocalDateReversals(zoneId: String): Option[Seq[Instant]] =
    localDateReversalsByZoneId.computeIfAbsent(zoneId,
      id => Try(DateTimeUtils.getZoneId(id).getRules).toOption.flatMap(localDateReversals))

  /**
   * Instants at which the local date runs backwards in a zone with the given rules, that is where its clocks fell back
   * across midnight, or None if its current rules keep doing it. Falling back at midnight itself (for ex, Asia/Beirut)
   * repeats the previous date without running backwards, since the local date before the transition is that of the
   * instant just before it
   */
  def localDateReversals(rules: ZoneRules): Option[Seq[Instant]] = {
    def isReversal(transition: ZoneOffsetTransition): Boolean = transition.isOverlap &&
      transition.getDateTimeAfter.toLocalDate.isBefore(transition.getDateTimeBefore.minusNanos(1).toLocalDate)
    // NOTE: A rule falls back at the same time of day every year, so a single year shows whether it reverses the date
    if (rules.getTransitionRules.asScala.exists(rule => isReversal(rule.createTransition(2000)))) {
      None
    } else {
      Some(rules.getTransitions.asScala.filter(isReversal).map(_.getInstant).toList)
    }
  }
}
