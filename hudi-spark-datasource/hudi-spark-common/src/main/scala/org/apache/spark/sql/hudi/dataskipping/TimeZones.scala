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

import org.apache.spark.sql.catalyst.util.DateTimeUtils

import java.time.Instant
import java.time.zone.{ZoneOffsetTransition, ZoneRules}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Time zone checks shared by the rules whose ordering depends on a zone's offset: `TimeZoneShiftRule`,
 * `DateTimeFormatRule` through `DateTimeOrdering`, and `CastRule` for a timestamp's local date.
 */
object TimeZones {

  /**
   * Whether the zone never changes its offset, neither for DST nor historically. It is resolved the way Spark
   * evaluates it, so for ex +8:00 is +08:00 and the short ID PST is America/Los_Angeles; an unknown zone is not
   * fixed.
   */
  def isFixedOffsetZone(timeZoneId: String): Boolean = rulesOf(timeZoneId).exists(_.isFixedOffset)

  private val localDateReversalsByZoneId = new ConcurrentHashMap[String, Option[Seq[Instant]]]()

  /**
   * Instants at which the local date runs backwards in the zone, resolved the way Spark evaluates it and
   * cached per zone id (see the overload over `ZoneRules`). None, so that the rendering takes no step, if
   * there is no zone (not analyzed), the zone is unknown, or its current rules keep doing it.
   */
  def localDateReversals(timeZoneId: Option[String]): Option[Seq[Instant]] =
    timeZoneId.flatMap { id =>
      localDateReversalsByZoneId.computeIfAbsent(id, _ => rulesOf(id).flatMap(rules => localDateReversals(rules)))
    }

  /**
   * Instants at which the local date runs backwards in a zone with the given rules, that is where its clocks fell back
   * across midnight, or None if its current rules keep doing it. Falling back at midnight itself (for ex, Asia/Beirut)
   * repeats the previous date without running backwards, since the local date before the transition is that of the
   * instant just before it. The instants are in chronological order, as `ZoneRules.getTransitions` lists them
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

  // The rules of the zone, resolved the way Spark evaluates it; None for an unknown zone
  private def rulesOf(timeZoneId: String): Option[ZoneRules] = Try(DateTimeUtils.getZoneId(timeZoneId).getRules).toOption
}
