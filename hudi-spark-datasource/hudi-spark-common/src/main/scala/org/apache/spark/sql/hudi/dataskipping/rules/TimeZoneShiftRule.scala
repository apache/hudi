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

import org.apache.spark.sql.catalyst.expressions.{Expression, FromUTCTimestamp, Literal, ToUTCTimestamp}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step, TimeZones}

/**
 * Time zone shifts: `FromUTCTimestamp`, `ToUTCTimestamp`.
 *
 * Shared guard: [[isFixedOffsetTimeZone]], a literal time zone whose offset never changes.
 *
 * NOTE: Both shift by the zone's offset at that time, so they only keep the ordering for a zone whose offset never
 *       changes, DST or historical. Local time runs backwards when the offset drops (for ex, 05:30Z is 01:30 EDT but
 *       the later 06:10Z is 01:10 EST), and a local time in the gap of a rising offset shifts past later ones (for ex,
 *       02:30 maps to 07:30Z but the later 03:00 to 07:00Z)
 */
object TimeZoneShiftRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    case FromUTCTimestamp(timestamp, timeZone) if isFixedOffsetTimeZone(timeZone) =>
      Seq(Step(term, timestamp, Increasing, fileGuard = None, partial = false))
    case ToUTCTimestamp(timestamp, timeZone) if isFixedOffsetTimeZone(timeZone) =>
      Seq(Step(term, timestamp, Increasing, fileGuard = None, partial = false))

    case _ => Nil
  }

  // Only a literal zone is matched, since the optimizer folds a constant zone to a literal before data
  // skipping runs; `TimeZones.isFixedOffsetZone` resolves it the way Spark evaluates it
  private def isFixedOffsetTimeZone(expr: Expression): Boolean = expr match {
    case Literal(timeZone, _) if timeZone != null => TimeZones.isFixedOffsetZone(timeZone.toString)
    case _ => false
  }
}
