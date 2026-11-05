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

import org.apache.spark.sql.catalyst.expressions.{Expression, FromUTCTimestamp, ToUTCTimestamp}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step}

/**
 * Time zone shifts: `FromUTCTimestamp`, `ToUTCTimestamp`.
 *
 * Shared guard: the time zone operand is a literal whose wall clock does not reorder the shift
 * ([[DateTimeOrdering]]):
 *  - `FromUTCTimestamp` reads the wall clock of the zone at the instant, which repeats across a
 *    fall-back (for ex, in America/Los_Angeles 08:50Z shifts to 01:50 and 09:10Z to 01:10): the zone
 *    must have no overlap.
 *  - `ToUTCTimestamp` reads the timestamp as a wall clock of the zone, and one inside a spring-forward
 *    gap is shifted forward past later ones: the zone must have no gap.
 */
object TimeZoneShiftRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    case FromUTCTimestamp(timestamp, zone) if DateTimeOrdering.isZoneWithoutOverlap(zone) =>
      Seq(Step(term, timestamp, Increasing, fileGuard = None, partial = false))
    case ToUTCTimestamp(timestamp, zone) if DateTimeOrdering.isZoneWithoutGap(zone) =>
      Seq(Step(term, timestamp, Increasing, fileGuard = None, partial = false))

    case _ => Nil
  }
}
