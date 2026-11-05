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
 * Shared guard: none; any time zone operand is accepted today.
 *
 * NOTE: The offset jumps at DST transitions, so the shift is not monotonic across a fall-back (#20449)
 */
object TimeZoneShiftRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    case FromUTCTimestamp(timestamp, _) => Seq(Step(term, timestamp, Increasing, fileGuard = None, partial = false))
    case ToUTCTimestamp(timestamp, _) => Seq(Step(term, timestamp, Increasing, fileGuard = None, partial = false))

    case _ => Nil
  }
}
