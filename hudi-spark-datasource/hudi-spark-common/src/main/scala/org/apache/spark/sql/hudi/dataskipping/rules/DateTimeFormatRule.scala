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

import org.apache.spark.sql.catalyst.expressions.{DateFormatClass, Expression, FromUnixTime, ParseToDate, ParseToTimestamp, ToUnixTimestamp}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step}

/**
 * Date/time formatting and parsing: `DateFormatClass`, `FromUnixTime`, `ToUnixTimestamp`,
 * `ParseToDate` and `ParseToTimestamp`. The two parsing expressions have different constructor
 * arities across Spark versions, so they are matched by type and step into `left` (the parsed
 * input, the first constructor field on every supported version) rather than by extractor.
 *
 * Shared guard: none; ANY format is accepted today, though most (for ex, 'dd-MM-yyyy') do not
 * preserve ordering (#19481). Parsing is `partial`: unparseable input yields NULL.
 */
object DateTimeFormatRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    case DateFormatClass(timestamp, _, _) => Seq(Step(term, timestamp, Increasing, fileGuard = None, partial = false))
    case FromUnixTime(seconds, _, _) => Seq(Step(term, seconds, Increasing, fileGuard = None, partial = false))
    case ToUnixTimestamp(time, _, _, _) => Seq(Step(term, time, Increasing, fileGuard = None, partial = true))
    case p: ParseToDate => Seq(Step(term, p.left, Increasing, fileGuard = None, partial = true))
    case p: ParseToTimestamp => Seq(Step(term, p.left, Increasing, fileGuard = None, partial = true))
    case _ => Nil
  }
}
