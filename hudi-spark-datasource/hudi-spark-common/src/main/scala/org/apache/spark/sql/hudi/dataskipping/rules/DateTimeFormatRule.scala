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

import org.apache.hudi.SparkAdapterSupport.sparkAdapter

import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.catalyst.expressions.{DateFormatClass, Expression, FromUnixTime, ToUnixTimestamp}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step}

/**
 * Date/time formatting and parsing: `DateFormatClass`, `FromUnixTime`, `ToUnixTimestamp`, plus the
 * Spark-version-specific parsing shapes (`ParseToDate`, `ParseToTimestamp`) through
 * `HoodieCatalystExpressionUtils.unapplyDateParsingChild`.
 *
 * Shared guard: none; ANY format is accepted today, though most (for ex, 'dd-MM-yyyy') do not
 * preserve ordering (#19481). Parsing is `partial`: unparseable input yields NULL.
 */
object DateTimeFormatRule extends RangeRule {

  private lazy val exprUtils: HoodieCatalystExpressionUtils = sparkAdapter.getCatalystExpressionUtils

  override def steps(term: Expression): Seq[Step] = term match {
    case DateFormatClass(timestamp, _, _) => Seq(Step(term, timestamp, Increasing, fileGuard = None, partial = false))
    case FromUnixTime(seconds, _, _) => Seq(Step(term, seconds, Increasing, fileGuard = None, partial = false))
    case ToUnixTimestamp(time, _, _, _) => Seq(Step(term, time, Increasing, fileGuard = None, partial = true))

    // Date/time parsing expressions whose shapes are Spark-version-specific
    case _ => exprUtils.unapplyDateParsingChild(term) match {
      case Some(child) => Seq(Step(term, child, Increasing, fileGuard = None, partial = true))
      case None => Nil
    }
  }
}
