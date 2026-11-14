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

import org.apache.spark.sql.catalyst.expressions.{Exp, Expm1, Expression, Log, Log10, Log1p, Log2}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step}

/**
 * Unary math functions that are strictly increasing over their domain:
 * `Exp`, `Expm1`, `Log`, `Log10`, `Log1p`, `Log2`.
 *
 * Shared guard: none. The log family is `partial`: it returns NULL outside its domain (for ex,
 * `log(-1)`), so a non-NULL column can map to a NULL term.
 */
object MonotonicFunctionRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    case Exp(child) => Seq(Step(term, child, Increasing, fileGuard = None, partial = false))
    case Expm1(child) => Seq(Step(term, child, Increasing, fileGuard = None, partial = false))

    case Log(child) => Seq(Step(term, child, Increasing, fileGuard = None, partial = true))
    case Log10(child) => Seq(Step(term, child, Increasing, fileGuard = None, partial = true))
    case Log1p(child) => Seq(Step(term, child, Increasing, fileGuard = None, partial = true))
    case Log2(child) => Seq(Step(term, child, Increasing, fileGuard = None, partial = true))

    case _ => Nil
  }
}
