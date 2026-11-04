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

import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, StartsWith}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral

/**
 * Stage 4 (Verdict): given the term's range in a file, could any row of the file pass `term OP value`?
 *
 * Invariant: TRUE whenever unsure. A negated op (NotEq / NotIn / NotStartsWith) only drops a file
 * whose every value fails, which is NOT the negation of the positive verdict.
 */
object Verdict {

  def apply(leaf: Leaf, r: Range): Expression = leaf.op match {
    case Op.Eq => mayContain(r, single(leaf))
    // Only a file holding nothing but `value` fails `term != value`
    case Op.NotEq => Not(onlyContains(r, single(leaf)))

    case Op.Lt => LessThan(r.lo, single(leaf))
    case Op.LtEq => LessThanOrEqual(r.lo, single(leaf))
    case Op.Gt => GreaterThan(r.hi, single(leaf))
    case Op.GtEq => GreaterThanOrEqual(r.hi, single(leaf))

    case Op.In => leaf.values.map(v => mayContain(r, v)).reduce(Or)
    case Op.NotIn => Not(leaf.values.map(v => onlyContains(r, v)).reduce(Or))

    // Bug #20430: checks `lo <= p AND hi >= p`, which drops a file whose min extends `p` (min 'abcd' > 'abc')
    case Op.StartsWith => mayContain(r, single(leaf))
    case Op.NotStartsWith =>
      val prefix = single(leaf)
      Not(And(StartsWith(r.lo, prefix), StartsWith(r.hi, prefix)))

    // A NULL null-count means the file holds no stats for the column, so it may hold NULLs
    case Op.IsNull => r.nulls match {
      case Some(nulls) => Or(IsNull(nulls), GreaterThan(nulls, Literal(0)))
      case None => TrueLiteral
    }
    case Op.IsNotNull => (r.nulls, r.rows) match {
      case (Some(nulls), Some(rows)) => Or(Or(IsNull(nulls), IsNull(rows)), LessThan(nulls, rows))
      case _ => TrueLiteral
    }
  }

  // Some value of the file may equal `v`: lo <= v <= hi
  private def mayContain(r: Range, v: Expression): Expression =
    And(LessThanOrEqual(r.lo, v), GreaterThanOrEqual(r.hi, v))

  // Every value of the file equals `v`: lo = v = hi
  private def onlyContains(r: Range, v: Expression): Expression =
    And(EqualTo(r.lo, v), EqualTo(r.hi, v))

  private def single(leaf: Leaf): Expression = leaf.values.head
}
