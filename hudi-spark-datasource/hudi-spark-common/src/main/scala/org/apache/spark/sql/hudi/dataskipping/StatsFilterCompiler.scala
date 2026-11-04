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

import org.apache.spark.sql.catalyst.expressions.{And, Coalesce, Expression, Not, Or}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral

/**
 * Compiles a filter on the data table into a filter on the stats table that answers, per file,
 * "could any row in this file pass?". Four stages:
 *
 *  1. Shape (here): how conditions combine (AND / OR / NOT).
 *  2. [[Leaf]]: the canonical `term OP constant`.
 *  3. [[TermRange]]: what values the term can take in a file, from a [[RangeSource]] and [[RangeRules]].
 *  4. [[Verdict]]: given the range, the boolean check per OP.
 *
 * Invariants:
 *  - Keep on unknown: anything not understood compiles to TRUE (keep every file), never to FALSE.
 *  - NOT never wraps a verdict. NOT over AND / OR is pushed down here; below that, NOT only reaches
 *    a leaf as NotEq / NotIn / NotStartsWith, and any other NOT is TRUE.
 *  - Compile only: the result is an expression over the stats table; Spark evaluates it.
 */
object StatsFilterCompiler {

  /**
   * @param strictResolution when true, an unresolved node (at any level, AND / OR included) compiles
   *                         to TRUE. False for expression indexes
   */
  def compile(filter: Expression, source: RangeSource, strictResolution: Boolean): Expression = filter match {
    case e if strictResolution && !e.resolved => TrueLiteral

    // Stage 1 (Shape). An OR keeps a file if either side may hold a match, so one unknown side makes
    // the whole OR unknown
    case Or(left, right) =>
      val l = compile(left, source, strictResolution)
      val r = compile(right, source, strictResolution)
      if (l.equals(TrueLiteral) || r.equals(TrueLiteral)) TrueLiteral else Or(l, r)

    // An AND keeps a file only if both sides may; an unknown side is dropped
    case And(left, right) =>
      val l = compile(left, source, strictResolution)
      val r = compile(right, source, strictResolution)
      if (l.equals(TrueLiteral)) r else if (r.equals(TrueLiteral)) l else And(l, r)

    // De Morgan, so that NOT reaches the leaves
    case Not(And(left, right)) => compile(Or(Not(left), Not(right)), source, strictResolution)
    case Not(Or(left, right)) => compile(And(Not(left), Not(right)), source, strictResolution)

    case leaf => compileLeaf(leaf, source)
  }

  // Stages 2 to 4, for one leaf. A verdict over a date rendering also keeps the files spanning a local date
  // reversal ([[LocalDateReversalGuard]])
  private def compileLeaf(e: Expression, source: RangeSource): Expression =
    Leaf.parse(e)
      .flatMap(l => TermRange.derive(l.term, source).flatMap(r =>
        LocalDateReversalGuard(e, source, Verdict(l, r)).map(keepOnNull(_, r))))
      .getOrElse(TrueLiteral)

  /**
   * A NULL verdict drops the file. Where a NULL bound means unknown (a partial or guarded step
   * derived the range, see [[TermRange]]), the verdict must keep the file instead: here and only
   * here. A column or a total, unguarded chain keeps the NULL verdict, which drops a file of only
   * NULLs (no row of it passes `term OP value`)
   */
  private def keepOnNull(verdict: Expression, r: TermRange): Expression =
    if (r.nullBoundIsUnknown) Coalesce(Seq(verdict, TrueLiteral)) else verdict
}
