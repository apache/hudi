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

import org.apache.spark.sql.catalyst.expressions.{And, Expression, If, Literal, TryEval}
import org.apache.spark.sql.hudi.dataskipping.rules.{CastRule, ConstantOperandRule, DateTimeFormatRule, MonotonicFunctionRule, PrefixRule, TimeZoneShiftRule}

/**
 * Stage 3 (Range), rules: how does a function of the child map the child's range?
 *
 * A [[Step]] says: `term` is a function of `child` with this [[Direction]], so the range of `term`
 * is the range of `child` pushed through it. Invariant: a rule only emits a step when its guard
 * holds for this `term`; a term no rule knows has no range (and the file is kept).
 *
 * Emitting a Step is a correctness claim: `term` is order-preserving in `child`.
 * <ul>
 *   <li>[[Increasing]]: a <= b implies f(a) <= f(b)</li>
 *   <li>[[Decreasing]]: a <= b implies f(a) >= f(b)</li>
 * </ul>
 * The claim must hold for every value the file's range can hold (or wherever the step's
 * `fileGuard` is true), except values for which `term` is undefined when the step is `partial`. If it
 * is false, data skipping drops files that have matching rows. When unsure, return Nil: that only
 * costs pruning, never correctness.
 */
sealed trait Direction

/**
 * Order-preserving (non-decreasing): a <= b implies f(a) <= f(b), so f(lo) is the low bound and
 * f(hi) the high bound. Every step today is increasing.
 */
case object Increasing extends Direction

/**
 * Order-reversing (non-increasing): a <= b implies f(a) >= f(b), so f(hi) is the low bound and
 * f(lo) the high bound. Unused today; negative factors would use it.
 *
 * NOTE: A chain with a decreasing step derives no range when the term also references the column
 *       off the chain; see [[TermRange.derive]]
 */
case object Decreasing extends Direction

/**
 * One rule application.
 *
 * @param term      the function application being ranged, for ex `x + 1`
 * @param child     the operand whose range is pushed through, for ex `x`
 * @param direction whether `term` grows or shrinks with `child`
 * @param fileGuard per-file condition under which the mapping holds (for ex, no overflow); outside it
 *                  the bounds become NULL, i.e. unknown, and [[StatsFilterCompiler]] keeps the file
 * @param partial   whether `term` can be undefined for a non-NULL `child` (for ex, `log` of a negative,
 *                  or a parse of an unparseable string). Spark reports that as NULL or, depending on
 *                  the session (for ex, ANSI), as an exception; the rebuilt bounds are wrapped in
 *                  [[TryEval]] so that both are a NULL bound, i.e. unknown, and the file is kept. The
 *                  null / value counts of the child no longer describe `term`
 */
case class Step(term: Expression,
                child: Expression,
                direction: Direction,
                fileGuard: Option[Step.Guard],
                partial: Boolean) {

  def apply(r: TermRange): TermRange = {
    val (lo, hi) = direction match {
      case Increasing => (r.lo, r.hi)
      case Decreasing => (r.hi, r.lo)
    }
    TermRange(
      lo = guarded(nullOnFailure(rebuild(lo)), r),
      hi = guarded(nullOnFailure(rebuild(hi)), r),
      nulls = if (partial) None else r.nulls,
      rows = if (partial) None else r.rows,
      nullBoundIsUnknown = r.nullBoundIsUnknown || partial || fileGuard.isDefined)
  }

  // NOTE: Uses `term.dataType` (same as the rebuilt bound's) since stats-table bounds are unresolved here
  private def guarded(bound: Expression, r: TermRange): Expression = fileGuard match {
    case Some(guard) => If(guard(r), bound, Literal(null, term.dataType))
    case None => bound
  }

  // NOTE: TryEval is in every supported Spark version, so no version-specific shape (for ex, a
  //       `failOnError = false` copy of each parse expression) is needed
  private def nullOnFailure(bound: Expression): Expression = if (partial) TryEval(bound) else bound

  /**
   * `term` with its `child` slot replaced by `x`. Version-specific shapes (for ex, `ParseToDate`)
   * may not hold the child as a direct child; then any matching sub-tree is replaced
   */
  private def rebuild(x: Expression): Expression =
    if (term.children.exists(_ fastEquals child)) {
      term.mapChildren(c => if (c fastEquals child) x else c)
    } else {
      term.transformDown { case c if c fastEquals child => x }
    }
}

object Step {

  /** A per-file condition over the range of the step's child, see `fileGuard` of [[Step]]. */
  type Guard = TermRange => Expression

  /** Both guards: the mapping holds where each one does. A missing guard leaves the other as is. */
  def andGuards(a: Option[Guard], b: Option[Guard]): Option[Guard] = (a, b) match {
    case (Some(x), Some(y)) => Some(r => And(x(r), y(r)))
    case _ => a.orElse(b)
  }
}

/**
 * A family of functions sharing one guard. Not sealed: families live one per file under `rules`.
 *
 * `steps` returns every step whose guard holds, in operand order; usually zero or one, two
 * for operators accepted on either side (for ex, `a | 1` and `1 | a`). The first whose child derives
 * a range wins, as in a Scala match falling through to its next case.
 */
trait RangeRule {
  def steps(term: Expression): Seq[Step]
}

object RangeRules {

  // NOTE: Families match disjoint expression classes, so their order does not change the outcome
  val all: Seq[RangeRule] = Seq(
    DateTimeFormatRule,
    TimeZoneShiftRule,
    PrefixRule,
    ConstantOperandRule,
    MonotonicFunctionRule,
    CastRule)
}
