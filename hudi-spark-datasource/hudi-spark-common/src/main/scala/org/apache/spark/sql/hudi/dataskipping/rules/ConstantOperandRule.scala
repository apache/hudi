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

import org.apache.spark.sql.catalyst.expressions.{Add, BitwiseOr, DateAdd, DateDiff, DateSub, Divide, Expression, Literal, Multiply, ShiftRight}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step}
import org.apache.spark.sql.types.{Decimal, DoubleType, FloatType}

import scala.util.Try

/**
 * Binary operators with the column on one side and a constant on the other:
 * `Add`, `Multiply`, `Divide`, `DateAdd`, `DateSub`, `DateDiff`, `ShiftRight`, `BitwiseOr`.
 *
 * Shared guard: the other operand is constant. Today that is enforced per function:
 *  - `Add`: float / double result, and the other side is a finite constant (either side)
 *  - `Multiply`: float / double result, and the other side is a positive numeric literal (either side)
 *  - `Divide`: the divisor is a positive numeric literal (column as dividend only)
 *  - `DateAdd`, `DateSub`, `DateDiff` (end only), `ShiftRight`, `BitwiseOr`: ANY second operand is
 *    accepted today, constant or not
 *
 * NOTE: A column-referencing other operand could reverse the ordering (for ex, A + A * -2 = -A), and a
 *       non-deterministic one is re-evaluated against the min/max bounds (for ex, A + rand())
 *
 * NOTE: Integral `Add` and `Multiply` wrap around on overflow in non-ANSI mode, so the max can map
 *       below the min (for ex, Long.MaxValue * 2 = -2), and decimal results overflow into NULL.
 *       Float / double results round monotonically and saturate to infinity instead. `ShiftLeft`
 *       is integral only, so it is not matched at all (#20460)
 */
object ConstantOperandRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    case DateAdd(start, _) => Seq(Step(term, start, Increasing, fileGuard = None, partial = false))
    case DateSub(start, _) => Seq(Step(term, start, Increasing, fileGuard = None, partial = false))
    // NOTE: DateDiff(end, start) is end - start, so it reverses the ordering of its start date,
    //       which hence must not be matched
    case DateDiff(end, _) => Seq(Step(term, end, Increasing, fileGuard = None, partial = false))

    case Add(left, right, _) if hasFloatingPointResult(term) =>
      Seq(
        when(isFiniteConstant(right))(Step(term, left, Increasing, fileGuard = None, partial = false)),
        when(isFiniteConstant(left))(Step(term, right, Increasing, fileGuard = None, partial = false))
      ).flatten
    case Multiply(left, right, _) if hasFloatingPointResult(term) =>
      Seq(
        when(isPositiveNumericLiteral(right))(Step(term, left, Increasing, fileGuard = None, partial = false)),
        when(isPositiveNumericLiteral(left))(Step(term, right, Increasing, fileGuard = None, partial = false))
      ).flatten
    case Divide(dividend, divisor, _) if isPositiveNumericLiteral(divisor) =>
      Seq(Step(term, dividend, Increasing, fileGuard = None, partial = false))

    // NOTE: BitwiseOr is not monotonic (for ex, 1 < 2 but 1 | 2 = 3 > 2 | 2 = 2), #20449
    case BitwiseOr(left, right) =>
      Seq(
        Step(term, left, Increasing, fileGuard = None, partial = false),
        Step(term, right, Increasing, fileGuard = None, partial = false))

    case ShiftRight(value, _) => Seq(Step(term, value, Increasing, fileGuard = None, partial = false))

    case _ => Nil
  }

  private def when(guard: Boolean)(step: => Step): Option[Step] = if (guard) Some(step) else None

  // NOTE: Expression indexes skip the resolution check, and some unresolved terms throw on `dataType`
  private def hasFloatingPointResult(term: Expression): Boolean = Try(term.dataType).toOption.exists {
    case FloatType | DoubleType => true
    case _ => false
  }

  // An infinite addend maps the opposite infinity to NaN, which sorts above every number (for ex,
  // -Infinity + Infinity = NaN > 1 + Infinity). Foldable operands evaluate without input, which is
  // how the optimizer folds them into literals
  private def isFiniteConstant(expr: Expression): Boolean =
    expr.foldable && Try(expr.eval()).toOption.exists {
      case f: Float => !f.isInfinite && !f.isNaN
      case d: Double => !d.isInfinite && !d.isNaN
      case _ => false
    }

  // Negative factors reverse the ordering and zero collapses it. Infinite factors can yield NaN
  // (0 * Infinity, Infinity / Infinity), which sorts above every number. Non-literal factors are
  // not matched since the optimizer folds constant factors to literals before data skipping runs
  private def isPositiveNumericLiteral(expr: Expression): Boolean = expr match {
    case Literal(value, _) => value match {
      case b: Byte => b > 0
      case s: Short => s > 0
      case i: Int => i > 0
      case l: Long => l > 0
      case f: Float => f > 0 && !f.isInfinite
      case d: Double => d > 0 && !d.isInfinite
      case dec: Decimal => dec.toBigDecimal.signum > 0
      case _ => false
    }
    case _ => false
  }
}
