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

import org.apache.spark.sql.catalyst.expressions.{Add, And, BitwiseOr, DateAdd, DateDiff, DateSub, Divide, Expression, GreaterThanOrEqual, LessThanOrEqual, Literal, Multiply, ShiftLeft, ShiftRight}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step, TermRange}
import org.apache.spark.sql.types.{ByteType, DataType, Decimal, DoubleType, FloatType, IntegerType, LongType, ShortType}

import scala.util.Try

/**
 * Binary operators with the column on one side and a constant on the other:
 * `Add`, `Multiply`, `Divide`, `DateAdd`, `DateSub`, `DateDiff`, `ShiftLeft`, `ShiftRight`, `BitwiseOr`.
 *
 * Shared guard: the other operand is constant. Today that is enforced per function:
 *  - `Add`: float / double result and a finite constant, or integral result and a constant (either side)
 *  - `Multiply`: float / double or integral result, and the other side is a positive numeric literal
 *    (either side)
 *  - `Divide`: the divisor is a positive numeric literal (column as dividend only)
 *  - `ShiftLeft`: the shift amount is constant
 *  - `DateAdd`, `DateSub`, `DateDiff` (end only), `ShiftRight`, `BitwiseOr`: ANY second operand is
 *    accepted today, constant or not
 *
 * NOTE: A column-referencing other operand could reverse the ordering (for ex, A + A * -2 = -A), and a
 *       non-deterministic one is re-evaluated against the min/max bounds (for ex, A + rand())
 *
 * NOTE: Integral `Add`, `Multiply` and `ShiftLeft` wrap around on overflow in non-ANSI mode, so the
 *       max can map below the min (for ex, Long.MaxValue * 2 = -2), and throw in ANSI mode. Their
 *       steps carry a file guard that the file's range does not overflow (see [[IntegralType]]).
 *       Float / double results round monotonically and saturate to infinity instead. Decimal results
 *       overflow into NULL and are not matched (#20460)
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
    case Add(left, right, _) =>
      IntegralType.of(term).toSeq.flatMap { t =>
        Seq(
          integralConstant(right).filter(t.holds).map(c => guardedStep(term, left, t.addFits(c))),
          integralConstant(left).filter(t.holds).map(c => guardedStep(term, right, t.addFits(c)))
        ).flatten
      }

    case Multiply(left, right, _) if hasFloatingPointResult(term) =>
      Seq(
        when(isPositiveNumericLiteral(right))(Step(term, left, Increasing, fileGuard = None, partial = false)),
        when(isPositiveNumericLiteral(left))(Step(term, right, Increasing, fileGuard = None, partial = false))
      ).flatten
    case Multiply(left, right, _) =>
      IntegralType.of(term).toSeq.flatMap { t =>
        Seq(
          positiveIntegralLiteral(right).filter(t.holds).map(c => guardedStep(term, left, t.multiplyFits(c))),
          positiveIntegralLiteral(left).filter(t.holds).map(c => guardedStep(term, right, t.multiplyFits(c)))
        ).flatten
      }

    case Divide(dividend, divisor, _) if isPositiveNumericLiteral(divisor) =>
      Seq(Step(term, dividend, Increasing, fileGuard = None, partial = false))

    // NOTE: BitwiseOr is not monotonic (for ex, 1 < 2 but 1 | 2 = 3 > 2 | 2 = 2), #20449
    case BitwiseOr(left, right) =>
      Seq(
        Step(term, left, Increasing, fileGuard = None, partial = false),
        Step(term, right, Increasing, fileGuard = None, partial = false))

    // NOTE: Spark only shifts int and bigint values, by an int amount
    case ShiftLeft(value, amount) =>
      IntegralType.of(term).filter(t => t.dataType == IntegerType || t.dataType == LongType).toSeq.flatMap { t =>
        integralConstant(amount).map(n => guardedStep(term, value, t.shiftLeftFits(n)))
      }
    case ShiftRight(value, _) => Seq(Step(term, value, Increasing, fileGuard = None, partial = false))

    case _ => Nil
  }

  private def when(guard: Boolean)(step: => Step): Option[Step] = if (guard) Some(step) else None

  private def guardedStep(term: Expression, child: Expression, fileGuard: TermRange => Expression): Step =
    Step(term, child, Increasing, fileGuard = Some(fileGuard), partial = false)

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

  // NOTE: Foldable operands evaluate without input, which is how the optimizer folds them into literals
  private def integralConstant(expr: Expression): Option[Long] =
    if (expr.foldable) Try(expr.eval()).toOption.collect(integralValue) else None

  // NOTE: Literal only, as `isPositiveNumericLiteral`
  private def positiveIntegralLiteral(expr: Expression): Option[Long] = expr match {
    case Literal(value, _) => integralValue.lift(value).filter(_ > 0)
    case _ => None
  }

  private val integralValue: PartialFunction[Any, Long] = {
    case b: Byte => b.toLong
    case s: Short => s.toLong
    case i: Int => i.toLong
    case l: Long => l
  }

  /**
   * An integral result type and its limits. Past them, integral arithmetic wraps around in non-ANSI
   * mode and throws in ANSI mode. Each guard holds when no value of the file's range goes past them:
   * the step is increasing, so checking the min and the max covers every value in between. A file
   * that may overflow gets a NULL bound and is kept, and a file that holds the guard never evaluates
   * an overflowing bound, so ANSI does not throw.
   */
  private case class IntegralType(dataType: DataType, min: Long, max: Long, bits: Int) {

    /**
     * Whether `c` is a value of this type. Resolved operands always are; unresolved ones (expression
     * indexes) may not be, and their limits would not be either
     */
    def holds(c: Long): Boolean = min <= c && c <= max

    /** `x + c` does not overflow: x <= max - c for c >= 0, x >= min - c for c < 0. */
    def addFits(c: Long)(r: TermRange): Expression =
      if (c >= 0) LessThanOrEqual(r.hi, literal(max - c)) else GreaterThanOrEqual(r.lo, literal(min - c))

    /**
     * `x * c` does not overflow, for c > 0: min / c <= x <= max / c. Long division truncates towards
     * zero, so `min / c` rounds up and `max / c` rounds down, as needed.
     */
    def multiplyFits(c: Long)(r: TermRange): Expression = within(r, min / c, max / c)

    /**
     * `x << n` does not overflow: it is x * 2^s, with s the low bits of n (5 for int, 6 for bigint), as
     * the JVM shifts. Arithmetic right shifts round towards negative infinity, so `min >> s` and
     * `max >> s` are the exact limits.
     */
    def shiftLeftFits(n: Long)(r: TermRange): Expression = {
      val s = (n & (bits - 1)).toInt
      within(r, min >> s, max >> s)
    }

    private def within(r: TermRange, lo: Long, hi: Long): Expression =
      And(GreaterThanOrEqual(r.lo, literal(lo)), LessThanOrEqual(r.hi, literal(hi)))

    // NOTE: For a constant this type [[holds]], every limit above is within it, so the narrowing
    //       conversions below are exact
    private def literal(v: Long): Literal = dataType match {
      case ByteType => Literal(v.toByte, ByteType)
      case ShortType => Literal(v.toShort, ShortType)
      case IntegerType => Literal(v.toInt, IntegerType)
      case LongType => Literal(v, LongType)
    }
  }

  private object IntegralType {
    // NOTE: Expression indexes skip the resolution check, and some unresolved terms throw on `dataType`
    def of(term: Expression): Option[IntegralType] = Try(term.dataType).toOption.collect {
      case ByteType => IntegralType(ByteType, Byte.MinValue, Byte.MaxValue, 8)
      case ShortType => IntegralType(ShortType, Short.MinValue, Short.MaxValue, 16)
      case IntegerType => IntegralType(IntegerType, Int.MinValue, Int.MaxValue, 32)
      case LongType => IntegralType(LongType, Long.MinValue, Long.MaxValue, 64)
    }
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
