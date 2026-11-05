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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, If, In, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, StartsWith, UnaryMinus}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Unit tests of the stages on their own: [[Leaf.parse]], [[Verdict]], [[Step.apply]] and
 * [[TermRange.derive]] over a stub rule table. End-to-end outputs are pinned by
 * [[TestDataSkippingGoldenFilters]].
 */
class TestDataSkippingStages {

  private val x = AttributeReference("x", DoubleType)()
  private val y = AttributeReference("y", DoubleType)()
  private val ten = Literal(10.0d)

  // A range whose bounds and counts are markers, to see where each one lands
  private val markers = TermRange(
    lo = UnresolvedAttribute("lo"),
    hi = UnresolvedAttribute("hi"),
    nulls = Some(UnresolvedAttribute("nulls")),
    rows = Some(UnresolvedAttribute("rows")))

  private val source = ColumnStatsSource(Seq("x"))
  private lazy val xRange = source.rangeOf(x).get

  // ---------------------------------------------------------------------------------------------
  // Stage 2: Leaf
  // ---------------------------------------------------------------------------------------------

  @Test
  def testLeafPutsTheTermOnTheLeft(): Unit = {
    val term = Add(x, Literal(1.0d))
    assertEquals(Some(Leaf(term, Op.Gt, Seq(ten))), Leaf.parse(LessThan(ten, term)))
    assertEquals(Some(Leaf(term, Op.GtEq, Seq(ten))), Leaf.parse(LessThanOrEqual(ten, term)))
    assertEquals(Some(Leaf(term, Op.Lt, Seq(ten))), Leaf.parse(GreaterThan(ten, term)))
    assertEquals(Some(Leaf(term, Op.LtEq, Seq(ten))), Leaf.parse(GreaterThanOrEqual(ten, term)))
    assertEquals(Some(Leaf(term, Op.Eq, Seq(ten))), Leaf.parse(EqualTo(ten, term)))
    assertEquals(Some(Leaf(term, Op.NotEq, Seq(ten))), Leaf.parse(Not(EqualTo(ten, term))))
    // Already canonical
    assertEquals(Some(Leaf(term, Op.Lt, Seq(ten))), Leaf.parse(LessThan(term, ten)))
  }

  @Test
  def testLeafRejectsColumnsOnBothSidesOrNone(): Unit = {
    assertEquals(None, Leaf.parse(LessThan(x, y)))
    assertEquals(None, Leaf.parse(LessThan(Add(x, y), ten)))
    assertEquals(None, Leaf.parse(LessThan(ten, Literal(1.0d))))
    assertEquals(None, Leaf.parse(In(x, Seq(ten, y))))
    // IS NULL only takes a bare column, and NOT is only understood over =, IN and STARTS WITH
    assertEquals(None, Leaf.parse(IsNull(Add(x, Literal(1.0d)))))
    assertEquals(None, Leaf.parse(Not(LessThan(x, ten))))
  }

  @Test
  def testLeafNullSafeEqualityToNullIsIsNull(): Unit = {
    assertEquals(Some(Leaf(x, Op.IsNull, Nil)), Leaf.parse(EqualNullSafe(x, Literal(null, DoubleType))))
    assertEquals(None, Leaf.parse(EqualNullSafe(x, ten)))
  }

  // ---------------------------------------------------------------------------------------------
  // Stage 4: Verdict
  // ---------------------------------------------------------------------------------------------

  @Test
  def testVerdictPerOp(): Unit = {
    val (lo, hi, nulls, rows) = (markers.lo, markers.hi, markers.nulls.get, markers.rows.get)
    val one = Literal(1.0d)
    val prefix = Literal("ab")
    def verdict(op: Op, values: Expression*): Expression = Verdict(Leaf(x, op, values), markers)

    assertEquals(And(LessThanOrEqual(lo, ten), GreaterThanOrEqual(hi, ten)), verdict(Op.Eq, ten))
    assertEquals(Not(And(EqualTo(lo, ten), EqualTo(hi, ten))), verdict(Op.NotEq, ten))
    assertEquals(LessThan(lo, ten), verdict(Op.Lt, ten))
    assertEquals(LessThanOrEqual(lo, ten), verdict(Op.LtEq, ten))
    assertEquals(GreaterThan(hi, ten), verdict(Op.Gt, ten))
    assertEquals(GreaterThanOrEqual(hi, ten), verdict(Op.GtEq, ten))
    assertEquals(
      Or(And(LessThanOrEqual(lo, ten), GreaterThanOrEqual(hi, ten)), And(LessThanOrEqual(lo, one), GreaterThanOrEqual(hi, one))),
      verdict(Op.In, ten, one))
    assertEquals(Not(Or(And(EqualTo(lo, ten), EqualTo(hi, ten)), And(EqualTo(lo, one), EqualTo(hi, one)))),
      verdict(Op.NotIn, ten, one))
    // Bug #20430, pinned as is: STARTS WITH checks lo <= p <= hi
    assertEquals(And(LessThanOrEqual(lo, prefix), GreaterThanOrEqual(hi, prefix)), verdict(Op.StartsWith, prefix))
    assertEquals(Not(And(StartsWith(lo, prefix), StartsWith(hi, prefix))), verdict(Op.NotStartsWith, prefix))
    assertEquals(Or(IsNull(nulls), GreaterThan(nulls, Literal(0))), verdict(Op.IsNull))
    assertEquals(Or(Or(IsNull(nulls), IsNull(rows)), LessThan(nulls, rows)), verdict(Op.IsNotNull))
  }

  @Test
  def testVerdictWithoutCountsKeepsTheFile(): Unit = {
    val noCounts = markers.copy(nulls = None, rows = None)
    assertEquals(TrueLiteral, Verdict(Leaf(x, Op.IsNull, Nil), noCounts))
    assertEquals(TrueLiteral, Verdict(Leaf(x, Op.IsNotNull, Nil), noCounts))
  }

  // ---------------------------------------------------------------------------------------------
  // Stage 3: Step and TermRange
  // ---------------------------------------------------------------------------------------------

  @Test
  def testIncreasingStepKeepsBoundsInPlace(): Unit = {
    val term = Add(x, Literal(1.0d))
    val step = Step(term, x, Increasing, fileGuard = None, partial = false)
    assertEquals(
      TermRange(Add(markers.lo, Literal(1.0d)), Add(markers.hi, Literal(1.0d)), markers.nulls, markers.rows),
      step(markers))
  }

  @Test
  def testDecreasingStepSwapsBounds(): Unit = {
    val step = Step(UnaryMinus(x), x, Decreasing, fileGuard = None, partial = false)
    assertEquals(TermRange(UnaryMinus(markers.hi), UnaryMinus(markers.lo), markers.nulls, markers.rows), step(markers))
  }

  @Test
  def testFileGuardNullsBothBoundsOutsideTheGuard(): Unit = {
    val term = Add(x, Literal(1.0d))
    val guard: TermRange => Expression = r => LessThan(r.hi, Literal(100.0d))
    val step = Step(term, x, Increasing, fileGuard = Some(guard), partial = false)
    val unknown = Literal(null, DoubleType)
    assertEquals(
      TermRange(
        lo = If(guard(markers), Add(markers.lo, Literal(1.0d)), unknown),
        hi = If(guard(markers), Add(markers.hi, Literal(1.0d)), unknown),
        nulls = markers.nulls,
        rows = markers.rows),
      step(markers))
  }

  @Test
  def testPartialStepDropsCounts(): Unit = {
    val step = Step(Abs(x), x, Increasing, fileGuard = None, partial = true)
    assertEquals(TermRange(Abs(markers.lo), Abs(markers.hi), None, None), step(markers))
  }

  /** `-child`, order-reversing. */
  private object NegateRule extends RangeRule {
    override def steps(term: Expression): Seq[Step] = term match {
      case negate: UnaryMinus => Seq(Step(term, negate.child, Decreasing, fileGuard = None, partial = false))
      case _ => Nil
    }
  }

  /** `left + right` ranged through `left` whatever `right` is, so `right` may hold the column off the chain. */
  private object AddLeftRule extends RangeRule {
    override def steps(term: Expression): Seq[Step] = term match {
      case add: Add => Seq(Step(term, add.left, Increasing, fileGuard = None, partial = false))
      case _ => Nil
    }
  }

  private val stubRules = Seq(NegateRule, AddLeftRule)

  @Test
  def testDeriveThroughDecreasingStep(): Unit = {
    assertEquals(
      Some(TermRange(UnaryMinus(xRange.hi), UnaryMinus(xRange.lo), xRange.nulls, xRange.rows)),
      TermRange.derive(UnaryMinus(x), source, stubRules))
    // An increasing step above a decreasing one, with a constant other operand
    assertEquals(
      Some(TermRange(Add(UnaryMinus(xRange.hi), ten), Add(UnaryMinus(xRange.lo), ten), xRange.nulls, xRange.rows)),
      TermRange.derive(Add(UnaryMinus(x), ten), source, stubRules))
  }

  @Test
  def testDeriveRebindsOffChainReferencesOnIncreasingChains(): Unit = {
    // Matches the pre-stage translator: the off-chain `abs(x)` is re-bound to the same bound
    assertEquals(
      Some(TermRange(Add(xRange.lo, Abs(xRange.lo)), Add(xRange.hi, Abs(xRange.hi)), xRange.nulls, xRange.rows)),
      TermRange.derive(Add(x, Abs(x)), source, stubRules))
  }

  @Test
  def testDeriveGivesNoRangeForDecreasingChainWithOffChainReferences(): Unit = {
    // Re-binding would pair x's min with the low bound -max(x), which bounds nothing
    assertEquals(None, TermRange.derive(Add(UnaryMinus(x), Abs(x)), source, stubRules))
    assertEquals(None, TermRange.derive(UnaryMinus(Add(x, Abs(x))), source, stubRules))
  }

  @Test
  def testDeriveWithProductionRules(): Unit = {
    assertTrue(TermRange.derive(Add(x, Literal(1.0d)), source).isDefined)
    // No production rule is decreasing yet
    assertEquals(None, TermRange.derive(UnaryMinus(x), source))
    assertEquals(None, TermRange.derive(Add(AttributeReference("i", IntegerType)(), Literal(1)), source))
  }
}
