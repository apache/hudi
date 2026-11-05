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

package org.apache.hudi

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference, Coalesce, DateFormatClass, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, If, In, InSet, IsNull, LeafExpression, LessThan, LessThanOrEqual, Literal, Log, Not, Or, StartsWith, TryEval, UnaryMinus}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.hudi.dataskipping.{ColumnStatsSource, Decreasing, Increasing, Leaf, Op, RangeRule, StatsFilterCompiler, Step, TermRange, Verdict}
import org.apache.spark.sql.hudi.dataskipping.rules.DateTimeOrdering
import org.apache.spark.sql.hudi.dataskipping.rules.DateTimeOrdering.Pattern
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
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
    rows = Some(UnresolvedAttribute("rows")),
    nullBoundIsUnknown = false)

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
  def testLeafKeepsInSetValuesAsIs(): Unit = {
    // Literal.apply throws on struct values, so parsing must not turn them into literals
    val struct = AttributeReference("st", StructType(Seq(StructField("f", IntegerType))))()
    val values: Set[Any] = Set(InternalRow(1), InternalRow(2))
    assertEquals(Some(Leaf(struct, Op.InSet(values), Nil)), Leaf.parse(InSet(struct, values)))
    // An unindexed column has no range, so the leaf compiles to TRUE without building them (the
    // resolution check is off, so that the leaf is reached)
    assertEquals(TrueLiteral, StatsFilterCompiler.compile(InSet(struct, values), source, strictResolution = false))
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
  def testInSetVerdictBuildsLiterals(): Unit = {
    val (lo, hi) = (markers.lo, markers.hi)
    assertEquals(And(LessThanOrEqual(lo, ten), GreaterThanOrEqual(hi, ten)),
      Verdict(Leaf(x, Op.InSet(Set(10.0d)), Nil), markers))
    // Strings are held as UTF8String, which Literal.apply does not accept
    val prefix = Literal("ab")
    assertEquals(And(LessThanOrEqual(lo, prefix), GreaterThanOrEqual(hi, prefix)),
      Verdict(Leaf(x, Op.InSet(Set(UTF8String.fromString("ab"))), Nil), markers))
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
      TermRange(Add(markers.lo, Literal(1.0d)), Add(markers.hi, Literal(1.0d)), markers.nulls, markers.rows,
        nullBoundIsUnknown = false),
      step(markers))
  }

  @Test
  def testDecreasingStepSwapsBounds(): Unit = {
    val step = Step(UnaryMinus(x), x, Decreasing, fileGuard = None, partial = false)
    assertEquals(
      TermRange(UnaryMinus(markers.hi), UnaryMinus(markers.lo), markers.nulls, markers.rows, nullBoundIsUnknown = false),
      step(markers))
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
        rows = markers.rows,
        nullBoundIsUnknown = true),
      step(markers))
  }

  @Test
  def testPartialStepDropsCountsAndTurnsFailuresIntoNull(): Unit = {
    val step = Step(Abs(x), x, Increasing, fileGuard = None, partial = true)
    assertEquals(
      TermRange(TryEval(Abs(markers.lo)), TryEval(Abs(markers.hi)), None, None, nullBoundIsUnknown = true),
      step(markers))
  }

  @Test
  def testNullBoundIsUnknownSticksAlongTheChain(): Unit = {
    // A total, unguarded step above a partial one: its bounds are NULL wherever the partial one's are
    val partial = Step(Abs(x), x, Increasing, fileGuard = None, partial = true)
    val total = Step(Add(Abs(x), ten), Abs(x), Increasing, fileGuard = None, partial = false)
    assertTrue(total(partial(markers)).nullBoundIsUnknown)
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

  /** `abs(child)` with a file guard, for ex one that only holds when the max is not too large. */
  private object GuardedAbsRule extends RangeRule {
    override def steps(term: Expression): Seq[Step] = term match {
      case abs: Abs =>
        Seq(Step(term, abs.child, Increasing, fileGuard = Some(r => LessThan(r.hi, ten)), partial = false))
      case _ => Nil
    }
  }

  private val stubRules = Seq(NegateRule, AddLeftRule)

  @Test
  def testDeriveThroughDecreasingStep(): Unit = {
    assertEquals(
      Some(TermRange(UnaryMinus(xRange.hi), UnaryMinus(xRange.lo), xRange.nulls, xRange.rows, nullBoundIsUnknown = false)),
      TermRange.derive(UnaryMinus(x), source, stubRules))
    // An increasing step above a decreasing one, with a constant other operand
    assertEquals(
      Some(TermRange(Add(UnaryMinus(xRange.hi), ten), Add(UnaryMinus(xRange.lo), ten), xRange.nulls, xRange.rows,
        nullBoundIsUnknown = false)),
      TermRange.derive(Add(UnaryMinus(x), ten), source, stubRules))
  }

  @Test
  def testDeriveRebindsOffChainReferencesOnIncreasingChains(): Unit = {
    // Matches the pre-stage translator: the off-chain `abs(x)` is re-bound to the same bound
    assertEquals(
      Some(TermRange(Add(xRange.lo, Abs(xRange.lo)), Add(xRange.hi, Abs(xRange.hi)), xRange.nulls, xRange.rows,
        nullBoundIsUnknown = false)),
      TermRange.derive(Add(x, Abs(x)), source, stubRules))
  }

  @Test
  def testDeriveGivesNoRangeForDecreasingChainWithOffChainReferences(): Unit = {
    // Re-binding would pair x's min with the low bound -max(x), which bounds nothing
    assertEquals(None, TermRange.derive(Add(UnaryMinus(x), Abs(x)), source, stubRules))
    assertEquals(None, TermRange.derive(UnaryMinus(Add(x, Abs(x))), source, stubRules))
  }

  @Test
  def testDeriveThroughGuardedChainMarksNullBoundsUnknown(): Unit = {
    // Outside the guard the bounds are NULL, which StatsFilterCompiler turns into keeping the file
    val unknown = Literal(null, DoubleType)
    val guard = LessThan(xRange.hi, ten)
    assertEquals(
      Some(TermRange(If(guard, Abs(xRange.lo), unknown), If(guard, Abs(xRange.hi), unknown), xRange.nulls, xRange.rows,
        nullBoundIsUnknown = true)),
      TermRange.derive(Abs(x), source, Seq(GuardedAbsRule)))
    assertTrue(TermRange.derive(Add(Abs(x), ten), source, Seq(GuardedAbsRule, AddLeftRule)).get.nullBoundIsUnknown)
    assertFalse(TermRange.derive(Add(x, ten), source, Seq(AddLeftRule)).get.nullBoundIsUnknown)
  }

  @Test
  def testDeriveWithProductionRules(): Unit = {
    assertTrue(TermRange.derive(Add(x, Literal(1.0d)), source).isDefined)
    // No production rule is decreasing yet
    assertEquals(None, TermRange.derive(UnaryMinus(x), source))
    assertEquals(None, TermRange.derive(Add(AttributeReference("i", IntegerType)(), Literal(1)), source))
  }

  // ---------------------------------------------------------------------------------------------
  // Stage 4: keeping the file on a NULL verdict
  // ---------------------------------------------------------------------------------------------

  /**
   * The compiled `filter` evaluated over a file whose stats are all NULL, for ex one holding only
   * NULLs. NULL drops the file, like FALSE.
   */
  private def overNullStats(filter: Expression, statsSource: ColumnStatsSource, statsType: DataType): Any =
    StatsFilterCompiler.compile(filter, statsSource, strictResolution = true).transform {
      case stat: LeafExpression if !stat.isInstanceOf[Literal] => Literal(null, statsType)
    }.eval()

  @Test
  def testKeepOnNullOnlyForPartialOrGuardedChains(): Unit = {
    // A column: its bounds are NULL only when all its values are, which no row passes; IN included
    assertEquals(null, overNullStats(LessThan(x, ten), source, DoubleType))
    assertEquals(null, overNullStats(In(x, Seq(ten, Literal(1.0d))), source, DoubleType))
    // A total, unguarded chain: same
    assertEquals(null, overNullStats(LessThan(Add(x, Literal(1.0d)), ten), source, DoubleType))
    // A partial chain: log is undefined below 0, so a NULL bound may stand for matching rows
    assertEquals(true, overNullStats(LessThan(Log(x), ten), source, DoubleType))
    // A guarded chain: date_format only holds within years 0001 to 9999
    val ts = AttributeReference("ts", TimestampType)()
    val formatted = EqualTo(DateFormatClass(ts, Literal("yyyy-MM-dd"), Some("UTC")), Literal("2024-01-01"))
    assertEquals(true, overNullStats(formatted, ColumnStatsSource(Seq("ts")), TimestampType))
  }

  @Test
  def testKeepOnNullWrapsOnlyPartialOrGuardedVerdicts(): Unit = {
    assertEquals(LessThan(xRange.lo, ten), StatsFilterCompiler.compile(LessThan(x, ten), source, strictResolution = true))
    assertEquals(
      Coalesce(Seq(LessThan(TryEval(Log(xRange.lo)), ten), TrueLiteral)),
      StatsFilterCompiler.compile(LessThan(Log(x), ten), source, strictResolution = true))
  }

  // ---------------------------------------------------------------------------------------------
  // Stage 3: date / time ordering guards
  // ---------------------------------------------------------------------------------------------

  private val dateOnly = Literal("yyyy-MM-dd")
  private val timeOfDay = Literal("yyyy-MM-dd HH:mm:ss")

  @Test
  def testDateTimePatternAllowlist(): Unit = {
    assertEquals(Some(Pattern(hasTimeOfDay = false)), DateTimeOrdering.pattern(dateOnly))
    assertEquals(Some(Pattern(hasTimeOfDay = true)), DateTimeOrdering.pattern(timeOfDay))
    // Not year-first, or not fixed-width
    assertEquals(None, DateTimeOrdering.pattern(Literal("MM/dd/yyyy")))
    assertEquals(None, DateTimeOrdering.pattern(Literal("dd-MM-yyyy")))
    assertEquals(None, DateTimeOrdering.pattern(Literal("yyyy-M-d")))
    assertEquals(None, DateTimeOrdering.pattern(Literal("yyyy-MMM-dd")))
    // Only literals
    assertEquals(None, DateTimeOrdering.pattern(AttributeReference("f", StringType)()))
    assertEquals(None, DateTimeOrdering.pattern(Literal(null, StringType)))
  }

  @Test
  def testDateTimeZoneGuards(): Unit = {
    // UTC and fixed offsets never shift, so a time-of-day parse is accepted
    Seq("UTC", "+08:00", "-05:30", "GMT+8").foreach { zone =>
      assertTrue(DateTimeOrdering.parsePreservesOrder(timeOfDay, Some(zone), zoneDependent = true), zone)
    }

    // America/Los_Angeles skips 02:00 to 03:00 when springing forward, so a time-of-day parse is
    // refused...
    val losAngeles = Some("America/Los_Angeles")
    assertFalse(DateTimeOrdering.parsePreservesOrder(timeOfDay, losAngeles, zoneDependent = true))
    // ... unless parsing into TIMESTAMP_NTZ, which reads the wall clock as is
    assertTrue(DateTimeOrdering.parsePreservesOrder(timeOfDay, losAngeles, zoneDependent = false))
    // ... while a date-only parse is monotone in every zone
    assertTrue(DateTimeOrdering.parsePreservesOrder(dateOnly, losAngeles, zoneDependent = true))
    // Asia/Kolkata has had no DST since 1945, but its offset changed, so it is not fixed
    assertFalse(DateTimeOrdering.parsePreservesOrder(timeOfDay, Some("Asia/Kolkata"), zoneDependent = true))

    // No time zone (not analyzed) or an unknown one
    assertFalse(DateTimeOrdering.parsePreservesOrder(dateOnly, None, zoneDependent = false))
    assertFalse(DateTimeOrdering.parsePreservesOrder(timeOfDay, Some("Not/A_Zone"), zoneDependent = true))
    // A pattern off the allowlist, in any zone
    assertFalse(DateTimeOrdering.formatPreservesOrder(Literal("MM/dd/yyyy")))
    assertFalse(DateTimeOrdering.parsePreservesOrder(Literal("yyyy-M-d"), Some("UTC"), zoneDependent = true))
    // Formatting takes no zone guard here (#20521, #20526)
    assertTrue(DateTimeOrdering.formatPreservesOrder(timeOfDay))
  }

  @Test
  def testDateTimeGuardsRefuseLegacyParserPolicy(): Unit = {
    def withPolicy(policy: String)(body: => Unit): Unit = {
      val conf = new SQLConf
      conf.setConfString(SQLConf.LEGACY_TIME_PARSER_POLICY.key, policy)
      SQLConf.withExistingConf(conf)(body)
    }
    // SimpleDateFormat parses leniently (for ex, '2022-3-6' with 'yyyy-MM-dd')
    withPolicy("LEGACY") {
      assertFalse(DateTimeOrdering.formatPreservesOrder(dateOnly))
      assertFalse(DateTimeOrdering.parsePreservesOrder(dateOnly, Some("UTC"), zoneDependent = true))
    }
    Seq("CORRECTED", "EXCEPTION").foreach { policy =>
      withPolicy(policy) {
        assertTrue(DateTimeOrdering.formatPreservesOrder(dateOnly), policy)
        assertTrue(DateTimeOrdering.parsePreservesOrder(dateOnly, Some("UTC"), zoneDependent = true), policy)
      }
    }
  }
}
