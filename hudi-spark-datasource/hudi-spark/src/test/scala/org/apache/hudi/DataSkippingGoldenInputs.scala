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
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference, BinaryOperator, BitwiseOr, Cast, Coalesce, Concat, DateAdd, DateDiff, DateFormatClass, DateSub, DayOfMonth, Divide, EqualNullSafe, EqualTo, Exp, Expm1, Expression, FromUnixTime, FromUTCTimestamp, GreaterThanOrEqual, In, InSet, IsNotNull, IsNull, LessThan, Literal, Log, Log10, Log1p, Log2, Lower, Multiply, Not, Or, ParseToDate, ParseToTimestamp, ShiftLeft, ShiftRight, StartsWith, Substring, TimeZoneAwareExpression, ToUnixTimestamp, ToUTCTimestamp, UnixTimestamp, Upper}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import java.time.LocalDate

import scala.util.{Random, Try}

/**
 * Inputs of [[TestDataSkippingGoldenFilters]]: filters over a fixed set of columns, each paired in
 * the golden file with the index filter it compiles to.
 *
 * The golden file was generated from the translator before the four-stage split, so it records the
 * pre-split behavior. Inputs are rendered by [[render]] rather than `Expression.sql`, which is not
 * stable across Spark versions and formats timestamps in the session time zone.
 */
object DataSkippingGoldenInputs {

  val indexedCols: Seq[String] = Seq("a", "l", "d", "ts", "s", "x", "dec", "sm", "tn")

  private val a = AttributeReference("a", IntegerType)()
  // Not indexed
  private val b = AttributeReference("b", IntegerType)()
  private val l = AttributeReference("l", LongType)()
  private val d = AttributeReference("d", DateType)()
  private val ts = AttributeReference("ts", TimestampType)()
  private val s = AttributeReference("s", StringType)()
  private val x = AttributeReference("x", DoubleType)()
  private val dec = AttributeReference("dec", DecimalType(10, 2))()
  // Only in [[overflowTerms]], so not in [[columns]]
  private val sm = AttributeReference("sm", ShortType)()
  private val tn = AttributeReference("tn", ByteType)()

  private val columns: Seq[Expression] = Seq(a, b, l, d, ts, s, x, dec)

  // Never indexed: column stats only cover primitive columns
  private val struct = AttributeReference("st", StructType(Seq(StructField("f", IntegerType))))()
  private val array = AttributeReference("arr", ArrayType(IntegerType))()

  /**
   * IN over struct and array columns, which have no range: they compile to TRUE, without turning the
   * [[InSet]] values into literals ([[Literal#apply]] throws on struct and array values).
   */
  private val nestedLeaves: Seq[Expression] = Seq(struct, array).flatMap { column =>
    val v = Literal.default(column.dataType)
    Seq(InSet(column, Set(v.value)), In(column, Seq(v)), EqualTo(column, v))
  }

  private val day = Literal(LocalDate.of(2024, 1, 1))

  /** Terms built from one column, accepted or rejected by the range rules. */
  private val terms: Seq[Expression] = columns ++ Seq(
    Add(a, Literal(1)), Add(Literal(1), a), Add(a, b), Add(a, a), Add(b, Literal(1)), Add(a, Literal("x")),
    Multiply(a, Literal(2)), Multiply(Literal(2), a), Multiply(a, Literal(-2)), Multiply(Literal(0), a),
    Multiply(a, a), Multiply(x, Literal(Double.PositiveInfinity)),
    Multiply(dec, Literal(Decimal(1.5), DecimalType(2, 1))),
    Divide(x, Literal(2.0)), Divide(Literal(2.0), x), Divide(x, Literal(-2.0)), Divide(x, x),
    BitwiseOr(a, Literal(1)), BitwiseOr(Literal(1), a), BitwiseOr(Abs(a), a), BitwiseOr(a, a),
    ShiftLeft(a, Literal(1)), ShiftRight(a, Literal(2)), ShiftLeft(a, a), ShiftRight(Literal(8), a),
    Exp(x), Expm1(x), Log(x), Log10(x), Log1p(x), Log2(x), Log(Exp(x)), Exp(Cast(a, DoubleType)),
    Cast(a, LongType), Cast(a, ShortType), Cast(s, IntegerType), Cast(ts, StringType), Cast(a, StringType),
    Cast(Add(a, Literal(1)), LongType), Cast(Cast(a, LongType), DoubleType), Cast(dec, DoubleType),
    Cast(s, DateType), Cast(d, TimestampType), Cast(a, BooleanType), Cast(ts, LongType), Cast(ts, IntegerType),
    // Unresolved casts on every Spark version: a typed child (ranged in expression index mode) and
    // an untyped one (dataType throws, not ranged)
    Cast(Add(a, Literal("x")), LongType), Cast(UnresolvedFunction(Seq("f"), Seq(a), false), LongType),
    DateAdd(d, Literal(1)), DateAdd(d, DayOfMonth(d)), DateAdd(d, a), DateSub(d, Literal(3)),
    DateSub(d, DayOfMonth(d)), DateDiff(d, day), DateDiff(day, d), DateDiff(d, d),
    DateAdd(Cast(ts, DateType), Literal(1)),
    DateFormatClass(ts, Literal("yyyy-MM-dd"), Some("UTC")), DateFormatClass(ts, Cast(ts, StringType), Some("UTC")),
    FromUnixTime(l, Literal("yyyy-MM-dd"), Some("UTC")), ToUnixTimestamp(s, Literal("yyyy-MM-dd"), Some("UTC")),
    ToUnixTimestamp(ts, Literal("yyyy-MM-dd"), Some("UTC")),
    FromUTCTimestamp(ts, Literal("UTC")), ToUTCTimestamp(ts, Literal("America/New_York")),
    new org.apache.spark.sql.catalyst.expressions.Left(s, Literal(2)), Substring(s, Literal(1), Literal(2)),
    Upper(s), Lower(s), Abs(a),
    new ParseToDate(s), new ParseToDate(s, Literal("yyyy-MM-dd")), new ParseToTimestamp(s, Literal("yyyy-MM-dd")),
    new ParseToDate(Cast(ts, StringType)), Cast(new ParseToDate(s), TimestampType),
    Add(Cast(a, LongType), Literal(1L)), Add(DateDiff(d, day), Literal(1)),
    Concat(Seq(s, s)), Coalesce(Seq(a, Literal(1))),
    UnresolvedAttribute("a"), Add(UnresolvedAttribute("a"), Literal(1)))

  /**
   * Date / time terms added after the golden file was first generated. They are kept out of [[terms]]
   * so that the seeded trees, which sample the leaves of [[terms]], stay the same.
   *
   * `to_date` / `to_timestamp` with a format reach data skipping as their optimizer replacement
   * (`GetTimestamp`, under a cast to date for `to_date`), built here through the 2-arg constructors,
   * which are stable across Spark versions unlike `GetTimestamp`'s.
   */
  private val dateTimeTerms: Seq[Expression] = Seq(
    new ParseToTimestamp(s, Literal("yyyy-MM-dd")).replacement,
    new ParseToTimestamp(s, Literal("MM/dd/yyyy")).replacement,
    new ParseToTimestamp(ts, Literal("MM/dd/yyyy")).replacement,
    new ParseToDate(s, Literal("yyyy-MM-dd HH:mm:ss")).replacement,
    UnixTimestamp(s, Literal("yyyy-MM-dd"), Some("UTC")), UnixTimestamp(s, Literal("yyyy-M-d"), Some("UTC")),
    UnixTimestamp(d, Literal("MM/dd/yyyy"), Some("UTC")),
    ToUnixTimestamp(s, Literal("yyyy-MM-dd HH:mm:ss"), Some("America/Los_Angeles")),
    ToUnixTimestamp(s, Literal("yyyy-MM-dd"), Some("America/Los_Angeles")),
    DateFormatClass(ts, Literal("MM/dd/yyyy"), Some("UTC")),
    FromUnixTime(l, Literal("dd-MM-yyyy"), Some("UTC")))

  /**
   * Overflowing arithmetic terms (#20460) added after the golden file was first generated, kept out
   * of [[terms]] for the same reason as [[dateTimeTerms]].
   *
   * An int shifts by the low 5 bits of the amount, which 33 pins (a 6-bit mask would shift by 33).
   * The smallint and tinyint terms take a literal of their own type, as an int literal would widen
   * the result to int. Decimal results overflow into NULL instead, so their ranges are partial.
   */
  private val overflowTerms: Seq[Expression] = Seq(
    ShiftLeft(a, Literal(33)),
    Add(sm, Literal(1.toShort)),
    Multiply(tn, Literal(3.toByte)),
    Add(dec, Literal(Decimal(1), DecimalType(10, 2))),
    Divide(dec, Literal(Decimal(2), DecimalType(10, 2))))

  /**
   * Sets a time zone wherever the analyzer would, so that resolution does not depend on the Spark
   * version (for ex, `cast(s as date)` needs a time zone on Spark 3.5 but not on Spark 4). Inputs
   * meant to be unresolved are unresolved for another reason.
   */
  private def withTimeZone(e: Expression): Expression = e.transform {
    case tz: TimeZoneAwareExpression if tz.timeZoneId.isEmpty => tz.withTimeZone("UTC")
  }

  private def dataTypeOf(t: Expression): DataType = Try(t.dataType).getOrElse(IntegerType)

  /** One leaf per operator over `t`, plus the null and bare-column only shapes for columns. */
  private def leaves(t: Expression): Seq[Expression] = {
    val dt = dataTypeOf(t)
    val v = Literal.default(dt)
    val nullValue = Literal.create(null, dt)
    val perOp = Seq(
      EqualTo(t, v), LessThan(v, t), GreaterThanOrEqual(t, v), Not(EqualTo(t, v)), EqualTo(t, Cast(v, dt)),
      In(t, Seq(v, nullValue)), Not(In(t, Seq(v))),
      StartsWith(t, Literal("ab")), Not(StartsWith(t, Literal("ab"))))
    val columnOnly = if (columns.contains(t)) {
      Seq(InSet(t, Set(v.value)), IsNull(t), IsNotNull(t), Not(IsNull(t)), EqualNullSafe(t, nullValue),
        EqualTo(t, b), In(t, Seq(v, b)))
    } else {
      Nil
    }
    perOp ++ columnOnly
  }

  /**
   * Every input, in golden file order: the leaves, a seeded sample of AND / OR / NOT trees, the
   * struct and array leaves, the leaves of [[dateTimeTerms]], then the leaves of [[overflowTerms]].
   */
  def inputs: Seq[Expression] = {
    val allLeaves = terms.map(withTimeZone).flatMap(leaves).map(withTimeZone)
    val rnd = new Random(42)
    def tree(depth: Int): Expression =
      if (depth == 0 || rnd.nextInt(3) == 0) {
        allLeaves(rnd.nextInt(allLeaves.size))
      } else {
        rnd.nextInt(5) match {
          case 0 => And(tree(depth - 1), tree(depth - 1))
          case 1 => Or(tree(depth - 1), tree(depth - 1))
          case 2 => Not(And(tree(depth - 1), tree(depth - 1)))
          case 3 => Not(Or(tree(depth - 1), tree(depth - 1)))
          case _ => Not(tree(depth - 1))
        }
      }
    allLeaves ++ (1 to 60).map(_ => tree(3)) ++ nestedLeaves ++
      dateTimeTerms.map(withTimeZone).flatMap(leaves).map(withTimeZone) ++
      overflowTerms.map(withTimeZone).flatMap(leaves).map(withTimeZone)
  }

  /**
   * One golden line per input and mode: `cs` (column stats, strict resolution) for every input,
   * `ei` (expression index) only for unresolved inputs, the only ones the mode changes.
   */
  def goldenLines(translate: (Expression, Boolean) => Expression): Seq[String] =
    inputs.flatMap { e =>
      val modes = if (e.resolved) Seq(false) else Seq(false, true)
      modes.map { isExpressionIndex =>
        val output = Try(render(translate(e, isExpressionIndex))).recover {
          case t => s"!${t.getClass.getName}"
        }.get
        s"${if (isExpressionIndex) "ei" else "cs"}\t${render(e)}\t$output"
      }
    }

  /**
   * A rendering that only depends on the expression tree: operator symbols, function names,
   * column names, and literals as `value:type` (dates as epoch days, timestamps as epoch micros).
   */
  def render(e: Expression): String = e match {
    case attr: AttributeReference => attr.name
    case attr: UnresolvedAttribute => attr.name
    case Literal(null, dt) => s"null:${dt.catalogString}"
    case Literal(value, StringType) => s"'$value'"
    case Literal(value: ArrayData, ArrayType(elementType, _)) =>
      value.toSeq[Any](elementType).map(e => render(Literal(e, elementType))).mkString("array(", ", ", ")")
    case Literal(value: InternalRow, structType: StructType) =>
      value.toSeq(structType).zip(structType.fields)
        .map { case (e, field) => render(Literal(e, field.dataType)) }.mkString("struct(", ", ", ")")
    case Literal(value, dt) => s"$value:${dt.catalogString}"
    case Cast(child, dt, _, _) => s"cast(${render(child)} as ${dt.catalogString})"
    case Not(child) => s"NOT ${render(child)}"
    case IsNull(child) => s"${render(child)} IS NULL"
    case IsNotNull(child) => s"${render(child)} IS NOT NULL"
    case In(value, list) => s"${render(value)} IN (${list.map(render).mkString(", ")})"
    case InSet(value, set) =>
      s"${render(value)} INSET (${set.toSeq.map(v => render(Literal(v, value.dataType))).sorted.mkString(", ")})"
    case op: BinaryOperator => s"(${render(op.left)} ${op.sqlOperator} ${render(op.right)})"
    case other => s"${other.prettyName}(${other.children.map(render).mkString(", ")})"
  }
}
