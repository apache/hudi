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

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, BitwiseOr, Cast, DateAdd, DateDiff, DateSub, Divide, Exp, Expression, Literal, Log, Lower, Multiply, ParseToDate, ShiftLeft, Sqrt, Upper}
import org.apache.spark.sql.types.{DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

/**
 * Branch coverage for the order-preserving transformation matcher in
 * [[org.apache.spark.sql.BaseHoodieCatalystExpressionUtils]]. This is what lets data skipping map a
 * transformed column reference back to its source attribute, so each case is pinned to the exact
 * source [[AttributeReference]] it must recover, and non-order-preserving shapes must not match.
 */
class TestHoodieCatalystExpressionUtils extends SparkAdapterSupport {

  private val intAttr = AttributeReference("i", IntegerType)()
  private val strAttr = AttributeReference("s", StringType)()
  private val dblAttr = AttributeReference("d", DoubleType)()
  private val dateAttr = AttributeReference("dt", DateType)()
  private val longAttr = AttributeReference("l", LongType)()
  private val decAttr = AttributeReference("dec", DecimalType(10, 2))()

  private def matched(expr: Expression): Option[AttributeReference] =
    sparkAdapter.getCatalystExpressionUtils.tryMatchAttributeOrderingPreservingTransformation(expr)

  @Test
  def testIdentityAttributeMatches(): Unit = {
    assertEquals(Some(intAttr), matched(intAttr))
  }

  @Test
  def testArithmeticTransformationsPreserveOrdering(): Unit = {
    assertEquals(Some(intAttr), matched(Add(intAttr, Literal(1))))
    assertEquals(Some(intAttr), matched(Add(Literal(1), intAttr)))
    assertEquals(Some(dblAttr), matched(Multiply(dblAttr, Literal(2.0d))))
    assertEquals(Some(dblAttr), matched(Multiply(Literal(2.0d), dblAttr)))
    assertEquals(Some(dblAttr), matched(Divide(dblAttr, Literal(2.0d))))
    assertEquals(Some(intAttr), matched(BitwiseOr(intAttr, Literal(1))))
    assertEquals(Some(intAttr), matched(BitwiseOr(Literal(1), intAttr)))
    assertEquals(Some(intAttr), matched(ShiftLeft(intAttr, Literal(1))))
  }

  @Test
  def testUnaryMathTransformationsPreserveOrdering(): Unit = {
    assertEquals(Some(dblAttr), matched(Exp(dblAttr)))
    assertEquals(Some(dblAttr), matched(Log(dblAttr)))
  }

  @Test
  def testDateTransformationsPreserveOrdering(): Unit = {
    assertEquals(Some(dateAttr), matched(DateAdd(dateAttr, Literal(1))))
    assertEquals(Some(dateAttr), matched(DateSub(dateAttr, Literal(1))))
    // DateDiff(end, start) = end - start preserves the ordering of its end date.
    assertEquals(Some(dateAttr), matched(DateDiff(dateAttr, Literal(java.sql.Date.valueOf("2022-03-07")))))
  }

  @Test
  def testDateParsingTransformationsPreserveOrdering(): Unit = {
    // ParseToDate's case-class shape differs across Spark versions, so the matcher dispatches it
    // through the per-version unapplyOrderPreservingDateParsing hook. The 1-arg auxiliary
    // constructor is stable on all profiles, letting this pin that hook everywhere.
    assertEquals(Some(strAttr), matched(new ParseToDate(strAttr)))
  }

  @Test
  def testUpCastPreservesOrderingButNumericToStringDoesNot(): Unit = {
    // Widening a numeric column preserves ordering, so the source attribute is recovered.
    assertEquals(Some(intAttr), matched(Cast(intAttr, LongType)))
    // Casting a numeric column to string can reorder values, so it must not match.
    assertEquals(None, matched(Cast(intAttr, StringType)))
    // A narrowing numeric cast wraps around in non-ANSI mode and does not preserve ordering.
    assertEquals(None, matched(Cast(longAttr, IntegerType)))
  }

  @Test
  def testNumericCastOrderingFollowsUpCastRules(): Unit = {
    // Widening precedence casts preserve ordering, including lossy-but-monotonic long to float.
    assertEquals(Some(intAttr), matched(Cast(intAttr, DoubleType)))
    assertEquals(Some(longAttr), matched(Cast(longAttr, FloatType)))
    // Decimal widening is an up-cast; narrowing precision is not.
    assertEquals(Some(decAttr), matched(Cast(decAttr, DecimalType(12, 2))))
    assertEquals(None, matched(Cast(decAttr, DecimalType(8, 2))))
    // Decimal to float/double rounds monotonically and is allowed explicitly.
    assertEquals(Some(decAttr), matched(Cast(decAttr, DoubleType)))
    assertEquals(Some(decAttr), matched(Cast(decAttr, FloatType)))
    // Cast.canUpCast rejects double to float even though rounding is weakly monotonic; the
    // conservative answer only costs pruning opportunity.
    assertEquals(None, matched(Cast(dblAttr, FloatType)))
    // String to numeric is not order-preserving.
    assertEquals(None, matched(Cast(strAttr, IntegerType)))
    // Identity string cast keeps the ordering.
    assertEquals(Some(strAttr), matched(Cast(strAttr, StringType)))
  }

  @Test
  def testMultiplyDivideRequirePositiveFiniteFloatingPointFactor(): Unit = {
    // Negative factors reverse ordering.
    assertEquals(None, matched(Multiply(dblAttr, Literal(-1.0d))))
    assertEquals(None, matched(Multiply(Literal(-1.0d), dblAttr)))
    assertEquals(None, matched(Divide(dblAttr, Literal(-2.0d))))
    // Zero collapses ordering and makes division undefined.
    assertEquals(None, matched(Multiply(dblAttr, Literal(0.0d))))
    assertEquals(None, matched(Divide(dblAttr, Literal(0.0d))))
    // Infinite and NaN factors turn finite values into NaN (for ex, 0 * Infinity).
    assertEquals(None, matched(Multiply(dblAttr, Literal(Double.PositiveInfinity))))
    assertEquals(None, matched(Divide(dblAttr, Literal(Double.PositiveInfinity))))
    assertEquals(None, matched(Multiply(dblAttr, Literal(Double.NaN))))
    // Typed null literals carry a null value.
    assertEquals(None, matched(Multiply(dblAttr, Literal(null, DoubleType))))
    // Non-literal operands cannot be validated statically, even when reference-free.
    assertEquals(None, matched(Multiply(dblAttr, Add(Literal(1.0d), Literal(1.0d)))))
    // Self-multiplication is not monotonic over negative values.
    assertEquals(None, matched(Multiply(dblAttr, dblAttr)))
    // Integral results wrap around on overflow and decimal results overflow into NULL, so even
    // positive factors do not preserve ordering for them.
    assertEquals(None, matched(Multiply(longAttr, Literal(2L))))
    assertEquals(None, matched(Multiply(Literal(2), intAttr)))
    assertEquals(None, matched(Multiply(decAttr, Literal(Decimal(2), DecimalType(1, 0)))))
    assertEquals(None, matched(Divide(decAttr, Literal(Decimal(2), DecimalType(1, 0)))))
    // Positive finite factors match in the supported operand positions, including after an
    // up-cast of an integral column.
    assertEquals(Some(dblAttr), matched(Multiply(dblAttr, Literal(2.5d))))
    assertEquals(Some(dblAttr), matched(Multiply(Literal(2.5d), dblAttr)))
    assertEquals(Some(dblAttr), matched(Divide(dblAttr, Literal(3.0d))))
    assertEquals(Some(intAttr), matched(Multiply(Cast(intAttr, FloatType), Literal(2.0f))))
  }

  @Test
  def testCollationChangingStringCastsDoNotPreserveOrdering(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_0, "String collations only exist on Spark 4.x")
    // StringType("UTF8_LCASE") is a Spark 4 API, so it is obtained reflectively to keep this
    // file compiling against the Spark 3 profiles.
    val lcase = StringType.getClass.getMethod("apply", classOf[String])
      .invoke(StringType, "UTF8_LCASE").asInstanceOf[DataType]
    val collatedAttr = AttributeReference("cs", lcase)()
    // Changing collation changes the sort order, in either direction.
    assertEquals(None, matched(Cast(collatedAttr, StringType)))
    assertEquals(None, matched(Cast(strAttr, lcase)))
    // A collation-preserving cast keeps the ordering.
    assertEquals(Some(collatedAttr), matched(Cast(collatedAttr, lcase)))
  }

  @Test
  def testNestedComposition(): Unit = {
    assertEquals(Some(dblAttr), matched(Add(Multiply(dblAttr, Literal(2.0d)), Literal(3.0d))))
  }

  @Test
  def testNonOrderPreservingExpressionsDoNotMatch(): Unit = {
    // DateDiff(end, start) = end - start reverses the ordering of its start date.
    assertEquals(None, matched(DateDiff(Literal(java.sql.Date.valueOf("2022-03-09")), dateAttr)))
    // A bare literal carries no attribute.
    assertEquals(None, matched(Literal(5)))
    // No attribute on either operand.
    assertEquals(None, matched(Add(Literal(1), Literal(2))))
    // Sqrt is not one of the whitelisted order-preserving transformations.
    assertEquals(None, matched(Sqrt(dblAttr)))
    // Case folding breaks the binary ordering of strings: "Zebra" < "apple", but lower("Zebra") > "apple".
    assertEquals(None, matched(Lower(strAttr)))
    assertEquals(None, matched(Upper(strAttr)))
  }
}
