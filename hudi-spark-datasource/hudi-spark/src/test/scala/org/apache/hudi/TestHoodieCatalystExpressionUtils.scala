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

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, BitwiseOr, Cast, DateAdd, DateSub, Divide, Exp, Expression, Literal, Log, Lower, Multiply, ParseToDate, ShiftLeft, Sqrt, Upper}
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
    assertEquals(Some(intAttr), matched(Multiply(intAttr, Literal(2))))
    assertEquals(Some(intAttr), matched(Multiply(Literal(2), intAttr)))
    assertEquals(Some(intAttr), matched(Divide(intAttr, Literal(2))))
    assertEquals(Some(intAttr), matched(BitwiseOr(intAttr, Literal(1))))
    assertEquals(Some(intAttr), matched(BitwiseOr(Literal(1), intAttr)))
    assertEquals(Some(intAttr), matched(ShiftLeft(intAttr, Literal(1))))
  }

  @Test
  def testUnaryMathAndStringTransformationsPreserveOrdering(): Unit = {
    assertEquals(Some(dblAttr), matched(Exp(dblAttr)))
    assertEquals(Some(dblAttr), matched(Log(dblAttr)))
    assertEquals(Some(strAttr), matched(Upper(strAttr)))
    assertEquals(Some(strAttr), matched(Lower(strAttr)))
  }

  @Test
  def testDateTransformationsPreserveOrdering(): Unit = {
    assertEquals(Some(dateAttr), matched(DateAdd(dateAttr, Literal(1))))
    assertEquals(Some(dateAttr), matched(DateSub(dateAttr, Literal(1))))
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
    // Cast.canUpCast rejects double to float even though rounding is weakly monotonic; the
    // conservative answer only costs pruning opportunity.
    assertEquals(None, matched(Cast(dblAttr, FloatType)))
    // String to numeric is not order-preserving.
    assertEquals(None, matched(Cast(strAttr, IntegerType)))
    // Identity string cast keeps the ordering.
    assertEquals(Some(strAttr), matched(Cast(strAttr, StringType)))
  }

  @Test
  def testMultiplyDivideRequireStrictlyPositiveLiteral(): Unit = {
    // Negative factors reverse ordering.
    assertEquals(None, matched(Multiply(intAttr, Literal(-1))))
    assertEquals(None, matched(Multiply(Literal(-1), intAttr)))
    assertEquals(None, matched(Divide(intAttr, Literal(-2))))
    // Zero collapses ordering and makes division undefined.
    assertEquals(None, matched(Multiply(intAttr, Literal(0))))
    assertEquals(None, matched(Divide(intAttr, Literal(0))))
    // Typed null literals carry a null value.
    assertEquals(None, matched(Multiply(intAttr, Literal(null, IntegerType))))
    // Non-literal operands cannot be validated statically, even when reference-free.
    assertEquals(None, matched(Multiply(intAttr, Add(Literal(1), Literal(1)))))
    // Self-multiplication is not monotonic over negative values.
    assertEquals(None, matched(Multiply(intAttr, intAttr)))
    // Strictly positive literals of any numeric type match in the supported operand positions.
    assertEquals(Some(intAttr), matched(Multiply(intAttr, Literal(2.5d))))
    assertEquals(Some(dblAttr), matched(Multiply(Literal(Decimal(2)), dblAttr)))
    assertEquals(Some(intAttr), matched(Divide(intAttr, Literal(3L))))
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
    assertEquals(Some(intAttr), matched(Add(Multiply(intAttr, Literal(2)), Literal(3))))
  }

  @Test
  def testNonOrderPreservingExpressionsDoNotMatch(): Unit = {
    // A bare literal carries no attribute.
    assertEquals(None, matched(Literal(5)))
    // No attribute on either operand.
    assertEquals(None, matched(Add(Literal(1), Literal(2))))
    // Sqrt is not one of the whitelisted order-preserving transformations.
    assertEquals(None, matched(Sqrt(dblAttr)))
  }
}
