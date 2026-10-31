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
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType}
import org.junit.jupiter.api.Assertions.assertEquals
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

  private def matched(expr: Expression): Option[AttributeReference] =
    sparkAdapter.getCatalystExpressionUtils.tryMatchAttributeOrderingPreservingTransformation(expr)

  @Test
  def testIdentityAttributeMatches(): Unit = {
    assertEquals(Some(intAttr), matched(intAttr))
  }

  @Test
  def testArithmeticTransformationsPreserveOrdering(): Unit = {
    // Float/double Add/Multiply cannot wrap, so they still translate
    assertEquals(Some(dblAttr), matched(Add(dblAttr, Literal(1.0d))))
    assertEquals(Some(dblAttr), matched(Add(Literal(1.0d), dblAttr)))
    assertEquals(Some(dblAttr), matched(Multiply(dblAttr, Literal(2.0d))))
    assertEquals(Some(dblAttr), matched(Multiply(Literal(2.0d), dblAttr)))
    assertEquals(Some(intAttr), matched(Divide(intAttr, Literal(2))))
    assertEquals(Some(intAttr), matched(BitwiseOr(intAttr, Literal(1))))
    assertEquals(Some(intAttr), matched(BitwiseOr(Literal(1), intAttr)))
  }

  @Test
  def testIntegralAddMultiplyShiftLeftDoNotMatch(): Unit = {
    // Integral Add/Multiply wrap on overflow (#20460); ShiftLeft is integral-only and wraps
    assertEquals(None, matched(Add(intAttr, Literal(1))))
    assertEquals(None, matched(Add(Literal(1), intAttr)))
    assertEquals(None, matched(Add(longAttr, Literal(1L))))
    assertEquals(None, matched(Multiply(intAttr, Literal(2))))
    assertEquals(None, matched(Multiply(Literal(2), intAttr)))
    assertEquals(None, matched(Multiply(longAttr, Literal(2L))))
    assertEquals(None, matched(ShiftLeft(intAttr, Literal(1))))
    assertEquals(None, matched(ShiftLeft(longAttr, Literal(1))))
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
    // TODO(#19445): a narrowing numeric cast wraps around in non-ANSI mode and does not preserve
    // ordering, so this should be None; pinning the current (incorrect) behavior until
    // isCastPreservingOrdering rejects it.
    assertEquals(Some(longAttr), matched(Cast(longAttr, IntegerType)))
  }

  @Test
  def testNestedComposition(): Unit = {
    assertEquals(Some(dblAttr), matched(Add(Multiply(dblAttr, Literal(2.0d)), Literal(3.0d))))
    // Nested integral Add/Multiply must not match either
    assertEquals(None, matched(Add(Multiply(intAttr, Literal(2)), Literal(3))))
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
