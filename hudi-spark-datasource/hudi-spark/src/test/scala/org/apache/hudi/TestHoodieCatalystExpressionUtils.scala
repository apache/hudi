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
    // TODO(#19445): a narrowing numeric cast wraps around in non-ANSI mode and does not preserve
    // ordering, so this should be None; pinning the current (incorrect) behavior until
    // isCastPreservingOrdering rejects it.
    assertEquals(Some(longAttr), matched(Cast(longAttr, IntegerType)))
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
