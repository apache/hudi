/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.ExpressionConverter.convertFilter
import org.apache.hudi.expression.{Predicates, AttributeReference => HAttributeReference, Expression => HExpression, Literal => HLiteral}
import org.apache.hudi.internal.schema.Types
import org.apache.hudi.testutils.HoodieClientTestHarness
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{Assertions, Test}

import scala.jdk.CollectionConverters.seqAsJavaListConverter

class TestExpressionConverter extends HoodieClientTestHarness {

  @Test
  def testConvertInExpression(): Unit = {
    val filterExpr = expr("col1 IN (1, 2, 3)")

    val result = ExpressionConverter.convertFilter(filterExpr.expr.transformUp {
      case UnresolvedAttribute(nameParts) => AttributeReference(nameParts.mkString("."), IntegerType)()}, "UTC")
      .get

    val expected = Predicates.in(
      new HAttributeReference("col1", Types.IntType.get()),
      Seq(1, 2, 3).map(v => new HLiteral(v, Types.IntType.get()).asInstanceOf[HExpression]).asJava)

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertInSetExpression(): Unit = {
    val filterExpr = InSet(AttributeReference("col1", StringType)(), Set("value1", "value2", "value3"))

    val result = ExpressionConverter.convertFilter(filterExpr, "UTC").get

    val expected = Predicates.in(
      new HAttributeReference("col1", Types.StringType.get()),
      Seq("value1", "value2", "value3").map(v => new HLiteral(v, Types.StringType.get()).asInstanceOf[HExpression]).asJava)

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertEqualToExpression(): Unit = {
    val filter = EqualTo(AttributeReference("col1", LongType)(), Literal(1L))
    val result = convertFilter(filter, "UTC").get

    val expected = Predicates.eq(
      new HAttributeReference("col1", Types.LongType.get()),
      new HLiteral(1L, Types.LongType.get()).asInstanceOf[HExpression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertGreaterThanExpression(): Unit = {
    val filter = GreaterThan(AttributeReference("col3", DoubleType)(), Literal(3.0D))
    val result = convertFilter(filter, "UTC").get

    val expected = Predicates.gt(
      new HAttributeReference("col3", Types.DoubleType.get()),
      new HLiteral(3.0D, Types.DoubleType.get()).asInstanceOf[HExpression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertGreaterThanOrEqualExpression(): Unit = {
    val filter = GreaterThanOrEqual(AttributeReference("col4", FloatType)(), Literal(4.0f))
    val result = convertFilter(filter, "UTC").get

    val expected = Predicates.gteq(
      new HAttributeReference("col4", Types.FloatType.get()),
      new HLiteral(4.0f, Types.FloatType.get()).asInstanceOf[HExpression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertLessThanExpression(): Unit = {
    val filter = LessThan(AttributeReference("col5", StringType)(), Literal("abc"))
    val result = convertFilter(filter, "UTC").get

    val expected = Predicates.lt(
      new HAttributeReference("col5", Types.StringType.get()),
      new HLiteral("abc", Types.StringType.get()).asInstanceOf[HExpression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertLessThanOrEqualExpression(): Unit = {
    val filter = LessThanOrEqual(AttributeReference("col6", BooleanType)(), Literal(true))
    val result = convertFilter(filter, "UTC").get

    val expected = Predicates.lteq(
      new HAttributeReference("col6", Types.BooleanType.get()),
      new HLiteral(true, Types.BooleanType.get()).asInstanceOf[HExpression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertStartsWithExpression(): Unit = {
    val filter = StartsWith(AttributeReference("col2", StringType)(), Literal("prefix"))
    val result = convertFilter(filter, "UTC").get

    val expected = Predicates.startsWith(
      new HAttributeReference("col2", Types.StringType.get()),
      new HLiteral("prefix", Types.StringType.get()).asInstanceOf[HExpression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertAndExpression(): Unit = {
    val filter = And(
      EqualTo(AttributeReference("col1", IntegerType)(), Literal(1)),
      GreaterThan(AttributeReference("col2", FloatType)(), Literal(2.0))
    )
    val result = convertFilter(filter, "UTC").get

    val expected = Predicates.and(
      Predicates.eq(
        new HAttributeReference("col1", Types.IntType.get()),
        new HLiteral(1, Types.IntType.get()).asInstanceOf[HExpression]),
      Predicates.gt(
        new HAttributeReference("col2", Types.FloatType.get()),
        new HLiteral(2.0, Types.FloatType.get()).asInstanceOf[HExpression])
    )

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertOrEqualExpression(): Unit = {
    val filter = Or(
      LessThan(AttributeReference("col1", ShortType)(), Literal(10.toShort)),
      GreaterThanOrEqual(AttributeReference("col2", DoubleType)(), Literal(100.0D))
    )
    val result = convertFilter(filter, "UTC").get

    val expected = Predicates.or(
      Predicates.lt(
        new HAttributeReference("col1", Types.IntType.get()),
        new HLiteral(10.toShort, Types.IntType.get()).asInstanceOf[HExpression]),
      Predicates.gteq(
        new HAttributeReference("col2", Types.DoubleType.get()),
        new HLiteral(100.0D, Types.DoubleType.get()).asInstanceOf[HExpression])
    )

    Assertions.assertEquals(result.toString, expected.toString)
  }
}
