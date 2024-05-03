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

import org.apache.hudi.SparkFilterHelper.convertFilter
import org.apache.hudi.expression.{Expression, NameReference, Predicates, Literal => HLiteral}
import org.apache.hudi.testutils.HoodieSparkClientTestHarness

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{Assertions, Test}

import scala.collection.JavaConverters._

class TestSparkFilterHelper extends HoodieSparkClientTestHarness with SparkAdapterSupport  {

  @Test
  def testConvertInExpression(): Unit = {
    val filterExpr = sparkAdapter.translateFilter(
      expr("col1 IN (1, 2, 3)").expr.transformUp {
        case UnresolvedAttribute(nameParts) => AttributeReference(nameParts.mkString("."), IntegerType)()
      })

    val result = SparkFilterHelper.convertFilter(filterExpr.get).get

    val expected = Predicates.in(
      new NameReference("col1"),
      Seq(1, 2, 3).map(v => HLiteral.from(v).asInstanceOf[Expression]).asJava)

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertInSetExpression(): Unit = {
    val filterExpr = sparkAdapter.translateFilter(
      InSet(AttributeReference("col1", StringType)(), Set("value1", "value2", "value3").map(UTF8String.fromString)))

    val result = SparkFilterHelper.convertFilter(filterExpr.get).get

    val expected = Predicates.in(
      new NameReference("col1"),
      Seq("value1", "value2", "value3").map(v => HLiteral.from(v).asInstanceOf[Expression]).asJava)

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertEqualToExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(EqualTo(AttributeReference("col1", LongType)(), Literal(1L)))
    val result = convertFilter(filter.get).get

    val expected = Predicates.eq(
      new NameReference("col1"),
      HLiteral.from(1L).asInstanceOf[Expression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertGreaterThanExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(GreaterThan(AttributeReference("col3", DoubleType)(), Literal(3.0D)))
    val result = convertFilter(filter.get).get

    val expected = Predicates.gt(
      new NameReference("col3"),
      HLiteral.from(3.0D).asInstanceOf[Expression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertGreaterThanOrEqualExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(GreaterThanOrEqual(AttributeReference("col4", FloatType)(), Literal(4.0f)))
    val result = convertFilter(filter.get).get

    val expected = Predicates.gteq(
      new NameReference("col4"),
      HLiteral.from(4.0f).asInstanceOf[Expression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertLessThanExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(LessThan(AttributeReference("col5", StringType)(), Literal("abc")))
    val result = convertFilter(filter.get).get

    val expected = Predicates.lt(
      new NameReference("col5"),
      HLiteral.from("abc").asInstanceOf[Expression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertLessThanOrEqualExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(LessThanOrEqual(AttributeReference("col6", BooleanType)(), Literal(true)))
    val result = convertFilter(filter.get).get

    val expected = Predicates.lteq(
      new NameReference("col6"),
      HLiteral.from(true).asInstanceOf[Expression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertStartsWithExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(StartsWith(AttributeReference("col2", StringType)(), Literal("prefix")))
    val result = convertFilter(filter.get).get

    val expected = Predicates.startsWith(
      new NameReference("col2"),
      HLiteral.from("prefix").asInstanceOf[Expression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertContainsExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(Contains(AttributeReference("col2", StringType)(), Literal("prefix")))
    val result = convertFilter(filter.get).get

    val expected = Predicates.contains(
      new NameReference("col2"),
      HLiteral.from("prefix").asInstanceOf[Expression])

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertAndExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(And(
      EqualTo(AttributeReference("col1", IntegerType)(), Literal(1)),
      GreaterThan(AttributeReference("col2", FloatType)(), Literal(2.0F))))
    val result = convertFilter(filter.get).get

    val expected = Predicates.and(
      Predicates.eq(
        new NameReference("col1"),
        HLiteral.from(1).asInstanceOf[Expression]),
      Predicates.gt(
        new NameReference("col2"),
        HLiteral.from(2.0F).asInstanceOf[Expression]))

    Assertions.assertEquals(result.toString, expected.toString)
  }

  @Test
  def testConvertOrEqualExpression(): Unit = {
    val filter = sparkAdapter.translateFilter(Or(
      LessThan(AttributeReference("col1", ShortType)(), Literal(10.toShort)),
      GreaterThanOrEqual(AttributeReference("col2", DoubleType)(), Literal(100.0D))))
    val result = convertFilter(filter.get).get

    val expected = Predicates.or(
      Predicates.lt(
        new NameReference("col1"),
        HLiteral.from(10.toShort).asInstanceOf[Expression]),
      Predicates.gteq(
        new NameReference("col2"),
        HLiteral.from(100.0D).asInstanceOf[Expression]))

    Assertions.assertEquals(result.toString, expected.toString)
  }
}
