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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, Contains, EndsWith, EqualNullSafe, EqualTo, Expression, GreaterThan, In, IsNotNull, IsNull, LessThanOrEqual, Literal, Not, Or, StartsWith}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame}
import org.junit.jupiter.api.Test

/**
 * Coverage for [[FileFormatUtilsForFileGroupReader.applyFiltersToPlan]], which lowers pushed-down
 * data-source [[org.apache.spark.sql.sources.Filter]]s into a Catalyst [[Filter]] on the plan. Each
 * case pins the exact translated Catalyst expression so a wrong mapping would fail, and the
 * no-filter case must return the input plan untouched.
 */
class TestFileFormatUtilsForFileGroupReader {

  private val a: Attribute = AttributeReference("a", IntegerType)()
  private val b: Attribute = AttributeReference("b", StringType)()
  private val tableSchema: StructType = new StructType().add("a", IntegerType).add("b", StringType)
  private val resolved: Seq[Attribute] = Seq(a, b)
  private val plan: LocalRelation = LocalRelation(a, b)

  private def condOf(filters: Seq[sources.Filter]): Expression =
    FileFormatUtilsForFileGroupReader.applyFiltersToPlan(plan, tableSchema, resolved, filters) match {
      case Filter(cond, child) =>
        assertSame(plan, child)
        cond
      case other => throw new AssertionError(s"expected a Filter, got $other")
    }

  @Test
  def testComparisonAndNullFilters(): Unit = {
    assertEquals(EqualTo(a, Literal(1)), condOf(Seq(sources.EqualTo("a", 1))))
    assertEquals(EqualNullSafe(a, Literal(1)), condOf(Seq(sources.EqualNullSafe("a", 1))))
    assertEquals(GreaterThan(a, Literal(5)), condOf(Seq(sources.GreaterThan("a", 5))))
    assertEquals(LessThanOrEqual(a, Literal(5)), condOf(Seq(sources.LessThanOrEqual("a", 5))))
    assertEquals(IsNull(b), condOf(Seq(sources.IsNull("b"))))
    assertEquals(IsNotNull(b), condOf(Seq(sources.IsNotNull("b"))))
  }

  @Test
  def testStringAndInFilters(): Unit = {
    assertEquals(Contains(b, Literal("x")), condOf(Seq(sources.StringContains("b", "x"))))
    assertEquals(StartsWith(b, Literal("x")), condOf(Seq(sources.StringStartsWith("b", "x"))))
    assertEquals(EndsWith(b, Literal("x")), condOf(Seq(sources.StringEndsWith("b", "x"))))
    assertEquals(
      In(a, Seq(Literal(1), Literal(2), Literal(3))),
      condOf(Seq(sources.In("a", Array[Any](1, 2, 3)))))
  }

  @Test
  def testConstantFilters(): Unit = {
    assertEquals(Literal(true, BooleanType), condOf(Seq(sources.AlwaysTrue())))
    assertEquals(Literal(false, BooleanType), condOf(Seq(sources.AlwaysFalse())))
  }

  @Test
  def testCompositeFilters(): Unit = {
    // A nested and/or/not tree is lowered structurally.
    assertEquals(
      Or(EqualTo(a, Literal(1)), Not(IsNull(b))),
      condOf(Seq(sources.Or(sources.EqualTo("a", 1), sources.Not(sources.IsNull("b"))))))
  }

  @Test
  def testMultipleFiltersAreAndedInOrder(): Unit = {
    // Several top-level filters combine left-to-right via And.
    assertEquals(
      And(GreaterThan(a, Literal(5)), IsNotNull(b)),
      condOf(Seq(sources.GreaterThan("a", 5), sources.IsNotNull("b"))))
  }

  @Test
  def testNoFiltersReturnsPlanUnchanged(): Unit = {
    val result = FileFormatUtilsForFileGroupReader.applyFiltersToPlan(
      plan, tableSchema, resolved, Seq.empty)
    assertSame(plan, result)
  }
}
