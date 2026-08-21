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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNull, assertTrue}
import org.junit.jupiter.api.Test

class TestProcedureParameter {

  @Test
  def testRequiredParameter(): Unit = {
    val p = ProcedureParameter.required(0, "path", DataTypes.StringType)
    assertEquals(0, p.index)
    assertEquals("path", p.name)
    assertEquals(DataTypes.StringType, p.dataType)
    assertTrue(p.required)
    assertNull(p.default)
  }

  @Test
  def testOptionalParameterWithDefault(): Unit = {
    val p = ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 10)
    assertEquals(2, p.index)
    assertEquals("limit", p.name)
    assertEquals(DataTypes.IntegerType, p.dataType)
    assertFalse(p.required)
    assertEquals(10, p.default.asInstanceOf[Int])
  }

  @Test
  def testHashCodeIsContentBased(): Unit = {
    val a = ProcedureParameter.optional(1, "col", DataTypes.StringType, "def")
    val b = ProcedureParameter.optional(1, "col", DataTypes.StringType, "def")
    // hashCode is derived from all fields, so equal-valued parameters share a hash.
    assertEquals(a.hashCode, b.hashCode)
  }

  @Test
  def testEqualsGuardsAgainstRegressions(): Unit = {
    val a = ProcedureParameter.optional(1, "col", DataTypes.StringType, "def")
    val b = ProcedureParameter.optional(1, "col", DataTypes.StringType, "def")
    // Equal-valued params are equal; guards the self-recursive `equals` (StackOverflow) fixed in #19167.
    assertEquals(a, b)
    // A wrong-type argument returns false instead of throwing ClassCastException (also #19167).
    assertFalse(a.equals("not a param"))
  }

  @Test
  def testToStringContainsAllFields(): Unit = {
    val p = ProcedureParameter.optional(1, "col", DataTypes.StringType, "def")
    val s = p.toString
    assertTrue(s.contains("index='1'"))
    assertTrue(s.contains("name='col'"))
    assertTrue(s.contains("required=false"))
    assertTrue(s.contains("default=def"))
  }
}
