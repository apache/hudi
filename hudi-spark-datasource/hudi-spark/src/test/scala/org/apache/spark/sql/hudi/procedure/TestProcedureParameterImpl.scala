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

package org.apache.spark.sql.hudi.procedure

import org.apache.spark.sql.hudi.command.procedures.ProcedureParameter
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertTrue}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for ProcedureParameterImpl.equals. The implementation used to call this == other for
 * the identity check, which dispatches straight back into equals and recurses until it overflows
 * the stack, and it cast the argument before the null/type guard, so a foreign argument threw
 * ClassCastException. Both are self-contained checks, so no Spark session is needed.
 */
class TestProcedureParameterImpl extends AnyFunSuite {

  test("Test ProcedureParameterImpl equals identity, foreign types and null") {
    val param = ProcedureParameter.optional(0, "table", DataTypes.StringType, "default")

    // Self comparison must terminate instead of recursing into equals.
    assertTrue(param == param)
    assertTrue(param.equals(param))

    // A foreign type and null are rejected by the guard rather than blowing up on the cast.
    assertFalse(param.equals("notAParam"))
    assertFalse(param.equals(null))
  }

  test("Test ProcedureParameterImpl equals and hashCode over field values") {
    val required = ProcedureParameter.required(2, "instant", DataTypes.StringType)
    val sameAsRequired = ProcedureParameter.required(2, "instant", DataTypes.StringType)
    assertEquals(required, sameAsRequired)
    assertEquals(required.hashCode(), sameAsRequired.hashCode())

    val optional = ProcedureParameter.optional(1, "dry_run", DataTypes.BooleanType, true)
    val sameAsOptional = ProcedureParameter.optional(1, "dry_run", DataTypes.BooleanType, true)
    assertEquals(optional, sameAsOptional)
    assertEquals(optional.hashCode(), sameAsOptional.hashCode())

    // One differing field at a time.
    assertNotEquals(optional, ProcedureParameter.optional(2, "dry_run", DataTypes.BooleanType, true))
    assertNotEquals(optional, ProcedureParameter.optional(1, "backup", DataTypes.BooleanType, true))
    assertNotEquals(optional, ProcedureParameter.optional(1, "dry_run", DataTypes.StringType, true))
    assertNotEquals(optional, ProcedureParameter.optional(1, "dry_run", DataTypes.BooleanType, false))
    assertNotEquals(optional, ProcedureParameter.required(1, "dry_run", DataTypes.BooleanType))
  }
}
