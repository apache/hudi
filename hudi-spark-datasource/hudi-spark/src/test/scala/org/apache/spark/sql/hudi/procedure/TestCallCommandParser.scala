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

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.util.CollectionUtils.createImmutableList

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{CallCommand, NamedArgument, PositionalArgument}
import org.apache.spark.sql.hudi.command.procedures.HelpProcedure
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.math.BigDecimal

import scala.collection.JavaConverters

class TestCallCommandParser extends HoodieSparkSqlTestBase {

  test("Test Call Produce with Positional Arguments") {
    val call = spark.sql(s"CALL c.n.${HelpProcedure.NAME}('help', 2, 3L, true, 1.0D, 9.0e1, 900e-1BD)").queryExecution.logical.asInstanceOf[CallCommand]
    assertResult(createImmutableList("c", "n", HelpProcedure.NAME))(JavaConverters.seqAsJavaListConverter(call.name).asJava)

    assertResult(7)(call.args.size)

    checkArg(call, 0, "help", DataTypes.StringType)
    checkArg(call, 1, 2, DataTypes.IntegerType)
    checkArg(call, 2, 3L, DataTypes.LongType)
    checkArg(call, 3, true, DataTypes.BooleanType)
    checkArg(call, 4, 1.0D, DataTypes.DoubleType)
    checkArg(call, 5, 9.0e1, DataTypes.DoubleType)
    checkArg(call, 6, new BigDecimal("900e-1"), DataTypes.createDecimalType(3, 1))
  }

  test("Test Call Produce with Named Arguments") {
    val call = spark.sql(s"CALL system.${HelpProcedure.NAME}(cmd => 'help', c1 => 1, c2 => '2', c3 => true)").queryExecution.logical.asInstanceOf[CallCommand]
    assertResult(createImmutableList("system", HelpProcedure.NAME))(JavaConverters.seqAsJavaListConverter(call.name).asJava)

    assertResult(4)(call.args.size)

    checkArg(call, 0, "cmd","help", DataTypes.StringType)
    checkArg(call, 1, "c1", 1, DataTypes.IntegerType)
    checkArg(call, 2, "c2", "2", DataTypes.StringType)
    checkArg(call, 3, "c3", true, DataTypes.BooleanType)
  }

  test("Test Call Produce with Var Substitution") {
    val call = spark.sql(s"CALL system.${HelpProcedure.NAME}('help', '$${spark.extra.prop}')").queryExecution.logical.asInstanceOf[CallCommand]
    assertResult(createImmutableList("system", HelpProcedure.NAME))(JavaConverters.seqAsJavaListConverter(call.name).asJava)

    assertResult(2)(call.args.size)

    checkArg(call, 0, "help", DataTypes.StringType)
    checkArg(call, 1, "value", DataTypes.StringType)
  }

  test("Test Call Produce with Mixed Arguments") {
    val call = spark.sql(s"CALL system.${HelpProcedure.NAME}('help', c1 => 1, '2')").queryExecution.logical.asInstanceOf[CallCommand]
    assertResult(createImmutableList("system", HelpProcedure.NAME))(JavaConverters.seqAsJavaListConverter(call.name).asJava)

    assertResult(3)(call.args.size)

    checkArg(call, 0, "help", DataTypes.StringType)
    checkArg(call, 1, "c1", 1, DataTypes.IntegerType)
    checkArg(call, 2, "2", DataTypes.StringType)
  }

  test("Test Call Parse Error") {
    checkParseExceptionContain("CALL cat.system radish kebab")(s"Syntax error at or near '${if (HoodieSparkUtils.gteqSpark4_0) "radish" else "CALL"}'")
  }

  test("Test Call Produce with semicolon") {
    val call = spark.sql(s"CALL system.${HelpProcedure.NAME}('help', c1 => 1, '2');").queryExecution.logical.asInstanceOf[CallCommand]
    assertResult(createImmutableList("system", HelpProcedure.NAME))(JavaConverters.seqAsJavaListConverter(call.name).asJava)

    assertResult(3)(call.args.size)

    checkArg(call, 0, "help", DataTypes.StringType)
    checkArg(call, 1, "c1", 1, DataTypes.IntegerType)
    checkArg(call, 2, "2", DataTypes.StringType)
  }

  protected def checkParseExceptionContain(sql: String)(errorMsg: String): Unit = {
    var hasException = false
    try {
      spark.sql(sql)
    } catch {
      case e: Throwable if e.getMessage.contains(errorMsg) => hasException = true
      case f: Throwable => fail("Exception should contain: " + errorMsg + ", error message: " + f.getMessage, f)
    }
    assertResult(true)(hasException)
  }

  private def checkArg(call: CallCommand, index: Int, expectedValue: Any, expectedType: DataType): Unit = {
    checkArg(call, index, null, expectedValue, expectedType)
  }

  private def checkArg(call: CallCommand, index: Int, expectedName: String, expectedValue: Any, expectedType: DataType): Unit = {
    if (expectedName != null) {
      val arg = checkCast(call.args.apply(index), classOf[NamedArgument])
      assertResult(expectedName)(arg.name)
    }
    else {
      val arg = call.args.apply(index)
      checkCast(arg, classOf[PositionalArgument])
    }
    val expectedExpr = toSparkLiteral(expectedValue, expectedType)
    val actualExpr = call.args.apply(index).expr
    assertResult(expectedExpr.dataType)(actualExpr.dataType)
  }

  private def toSparkLiteral(value: Any, dataType: DataType) = Literal.create(value, dataType)

  private def checkCast[T](value: Any, expectedClass: Class[T]) = {
    assertResult(true)(expectedClass.isInstance(value))
    expectedClass.cast(value)
  }
}
