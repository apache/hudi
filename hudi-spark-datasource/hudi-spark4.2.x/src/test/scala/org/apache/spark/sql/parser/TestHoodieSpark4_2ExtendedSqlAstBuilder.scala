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

package org.apache.spark.sql.parser

import org.apache.hudi.spark.sql.parser.HoodieSqlBaseLexer

import org.antlr.v4.runtime.{CharStreams, ParserRuleContext}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class TestHoodieSpark4_2ExtendedSqlAstBuilder {

  private class Builder extends HoodieSpark4_2ExtendedSqlAstBuilder(null, null) {
    def validate(value: String): Unit = {
      val token = new HoodieSqlBaseLexer(CharStreams.fromString("x")).nextToken()
      val ctx = new ParserRuleContext()
      ctx.start = token
      ctx.stop = token
      validateLinesTerminatedBy(value, ctx)
    }
  }

  @Test
  def testLinesTerminatedByOnlyAcceptsNewline(): Unit = {
    val builder = new Builder

    builder.validate("\n")
    val error = assertThrows(classOf[ParseException], () => builder.validate(";"))
    assertTrue(error.getMessage.contains("LINES TERMINATED BY only supports newline"), error.getMessage)
  }
}
