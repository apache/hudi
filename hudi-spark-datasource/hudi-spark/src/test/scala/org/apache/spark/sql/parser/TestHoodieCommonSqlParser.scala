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

import org.apache.hudi.HoodieSparkUtils

import org.apache.spark.sql.execution.SparkSqlParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

/**
 * Validates that the session parser forwards the members added to `ParserInterface` in Spark 4.3.
 * They are invoked reflectively because this test compiles against every Spark version.
 */
class TestHoodieCommonSqlParser {

  @Test
  def testForwardsSpark43ParserMembers(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_3)
    val delegate = new SparkSqlParser()
    val parser = new HoodieCommonSqlParser(null, delegate)

    Seq("parseTemporalTableIdentifier" -> "db.tbl@v1", "splitStatements" -> "select ';'; select 1").foreach {
      case (method, sql) =>
        assertEquals(invoke(delegate, method, sql), invoke(parser, method, sql))
    }
  }

  private def invoke(target: AnyRef, method: String, sql: String): AnyRef =
    target.getClass.getMethod(method, classOf[String]).invoke(target, sql)
}
