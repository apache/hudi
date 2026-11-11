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

import org.apache.spark.sql.execution.SparkSqlParser
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test

/**
 * Validates that the extended parser forwards the [[org.apache.spark.sql.catalyst.parser.ParserInterface]]
 * members added in Spark 4.3 to Spark's parser.
 */
class TestHoodieSpark4_3ExtendedSqlParser {

  private val delegate = new SparkSqlParser()
  private val parser = new HoodieSpark4_3ExtendedSqlParser(null, delegate)

  @Test
  def testParseTemporalTableIdentifierKeepsTimeTravelSuffix(): Unit = {
    val identifier = parser.parseTemporalTableIdentifier("db.tbl@v1")

    assertEquals(Seq("db", "tbl"), identifier.nameParts)
    assertEquals(Some("1"), identifier.version)
    assertFalse(parser.parseTemporalTableIdentifier("db.tbl").isTemporal)
  }

  @Test
  def testSplitStatementsIgnoresSemicolonsInLiterals(): Unit = {
    val result = parser.splitStatements("select ';'; alter table t compaction; select 1")

    assertEquals(delegate.splitStatements("select ';'; alter table t compaction; select 1"), result)
    assertEquals(2, result.completeStatements.size)
    assertEquals("select ';'", result.completeStatements.head.statement)
  }
}
