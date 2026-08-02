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

package org.apache.spark.sql.hudi.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue, Transform}
import org.scalatest.Assertions

/**
 * Shared helpers for the extended-parser coverage tests (e.g. TestCreateTable, TestBlobDataType),
 * which route a full CREATE TABLE through the extended AST builder and pull partition transforms and
 * their arguments out of the resulting [[CreateTable]] plan. Kept in one place so the parse entry
 * point and the transform inspection stay consistent across the per-type test suites that share it.
 */
trait ExtendedParserTestHelpers extends Assertions {

  /** Satisfied by HoodieSparkSqlTestBase's `protected lazy val spark`. */
  protected def spark: SparkSession

  protected def parseCreateTable(sql: String): CreateTable =
    spark.sessionState.sqlParser.parsePlan(sql).asInstanceOf[CreateTable]

  /**
   * Asserts that `sql` fails with a [[ParseException]] (not merely some Exception) whose message
   * contains `expected`. Matching stays on a substring so the same assertion holds on Spark 4.x,
   * where the builders wrap the message in an "Operation not allowed: " prefix.
   */
  protected def interceptParse(sql: String)(expected: String): Unit = {
    val e = intercept[ParseException] {
      spark.sql(sql)
    }
    assert(e.getMessage.contains(expected), s"actual: ${e.getMessage}")
  }

  protected def transformByName(plan: CreateTable, name: String): Transform =
    plan.partitioning.find(_.name == name)
      .getOrElse(fail(s"No partition transform named $name in ${plan.partitioning.mkString(", ")}"))

  protected def transformFieldRefs(t: Transform): Seq[Seq[String]] =
    t.arguments().collect { case r: FieldReference => r.fieldNames().toSeq }.toSeq

  protected def firstLiteralArg(t: Transform): LiteralValue[_] =
    t.arguments().collectFirst { case l: LiteralValue[_] => l }
      .getOrElse(fail(s"No literal argument in transform ${t.name}"))
}
