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

package org.apache.spark.sql.hudi

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{Project, TimeTravelRelation}

class TestTimeTravelParser extends TestHoodieSqlBase {
  private val parser = spark.sessionState.sqlParser

  test("time travel of timestamp") {
    val timeTravelPlan1 = parser.parsePlan("SELECT * FROM A.B " +
      "TIMESTAMP AS OF '2019-01-29 00:37:58'")

    assertResult(Project(Seq(UnresolvedStar(None)),
      TimeTravelRelation(
        UnresolvedRelation(new TableIdentifier("B", Option.apply("A"))),
        Some(Literal("2019-01-29 00:37:58")),
        None))) {
      timeTravelPlan1
    }

    val timeTravelPlan2 = parser.parsePlan("SELECT * FROM A.B " +
      "TIMESTAMP AS OF 1643119574")

    assertResult(Project(Seq(UnresolvedStar(None)),
      TimeTravelRelation(
        UnresolvedRelation(new TableIdentifier("B", Option.apply("A"))),
        Some(Literal(1643119574)),
        None))) {
      timeTravelPlan2
    }
  }

  test("time travel of version") {
    val timeTravelPlan1 = parser.parsePlan("SELECT * FROM A.B " +
      "VERSION AS OF 'Snapshot123456789'")

    assertResult(Project(Seq(UnresolvedStar(None)),
      TimeTravelRelation(
        UnresolvedRelation(new TableIdentifier("B", Option.apply("A"))),
        None,
        Some("Snapshot123456789")))) {
      timeTravelPlan1
    }

    val timeTravelPlan2 = parser.parsePlan("SELECT * FROM A.B " +
      "VERSION AS OF 'Snapshot01'")

    assertResult(Project(Seq(UnresolvedStar(None)),
      TimeTravelRelation(
        UnresolvedRelation(new TableIdentifier("B", Option.apply("A"))),
        None,
        Some("Snapshot01")))) {
      timeTravelPlan2
    }
  }
}
