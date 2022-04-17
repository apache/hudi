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

package org.apache.spark.sql.hudi.index

import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.spark.sql.catalyst.plans.logical.{CreateIndexCommand, DropIndexCommand, RefreshIndexCommand, ShowIndexCommand}
import org.apache.spark.sql.hudi.TestHoodieSqlBase
import org.apache.spark.sql.hudi.command.index.HoodieTableIndexColumn

class TestIndexCommandParser extends TestHoodieSqlBase {
  private val parser = spark.sessionState.sqlParser

  test("Test create index command parser") {
    var createIndex = parser.parsePlan(
      s"""CREATE INDEX index1 ON db.hudi_table1 (a, b)
         | USING BLOOM""".stripMargin
    ).asInstanceOf[CreateIndexCommand]
    assertResult("index1")(createIndex.indexName)
    assertResult(IndexType.BLOOM)(createIndex.indexType)

    createIndex = parser.parsePlan(
      s"""CREATE INDEX index2 ON db.hudi_table1 (a, b)
         | USING BLOOM PROPERTIES(a=1, b=2)""".stripMargin
    ).asInstanceOf[CreateIndexCommand]
    assertResult("index2")(createIndex.indexName)
    assertResult(Set("a", "b"))(createIndex.indexProperties.get.keys)

    createIndex = parser.parsePlan(
      s"""CREATE INDEX IF NOT EXISTS index3 ON TABLE db.hudi_table1 (a, b)
         | USING BLOOM PROPERTIES(a=1, b=2)""".stripMargin
    ).asInstanceOf[CreateIndexCommand]
    assertResult("index3")(createIndex.indexName)
    assertResult(IndexType.BLOOM)(createIndex.indexType)
    assertResult(createIndex.indexColumns)(Seq(
      HoodieTableIndexColumn("a", isAscending = true),
      HoodieTableIndexColumn("b", isAscending = true)))

    createIndex = parser.parsePlan(
      s"""CREATE INDEX IF NOT EXISTS index4 ON TABLE db.hudi_table1 (a ASC, b DESC)
         | USING BLOOM PROPERTIES(a=1, b=2)""".stripMargin
    ).asInstanceOf[CreateIndexCommand]
    assertResult("index4")(createIndex.indexName)
    assertResult(IndexType.BLOOM)(createIndex.indexType)
    assertResult(createIndex.indexColumns)(Seq(
      HoodieTableIndexColumn("a", isAscending = true),
      HoodieTableIndexColumn("b", isAscending = false)))
  }

  test("Test drop index command parser") {
    var dropIndex = parser.parsePlan(
      s"""DROP INDEX index_a ON db.hudi_table1""".stripMargin
    ).asInstanceOf[DropIndexCommand]
    assertResult("index_a")(dropIndex.indexName)

    dropIndex = parser.parsePlan(
      s"""DROP INDEX IF EXISTS index_b ON TABLE db.hudi_table1""".stripMargin
    ).asInstanceOf[DropIndexCommand]
    assertResult("index_b")(dropIndex.indexName)

    dropIndex = parser.parsePlan(
      s"""DROP INDEX IF EXISTS index_c ON db.hudi_table1""".stripMargin
    ).asInstanceOf[DropIndexCommand]
    assertResult("index_c")(dropIndex.indexName)
  }

  test("Test refresh index command parser") {
    var refreshIndex = parser.parsePlan(
      s"""REFRESH INDEX index_a ON db.hudi_table1""".stripMargin
    ).asInstanceOf[RefreshIndexCommand]
    assertResult("index_a")(refreshIndex.indexName)

    refreshIndex = parser.parsePlan(
      s"""REFRESH INDEX index_b ON TABLE db.hudi_table1""".stripMargin
    ).asInstanceOf[RefreshIndexCommand]
    assertResult("index_b")(refreshIndex.indexName)

    refreshIndex = parser.parsePlan(
      s"""REFRESH INDEX index_c ON db.hudi_table1""".stripMargin
    ).asInstanceOf[RefreshIndexCommand]
    assertResult("index_c")(refreshIndex.indexName)
  }

  test("Test show index command parser") {
    var showIndex = parser.parsePlan(
      s"""SHOW INDEX ON db.hudi_table1""".stripMargin
    ).asInstanceOf[ShowIndexCommand]
    assertResult(true)(showIndex.indexName.isEmpty)

    showIndex = parser.parsePlan(
      s"""SHOW INDEX index_b ON db.hudi_table1""".stripMargin
    ).asInstanceOf[ShowIndexCommand]
    assertResult("index_b")(showIndex.indexName.get)

    showIndex = parser.parsePlan(
      s"""SHOW INDEX index_c ON TABLE db.hudi_table1""".stripMargin
    ).asInstanceOf[ShowIndexCommand]
    assertResult("index_c")(showIndex.indexName.get)
  }
}
