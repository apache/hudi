/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.command.index

import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.command.{CreateIndexCommand, DropIndexCommand, ShowIndexesCommand}

class TestIndexSyntax extends HoodieSparkSqlTestBase {
  private val sqlParser: ParserInterface = spark.sessionState.sqlParser
  private val analyzer: Analyzer = spark.sessionState.analyzer

  test("Test Create/Drop/Show/Refresh Index") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

        var logicalPlan = sqlParser.parsePlan(s"show indexes from default.$tableName")
        var resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertResult(s"`default`.`$tableName`")(resolvedLogicalPlan.asInstanceOf[ShowIndexesCommand].tableId.toString())

        logicalPlan = sqlParser.parsePlan(s"create index idx_name on $tableName using lucene (name) options(block_size=1024)")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertResult(s"`default`.`$tableName`")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].tableId.toString())
        assertResult("idx_name")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("lucene")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
        assertResult(Map("block_size" -> "1024"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].properties)

        logicalPlan = sqlParser.parsePlan(s"create index if not exists idx_price on $tableName using lucene (price options(order='desc')) options(block_size=512)")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertResult(s"`default`.`$tableName`")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].tableId.toString())
        assertResult("idx_price")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("lucene")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(Map("order" -> "desc"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].columns.head._2)
        assertResult(Map("block_size" -> "512"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].properties)

        logicalPlan = sqlParser.parsePlan(s"drop index if exists idx_name on $tableName")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertResult(s"`default`.`$tableName`")(resolvedLogicalPlan.asInstanceOf[DropIndexCommand].tableId.toString())
        assertResult("idx_name")(resolvedLogicalPlan.asInstanceOf[DropIndexCommand].indexName)
        assertResult(true)(resolvedLogicalPlan.asInstanceOf[DropIndexCommand].ignoreIfNotExists)
      }
    }
  }
}
