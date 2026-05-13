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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.ScalaAssertionSupport
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import java.util.function.Consumer

/**
 * Guards both the leaf-ness of [[MergeIntoHoodieTableCommand]] (Catalyst optimizer
 * safety) and the new `innerChildren` exposure (lineage / EXPLAIN reachability).
 */
class TestMergeIntoHoodieTableCommandInnerChildren extends HoodieClientTestBase with ScalaAssertionSupport {

  private var spark: SparkSession = _
  private var sourceTableName: String = _

  @BeforeEach
  override def setUp() {
    setTableName("hoodie_merge_target")
    sourceTableName = "hoodie_merge_source"
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  override def getSparkSessionExtensionsInjector: org.apache.hudi.common.util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @Test
  def testMergeIntoExposesAnalyzedMergeIntoTableViaInnerChildren(): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |LOCATION '$basePath/$tableName'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")

    spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW $sourceTableName AS " +
      s"SELECT 1 AS id, 'a1_updated' AS name, 99.0 AS price, 2000L AS ts " +
      s"UNION ALL " +
      s"SELECT 2, 'a2', 20.0, 2000L")

    val mergeSql =
      s"""
         |MERGE INTO $tableName AS t
         |USING $sourceTableName AS s
         |ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.name = s.name, t.price = s.price, t.ts = s.ts
         |WHEN NOT MATCHED THEN INSERT (id, name, price, ts) VALUES (s.id, s.name, s.price, s.ts)
         """.stripMargin

    val analyzed = spark.sql(mergeSql).queryExecution.analyzed

    val cmdOpt = analyzed.collectFirst { case c: MergeIntoHoodieTableCommand => c }
    assertTrue(cmdOpt.isDefined,
      s"Expected MergeIntoHoodieTableCommand in analyzed plan, got:\n$analyzed")
    val cmd = cmdOpt.get

    assertEquals(0, cmd.children.size,
      s"MergeIntoHoodieTableCommand must remain a Catalyst leaf, got children: ${cmd.children}")

    assertEquals(1, cmd.innerChildren.size,
      s"Expected innerChildren to expose exactly one node (MergeIntoTable), got: ${cmd.innerChildren}")

    val inner = cmd.innerChildren.head
    assertTrue(inner.isInstanceOf[MergeIntoTable],
      s"Expected innerChildren(0) to be MergeIntoTable, got: ${inner.getClass.getName}")

    val mergeIntoTable = inner.asInstanceOf[MergeIntoTable]

    val sourceLeaves = mergeIntoTable.sourceTable.collectLeaves()
    assertTrue(sourceLeaves.nonEmpty,
      s"Expected at least one leaf in source plan, got: ${mergeIntoTable.sourceTable}")

    val targetLeaves = mergeIntoTable.targetTable.collectLeaves()
    assertTrue(targetLeaves.exists(_.isInstanceOf[LogicalRelation]),
      s"Expected target plan to contain a LogicalRelation, got: ${mergeIntoTable.targetTable}")

    assertTrue(mergeIntoTable.mergeCondition != null, "Expected non-null mergeCondition")
    assertEquals(1, mergeIntoTable.matchedActions.size,
      s"Expected exactly one matched action, got: ${mergeIntoTable.matchedActions}")
    assertEquals(1, mergeIntoTable.notMatchedActions.size,
      s"Expected exactly one notMatched action, got: ${mergeIntoTable.notMatchedActions}")
  }

  @Test
  def testExplainShowsSourceTableViaInnerChildren(): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id int,
         |  name string,
         |  ts long
         |) USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |LOCATION '$basePath/$tableName'
         """.stripMargin)

    spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 1000)")

    spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW $sourceTableName AS " +
      s"SELECT 1 AS id, 'a1_updated' AS name, 2000L AS ts")

    val mergeSql =
      s"""
         |MERGE INTO $tableName AS t
         |USING $sourceTableName AS s
         |ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.name = s.name, t.ts = s.ts
         """.stripMargin

    val explain = spark.sql(s"EXPLAIN $mergeSql").collect().map(_.getString(0)).mkString("\n")

    assertTrue(explain.contains("MergeIntoHoodieTableCommand"),
      s"Expected EXPLAIN output to contain MergeIntoHoodieTableCommand, got:\n$explain")
    assertTrue(explain.contains(sourceTableName),
      s"Expected EXPLAIN output to contain source view '$sourceTableName' via innerChildren, got:\n$explain")
  }
}
