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

import org.apache.hudi.{HoodieFileIndex, HoodieIncrementalFileIndex, ScalaAssertionSupport}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.function.Consumer

/**
 * Verifies that [[HoodieIncrementalRelationIdentifier]] enriches path-based incremental
 * reads (and only those), so that lineage tooling sees a real table identifier instead
 * of falling back to the relation's class name.
 */
class TestHoodieIncrementalRelationIdentifier extends HoodieClientTestBase with ScalaAssertionSupport {

  private var spark: SparkSession = _

  @BeforeEach
  override def setUp() {
    setTableName("hoodie_incr_id_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  override def getSparkSessionExtensionsInjector: org.apache.hudi.common.util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @ParameterizedTest
  @ValueSource(strings = Array("cow", "mor"))
  def testPathBasedIncrementalReadGetsCatalogTable(tableType: String): Unit = {
    val tablePath = s"$basePath/$tableName"
    createAndPopulateTable(tableType, tablePath)

    val firstInstant = firstCompletedInstantTime(tablePath)

    val df = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", firstInstant)
      .load(tablePath)

    val analyzed = df.queryExecution.analyzed

    val lrOpt = analyzed.collectFirst { case lr: LogicalRelation => lr }
    assertTrue(lrOpt.isDefined,
      s"Expected a LogicalRelation in analyzed plan, got:\n$analyzed")
    val lr = lrOpt.get

    // Sanity: confirm the path under test really exercises the incremental file index.
    val location = lr.relation.asInstanceOf[HadoopFsRelation].location
    assertTrue(location.isInstanceOf[HoodieIncrementalFileIndex],
      s"Expected HoodieIncrementalFileIndex, got: ${location.getClass.getName}")

    assertTrue(lr.catalogTable.isDefined,
      s"Expected catalogTable to be populated by HoodieIncrementalRelationIdentifier, got None")

    val ct = lr.catalogTable.get
    assertEquals(tableName, ct.identifier.table,
      s"Expected catalogTable identifier to use Hudi table name '$tableName', got '${ct.identifier.table}'")
    assertEquals(Some("hudi"), ct.provider,
      s"Expected provider 'hudi', got '${ct.provider}'")
    assertTrue(ct.storage.locationUri.isDefined,
      "Expected catalogTable.storage.locationUri to be populated from metaClient.getBasePath")
    assertTrue(ct.schema.fields.nonEmpty,
      "Expected catalogTable.schema to mirror the relation's resolved output schema")
  }

  @Test
  def testCatalogRegisteredIncrementalReadIsNotMutated(): Unit = {
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

    val analyzed = spark.sql(s"SELECT * FROM $tableName").queryExecution.analyzed
    val lrOpt = analyzed.collectFirst { case lr: LogicalRelation => lr }
    assertTrue(lrOpt.isDefined,
      s"Expected a LogicalRelation in analyzed plan, got:\n$analyzed")
    val lr = lrOpt.get

    assertTrue(lr.catalogTable.isDefined, "Expected catalogTable from catalog registration")
    assertEquals(tableName, lr.catalogTable.get.identifier.table)
  }

  @Test
  def testSnapshotPathBasedReadIsNotEnriched(): Unit = {
    val tablePath = s"$basePath/$tableName"
    createAndPopulateTable("cow", tablePath)

    val df = spark.read.format("hudi").load(tablePath)
    val analyzed = df.queryExecution.analyzed

    val lrOpt = analyzed.collectFirst { case lr: LogicalRelation => lr }
    assertTrue(lrOpt.isDefined,
      s"Expected a LogicalRelation in analyzed plan, got:\n$analyzed")
    val lr = lrOpt.get

    val location = lr.relation.asInstanceOf[HadoopFsRelation].location
    assertFalse(location.isInstanceOf[HoodieIncrementalFileIndex],
      s"Snapshot read should not produce a HoodieIncrementalFileIndex, got: ${location.getClass.getName}")
    assertTrue(location.isInstanceOf[HoodieFileIndex],
      s"Snapshot read should produce a HoodieFileIndex, got: ${location.getClass.getName}")
    assertFalse(lr.catalogTable.isDefined,
      "Snapshot path-based read must not be enriched by HoodieIncrementalRelationIdentifier")
  }

  // --- helpers ---

  private def createAndPopulateTable(tableType: String, tablePath: String): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) USING hudi
         |TBLPROPERTIES (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |LOCATION '$tablePath'
         """.stripMargin)
    // Two commits so the incremental range is never empty.
    spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10, 1000), (2, 'a2', 20, 2000)")
    spark.sql(s"INSERT INTO $tableName VALUES (3, 'a3', 30, 3000)")
  }

  private def firstCompletedInstantTime(tablePath: String): String = {
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(tablePath)
      .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration)).build()
    val instants: java.util.List[HoodieInstant] =
      metaClient.getCommitsTimeline.filterCompletedInstants.getInstants
    assertFalse(instants.isEmpty, s"Expected at least one completed instant at $tablePath")
    instants.get(0).requestedTime
  }
}
