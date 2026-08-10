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

package org.apache.spark.sql.hive

import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.hive.HiveSyncConfig
import org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest

import org.apache.hadoop.hive.metastore.api.{Database, EnvironmentContext, FieldSchema, Partition, SerDeInfo, StorageDescriptor, Table}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.File
import java.nio.file.Files
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

class TestSparkCatalogMetaStoreClient extends FunSuite with BeforeAndAfterAll {

  private val warehouseDir = Files.createTempDirectory("spark-catalog-metastore-client").toFile
  private val nameId = new AtomicInteger(0)

  private lazy val spark: SparkSession = {
    val sparkConf = getSparkConfForTest("TestSparkCatalogMetaStoreClient")
      .remove("spark.sql.catalog.spark_catalog")

    SparkSession.builder()
      .config("spark.sql.warehouse.dir", warehouseDir.getCanonicalPath)
      .config("spark.sql.session.timeZone", "UTC")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)
  }

  override protected def afterAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    if (!spark.sparkContext.isStopped) {
      spark.stop()
    }
    Utils.deleteRecursively(warehouseDir)
    super.afterAll()
  }

  override protected def test(testName: String, testTags: org.scalatest.Tag*)(testFun: => Any)(implicit pos: source.Position): Unit = {
    super.test(testName, testTags: _*)(
      try {
        testFun
      } finally {
        spark.sessionState.catalog.listDatabases().filter(_.startsWith("db_")).foreach { db =>
          spark.sql(s"drop database if exists $db cascade")
        }
      }
    )
  }

  test("exercise supported database and table APIs") {
    withTempDir { tmp =>
      val client = newClient()
      val databaseName = generateName("db")
      val tableName = generateName("tbl")

      client.createDatabase(new Database(databaseName, "test database", new File(tmp, databaseName).toURI.toString, new util.HashMap[String, String]()))
      assertEquals(databaseName, client.getDatabase(databaseName).getName)

      val createdTable = newTable(
        databaseName,
        tableName,
        new File(tmp, tableName).toURI.toString,
        Seq("id" -> "int", "name" -> "string"),
        Seq("dt" -> "string"),
        Map("comment" -> "v1"))

      client.createTable(createdTable)

      assertTrue(client.tableExists(databaseName, tableName))
      assertEquals(Seq("id", "name", "dt"), client.getSchema(databaseName, tableName).asScala.map(_.getName).toSeq)
      assertEquals("v1", client.getTable(databaseName, tableName).getParameters.get("comment"))

      val alteredTable = newTable(
        databaseName,
        tableName,
        new File(tmp, s"${tableName}_v2").toURI.toString,
        Seq("id" -> "int", "name" -> "string", "age" -> "int"),
        Seq("dt" -> "string"),
        Map("comment" -> "v2"))

      client.alter_table(databaseName, tableName, alteredTable)
      assertEquals(Seq("id", "name", "age", "dt"), client.getSchema(databaseName, tableName).asScala.map(_.getName).toSeq)
      assertEquals("v2", client.getTable(databaseName, tableName).getParameters.get("comment"))

      val environmentAlteredTable = newTable(
        databaseName,
        tableName,
        new File(tmp, s"${tableName}_v3").toURI.toString,
        Seq("id" -> "int", "name" -> "string", "age" -> "int"),
        Seq("dt" -> "string"),
        Map("comment" -> "env-context"))

      client.alter_table_with_environmentContext(databaseName, tableName, environmentAlteredTable, new EnvironmentContext())
      assertEquals("env-context", client.getTable(databaseName, tableName).getParameters.get("comment"))
    }
  }

  test("exercise supported partition and drop APIs") {
    withTempDir { tmp =>
      val client = newClient()
      val databaseName = generateName("db")
      val tableName = generateName("tbl")

      client.createDatabase(new Database(databaseName, "test database", new File(tmp, databaseName).toURI.toString, new util.HashMap[String, String]()))
      client.createTable(newTable(
        databaseName,
        tableName,
        new File(tmp, tableName).toURI.toString,
        Seq("id" -> "int"),
        Seq("dt" -> "string")))

      val partitionOne = newPartition(databaseName, tableName, Seq("2024-01-01"), new File(tmp, s"$tableName/dt=2024-01-01").toURI.toString)
      val partitionTwo = newPartition(databaseName, tableName, Seq("2024-01-02"), new File(tmp, s"$tableName/dt=2024-01-02").toURI.toString)

      val added = client.add_partitions(util.Arrays.asList(partitionOne, partitionTwo), false, true)
      assertEquals(2, added.size())

      val listedPartitions = client.listPartitions(databaseName, tableName, (-1).toShort).asScala.toSeq
      assertEquals(Set("2024-01-01", "2024-01-02"), listedPartitions.map(_.getValues.get(0)).toSet)
      assertNotNull(listedPartitions.find(_.getValues.get(0) == "2024-01-02").orNull)

      val listedByFilter = client.listPartitionsByFilter(databaseName, tableName, "dt='2024-01-02'", (-1).toShort).asScala.toSeq
      assertEquals(Set("2024-01-01", "2024-01-02"), listedByFilter.map(_.getValues.get(0)).toSet)

      val alteredPartition = newPartition(
        databaseName,
        tableName,
        Seq("2024-01-02"),
        new File(tmp, s"$tableName/dt=2024-01-02-updated").toURI.toString)
      client.alter_partitions(databaseName, tableName, util.Collections.singletonList(alteredPartition), new EnvironmentContext())

      val updatedLocation = client.listPartitions(databaseName, tableName, (-1).toShort).asScala
        .find(_.getValues.get(0) == "2024-01-02")
        .map(_.getSd.getLocation)
        .orNull
      assertTrue(updatedLocation.endsWith("dt=2024-01-02-updated"))

      assertTrue(client.dropPartition(databaseName, tableName, "dt=2024-01-01", false))
      val remainingPartitions = client.listPartitions(databaseName, tableName, (-1).toShort).asScala.toSeq
      assertEquals(Seq("2024-01-02"), remainingPartitions.map(_.getValues.get(0)))

      client.dropTable(databaseName, tableName)
      assertFalse(client.tableExists(databaseName, tableName))
    }
  }

  test("createTable accepts EXTERNAL=TRUE parameter (mirrors HMSDDLExecutor behavior)") {
    withTempDir { tmp =>
      val client = newClient()
      val databaseName = generateName("db")
      val tableName = generateName("tbl")

      client.createDatabase(new Database(databaseName, "test database", new File(tmp, databaseName).toURI.toString, new util.HashMap[String, String]()))

      // Hudi's HMSDDLExecutor.createTable sets BOTH `tableType=EXTERNAL_TABLE` and
      // `parameters[EXTERNAL]=TRUE` on the Hive Table object. Spark's
      // HiveExternalCatalog.verifyTableProperties rejects "EXTERNAL" as a property key
      // unless we strip it in toCatalogTable. This test mirrors that real-world shape.
      val createdTable = newTable(
        databaseName,
        tableName,
        new File(tmp, tableName).toURI.toString,
        Seq("id" -> "int", "name" -> "string"),
        Seq("dt" -> "string"),
        Map("EXTERNAL" -> "TRUE", "comment" -> "v1"))

      client.createTable(createdTable)
      assertTrue(client.tableExists(databaseName, tableName))
      assertEquals("v1", client.getTable(databaseName, tableName).getParameters.get("comment"))
    }
  }

  private def newClient(): SparkCatalogMetaStoreClient = {
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)
    new SparkCatalogMetaStoreClient(new HiveSyncConfig(new TypedProperties()))
  }

  private def newTable(databaseName: String,
                       tableName: String,
                       location: String,
                       columns: Seq[(String, String)],
                       partitionColumns: Seq[(String, String)],
                       parameters: Map[String, String] = Map.empty): Table = {
    val table = new Table()
    table.setDbName(databaseName)
    table.setTableName(tableName)
    table.setTableType("EXTERNAL_TABLE")
    table.setParameters(new util.HashMap[String, String](parameters.asJava))
    table.setPartitionKeys(partitionColumns.map { case (name, dataType) => fieldSchema(name, dataType) }.asJava)

    val serdeInfo = new SerDeInfo()
    serdeInfo.setParameters(new util.HashMap[String, String]())

    val storageDescriptor = new StorageDescriptor()
    storageDescriptor.setCols(columns.map { case (name, dataType) => fieldSchema(name, dataType) }.asJava)
    storageDescriptor.setLocation(location)
    storageDescriptor.setSerdeInfo(serdeInfo)
    table.setSd(storageDescriptor)
    table
  }

  private def newPartition(databaseName: String,
                           tableName: String,
                           values: Seq[String],
                           location: String): Partition = {
    val partition = new Partition()
    partition.setDbName(databaseName)
    partition.setTableName(tableName)
    partition.setValues(values.asJava)
    partition.setParameters(new util.HashMap[String, String]())

    val serdeInfo = new SerDeInfo()
    serdeInfo.setParameters(new util.HashMap[String, String]())

    val storageDescriptor = new StorageDescriptor()
    storageDescriptor.setLocation(location)
    storageDescriptor.setSerdeInfo(serdeInfo)
    partition.setSd(storageDescriptor)
    partition
  }

  private def fieldSchema(name: String, dataType: String): FieldSchema = {
    new FieldSchema(name, dataType, "")
  }

  private def withTempDir(f: File => Unit): Unit = {
    val tempDir = Utils.createTempDir()
    try {
      f(tempDir)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  private def generateName(prefix: String): String = {
    s"${prefix}_${nameId.incrementAndGet()}"
  }
}
