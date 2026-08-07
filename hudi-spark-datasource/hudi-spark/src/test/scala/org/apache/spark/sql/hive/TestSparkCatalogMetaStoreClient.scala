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

  test("supported client edge cases: empty partitions, no-op setMetaConf, default database location") {
    val client = newClient()

    // add_partitions with an empty list is a no-op that returns an empty result.
    assertTrue(client.add_partitions(new util.ArrayList[Partition](), false, true).isEmpty)

    // setMetaConf is intentionally a silent no-op, while getMetaConf stays unsupported.
    client.setMetaConf("hive.metastore.callerContext", "hudi")
    assertThrows[UnsupportedOperationException](client.getMetaConf("hive.metastore.callerContext"))

    // createDatabase without an explicit location falls back to the warehouse path.
    val databaseName = generateName("db")
    client.createDatabase(new Database(databaseName, "no-location db", null, new util.HashMap[String, String]()))
    assertNotNull(client.getDatabase(databaseName).getLocationUri)
    assertFalse(client.tableExists(databaseName, "missing_table"))
  }

  test("unsupported IMetaStoreClient operations throw UnsupportedOperationException") {
    // SparkCatalogMetaStoreClient only implements the subset of IMetaStoreClient exercised by
    // HoodieHiveSyncClient/HMSDDLExecutor. Every other method must fail fast rather than return
    // a misleading default. This locks in that contract across the delegated surface.
    val c = newClient()

    // Connection / config lifecycle.
    assertUnsupported(c.isCompatibleWith(null: org.apache.hadoop.hive.conf.HiveConf))
    assertUnsupported(c.isSameConfObj(null: org.apache.hadoop.hive.conf.HiveConf))
    assertUnsupported(c.setHiveAddedJars(null: String))
    assertUnsupported(c.isLocalMetaStore())
    assertUnsupported(c.reconnect())
    assertUnsupported(c.close())
    assertUnsupported(c.getMetaConf(null: String))
    assertUnsupported(c.flushCache())

    // Databases.
    assertUnsupported(c.getDatabases(null: String))
    assertUnsupported(c.getAllDatabases())
    assertUnsupported(c.dropDatabase(null: String))
    assertUnsupported(c.dropDatabase(null: String, false, false))
    assertUnsupported(c.dropDatabase(null: String, false, false, false))
    assertUnsupported(c.alterDatabase(null: String, null: org.apache.hadoop.hive.metastore.api.Database))

    // Tables.
    assertUnsupported(c.getTables(null: String, null: String))
    assertUnsupported(c.getTables(null: String, null: String, null: org.apache.hadoop.hive.metastore.TableType))
    assertUnsupported(c.getTableMeta(null: String, null: String, null: util.List[String]))
    assertUnsupported(c.getAllTables(null: String))
    assertUnsupported(c.listTableNamesByFilter(null: String, null: String, 0.toShort))
    assertUnsupported(c.dropTable(null: String, null: String, false, false))
    assertUnsupported(c.dropTable(null: String, null: String, false, false, false))
    assertUnsupported(c.dropTable(null: String, false))
    assertUnsupported(c.tableExists(null: String))
    assertUnsupported(c.getTable(null: String))
    assertUnsupported(c.getTableObjectsByName(null: String, null: util.List[String]))
    assertUnsupported(c.getFields(null: String, null: String))
    assertUnsupported(c.insertTable(null: org.apache.hadoop.hive.metastore.api.Table, false))

    // Partitions.
    assertUnsupported(c.appendPartition(null: String, null: String, null: util.List[String]))
    assertUnsupported(c.appendPartition(null: String, null: String, null: String))
    assertUnsupported(c.add_partition(null: Partition))
    assertUnsupported(c.add_partitions(null: util.List[Partition]))
    assertUnsupported(c.add_partitions_pspec(null: org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy))
    assertUnsupported(c.getPartition(null: String, null: String, null: util.List[String]))
    assertUnsupported(c.getPartition(null: String, null: String, null: String))
    assertUnsupported(c.getPartitionWithAuthInfo(null: String, null: String, null: util.List[String], null: String, null: util.List[String]))
    assertUnsupported(c.exchange_partition(null: util.Map[String, String], null: String, null: String, null: String, null: String))
    assertUnsupported(c.exchange_partitions(null: util.Map[String, String], null: String, null: String, null: String, null: String))
    assertUnsupported(c.listPartitionSpecs(null: String, null: String, 0))
    assertUnsupported(c.listPartitions(null: String, null: String, null: util.List[String], 0.toShort))
    assertUnsupported(c.listPartitionNames(null: String, null: String, 0.toShort))
    assertUnsupported(c.listPartitionNames(null: String, null: String, null: util.List[String], 0.toShort))
    assertUnsupported(c.listPartitionValues(null: org.apache.hadoop.hive.metastore.api.PartitionValuesRequest))
    assertUnsupported(c.getNumPartitionsByFilter(null: String, null: String, null: String))
    assertUnsupported(c.listPartitionSpecsByFilter(null: String, null: String, null: String, 0))
    assertUnsupported(c.listPartitionsByExpr(null: String, null: String, null: Array[Byte], null: String, 0.toShort, null: util.List[Partition]))
    assertUnsupported(c.listPartitionsWithAuthInfo(null: String, null: String, 0.toShort, null: String, null: util.List[String]))
    assertUnsupported(c.listPartitionsWithAuthInfo(null: String, null: String, null: util.List[String], 0.toShort, null: String, null: util.List[String]))
    assertUnsupported(c.getPartitionsByNames(null: String, null: String, null: util.List[String]))
    assertUnsupported(c.markPartitionForEvent(null: String, null: String, null: util.Map[String, String], null: org.apache.hadoop.hive.metastore.api.PartitionEventType))
    assertUnsupported(c.isPartitionMarkedForEvent(null: String, null: String, null: util.Map[String, String], null: org.apache.hadoop.hive.metastore.api.PartitionEventType))
    assertUnsupported(c.validatePartitionNameCharacters(null: util.List[String]))
    assertUnsupported(c.alter_table(null: String, null: String, null: org.apache.hadoop.hive.metastore.api.Table, false))
    assertUnsupported(c.dropPartition(null: String, null: String, null: util.List[String], false))
    assertUnsupported(c.dropPartition(null: String, null: String, null: util.List[String], null: org.apache.hadoop.hive.metastore.PartitionDropOptions))
    assertUnsupported(c.dropPartitions(null: String, null: String, null: util.List[org.apache.hadoop.hive.common.ObjectPair[java.lang.Integer, Array[Byte]]], false, false))
    assertUnsupported(c.dropPartitions(null: String, null: String, null: util.List[org.apache.hadoop.hive.common.ObjectPair[java.lang.Integer, Array[Byte]]], false, false, false))
    assertUnsupported(c.dropPartitions(null: String, null: String, null: util.List[org.apache.hadoop.hive.common.ObjectPair[java.lang.Integer, Array[Byte]]], null: org.apache.hadoop.hive.metastore.PartitionDropOptions))
    assertUnsupported(c.alter_partition(null: String, null: String, null: Partition))
    assertUnsupported(c.alter_partition(null: String, null: String, null: Partition, null: EnvironmentContext))
    assertUnsupported(c.alter_partitions(null: String, null: String, null: util.List[Partition]))
    assertUnsupported(c.renamePartition(null: String, null: String, null: util.List[String], null: Partition))
    assertUnsupported(c.partitionNameToVals(null: String))
    assertUnsupported(c.partitionNameToSpec(null: String))
    assertUnsupported(c.getConfigValue(null: String, null: String))

    // Indexes.
    assertUnsupported(c.createIndex(null: org.apache.hadoop.hive.metastore.api.Index, null: org.apache.hadoop.hive.metastore.api.Table))
    assertUnsupported(c.alter_index(null: String, null: String, null: String, null: org.apache.hadoop.hive.metastore.api.Index))
    assertUnsupported(c.getIndex(null: String, null: String, null: String))
    assertUnsupported(c.listIndexes(null: String, null: String, 0.toShort))
    assertUnsupported(c.listIndexNames(null: String, null: String, 0.toShort))
    assertUnsupported(c.dropIndex(null: String, null: String, null: String, false))

    // Column statistics.
    assertUnsupported(c.updateTableColumnStatistics(null: org.apache.hadoop.hive.metastore.api.ColumnStatistics))
    assertUnsupported(c.updatePartitionColumnStatistics(null: org.apache.hadoop.hive.metastore.api.ColumnStatistics))
    assertUnsupported(c.getTableColumnStatistics(null: String, null: String, null: util.List[String]))
    assertUnsupported(c.getPartitionColumnStatistics(null: String, null: String, null: util.List[String], null: util.List[String]))
    assertUnsupported(c.deletePartitionColumnStatistics(null: String, null: String, null: String, null: String))
    assertUnsupported(c.deleteTableColumnStatistics(null: String, null: String, null: String))
    assertUnsupported(c.getAggrColStatsFor(null: String, null: String, null: util.List[String], null: util.List[String]))
    assertUnsupported(c.setPartitionColumnStatistics(null: org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest))

    // Roles and privileges.
    assertUnsupported(c.create_role(null: org.apache.hadoop.hive.metastore.api.Role))
    assertUnsupported(c.drop_role(null: String))
    assertUnsupported(c.listRoleNames())
    assertUnsupported(c.grant_role(null: String, null: String, null: org.apache.hadoop.hive.metastore.api.PrincipalType, null: String, null: org.apache.hadoop.hive.metastore.api.PrincipalType, false))
    assertUnsupported(c.revoke_role(null: String, null: String, null: org.apache.hadoop.hive.metastore.api.PrincipalType, false))
    assertUnsupported(c.list_roles(null: String, null: org.apache.hadoop.hive.metastore.api.PrincipalType))
    assertUnsupported(c.get_privilege_set(null: org.apache.hadoop.hive.metastore.api.HiveObjectRef, null: String, null: util.List[String]))
    assertUnsupported(c.list_privileges(null: String, null: org.apache.hadoop.hive.metastore.api.PrincipalType, null: org.apache.hadoop.hive.metastore.api.HiveObjectRef))
    assertUnsupported(c.grant_privileges(null: org.apache.hadoop.hive.metastore.api.PrivilegeBag))
    assertUnsupported(c.revoke_privileges(null: org.apache.hadoop.hive.metastore.api.PrivilegeBag, false))
    assertUnsupported(c.get_principals_in_role(null: org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest))
    assertUnsupported(c.get_role_grants_for_principal(null: org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest))

    // Delegation tokens and master keys.
    assertUnsupported(c.getDelegationToken(null: String, null: String))
    assertUnsupported(c.renewDelegationToken(null: String))
    assertUnsupported(c.cancelDelegationToken(null: String))
    assertUnsupported(c.getTokenStrForm())
    assertUnsupported(c.addToken(null: String, null: String))
    assertUnsupported(c.removeToken(null: String))
    assertUnsupported(c.getToken(null: String))
    assertUnsupported(c.getAllTokenIdentifiers())
    assertUnsupported(c.addMasterKey(null: String))
    assertUnsupported(c.updateMasterKey(null: java.lang.Integer, null: String))
    assertUnsupported(c.removeMasterKey(null: java.lang.Integer))
    assertUnsupported(c.getMasterKeys())

    // Functions.
    assertUnsupported(c.createFunction(null: org.apache.hadoop.hive.metastore.api.Function))
    assertUnsupported(c.alterFunction(null: String, null: String, null: org.apache.hadoop.hive.metastore.api.Function))
    assertUnsupported(c.dropFunction(null: String, null: String))
    assertUnsupported(c.getFunction(null: String, null: String))
    assertUnsupported(c.getFunctions(null: String, null: String))
    assertUnsupported(c.getAllFunctions())

    // Transactions and locks.
    assertUnsupported(c.getValidTxns())
    assertUnsupported(c.getValidTxns(0L))
    assertUnsupported(c.openTxn(null: String))
    assertUnsupported(c.openTxns(null: String, 0))
    assertUnsupported(c.rollbackTxn(0L))
    assertUnsupported(c.commitTxn(0L))
    assertUnsupported(c.abortTxns(null: util.List[java.lang.Long]))
    assertUnsupported(c.showTxns())
    assertUnsupported(c.lock(null: org.apache.hadoop.hive.metastore.api.LockRequest))
    assertUnsupported(c.checkLock(0L))
    assertUnsupported(c.unlock(0L))
    assertUnsupported(c.showLocks())
    assertUnsupported(c.showLocks(null: org.apache.hadoop.hive.metastore.api.ShowLocksRequest))
    assertUnsupported(c.heartbeat(0L, 0L))
    assertUnsupported(c.heartbeatTxnRange(0L, 0L))
    assertUnsupported(c.compact(null: String, null: String, null: String, null: org.apache.hadoop.hive.metastore.api.CompactionType))
    assertUnsupported(c.compact(null: String, null: String, null: String, null: org.apache.hadoop.hive.metastore.api.CompactionType, null: util.Map[String, String]))
    assertUnsupported(c.compact2(null: String, null: String, null: String, null: org.apache.hadoop.hive.metastore.api.CompactionType, null: util.Map[String, String]))
    assertUnsupported(c.showCompactions())
    assertUnsupported(c.addDynamicPartitions(0L, null: String, null: String, null: util.List[String]))
    assertUnsupported(c.addDynamicPartitions(0L, null: String, null: String, null: util.List[String], null: org.apache.hadoop.hive.metastore.api.DataOperationType))

    // Notifications and file metadata.
    assertUnsupported(c.getNextNotification(0L, 0, null: org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter))
    assertUnsupported(c.getCurrentNotificationEventId())
    assertUnsupported(c.fireListenerEvent(null: org.apache.hadoop.hive.metastore.api.FireEventRequest))
    assertUnsupported(c.getFileMetadata(null: util.List[java.lang.Long]))
    assertUnsupported(c.getFileMetadataBySarg(null: util.List[java.lang.Long], null: java.nio.ByteBuffer, false))
    assertUnsupported(c.clearFileMetadata(null: util.List[java.lang.Long]))
    assertUnsupported(c.putFileMetadata(null: util.List[java.lang.Long], null: util.List[java.nio.ByteBuffer]))
    assertUnsupported(c.cacheFileMetadata(null: String, null: String, null: String, false))

    // Constraints.
    assertUnsupported(c.getPrimaryKeys(null: org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest))
    assertUnsupported(c.getForeignKeys(null: org.apache.hadoop.hive.metastore.api.ForeignKeysRequest))
    assertUnsupported(c.createTableWithConstraints(null: org.apache.hadoop.hive.metastore.api.Table, null: util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey], null: util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey]))
    assertUnsupported(c.dropConstraint(null: String, null: String, null: String))
    assertUnsupported(c.addPrimaryKey(null: util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey]))
    assertUnsupported(c.addForeignKey(null: util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey]))
  }

  private def assertUnsupported(fn: => Any): Unit = {
    assertThrows[UnsupportedOperationException](fn)
    ()
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
