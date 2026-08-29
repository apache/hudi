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
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hive.HiveSyncConfig
import org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest

import org.apache.hadoop.hive.common.ObjectPair
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, PartitionDropOptions, TableType}
import org.apache.hadoop.hive.metastore.api.{ColumnStatistics, CompactionType, Database, DataOperationType, EnvironmentContext, FieldSchema, FireEventRequest, ForeignKeysRequest, Function, GetPrincipalsInRoleRequest, GetRoleGrantsForPrincipalRequest, HiveObjectRef, Index, LockRequest, NoSuchObjectException, Partition, PartitionEventType, PartitionValuesRequest, PrimaryKeysRequest, PrincipalType, PrivilegeBag, Role, SerDeInfo, SetPartitionsStatsRequest, ShowLocksRequest, SQLForeignKey, SQLPrimaryKey, StorageDescriptor, Table}
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
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

      // HivePartitionUtil.partitionExists calls getPartition and treats NoSuchObjectException as
      // "absent", so a missing partition must raise that instead of returning null.
      assertEquals("2024-01-01", client.getPartition(databaseName, tableName, util.Collections.singletonList("2024-01-01")).getValues.get(0))
      assertThrows[NoSuchObjectException](client.getPartition(databaseName, tableName, util.Collections.singletonList("2099-01-01")))

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

    // setMetaConf is intentionally a silent no-op; HoodieHiveSyncClient forwards caller-context
    // values through it on every sync and there is no remote metastore to receive them.
    client.setMetaConf("hive.metastore.callerContext", "hudi")

    // close must not throw either: HoodieHiveSyncClient.close() calls it on every sync.
    client.close()

    // createDatabase without an explicit location falls back to the warehouse path.
    val databaseName = generateName("db")
    client.createDatabase(new Database(databaseName, "no-location db", null, new util.HashMap[String, String]()))
    val locationUri = client.getDatabase(databaseName).getLocationUri
    assertTrue(locationUri.contains(warehouseDir.getCanonicalPath), locationUri)
    assertFalse(client.tableExists(databaseName, "missing_table"))
  }

  test("unsupported IMetaStoreClient operations throw UnsupportedOperationException") {
    // SparkCatalogMetaStoreClient only implements the subset of IMetaStoreClient exercised by
    // HoodieHiveSyncClient/HMSDDLExecutor, plus close and setMetaConf which those callers invoke
    // unconditionally and which are deliberate no-ops. Every method outside that subset must fail
    // fast rather than return a misleading default. This locks in that contract across the
    // delegated surface.
    val client = newClient()

    // Connection / config lifecycle.
    assertUnsupported(client.isCompatibleWith(null: HiveConf))
    assertUnsupported(client.isSameConfObj(null: HiveConf))
    assertUnsupported(client.setHiveAddedJars(null: String))
    assertUnsupported(client.isLocalMetaStore())
    assertUnsupported(client.reconnect())
    assertUnsupported(client.getMetaConf(null: String))
    assertUnsupported(client.flushCache())

    // Databases.
    assertUnsupported(client.getDatabases(null: String))
    assertUnsupported(client.getAllDatabases())
    assertUnsupported(client.dropDatabase(null: String))
    assertUnsupported(client.dropDatabase(null: String, false, false))
    assertUnsupported(client.dropDatabase(null: String, false, false, false))
    assertUnsupported(client.alterDatabase(null: String, null: Database))

    // Tables.
    assertUnsupported(client.getTables(null: String, null: String))
    assertUnsupported(client.getTables(null: String, null: String, null: TableType))
    assertUnsupported(client.getTableMeta(null: String, null: String, null: util.List[String]))
    assertUnsupported(client.getAllTables(null: String))
    assertUnsupported(client.listTableNamesByFilter(null: String, null: String, 0.toShort))
    assertUnsupported(client.dropTable(null: String, null: String, false, false))
    assertUnsupported(client.dropTable(null: String, null: String, false, false, false))
    assertUnsupported(client.dropTable(null: String, false))
    assertUnsupported(client.tableExists(null: String))
    assertUnsupported(client.getTable(null: String))
    assertUnsupported(client.getTableObjectsByName(null: String, null: util.List[String]))
    assertUnsupported(client.getFields(null: String, null: String))
    assertUnsupported(client.insertTable(null: Table, false))

    // Partitions.
    assertUnsupported(client.appendPartition(null: String, null: String, null: util.List[String]))
    assertUnsupported(client.appendPartition(null: String, null: String, null: String))
    assertUnsupported(client.add_partition(null: Partition))
    assertUnsupported(client.add_partitions(null: util.List[Partition]))
    assertUnsupported(client.add_partitions_pspec(null: PartitionSpecProxy))
    assertUnsupported(client.getPartition(null: String, null: String, null: String))
    assertUnsupported(client.getPartitionWithAuthInfo(null: String, null: String, null: util.List[String], null: String, null: util.List[String]))
    assertUnsupported(client.exchange_partition(null: util.Map[String, String], null: String, null: String, null: String, null: String))
    assertUnsupported(client.exchange_partitions(null: util.Map[String, String], null: String, null: String, null: String, null: String))
    assertUnsupported(client.listPartitionSpecs(null: String, null: String, 0))
    assertUnsupported(client.listPartitions(null: String, null: String, null: util.List[String], 0.toShort))
    assertUnsupported(client.listPartitionNames(null: String, null: String, 0.toShort))
    assertUnsupported(client.listPartitionNames(null: String, null: String, null: util.List[String], 0.toShort))
    assertUnsupported(client.listPartitionValues(null: PartitionValuesRequest))
    assertUnsupported(client.getNumPartitionsByFilter(null: String, null: String, null: String))
    assertUnsupported(client.listPartitionSpecsByFilter(null: String, null: String, null: String, 0))
    assertUnsupported(client.listPartitionsByExpr(null: String, null: String, null: Array[Byte], null: String, 0.toShort, null: util.List[Partition]))
    assertUnsupported(client.listPartitionsWithAuthInfo(null: String, null: String, 0.toShort, null: String, null: util.List[String]))
    assertUnsupported(client.listPartitionsWithAuthInfo(null: String, null: String, null: util.List[String], 0.toShort, null: String, null: util.List[String]))
    assertUnsupported(client.getPartitionsByNames(null: String, null: String, null: util.List[String]))
    assertUnsupported(client.markPartitionForEvent(null: String, null: String, null: util.Map[String, String], null: PartitionEventType))
    assertUnsupported(client.isPartitionMarkedForEvent(null: String, null: String, null: util.Map[String, String], null: PartitionEventType))
    assertUnsupported(client.validatePartitionNameCharacters(null: util.List[String]))
    assertUnsupported(client.alter_table(null: String, null: String, null: Table, false))
    assertUnsupported(client.dropPartition(null: String, null: String, null: util.List[String], false))
    assertUnsupported(client.dropPartition(null: String, null: String, null: util.List[String], null: PartitionDropOptions))
    assertUnsupported(client.dropPartitions(null: String, null: String, null: util.List[ObjectPair[java.lang.Integer, Array[Byte]]], false, false))
    assertUnsupported(client.dropPartitions(null: String, null: String, null: util.List[ObjectPair[java.lang.Integer, Array[Byte]]], false, false, false))
    assertUnsupported(client.dropPartitions(null: String, null: String, null: util.List[ObjectPair[java.lang.Integer, Array[Byte]]], null: PartitionDropOptions))
    assertUnsupported(client.alter_partition(null: String, null: String, null: Partition))
    assertUnsupported(client.alter_partition(null: String, null: String, null: Partition, null: EnvironmentContext))
    assertUnsupported(client.alter_partitions(null: String, null: String, null: util.List[Partition]))
    assertUnsupported(client.renamePartition(null: String, null: String, null: util.List[String], null: Partition))
    assertUnsupported(client.partitionNameToVals(null: String))
    assertUnsupported(client.partitionNameToSpec(null: String))
    assertUnsupported(client.getConfigValue(null: String, null: String))

    // Indexes.
    assertUnsupported(client.createIndex(null: Index, null: Table))
    assertUnsupported(client.alter_index(null: String, null: String, null: String, null: Index))
    assertUnsupported(client.getIndex(null: String, null: String, null: String))
    assertUnsupported(client.listIndexes(null: String, null: String, 0.toShort))
    assertUnsupported(client.listIndexNames(null: String, null: String, 0.toShort))
    assertUnsupported(client.dropIndex(null: String, null: String, null: String, false))

    // Column statistics.
    assertUnsupported(client.updateTableColumnStatistics(null: ColumnStatistics))
    assertUnsupported(client.updatePartitionColumnStatistics(null: ColumnStatistics))
    assertUnsupported(client.getTableColumnStatistics(null: String, null: String, null: util.List[String]))
    assertUnsupported(client.getPartitionColumnStatistics(null: String, null: String, null: util.List[String], null: util.List[String]))
    assertUnsupported(client.deletePartitionColumnStatistics(null: String, null: String, null: String, null: String))
    assertUnsupported(client.deleteTableColumnStatistics(null: String, null: String, null: String))
    assertUnsupported(client.getAggrColStatsFor(null: String, null: String, null: util.List[String], null: util.List[String]))
    assertUnsupported(client.setPartitionColumnStatistics(null: SetPartitionsStatsRequest))

    // Roles and privileges.
    assertUnsupported(client.create_role(null: Role))
    assertUnsupported(client.drop_role(null: String))
    assertUnsupported(client.listRoleNames())
    assertUnsupported(client.grant_role(null: String, null: String, null: PrincipalType, null: String, null: PrincipalType, false))
    assertUnsupported(client.revoke_role(null: String, null: String, null: PrincipalType, false))
    assertUnsupported(client.list_roles(null: String, null: PrincipalType))
    assertUnsupported(client.get_privilege_set(null: HiveObjectRef, null: String, null: util.List[String]))
    assertUnsupported(client.list_privileges(null: String, null: PrincipalType, null: HiveObjectRef))
    assertUnsupported(client.grant_privileges(null: PrivilegeBag))
    assertUnsupported(client.revoke_privileges(null: PrivilegeBag, false))
    assertUnsupported(client.get_principals_in_role(null: GetPrincipalsInRoleRequest))
    assertUnsupported(client.get_role_grants_for_principal(null: GetRoleGrantsForPrincipalRequest))

    // Delegation tokens and master keys.
    assertUnsupported(client.getDelegationToken(null: String, null: String))
    assertUnsupported(client.renewDelegationToken(null: String))
    assertUnsupported(client.cancelDelegationToken(null: String))
    assertUnsupported(client.getTokenStrForm())
    assertUnsupported(client.addToken(null: String, null: String))
    assertUnsupported(client.removeToken(null: String))
    assertUnsupported(client.getToken(null: String))
    assertUnsupported(client.getAllTokenIdentifiers())
    assertUnsupported(client.addMasterKey(null: String))
    assertUnsupported(client.updateMasterKey(null: java.lang.Integer, null: String))
    assertUnsupported(client.removeMasterKey(null: java.lang.Integer))
    assertUnsupported(client.getMasterKeys())

    // Functions.
    assertUnsupported(client.createFunction(null: Function))
    assertUnsupported(client.alterFunction(null: String, null: String, null: Function))
    assertUnsupported(client.dropFunction(null: String, null: String))
    assertUnsupported(client.getFunction(null: String, null: String))
    assertUnsupported(client.getFunctions(null: String, null: String))
    assertUnsupported(client.getAllFunctions())

    // Transactions and locks.
    assertUnsupported(client.getValidTxns())
    assertUnsupported(client.getValidTxns(0L))
    assertUnsupported(client.openTxn(null: String))
    assertUnsupported(client.openTxns(null: String, 0))
    assertUnsupported(client.rollbackTxn(0L))
    assertUnsupported(client.commitTxn(0L))
    assertUnsupported(client.abortTxns(null: util.List[java.lang.Long]))
    assertUnsupported(client.showTxns())
    assertUnsupported(client.lock(null: LockRequest))
    assertUnsupported(client.checkLock(0L))
    assertUnsupported(client.unlock(0L))
    assertUnsupported(client.showLocks())
    assertUnsupported(client.showLocks(null: ShowLocksRequest))
    assertUnsupported(client.heartbeat(0L, 0L))
    assertUnsupported(client.heartbeatTxnRange(0L, 0L))
    assertUnsupported(client.compact(null: String, null: String, null: String, null: CompactionType))
    assertUnsupported(client.compact(null: String, null: String, null: String, null: CompactionType, null: util.Map[String, String]))
    assertUnsupported(client.compact2(null: String, null: String, null: String, null: CompactionType, null: util.Map[String, String]))
    assertUnsupported(client.showCompactions())
    assertUnsupported(client.addDynamicPartitions(0L, null: String, null: String, null: util.List[String]))
    assertUnsupported(client.addDynamicPartitions(0L, null: String, null: String, null: util.List[String], null: DataOperationType))

    // Notifications and file metadata.
    assertUnsupported(client.getNextNotification(0L, 0, null: IMetaStoreClient.NotificationFilter))
    assertUnsupported(client.getCurrentNotificationEventId())
    assertUnsupported(client.fireListenerEvent(null: FireEventRequest))
    assertUnsupported(client.getFileMetadata(null: util.List[java.lang.Long]))
    assertUnsupported(client.getFileMetadataBySarg(null: util.List[java.lang.Long], null: java.nio.ByteBuffer, false))
    assertUnsupported(client.clearFileMetadata(null: util.List[java.lang.Long]))
    assertUnsupported(client.putFileMetadata(null: util.List[java.lang.Long], null: util.List[java.nio.ByteBuffer]))
    assertUnsupported(client.cacheFileMetadata(null: String, null: String, null: String, false))

    // Constraints.
    assertUnsupported(client.getPrimaryKeys(null: PrimaryKeysRequest))
    assertUnsupported(client.getForeignKeys(null: ForeignKeysRequest))
    assertUnsupported(client.createTableWithConstraints(null: Table, null: util.List[SQLPrimaryKey], null: util.List[SQLForeignKey]))
    assertUnsupported(client.dropConstraint(null: String, null: String, null: String))
    assertUnsupported(client.addPrimaryKey(null: util.List[SQLPrimaryKey]))
    assertUnsupported(client.addForeignKey(null: util.List[SQLForeignKey]))
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
    serdeInfo.setSerializationLib(classOf[ParquetHiveSerDe].getName)
    serdeInfo.setParameters(new util.HashMap[String, String]())

    // Mirror the storage format HMSDDLExecutor registers; Spark's HiveClientImpl loads the
    // input and output format classes by name when it converts the table, so they must be set.
    val storageDescriptor = new StorageDescriptor()
    storageDescriptor.setCols(columns.map { case (name, dataType) => fieldSchema(name, dataType) }.asJava)
    storageDescriptor.setInputFormat(classOf[HoodieParquetInputFormat].getName)
    storageDescriptor.setOutputFormat(classOf[MapredParquetOutputFormat].getName)
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
    serdeInfo.setSerializationLib(classOf[ParquetHiveSerDe].getName)
    serdeInfo.setParameters(new util.HashMap[String, String]())

    val storageDescriptor = new StorageDescriptor()
    storageDescriptor.setInputFormat(classOf[HoodieParquetInputFormat].getName)
    storageDescriptor.setOutputFormat(classOf[MapredParquetOutputFormat].getName)
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
