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

import org.apache.hudi.hive.HiveSyncConfig

import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{Database, EnvironmentContext, FieldSchema, Partition, SerDeInfo, StorageDescriptor, Table}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTablePartition, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{Metadata, StructField, StructType}

import java.net.URI
import java.util

import scala.collection.JavaConverters._

/**
 * IMetaStoreClient implementation backed by Spark catalog/external-catalog APIs for
 * methods used by HoodieHiveSyncClient/HMSDDLExecutor.
 */
class SparkCatalogMetaStoreClient(syncConfig: HiveSyncConfig)
  extends IMetaStoreClient {

  private val sparkSession = SparkSession.getActiveSession.getOrElse(SparkSession.builder()
    .enableHiveSupport()
    .getOrCreate())

  private val externalCatalog = sparkSession.sessionState.catalog.externalCatalog

  override def createDatabase(database: Database): Unit = {
    val catalogDb = CatalogDatabase(
      name = database.getName,
      description = Option(database.getDescription).getOrElse(""),
      locationUri = Option(database.getLocationUri).map(new URI(_))
        .getOrElse(new URI(sparkSession.sessionState.conf.warehousePath)),
      properties = Option(database.getParameters).map(_.asScala.toMap).getOrElse(Map.empty))
    externalCatalog.createDatabase(catalogDb, ignoreIfExists = false)
  }

  override def createTable(table: Table): Unit = {
    externalCatalog.createTable(toCatalogTable(table), ignoreIfExists = false)
  }

  override def getTable(dbName: String, tableName: String): Table = {
    fromCatalogTable(externalCatalog.getTable(dbName, tableName))
  }

  // scalastyle:off method.name
  override def alter_table(dbName: String, tableName: String, table: Table): Unit = {
    val updated = toCatalogTable(table).copy(identifier = TableIdentifier(tableName, Some(dbName)))
    externalCatalog.alterTable(updated)
  }

  override def alter_table_with_environmentContext(dbName: String,
                                                   tableName: String,
                                                   table: Table,
                                                   environmentContext: EnvironmentContext): Unit = {
    alter_table(dbName, tableName, table)
  }

  override def listPartitions(dbName: String, tableName: String, max: Short): util.List[Partition] = {
    val table = getTable(dbName, tableName)
    val partitionKeys = table.getPartitionKeys.asScala.map(_.getName).toList
    externalCatalog.listPartitions(dbName, tableName, None).map(fromCatalogPartition(_, dbName, tableName, partitionKeys)).asJava
  }

  override def listPartitionsByFilter(dbName: String,
                                      tableName: String,
                                      filter: String,
                                      max: Short): util.List[Partition] = {
    // Spark external catalog does not expose Hive filter-string API; fall back to listing all.
    listPartitions(dbName, tableName, max)
  }

  override def add_partitions(parts: util.List[Partition], ifNotExists: Boolean, needResults: Boolean): util.List[Partition] = {
    if (parts == null || parts.isEmpty) {
      new util.ArrayList[Partition]()
    } else {
      val first = parts.get(0)
      val db = first.getDbName
      val tbl = first.getTableName
      val catalogParts = parts.asScala.map(toCatalogPartition).toSeq
      externalCatalog.createPartitions(db, tbl, catalogParts, ignoreIfExists = ifNotExists)
      if (needResults) parts else new util.ArrayList[Partition]()
    }
  }

  override def alter_partitions(dbName: String,
                                tableName: String,
                                newParts: util.List[Partition],
                                environmentContext: EnvironmentContext): Unit = {
    externalCatalog.alterPartitions(dbName, tableName, newParts.asScala.map(toCatalogPartition).toSeq)
  }

  override def dropPartition(dbName: String, tableName: String, partName: String, deleteData: Boolean): Boolean = {
    val spec = parsePartitionClause(partName)
    externalCatalog.dropPartitions(dbName, tableName, Seq(spec), ignoreIfNotExists = true, purge = true, retainData = !deleteData)
    true
  }
  // scalastyle:on method.name

  override def tableExists(dbName: String, tableName: String): Boolean = {
    sparkSession.catalog.tableExists(dbName, tableName)
  }

  override def getDatabase(dbName: String): Database = {
    val db = externalCatalog.getDatabase(dbName)
    new Database(db.name, db.description, db.locationUri.toString, db.properties.asJava)
  }

  override def getSchema(dbName: String, tableName: String): util.List[FieldSchema] = {
    val table = externalCatalog.getTable(dbName, tableName)
    val cols = table.schema.fields.map { f =>
      new FieldSchema(f.name, f.dataType.catalogString, Option(f.getComment()).map(_.toString).getOrElse(""))
    }
    val partitionCols = table.partitionColumnNames.map { name =>
      val dt = table.partitionSchema.fields.find(_.name == name).map(_.dataType.catalogString).getOrElse("string")
      new FieldSchema(name, dt, "")
    }
    (cols ++ partitionCols).toList.asJava
  }

  override def dropTable(dbName: String, tableName: String): Unit = {
    externalCatalog.dropTable(dbName, tableName, ignoreIfNotExists = true, purge = true)
  }

  // scalastyle:off
  private def unsupported[T](): T = {
    throw new UnsupportedOperationException("Method is not supported in SparkCatalogMetaStoreClient")
  }

  override def isCompatibleWith(arg0: org.apache.hadoop.hive.conf.HiveConf): Boolean = unsupported[Boolean]()
  override def setHiveAddedJars(arg0: String): Unit = unsupported[Unit]()
  override def isLocalMetaStore(): Boolean = unsupported[Boolean]()
  override def reconnect(): Unit = unsupported[Unit]()
  override def close(): Unit = unsupported[Unit]()
  override def setMetaConf(arg0: String, arg1: String): Unit = unsupported[Unit]()
  override def getMetaConf(arg0: String): String = unsupported[String]()
  override def getDatabases(arg0: String): java.util.List[String] = unsupported[java.util.List[String]]()
  override def getAllDatabases(): java.util.List[String] = unsupported[java.util.List[String]]()
  override def getTables(arg0: String, arg1: String): java.util.List[String] = unsupported[java.util.List[String]]()
  override def getTables(arg0: String, arg1: String, arg2: org.apache.hadoop.hive.metastore.TableType): java.util.List[String] = unsupported[java.util.List[String]]()
  override def getTableMeta(arg0: String, arg1: String, arg2: java.util.List[String]): java.util.List[org.apache.hadoop.hive.metastore.api.TableMeta] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.TableMeta]]()
  override def getAllTables(arg0: String): java.util.List[String] = unsupported[java.util.List[String]]()
  override def listTableNamesByFilter(arg0: String, arg1: String, arg2: Short): java.util.List[String] = unsupported[java.util.List[String]]()
  override def dropTable(arg0: String, arg1: String, arg2: Boolean, arg3: Boolean): Unit = unsupported[Unit]()
  override def dropTable(arg0: String, arg1: String, arg2: Boolean, arg3: Boolean, arg4: Boolean): Unit = unsupported[Unit]()
  override def dropTable(arg0: String, arg1: Boolean): Unit = unsupported[Unit]()
  override def tableExists(arg0: String): Boolean = unsupported[Boolean]()
  override def getTable(arg0: String): org.apache.hadoop.hive.metastore.api.Table = unsupported[org.apache.hadoop.hive.metastore.api.Table]()
  override def getTableObjectsByName(arg0: String, arg1: java.util.List[String]): java.util.List[org.apache.hadoop.hive.metastore.api.Table] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Table]]()
  override def appendPartition(arg0: String, arg1: String, arg2: java.util.List[String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def appendPartition(arg0: String, arg1: String, arg2: String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def add_partition(arg0: org.apache.hadoop.hive.metastore.api.Partition): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def add_partitions(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.Partition]): Int = unsupported[Int]()
  override def add_partitions_pspec(arg0: org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy): Int = unsupported[Int]()
  override def getPartition(arg0: String, arg1: String, arg2: java.util.List[String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def exchange_partition(arg0: java.util.Map[String, String], arg1: String, arg2: String, arg3: String, arg4: String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def exchange_partitions(arg0: java.util.Map[String, String], arg1: String, arg2: String, arg3: String, arg4: String): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def getPartition(arg0: String, arg1: String, arg2: String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def getPartitionWithAuthInfo(arg0: String, arg1: String, arg2: java.util.List[String], arg3: String, arg4: java.util.List[String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def listPartitionSpecs(arg0: String, arg1: String, arg2: Int): org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy = unsupported[org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy]()
  override def listPartitions(arg0: String, arg1: String, arg2: java.util.List[String], arg3: Short): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitionNames(arg0: String, arg1: String, arg2: Short): java.util.List[String] = unsupported[java.util.List[String]]()
  override def listPartitionNames(arg0: String, arg1: String, arg2: java.util.List[String], arg3: Short): java.util.List[String] = unsupported[java.util.List[String]]()
  override def listPartitionValues(arg0: org.apache.hadoop.hive.metastore.api.PartitionValuesRequest): org.apache.hadoop.hive.metastore.api.PartitionValuesResponse = unsupported[org.apache.hadoop.hive.metastore.api.PartitionValuesResponse]()
  override def getNumPartitionsByFilter(arg0: String, arg1: String, arg2: String): Int = unsupported[Int]()
  override def listPartitionSpecsByFilter(arg0: String, arg1: String, arg2: String, arg3: Int): org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy = unsupported[org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy]()
  override def listPartitionsByExpr(arg0: String, arg1: String, arg2: Array[Byte], arg3: String, arg4: Short, arg5: java.util.List[org.apache.hadoop.hive.metastore.api.Partition]): Boolean = unsupported[Boolean]()
  override def listPartitionsWithAuthInfo(arg0: String, arg1: String, arg2: Short, arg3: String, arg4: java.util.List[String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def getPartitionsByNames(arg0: String, arg1: String, arg2: java.util.List[String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitionsWithAuthInfo(arg0: String, arg1: String, arg2: java.util.List[String], arg3: Short, arg4: String, arg5: java.util.List[String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def markPartitionForEvent(arg0: String, arg1: String, arg2: java.util.Map[String, String], arg3: org.apache.hadoop.hive.metastore.api.PartitionEventType): Unit = unsupported[Unit]()
  override def isPartitionMarkedForEvent(arg0: String, arg1: String, arg2: java.util.Map[String, String], arg3: org.apache.hadoop.hive.metastore.api.PartitionEventType): Boolean = unsupported[Boolean]()
  override def validatePartitionNameCharacters(arg0: java.util.List[String]): Unit = unsupported[Unit]()
  override def alter_table(arg0: String, arg1: String, arg2: org.apache.hadoop.hive.metastore.api.Table, arg3: Boolean): Unit = unsupported[Unit]()
  override def dropDatabase(arg0: String): Unit = unsupported[Unit]()
  override def dropDatabase(arg0: String, arg1: Boolean, arg2: Boolean): Unit = unsupported[Unit]()
  override def dropDatabase(arg0: String, arg1: Boolean, arg2: Boolean, arg3: Boolean): Unit = unsupported[Unit]()
  override def alterDatabase(arg0: String, arg1: org.apache.hadoop.hive.metastore.api.Database): Unit = unsupported[Unit]()
  override def dropPartition(arg0: String, arg1: String, arg2: java.util.List[String], arg3: Boolean): Boolean = unsupported[Boolean]()
  override def dropPartition(arg0: String, arg1: String, arg2: java.util.List[String], arg3: org.apache.hadoop.hive.metastore.PartitionDropOptions): Boolean = unsupported[Boolean]()
  override def dropPartitions(arg0: String, arg1: String, arg2: java.util.List[org.apache.hadoop.hive.common.ObjectPair[java.lang.Integer, Array[Byte]]], arg3: Boolean, arg4: Boolean): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def dropPartitions(arg0: String, arg1: String, arg2: java.util.List[org.apache.hadoop.hive.common.ObjectPair[java.lang.Integer, Array[Byte]]], arg3: Boolean, arg4: Boolean, arg5: Boolean): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def dropPartitions(arg0: String, arg1: String, arg2: java.util.List[org.apache.hadoop.hive.common.ObjectPair[java.lang.Integer, Array[Byte]]], arg3: org.apache.hadoop.hive.metastore.PartitionDropOptions): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def alter_partition(arg0: String, arg1: String, arg2: org.apache.hadoop.hive.metastore.api.Partition): Unit = unsupported[Unit]()
  override def alter_partition(arg0: String, arg1: String, arg2: org.apache.hadoop.hive.metastore.api.Partition, arg3: org.apache.hadoop.hive.metastore.api.EnvironmentContext): Unit = unsupported[Unit]()
  override def alter_partitions(arg0: String, arg1: String, arg2: java.util.List[org.apache.hadoop.hive.metastore.api.Partition]): Unit = unsupported[Unit]()
  override def renamePartition(arg0: String, arg1: String, arg2: java.util.List[String], arg3: org.apache.hadoop.hive.metastore.api.Partition): Unit = unsupported[Unit]()
  override def getFields(arg0: String, arg1: String): java.util.List[org.apache.hadoop.hive.metastore.api.FieldSchema] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.FieldSchema]]()
  override def getConfigValue(arg0: String, arg1: String): String = unsupported[String]()
  override def partitionNameToVals(arg0: String): java.util.List[String] = unsupported[java.util.List[String]]()
  override def partitionNameToSpec(arg0: String): java.util.Map[String, String] = unsupported[java.util.Map[String, String]]()
  override def createIndex(arg0: org.apache.hadoop.hive.metastore.api.Index, arg1: org.apache.hadoop.hive.metastore.api.Table): Unit = unsupported[Unit]()
  override def alter_index(arg0: String, arg1: String, arg2: String, arg3: org.apache.hadoop.hive.metastore.api.Index): Unit = unsupported[Unit]()
  override def getIndex(arg0: String, arg1: String, arg2: String): org.apache.hadoop.hive.metastore.api.Index = unsupported[org.apache.hadoop.hive.metastore.api.Index]()
  override def listIndexes(arg0: String, arg1: String, arg2: Short): java.util.List[org.apache.hadoop.hive.metastore.api.Index] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Index]]()
  override def listIndexNames(arg0: String, arg1: String, arg2: Short): java.util.List[String] = unsupported[java.util.List[String]]()
  override def dropIndex(arg0: String, arg1: String, arg2: String, arg3: Boolean): Boolean = unsupported[Boolean]()
  override def updateTableColumnStatistics(arg0: org.apache.hadoop.hive.metastore.api.ColumnStatistics): Boolean = unsupported[Boolean]()
  override def updatePartitionColumnStatistics(arg0: org.apache.hadoop.hive.metastore.api.ColumnStatistics): Boolean = unsupported[Boolean]()
  override def getTableColumnStatistics(arg0: String, arg1: String, arg2: java.util.List[String]): java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]()
  override def getPartitionColumnStatistics(arg0: String, arg1: String, arg2: java.util.List[String], arg3: java.util.List[String]): java.util.Map[String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]] = unsupported[java.util.Map[String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]]()
  override def deletePartitionColumnStatistics(arg0: String, arg1: String, arg2: String, arg3: String): Boolean = unsupported[Boolean]()
  override def deleteTableColumnStatistics(arg0: String, arg1: String, arg2: String): Boolean = unsupported[Boolean]()
  override def create_role(arg0: org.apache.hadoop.hive.metastore.api.Role): Boolean = unsupported[Boolean]()
  override def drop_role(arg0: String): Boolean = unsupported[Boolean]()
  override def listRoleNames(): java.util.List[String] = unsupported[java.util.List[String]]()
  override def grant_role(arg0: String, arg1: String, arg2: org.apache.hadoop.hive.metastore.api.PrincipalType, arg3: String, arg4: org.apache.hadoop.hive.metastore.api.PrincipalType, arg5: Boolean): Boolean = unsupported[Boolean]()
  override def revoke_role(arg0: String, arg1: String, arg2: org.apache.hadoop.hive.metastore.api.PrincipalType, arg3: Boolean): Boolean = unsupported[Boolean]()
  override def list_roles(arg0: String, arg1: org.apache.hadoop.hive.metastore.api.PrincipalType): java.util.List[org.apache.hadoop.hive.metastore.api.Role] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Role]]()
  override def get_privilege_set(arg0: org.apache.hadoop.hive.metastore.api.HiveObjectRef, arg1: String, arg2: java.util.List[String]): org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet = unsupported[org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet]()
  override def list_privileges(arg0: String, arg1: org.apache.hadoop.hive.metastore.api.PrincipalType, arg2: org.apache.hadoop.hive.metastore.api.HiveObjectRef): java.util.List[org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege]]()
  override def grant_privileges(arg0: org.apache.hadoop.hive.metastore.api.PrivilegeBag): Boolean = unsupported[Boolean]()
  override def revoke_privileges(arg0: org.apache.hadoop.hive.metastore.api.PrivilegeBag, arg1: Boolean): Boolean = unsupported[Boolean]()
  override def getDelegationToken(arg0: String, arg1: String): String = unsupported[String]()
  override def renewDelegationToken(arg0: String): Long = unsupported[Long]()
  override def cancelDelegationToken(arg0: String): Unit = unsupported[Unit]()
  override def getTokenStrForm(): String = unsupported[String]()
  override def addToken(arg0: String, arg1: String): Boolean = unsupported[Boolean]()
  override def removeToken(arg0: String): Boolean = unsupported[Boolean]()
  override def getToken(arg0: String): String = unsupported[String]()
  override def getAllTokenIdentifiers(): java.util.List[String] = unsupported[java.util.List[String]]()
  override def addMasterKey(arg0: String): Int = unsupported[Int]()
  override def updateMasterKey(arg0: java.lang.Integer, arg1: String): Unit = unsupported[Unit]()
  override def removeMasterKey(arg0: java.lang.Integer): Boolean = unsupported[Boolean]()
  override def getMasterKeys(): Array[String] = unsupported[Array[String]]()
  override def createFunction(arg0: org.apache.hadoop.hive.metastore.api.Function): Unit = unsupported[Unit]()
  override def alterFunction(arg0: String, arg1: String, arg2: org.apache.hadoop.hive.metastore.api.Function): Unit = unsupported[Unit]()
  override def dropFunction(arg0: String, arg1: String): Unit = unsupported[Unit]()
  override def getFunction(arg0: String, arg1: String): org.apache.hadoop.hive.metastore.api.Function = unsupported[org.apache.hadoop.hive.metastore.api.Function]()
  override def getFunctions(arg0: String, arg1: String): java.util.List[String] = unsupported[java.util.List[String]]()
  override def getAllFunctions(): org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse]()
  override def getValidTxns(): org.apache.hadoop.hive.common.ValidTxnList = unsupported[org.apache.hadoop.hive.common.ValidTxnList]()
  override def getValidTxns(arg0: Long): org.apache.hadoop.hive.common.ValidTxnList = unsupported[org.apache.hadoop.hive.common.ValidTxnList]()
  override def openTxn(arg0: String): Long = unsupported[Long]()
  override def openTxns(arg0: String, arg1: Int): org.apache.hadoop.hive.metastore.api.OpenTxnsResponse = unsupported[org.apache.hadoop.hive.metastore.api.OpenTxnsResponse]()
  override def rollbackTxn(arg0: Long): Unit = unsupported[Unit]()
  override def commitTxn(arg0: Long): Unit = unsupported[Unit]()
  override def abortTxns(arg0: java.util.List[java.lang.Long]): Unit = unsupported[Unit]()
  override def showTxns(): org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse]()
  override def lock(arg0: org.apache.hadoop.hive.metastore.api.LockRequest): org.apache.hadoop.hive.metastore.api.LockResponse = unsupported[org.apache.hadoop.hive.metastore.api.LockResponse]()
  override def checkLock(arg0: Long): org.apache.hadoop.hive.metastore.api.LockResponse = unsupported[org.apache.hadoop.hive.metastore.api.LockResponse]()
  override def unlock(arg0: Long): Unit = unsupported[Unit]()
  override def showLocks(): org.apache.hadoop.hive.metastore.api.ShowLocksResponse = unsupported[org.apache.hadoop.hive.metastore.api.ShowLocksResponse]()
  override def showLocks(arg0: org.apache.hadoop.hive.metastore.api.ShowLocksRequest): org.apache.hadoop.hive.metastore.api.ShowLocksResponse = unsupported[org.apache.hadoop.hive.metastore.api.ShowLocksResponse]()
  override def heartbeat(arg0: Long, arg1: Long): Unit = unsupported[Unit]()
  override def heartbeatTxnRange(arg0: Long, arg1: Long): org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse = unsupported[org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse]()
  override def compact(arg0: String, arg1: String, arg2: String, arg3: org.apache.hadoop.hive.metastore.api.CompactionType): Unit = unsupported[Unit]()
  override def compact(arg0: String, arg1: String, arg2: String, arg3: org.apache.hadoop.hive.metastore.api.CompactionType, arg4: java.util.Map[String, String]): Unit = unsupported[Unit]()
  override def compact2(arg0: String, arg1: String, arg2: String, arg3: org.apache.hadoop.hive.metastore.api.CompactionType, arg4: java.util.Map[String, String]): org.apache.hadoop.hive.metastore.api.CompactionResponse = unsupported[org.apache.hadoop.hive.metastore.api.CompactionResponse]()
  override def showCompactions(): org.apache.hadoop.hive.metastore.api.ShowCompactResponse = unsupported[org.apache.hadoop.hive.metastore.api.ShowCompactResponse]()
  override def addDynamicPartitions(arg0: Long, arg1: String, arg2: String, arg3: java.util.List[String]): Unit = unsupported[Unit]()
  override def addDynamicPartitions(arg0: Long, arg1: String, arg2: String, arg3: java.util.List[String], arg4: org.apache.hadoop.hive.metastore.api.DataOperationType): Unit = unsupported[Unit]()
  override def insertTable(arg0: org.apache.hadoop.hive.metastore.api.Table, arg1: Boolean): Unit = unsupported[Unit]()
  override def getNextNotification(arg0: Long, arg1: Int, arg2: org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter): org.apache.hadoop.hive.metastore.api.NotificationEventResponse = unsupported[org.apache.hadoop.hive.metastore.api.NotificationEventResponse]()
  override def getCurrentNotificationEventId(): org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId = unsupported[org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId]()
  override def fireListenerEvent(arg0: org.apache.hadoop.hive.metastore.api.FireEventRequest): org.apache.hadoop.hive.metastore.api.FireEventResponse = unsupported[org.apache.hadoop.hive.metastore.api.FireEventResponse]()
  override def get_principals_in_role(arg0: org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest): org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse]()
  override def get_role_grants_for_principal(arg0: org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest): org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse]()
  override def getAggrColStatsFor(arg0: String, arg1: String, arg2: java.util.List[String], arg3: java.util.List[String]): org.apache.hadoop.hive.metastore.api.AggrStats = unsupported[org.apache.hadoop.hive.metastore.api.AggrStats]()
  override def setPartitionColumnStatistics(arg0: org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest): Boolean = unsupported[Boolean]()
  override def flushCache(): Unit = unsupported[Unit]()
  override def getFileMetadata(arg0: java.util.List[java.lang.Long]): java.lang.Iterable[java.util.Map.Entry[java.lang.Long, java.nio.ByteBuffer]] = unsupported[java.lang.Iterable[java.util.Map.Entry[java.lang.Long, java.nio.ByteBuffer]]]()
  override def getFileMetadataBySarg(arg0: java.util.List[java.lang.Long], arg1: java.nio.ByteBuffer, arg2: Boolean): java.lang.Iterable[java.util.Map.Entry[java.lang.Long, org.apache.hadoop.hive.metastore.api.MetadataPpdResult]] = unsupported[java.lang.Iterable[java.util.Map.Entry[java.lang.Long, org.apache.hadoop.hive.metastore.api.MetadataPpdResult]]]()
  override def clearFileMetadata(arg0: java.util.List[java.lang.Long]): Unit = unsupported[Unit]()
  override def putFileMetadata(arg0: java.util.List[java.lang.Long], arg1: java.util.List[java.nio.ByteBuffer]): Unit = unsupported[Unit]()
  override def isSameConfObj(arg0: org.apache.hadoop.hive.conf.HiveConf): Boolean = unsupported[Boolean]()
  override def cacheFileMetadata(arg0: String, arg1: String, arg2: String, arg3: Boolean): Boolean = unsupported[Boolean]()
  override def getPrimaryKeys(arg0: org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest): java.util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey]]()
  override def getForeignKeys(arg0: org.apache.hadoop.hive.metastore.api.ForeignKeysRequest): java.util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey]]()
  override def createTableWithConstraints(arg0: org.apache.hadoop.hive.metastore.api.Table, arg1: java.util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey], arg2: java.util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey]): Unit = unsupported[Unit]()
  override def dropConstraint(arg0: String, arg1: String, arg2: String): Unit = unsupported[Unit]()
  override def addPrimaryKey(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey]): Unit = unsupported[Unit]()
  override def addForeignKey(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey]): Unit = unsupported[Unit]()
  // scalastyle:on

  private def toCatalogTable(table: Table): CatalogTable = {
    val db = table.getDbName
    val tbl = table.getTableName
    val cols = Option(table.getSd).map(_.getCols).map(_.asScala.toList).getOrElse(Nil)
    val partCols = Option(table.getPartitionKeys).map(_.asScala.toList).getOrElse(Nil)

    val dataFields = cols.map(fs => StructField(fs.getName, CatalystSqlParser.parseDataType(fs.getType), nullable = true, Metadata.empty))
    val partitionFields = partCols.map(fs => StructField(fs.getName, CatalystSqlParser.parseDataType(fs.getType), nullable = true, Metadata.empty))

    CatalogTable(
      identifier = TableIdentifier(tbl, Some(db)),
      tableType = if ("EXTERNAL_TABLE".equalsIgnoreCase(table.getTableType)) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = Option(table.getSd).map(_.getLocation).map(new URI(_)),
        inputFormat = Option(table.getSd).map(_.getInputFormat),
        outputFormat = Option(table.getSd).map(_.getOutputFormat),
        serde = Option(table.getSd).flatMap(sd => Option(sd.getSerdeInfo)).map(_.getSerializationLib),
        compressed = false,
        properties = Option(table.getSd).flatMap(sd => Option(sd.getSerdeInfo)).flatMap(si => Option(si.getParameters)).map(_.asScala.toMap).getOrElse(Map.empty)),
      schema = StructType(dataFields ++ partitionFields),
      provider = Some("hudi"),
      partitionColumnNames = partCols.map(_.getName),
      properties = Option(table.getParameters).map(_.asScala.toMap).getOrElse(Map.empty))
  }

  private def fromCatalogTable(table: CatalogTable): Table = {
    val t = new Table()
    t.setDbName(table.identifier.database.getOrElse("default"))
    t.setTableName(table.identifier.table)
    t.setTableType(if (table.tableType == CatalogTableType.EXTERNAL) "EXTERNAL_TABLE" else "MANAGED_TABLE")
    t.setParameters(new util.HashMap[String, String](table.properties.asJava))

    val nonPartitionFields = table.schema.fields.filterNot(f => table.partitionColumnNames.contains(f.name))
    val cols = nonPartitionFields.map(f => new FieldSchema(f.name, f.dataType.catalogString, f.getComment().orNull)).toList.asJava
    val partCols = table.partitionColumnNames.map { name =>
      val dt = table.partitionSchema.fields.find(_.name == name).map(_.dataType.catalogString).getOrElse("string")
      new FieldSchema(name, dt, "")
    }.toList.asJava

    val serdeInfo = new SerDeInfo(null, table.storage.serde.orNull, new util.HashMap[String, String](table.storage.properties.asJava))
    val sd = new StorageDescriptor(cols, table.storage.locationUri.map(_.toString).orNull,
      table.storage.inputFormat.orNull, table.storage.outputFormat.orNull, false, 0, serdeInfo, null, null, null)
    t.setSd(sd)
    t.setPartitionKeys(partCols)
    t
  }

  private def fromCatalogPartition(part: CatalogTablePartition, db: String, table: String, partitionKeys: List[String]): Partition = {
    val values = partitionKeys.map(k => part.spec.getOrElse(k, "")).asJava
    val serdeInfo = new SerDeInfo()
    val sd = new StorageDescriptor()
    sd.setLocation(part.storage.locationUri.map(_.toString).orNull)
    sd.setInputFormat(part.storage.inputFormat.orNull)
    sd.setOutputFormat(part.storage.outputFormat.orNull)
    sd.setSerdeInfo(serdeInfo)
    new Partition(values, db, table, 0, 0, sd, new util.HashMap[String, String]())
  }

  private def toCatalogPartition(part: Partition): CatalogTablePartition = {
    val table = getTable(part.getDbName, part.getTableName)
    val keys = Option(table.getPartitionKeys).map(_.asScala.toList).getOrElse(Nil)
    val values = Option(part.getValues).map(_.asScala.toList).getOrElse(Nil)
    val spec = keys.zip(values).map { case (k, v) => (k.getName, v) }.toMap
    CatalogTablePartition(
      spec = spec,
      storage = CatalogStorageFormat(
        locationUri = Option(part.getSd).map(_.getLocation).map(new URI(_)),
        inputFormat = Option(part.getSd).map(_.getInputFormat),
        outputFormat = Option(part.getSd).map(_.getOutputFormat),
        serde = Option(part.getSd).flatMap(sd => Option(sd.getSerdeInfo)).map(_.getSerializationLib),
        compressed = false,
        properties = Option(part.getSd).flatMap(sd => Option(sd.getSerdeInfo)).flatMap(si => Option(si.getParameters)).map(_.asScala.toMap).getOrElse(Map.empty)),
      parameters = Option(part.getParameters).map(_.asScala.toMap).getOrElse(Map.empty))
  }

  private def parsePartitionClause(partName: String): Map[String, String] = {
    partName.split("/").flatMap { token =>
      token.split("=").toList match {
        case k :: v :: Nil =>
          Some(k.trim -> v.trim.stripPrefix("'").stripSuffix("'"))
        case _ => None
      }
    }.toMap
  }
}
