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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, CatalogStatistics, CatalogTable, CatalogTablePartition, InMemoryCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

/**
 * Test-only external catalog that rejects every table-, partition- and function-level
 * operation on the `default` database, mirroring production catalogs that only allow
 * connecting to `default`. Database-level operations behave normally, so a session can
 * still start, while any code path that silently falls back to `default` fails loudly.
 */
class StrictDefaultDatabaseCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends InMemoryCatalog(conf, hadoopConf) {

  private def validateDb(db: String): Unit = {
    if (DEFAULT_DATABASE.equalsIgnoreCase(db)) {
      throw new StrictDefaultDatabaseException(db)
    }
  }

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    validateDb(tableDefinition.database)
    super.createTable(tableDefinition, ignoreIfExists)
  }

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {
    validateDb(db)
    super.dropTable(db, table, ignoreIfNotExists, purge)
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    validateDb(db)
    super.renameTable(db, oldName, newName)
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    validateDb(tableDefinition.database)
    super.alterTable(tableDefinition)
  }

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = {
    validateDb(db)
    super.alterTableDataSchema(db, table, newDataSchema)
  }

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit = {
    validateDb(db)
    super.alterTableStats(db, table, stats)
  }

  override def getTable(db: String, table: String): CatalogTable = {
    validateDb(db)
    super.getTable(db, table)
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = {
    validateDb(db)
    super.getTablesByName(db, tables)
  }

  override def tableExists(db: String, table: String): Boolean = {
    validateDb(db)
    super.tableExists(db, table)
  }

  override def listTables(db: String): Seq[String] = {
    validateDb(db)
    super.listTables(db)
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    validateDb(db)
    super.listTables(db, pattern)
  }

  override def listViews(db: String, pattern: String): Seq[String] = {
    validateDb(db)
    super.listViews(db, pattern)
  }

  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean,
                         isSrcLocal: Boolean): Unit = {
    validateDb(db)
    super.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
  }

  override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec,
                             isOverwrite: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = {
    validateDb(db)
    super.loadPartition(db, table, loadPath, partition, isOverwrite, inheritTableSpecs, isSrcLocal)
  }

  override def loadDynamicPartitions(db: String, table: String, loadPath: String, partition: TablePartitionSpec,
                                     replace: Boolean, numDP: Int): Unit = {
    validateDb(db)
    super.loadDynamicPartitions(db, table, loadPath, partition, replace, numDP)
  }

  override def createPartitions(db: String, table: String, newParts: Seq[CatalogTablePartition],
                                ignoreIfExists: Boolean): Unit = {
    validateDb(db)
    super.createPartitions(db, table, newParts, ignoreIfExists)
  }

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean,
                              purge: Boolean, retainData: Boolean): Unit = {
    validateDb(db)
    super.dropPartitions(db, table, parts, ignoreIfNotExists, purge, retainData)
  }

  override def renamePartitions(db: String, table: String, fromSpecs: Seq[TablePartitionSpec],
                                toSpecs: Seq[TablePartitionSpec]): Unit = {
    validateDb(db)
    super.renamePartitions(db, table, fromSpecs, toSpecs)
  }

  override def alterPartitions(db: String, table: String, alterParts: Seq[CatalogTablePartition]): Unit = {
    validateDb(db)
    super.alterPartitions(db, table, alterParts)
  }

  override def getPartition(db: String, table: String, partSpec: TablePartitionSpec): CatalogTablePartition = {
    validateDb(db)
    super.getPartition(db, table, partSpec)
  }

  override def getPartitionOption(db: String, table: String,
                                  partSpec: TablePartitionSpec): Option[CatalogTablePartition] = {
    validateDb(db)
    super.getPartitionOption(db, table, partSpec)
  }

  override def listPartitionNames(db: String, table: String, partSpec: Option[TablePartitionSpec]): Seq[String] = {
    validateDb(db)
    super.listPartitionNames(db, table, partSpec)
  }

  override def listPartitions(db: String, table: String,
                              partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    validateDb(db)
    super.listPartitions(db, table, partialSpec)
  }

  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression],
                                      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    validateDb(db)
    super.listPartitionsByFilter(db, table, predicates, defaultTimeZoneId)
  }

  override def createFunction(db: String, func: CatalogFunction): Unit = {
    validateDb(db)
    super.createFunction(db, func)
  }

  override def dropFunction(db: String, funcName: String): Unit = {
    validateDb(db)
    super.dropFunction(db, funcName)
  }

  override def alterFunction(db: String, func: CatalogFunction): Unit = {
    validateDb(db)
    super.alterFunction(db, func)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    validateDb(db)
    super.renameFunction(db, oldName, newName)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    validateDb(db)
    super.getFunction(db, funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    validateDb(db)
    super.functionExists(db, funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    validateDb(db)
    super.listFunctions(db, pattern)
  }
}

/**
 * Thrown by [[StrictDefaultDatabaseCatalog]] when a table-, partition- or function-level
 * operation targets the `default` database.
 */
class StrictDefaultDatabaseException(db: String)
  extends RuntimeException(s"No valid database specified: the test catalog rejects table and function "
    + s"operations on database '$db'")
