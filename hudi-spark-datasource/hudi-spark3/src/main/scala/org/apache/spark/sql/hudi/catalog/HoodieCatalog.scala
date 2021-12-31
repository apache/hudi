/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.catalog

import org.apache.hadoop.fs.Path
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils, HoodieCatalogTable}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, ColumnChange, UpdateColumnType}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.hudi.command.{AlterHoodieTableAddColumnsCommand, AlterHoodieTableChangeColumnCommand, AlterHoodieTableRenameCommand, CreateHoodieTableCommand}
import org.apache.spark.sql.hudi.{HoodieSqlCommonUtils, SparkSqlUtils}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, _}

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

class HoodieCatalog extends DelegatingCatalogExtension with StagingTableCatalog with HoodieConfigHelper {

  val spark: SparkSession = SparkSession.active

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    if (SparkSqlUtils.isHoodieTable(properties.asScala.toMap)) {
      HoodieStagedTable(ident, this, schema, partitions, properties, TableCreationMode.STAGE_CREATE)
    } else {
      BaseStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    if (SparkSqlUtils.isHoodieTable(properties.asScala.toMap)) {
      HoodieStagedTable(ident, this, schema, partitions, properties, TableCreationMode.STAGE_REPLACE)
    } else {
      super.dropTable(ident)
      BaseStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageCreateOrReplace(ident: Identifier,
                                    schema: StructType,
                                    partitions: Array[Transform],
                                    properties: util.Map[String, String]): StagedTable = {
    if (SparkSqlUtils.isHoodieTable(properties.asScala.toMap)) {
      HoodieStagedTable(
        ident, this, schema, partitions, properties, TableCreationMode.CREATE_OR_REPLACE)
    } else {
      try super.dropTable(ident) catch {
        case _: NoSuchTableException => // ignore the exception
      }
      BaseStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      super.loadTable(ident) match {
        case v1: V1Table if HoodieSqlCommonUtils.isHoodieTable(v1.catalogTable) =>
          HoodieInternalV2Table(
            spark,
            v1.catalogTable.location.toString,
            catalogTable = Some(v1.catalogTable),
            tableIdentifier = Some(ident.toString))
        case o => o
      }
    } catch {
      case e: Exception =>
        throw e
    }
  }

  override def createTable(ident: Identifier,
                           schema: StructType,
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    createHoodieTable(ident, schema, partitions, properties, Map.empty, Option.empty, TableCreationMode.CREATE)
  }

  override def tableExists(ident: Identifier): Boolean = super.tableExists(ident)

  override def dropTable(ident: Identifier): Boolean = super.dropTable(ident)

  @throws[NoSuchTableException]
  @throws[TableAlreadyExistsException]
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    loadTable(oldIdent) match {
      case _: HoodieInternalV2Table =>
        new AlterHoodieTableRenameCommand(oldIdent.asTableIdentifier, newIdent.asTableIdentifier, false)
      case _ => super.renameTable(oldIdent, newIdent)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val tableIdent = TableIdentifier(ident.name(), ident.namespace().lastOption)
    // scalastyle:off
    val table = loadTable(ident) match {
      case hoodieTable: HoodieInternalV2Table => hoodieTable
      case _ => return super.alterTable(ident, changes: _*)
    }
    // scalastyle:on

    val grouped = changes.groupBy(c => c.getClass)

    grouped.foreach {
      case (t, newColumns) if t == classOf[AddColumn] =>
        AlterHoodieTableAddColumnsCommand(
          tableIdent,
          newColumns.asInstanceOf[Seq[AddColumn]].map { col =>
            StructField(
              col.fieldNames()(0),
              col.dataType(),
              col.isNullable)
          }).run(spark)
      case (t, columnChanges) if classOf[ColumnChange].isAssignableFrom(t) =>
        columnChanges.foreach {
          case dataType: UpdateColumnType =>
            val fieldName = dataType.fieldNames()(0)
            val newDataType = dataType.newDataType()
            val structField = StructField(fieldName, newDataType)
            AlterHoodieTableChangeColumnCommand(tableIdent, fieldName, structField).run(spark)
        }
      case (t, _) =>
        throw new UnsupportedOperationException(s"not supported table change: ${t.getClass}")
    }

    loadTable(ident)
  }

  def createHoodieTable(ident: Identifier,
                        schema: StructType,
                        partitions: Array[Transform],
                        allTableProperties: util.Map[String, String],
                        writeOptions: Map[String, String],
                        sourceQuery: Option[DataFrame],
                        operation: TableCreationMode): Table = {

    val (partitionColumns, maybeBucketSpec) = SparkSqlUtils.convertTransforms(partitions)
    val newSchema = schema
    val newPartitionColumns = partitionColumns
    val newBucketSpec = maybeBucketSpec

    val isByPath = isPathIdentifier(ident)

    val location = if (isByPath) {
      Option(ident.name())
    } else {
      Option(allTableProperties.get("location"))
    }
    //val id = TableIdentifier(ident.name(), ident.namespace().lastOption)
    val id = ident.asTableIdentifier

    val locUriOpt = location.map(CatalogUtils.stringToURI)
    val existingTableOpt = getExistingTableIfExists(id)
    val loc = locUriOpt
      .orElse(existingTableOpt.flatMap(_.storage.locationUri))
      .getOrElse(spark.sessionState.catalog.defaultTablePath(id))
    val storage = DataSource.buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = Option(loc))
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val commentOpt = Option(allTableProperties.get("comment"))

    val tableDesc = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = newSchema,
      provider = Option("hudi"),
      partitionColumnNames = newPartitionColumns,
      bucketSpec = newBucketSpec,
      properties = allTableProperties.asScala.toMap,
      comment = commentOpt)

    val hoodieCatalogTable = HoodieCatalogTable(spark, tableDesc)

    if (operation == TableCreationMode.STAGE_CREATE) {
      hoodieCatalogTable.initHoodieTable()
      saveSourceDF(sourceQuery, tableDesc.properties ++ buildHoodieInsertConfig(hoodieCatalogTable, spark, isOverwrite = false, Map.empty, Map.empty))
    } else {
      if (sourceQuery.isEmpty) {
        saveSourceDF(sourceQuery, tableDesc.properties)
      } else {
        saveSourceDF(sourceQuery, tableDesc.properties ++ buildHoodieInsertConfig(hoodieCatalogTable, spark, isOverwrite = false, Map.empty, Map.empty))
      }
    }

    new CreateHoodieTableCommand(tableDesc, false).run(spark)

    loadTable(ident)
  }

  private def isPathIdentifier(ident: Identifier) = {
    new Path(ident.name()).isAbsolute
  }

  protected def isPathIdentifier(table: CatalogTable): Boolean = {
    isPathIdentifier(table.identifier)
  }

  protected def isPathIdentifier(tableIdentifier: TableIdentifier) : Boolean = {
    isPathIdentifier(HoodieIdentifierHelper.of(tableIdentifier.database.toArray, tableIdentifier.table))
  }

  private def getExistingTableIfExists(table: TableIdentifier): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself
    val catalog = spark.sessionState.catalog
    // scalastyle:off
    if (isPathIdentifier(table)) {
      return None
    }
    // scalastyle:on
    val tableExists = catalog.tableExists(table)
    if (tableExists) {
      val oldTable = catalog.getTableMetadata(table)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw new HoodieException(
          s"$table is a view. You may not write data into a view.")
      }
      if (!HoodieSqlCommonUtils.isHoodieTable(oldTable)) {
        throw new HoodieException(s"$table is not a Hoodie table.")
      }
      Some(oldTable)
    } else {
      None
    }
  }

  private def saveSourceDF(sourceQuery: Option[Dataset[_]],
                           properties: Map[String, String]): Unit = {
    sourceQuery.map(df => {
      df.write.format("org.apache.hudi")
        .options(properties)
        .mode(SaveMode.Append)
        .save()
      df
    })
  }
}
