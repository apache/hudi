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
import org.apache.hudi.hive.util.ConfigUtils
import org.apache.hudi.sql.InsertMode
import org.apache.hudi.{DataSourceWriteOptions, SparkAdapterSupport}
import org.apache.spark.sql.HoodieSpark3SqlUtils.convertTransforms
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils, HoodieCatalogTable}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, ColumnChange, UpdateColumnComment, UpdateColumnType}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.hudi.command._
import org.apache.spark.sql.hudi.{HoodieSqlCommonUtils, ProvidesHoodieConfig}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, _}

import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

class HoodieCatalog extends DelegatingCatalogExtension
  with StagingTableCatalog
  with SparkAdapterSupport
  with ProvidesHoodieConfig {

  val spark: SparkSession = SparkSession.active

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    if (sparkAdapter.isHoodieTable(properties)) {
      HoodieStagedTable(ident, this, schema, partitions, properties, TableCreationMode.STAGE_CREATE)
    } else {
      BasicStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    if (sparkAdapter.isHoodieTable(properties)) {
      HoodieStagedTable(ident, this, schema, partitions, properties, TableCreationMode.STAGE_REPLACE)
    } else {
      super.dropTable(ident)
      BasicStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageCreateOrReplace(ident: Identifier,
                                    schema: StructType,
                                    partitions: Array[Transform],
                                    properties: util.Map[String, String]): StagedTable = {
    if (sparkAdapter.isHoodieTable(properties)) {
      HoodieStagedTable(
        ident, this, schema, partitions, properties, TableCreationMode.CREATE_OR_REPLACE)
    } else {
      try super.dropTable(ident) catch {
        case _: NoSuchTableException => // ignore the exception
      }
      BasicStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      super.loadTable(ident) match {
        case v1: V1Table if sparkAdapter.isHoodieTable(v1.catalogTable) =>
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

  override def dropTable(ident: Identifier): Boolean = {
    val table = loadTable(ident)
    table match {
      case _: HoodieInternalV2Table =>
        DropHoodieTableCommand(ident.asTableIdentifier, ifExists = true, isView = false, purge = false).run(spark)
        true
      case _ => super.dropTable(ident)
    }
  }

  override def purgeTable(ident: Identifier): Boolean = {
    val table = loadTable(ident)
    table match {
      case _: HoodieInternalV2Table =>
        DropHoodieTableCommand(ident.asTableIdentifier, ifExists = true, isView = false, purge = true).run(spark)
        true
      case _ => super.purgeTable(ident)
    }
  }

  @throws[NoSuchTableException]
  @throws[TableAlreadyExistsException]
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    loadTable(oldIdent) match {
      case _: HoodieInternalV2Table =>
        new AlterHoodieTableRenameCommand(oldIdent.asTableIdentifier, newIdent.asTableIdentifier, false).run(spark)
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
            val colName = UnresolvedAttribute(dataType.fieldNames()).name
            val newDataType = dataType.newDataType()
            val structField = StructField(colName, newDataType)
            AlterHoodieTableChangeColumnCommand(tableIdent, colName, structField).run(spark)
          case dataType: UpdateColumnComment =>
            val newComment = dataType.newComment()
            val colName = UnresolvedAttribute(dataType.fieldNames()).name
            val fieldOpt = table.schema().findNestedField(dataType.fieldNames(), includeCollections = true,
              spark.sessionState.conf.resolver).map(_._2)
            val field = fieldOpt.getOrElse {
              throw new AnalysisException(
                s"Couldn't find column $colName in:\n${table.schema().treeString}")
            }
            AlterHoodieTableChangeColumnCommand(tableIdent, colName, field.withComment(newComment)).run(spark)
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

    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions)
    val newSchema = schema
    val newPartitionColumns = partitionColumns
    val newBucketSpec = maybeBucketSpec

    val isByPath = isPathIdentifier(ident)

    val location = if (isByPath) Option(ident.name()) else Option(allTableProperties.get("location"))
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

    val tablePropertiesNew = new util.HashMap[String, String](allTableProperties)
    // put path to table properties.
    tablePropertiesNew.put("path", loc.getPath)

    val tableDesc = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = newSchema,
      provider = Option("hudi"),
      partitionColumnNames = newPartitionColumns,
      bucketSpec = newBucketSpec,
      properties = tablePropertiesNew.asScala.toMap,
      comment = commentOpt)

    val hoodieCatalogTable = HoodieCatalogTable(spark, tableDesc)

    if (operation == TableCreationMode.STAGE_CREATE) {
      val tablePath = hoodieCatalogTable.tableLocation
      val hadoopConf = spark.sessionState.newHadoopConf()
      assert(HoodieSqlCommonUtils.isEmptyPath(tablePath, hadoopConf),
        s"Path '$tablePath' should be empty for CTAS")
      hoodieCatalogTable.initHoodieTable()

      val tblProperties = hoodieCatalogTable.catalogProperties
      val options = Map(
        DataSourceWriteOptions.HIVE_CREATE_MANAGED_TABLE.key -> (tableDesc.tableType == CatalogTableType.MANAGED).toString,
        DataSourceWriteOptions.HIVE_TABLE_SERDE_PROPERTIES.key -> ConfigUtils.configToString(tblProperties.asJava),
        DataSourceWriteOptions.HIVE_TABLE_PROPERTIES.key -> ConfigUtils.configToString(tableDesc.properties.asJava),
        DataSourceWriteOptions.SQL_INSERT_MODE.key -> InsertMode.NON_STRICT.value(),
        DataSourceWriteOptions.SQL_ENABLE_BULK_INSERT.key -> "true"
      )
      saveSourceDF(sourceQuery, tableDesc.properties ++ buildHoodieInsertConfig(hoodieCatalogTable, spark, isOverwrite = false, Map.empty, options))
      CreateHoodieTableCommand.createTableInCatalog(spark, hoodieCatalogTable, ignoreIfExists = false)
    } else if (sourceQuery.isEmpty) {
      saveSourceDF(sourceQuery, tableDesc.properties)
      new CreateHoodieTableCommand(tableDesc, false).run(spark)
    } else {
      saveSourceDF(sourceQuery, tableDesc.properties ++ buildHoodieInsertConfig(hoodieCatalogTable, spark, isOverwrite = false, Map.empty, Map.empty))
      new CreateHoodieTableCommand(tableDesc, false).run(spark)
    }

    loadTable(ident)
  }

  private def isPathIdentifier(ident: Identifier) = new Path(ident.name()).isAbsolute

  protected def isPathIdentifier(table: CatalogTable): Boolean = {
    isPathIdentifier(table.identifier)
  }

  protected def isPathIdentifier(tableIdentifier: TableIdentifier): Boolean = {
    isPathIdentifier(HoodieIdentifier(tableIdentifier.database.toArray, tableIdentifier.table))
  }

  private def getExistingTableIfExists(table: TableIdentifier): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself
    val catalog = spark.sessionState.catalog
    // scalastyle:off
    if (isPathIdentifier(table)) return None
    // scalastyle:on
    val tableExists = catalog.tableExists(table)
    if (tableExists) {
      val oldTable = catalog.getTableMetadata(table)
      if (oldTable.tableType == CatalogTableType.VIEW) throw new HoodieException(
        s"$table is a view. You may not write data into a view.")
      if (!sparkAdapter.isHoodieTable(oldTable)) throw new HoodieException(s"$table is not a Hoodie table.")
      Some(oldTable)
    } else None
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
