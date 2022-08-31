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
import org.apache.hudi.sql.InsertMode
import org.apache.hudi.sync.common.util.ConfigUtils
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, SparkAdapterSupport}
import org.apache.spark.sql.HoodieSpark3CatalogUtils.MatchBucketTransform
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable.needFilterProps
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, ColumnChange, UpdateColumnComment, UpdateColumnType}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.hudi.analysis.HoodieV1OrV2Table
import org.apache.spark.sql.hudi.command._
import org.apache.spark.sql.hudi.{HoodieSqlCommonUtils, ProvidesHoodieConfig}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, _}

import java.net.URI
import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.mutable

class HoodieCatalog extends DelegatingCatalogExtension
  with StagingTableCatalog
  with SparkAdapterSupport
  with ProvidesHoodieConfig {

  val spark: SparkSession = SparkSession.active

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    if (sparkAdapter.isHoodieTable(properties)) {
      val locUriAndTableType = deduceTableLocationURIAndTableType(ident, properties)
      HoodieStagedTable(ident, locUriAndTableType, this, schema, partitions,
        properties, TableCreationMode.STAGE_CREATE)
    } else {
      BasicStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    if (sparkAdapter.isHoodieTable(properties)) {
      val locUriAndTableType = deduceTableLocationURIAndTableType(ident, properties)
      HoodieStagedTable(ident, locUriAndTableType, this, schema, partitions,
        properties, TableCreationMode.STAGE_REPLACE)
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
      val locUriAndTableType = deduceTableLocationURIAndTableType(ident, properties)
      HoodieStagedTable(ident, locUriAndTableType, this, schema, partitions,
        properties, TableCreationMode.CREATE_OR_REPLACE)
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
    super.loadTable(ident) match {
      case V1Table(catalogTable0) if sparkAdapter.isHoodieTable(catalogTable0) =>
        val catalogTable = catalogTable0.comment match {
          case Some(v) =>
            val newProps = catalogTable0.properties + (TableCatalog.PROP_COMMENT -> v)
            catalogTable0.copy(properties = newProps)
          case _ =>
            catalogTable0
        }

        val v2Table = HoodieInternalV2Table(
          spark = spark,
          path = catalogTable.location.toString,
          catalogTable = Some(catalogTable),
          tableIdentifier = Some(ident.toString))

        val schemaEvolutionEnabled: Boolean = spark.sessionState.conf.getConfString(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
          DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean

        // NOTE: PLEASE READ CAREFULLY
        //
        // Since Hudi relations don't currently implement DS V2 Read API, we by default fallback to V1 here.
        // Such fallback will have considerable performance impact, therefore it's only performed in cases
        // where V2 API have to be used. Currently only such use-case is using of Schema Evolution feature
        //
        // Check out HUDI-4178 for more details
        if (schemaEvolutionEnabled) {
          v2Table
        } else {
          v2Table.v1TableWrapper
        }

      case t => t
    }
  }

  override def createTable(ident: Identifier,
                           schema: StructType,
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    if (sparkAdapter.isHoodieTable(properties)) {
      val locUriAndTableType = deduceTableLocationURIAndTableType(ident, properties)
      createHoodieTable(ident, schema, locUriAndTableType, partitions, properties,
        Map.empty, Option.empty, TableCreationMode.CREATE)
    } else {
      super.createTable(ident, schema, partitions, properties)
    }
  }

  override def tableExists(ident: Identifier): Boolean = super.tableExists(ident)

  override def dropTable(ident: Identifier): Boolean = {
    val table = loadTable(ident)
    table match {
      case HoodieV1OrV2Table(_) =>
        DropHoodieTableCommand(ident.asTableIdentifier, ifExists = true, isView = false, purge = false).run(spark)
        true
      case _ => super.dropTable(ident)
    }
  }

  override def purgeTable(ident: Identifier): Boolean = {
    val table = loadTable(ident)
    table match {
      case HoodieV1OrV2Table(_) =>
        DropHoodieTableCommand(ident.asTableIdentifier, ifExists = true, isView = false, purge = true).run(spark)
        true
      case _ => super.purgeTable(ident)
    }
  }

  @throws[NoSuchTableException]
  @throws[TableAlreadyExistsException]
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    loadTable(oldIdent) match {
      case HoodieV1OrV2Table(_) =>
        AlterHoodieTableRenameCommand(oldIdent.asTableIdentifier, newIdent.asTableIdentifier, false).run(spark)
      case _ => super.renameTable(oldIdent, newIdent)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    loadTable(ident) match {
      case HoodieV1OrV2Table(table) => {
        val tableIdent = TableIdentifier(ident.name(), ident.namespace().lastOption)
        changes.groupBy(c => c.getClass).foreach {
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
                val fieldOpt = table.schema.findNestedField(dataType.fieldNames(), includeCollections = true,
                  spark.sessionState.conf.resolver).map(_._2)
                val field = fieldOpt.getOrElse {
                  throw new AnalysisException(
                    s"Couldn't find column $colName in:\n${table.schema.treeString}")
                }
                AlterHoodieTableChangeColumnCommand(tableIdent, colName, field.withComment(newComment)).run(spark)
            }
          case (t, _) =>
            throw new UnsupportedOperationException(s"not supported table change: ${t.getClass}")
        }

        loadTable(ident)
      }
      case _ => super.alterTable(ident, changes: _*)
    }
  }

  private def deduceTableLocationURIAndTableType(
      ident: Identifier, properties: util.Map[String, String]): (URI, CatalogTableType) = {
    val locOpt = if (isPathIdentifier(ident)) {
      Option(ident.name())
    } else {
      Option(properties.get("location"))
    }
    val tableType = if (locOpt.nonEmpty) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }
    val locUriOpt = locOpt.map(CatalogUtils.stringToURI)
    val tableIdent = ident.asTableIdentifier
    val existingTableOpt = getExistingTableIfExists(tableIdent)
    val locURI = locUriOpt
      .orElse(existingTableOpt.flatMap(_.storage.locationUri))
      .getOrElse(spark.sessionState.catalog.defaultTablePath(tableIdent))
    (locURI, tableType)
  }

  def createHoodieTable(ident: Identifier,
                        schema: StructType,
                        locUriAndTableType: (URI, CatalogTableType),
                        partitions: Array[Transform],
                        allTableProperties: util.Map[String, String],
                        writeOptions: Map[String, String],
                        sourceQuery: Option[DataFrame],
                        operation: TableCreationMode): Table = {

    val (partitionColumns, maybeBucketSpec) = HoodieCatalog.convertTransforms(partitions)
    val newSchema = schema
    val newPartitionColumns = partitionColumns
    val newBucketSpec = maybeBucketSpec

    val storage = DataSource.buildStorageFormatFromOptions(writeOptions.--(needFilterProps))
      .copy(locationUri = Option(locUriAndTableType._1))
    val commentOpt = Option(allTableProperties.get("comment"))

    val tablePropertiesNew = new util.HashMap[String, String](allTableProperties)
    // put path to table properties.
    tablePropertiesNew.put("path", locUriAndTableType._1.getPath)

    val tableDesc = new CatalogTable(
      identifier = ident.asTableIdentifier,
      tableType = locUriAndTableType._2,
      storage = storage,
      schema = newSchema,
      provider = Option("hudi"),
      partitionColumnNames = newPartitionColumns,
      bucketSpec = newBucketSpec,
      properties = tablePropertiesNew.asScala.toMap.--(needFilterProps),
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

object HoodieCatalog {
  def convertTransforms(transforms: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    transforms.foreach {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case MatchBucketTransform(numBuckets, col, sortCol) =>
        if (bucketSpec.nonEmpty) {
          throw new HoodieException("Multiple bucket transformations are not supported")
        } else if (sortCol.isEmpty) {
          bucketSpec = Some(BucketSpec(numBuckets, col.map(_.fieldNames.mkString(".")), Nil))
        } else {
          bucketSpec = Some(BucketSpec(numBuckets, col.map(_.fieldNames.mkString(".")),
            sortCol.map(_.fieldNames.mkString("."))))
        }

      case t => throw new HoodieException(s"Partitioning by transformation `$t` is not supported")
    }

    (identityCols, bucketSpec)
  }
}
