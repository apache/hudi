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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.{DataSourceUtils, HoodieSchemaConversionUtils, HoodieWriterUtils}
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieFailedWritesCleaningPolicy, WriteOperationType}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.schema.evolution.{ColumnChangeID, ColumnPositionType, HoodieSchemaChangeApplier, HoodieSchemaHistoryStorageManager, HoodieSchemaInternalSchemaBridge, HoodieSchemaSerDe}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.util.{CommitUtils, Option}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.internal.schema.action.TableChanges
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter
import org.apache.hudi.internal.schema.utils.SchemaChangeUtils
import org.apache.hudi.table.HoodieSparkTable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, DeleteColumn, RemoveProperty, SetProperty}
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.types.StructType

import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

case class AlterTableCommand(table: CatalogTable, changes: Seq[TableChange], changeType: ColumnChangeID) extends HoodieLeafRunnableCommand with Logging {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    changeType match {
      case ColumnChangeID.ADD => applyAddAction(sparkSession)
      case ColumnChangeID.DELETE => applyDeleteAction(sparkSession)
      case ColumnChangeID.UPDATE => applyUpdateAction(sparkSession)
      case ColumnChangeID.PROPERTY_CHANGE if (changes.filter(_.isInstanceOf[SetProperty]).size == changes.size) =>
        applyPropertySet(sparkSession)
      case ColumnChangeID.PROPERTY_CHANGE if (changes.filter(_.isInstanceOf[RemoveProperty]).size == changes.size) =>
        applyPropertyUnset(sparkSession)
      case ColumnChangeID.REPLACE => applyReplaceAction(sparkSession)
      case other => throw new RuntimeException(s"find unsupported alter command type: ${other}")
    }
    Seq.empty[Row]
  }

  def applyReplaceAction(sparkSession: SparkSession): Unit = {
    // convert to delete first then add again
    val deleteChanges = changes.filter(p => p.isInstanceOf[DeleteColumn]).map(_.asInstanceOf[DeleteColumn])
    val addChanges = changes.filter(p => p.isInstanceOf[AddColumn]).map(_.asInstanceOf[AddColumn])
    val (oldSchema, historySchema) = getEvolutionSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyAddAction2Schema(sparkSession, applyDeleteAction2Schema(sparkSession, oldSchema, deleteChanges), addChanges)
    val verifiedHistorySchema = inheritedHistory(oldSchema, historySchema)
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession)
    logInfo("column replace finished")
  }

  def applyAddAction2Schema(sparkSession: SparkSession, oldSchema: HoodieSchema, addChanges: Seq[AddColumn]): HoodieSchema = {
    var cur = oldSchema
    addChanges.foreach { addColumn =>
      val names = addColumn.fieldNames()
      val parentName = AlterTableCommand.getParentName(names)
      val fullName = names.mkString(".")
      val colType = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(addColumn.dataType(), names.last)
      val (positionType, positionRef) = addColumn.position() match {
        case after: TableChange.After =>
          (ColumnPositionType.AFTER, if (parentName.isEmpty) after.column() else parentName + "." + after.column())
        case _: TableChange.First =>
          (ColumnPositionType.FIRST, "")
        case _ =>
          (ColumnPositionType.NO_OPERATION, "")
      }
      cur = new HoodieSchemaChangeApplier(cur).applyAddChange(fullName, colType, addColumn.comment(), positionRef, positionType)
    }
    cur
  }

  private def applyDeleteAction2Schema(sparkSession: SparkSession, oldSchema: HoodieSchema, deleteChanges: Seq[DeleteColumn]): HoodieSchema = {
    val colNames = deleteChanges.map { c =>
      val name = c.fieldNames().mkString(".")
      checkSchemaChange(Seq(name), table)
      name
    }.toArray
    if (colNames.isEmpty) {
      oldSchema
    } else {
      val newSchema = new HoodieSchemaChangeApplier(oldSchema).applyDeleteChange(colNames: _*)
      // delete action should not change the getMaxColumnId field
      newSchema.setMaxColumnId(oldSchema.maxColumnId())
      newSchema
    }
  }


  def applyAddAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getEvolutionSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyAddAction2Schema(sparkSession, oldSchema, changes.map(_.asInstanceOf[AddColumn]))
    val verifiedHistorySchema = inheritedHistory(oldSchema, historySchema)
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession)
    logInfo("column add finished")
  }

  def applyDeleteAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getEvolutionSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyDeleteAction2Schema(sparkSession, oldSchema, changes.map(_.asInstanceOf[DeleteColumn]))
    val verifiedHistorySchema = inheritedHistory(oldSchema, historySchema)
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession)
    logInfo("column delete finished")
  }

  def applyUpdateAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getEvolutionSchemaAndHistorySchemaStr(sparkSession)
    // Accumulate all per-column updates into a single ColumnUpdateChange resolved against
    // the original schema. This preserves the legacy batch semantics where multiple
    // changes targeting the same field id (e.g. RENAME a→b combined with UPDATE a TYPE int)
    // both find the field by its original name; if we built a fresh applier per change
    // the second would resolve against the post-rename schema and fail.
    val oldInternal = HoodieSchemaInternalSchemaBridge.toInternalSchema(oldSchema)
    val updateChange = TableChanges.ColumnUpdateChange.get(oldInternal)
    changes.foreach {
      case updateType: TableChange.UpdateColumnType =>
        val newType = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
          updateType.newDataType(), updateType.fieldNames().last)
        updateChange.updateColumnType(updateType.fieldNames().mkString("."),
          InternalSchemaConverter.convertToField(newType))
      case updateComment: TableChange.UpdateColumnComment =>
        updateChange.updateColumnComment(updateComment.fieldNames().mkString("."), updateComment.newComment())
      case updateName: TableChange.RenameColumn =>
        val originalColName = updateName.fieldNames().mkString(".")
        checkSchemaChange(Seq(originalColName), table)
        updateChange.renameColumn(originalColName, updateName.newName())
      case updateNullAbility: TableChange.UpdateColumnNullability =>
        updateChange.updateColumnNullability(
          updateNullAbility.fieldNames().mkString("."), updateNullAbility.nullable())
      case updatePosition: TableChange.UpdateColumnPosition =>
        val names = updatePosition.fieldNames()
        val parentName = AlterTableCommand.getParentName(names)
        val fullName = names.mkString(".")
        updatePosition.position() match {
          case after: TableChange.After =>
            val refCol = if (parentName.isEmpty) after.column() else parentName + "." + after.column()
            updateChange.addPositionChange(fullName, refCol, "after")
          case _: TableChange.First =>
            updateChange.addPositionChange(fullName, "", "first")
          case _ =>
        }
      case _ =>
    }
    val newInternal = SchemaChangeUtils.applyTableChanges2Schema(oldInternal, updateChange)
    val newSchema = HoodieSchemaInternalSchemaBridge.toHoodieSchema(newInternal, oldSchema.getFullName)
    val verifiedHistorySchema = inheritedHistory(oldSchema, historySchema)
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession)
    logInfo("column update finished")
  }

  private def inheritedHistory(oldSchema: HoodieSchema, historySchema: String): String = {
    if (historySchema == null || historySchema.isEmpty) {
      HoodieSchemaSerDe.inheritHistory(oldSchema, "")
    } else {
      historySchema
    }
  }

  // to do support unset default value to columns, and apply them to the evolution schema
  def applyPropertyUnset(sparkSession: SparkSession): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val propKeys = changes.map(_.asInstanceOf[RemoveProperty]).map(_.property())
    // ignore NonExist unset
    propKeys.foreach { k =>
      if (!table.properties.contains(k) && k != TableCatalog.PROP_COMMENT) {
        logWarning(s"Cannot remove property [$k] because it is not currently set for the table.")
      }
    }
    val tableComment = if (propKeys.contains(TableCatalog.PROP_COMMENT)) None else table.comment
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties, comment = tableComment)
    catalog.alterTable(newTable)
    logInfo("table properties change finished")
  }

  // to do support set default value to columns, and apply them to the evolution schema
  def applyPropertySet(sparkSession: SparkSession): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val properties = changes.map(_.asInstanceOf[SetProperty]).map(f => f.property -> f.value).toMap
    // This overrides old properties and update the comment parameter of CatalogTable
    // with the newly added/modified comment since CatalogTable also holds comment as its
    // direct property.
    val newTable = table.copy(
      properties = table.properties ++ properties,
      comment = properties.get(TableCatalog.PROP_COMMENT).orElse(table.comment))
    catalog.alterTable(newTable)
    logInfo("table properties change finished")
  }

  def getEvolutionSchemaAndHistorySchemaStr(sparkSession: SparkSession): (HoodieSchema, String) = {
    val path = AlterTableCommand.getTableLocation(table, sparkSession)
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf())).build()
    val schemaUtil = new TableSchemaResolver(metaClient)

    val schema = schemaUtil.getTableEvolutionSchemaFromCommitMetadata().orElse(schemaUtil.getTableSchema)
    val historySchemaStr = schemaUtil.getTableHistorySchemaStrFromCommitMetadata.orElse("")
    (schema, historySchemaStr)
  }

  def checkSchemaChange(colNames: Seq[String], catalogTable: CatalogTable): Unit = {
    val primaryKeys = catalogTable.storage.properties.getOrElse("primaryKey", catalogTable.properties.getOrElse("primaryKey", "keyid")).split(",").map(_.trim)
    val orderingFields = Seq(catalogTable.storage.properties.getOrElse("orderingFields", catalogTable.properties.getOrElse("orderingFields", "ts"))).map(_.trim)
    val partitionKey = catalogTable.partitionColumnNames.map(_.trim)
    val checkNames = primaryKeys ++ orderingFields ++ partitionKey
    colNames.foreach { col =>
      if (checkNames.contains(col)) {
        throw new UnsupportedOperationException("cannot support apply changes for primaryKey/orderingFields/partitionKey")
      }
    }
  }
}

object AlterTableCommand extends Logging {

  /**
    * Generate an commit with new schema to change the table's schema.
    *
    * @param evolutionSchema new schema after change
    * @param historySchemaStr history schemas
    * @param table The hoodie table.
    * @param sparkSession The spark session.
    */
  def commitWithSchema(evolutionSchema: HoodieSchema, historySchemaStr: String, table: CatalogTable, sparkSession: SparkSession): Unit = {
    val schema = evolutionSchema
    val path = getTableLocation(table, sparkSession)
    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    val client = DataSourceUtils.createHoodieClient(
      jsc,
      schema.toString,
      path,
      table.identifier.table,
      HoodieWriterUtils.parametersWithWriteDefaults(
        HoodieOptionConfig.mapSqlOptionsToDataSourceWriteConfigs(table.storage.properties ++ table.properties) ++
        sparkSession.sessionState.conf.getAllConfs ++ Map(
        HoodieCleanConfig.AUTO_CLEAN.key -> "false",
        HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key -> HoodieFailedWritesCleaningPolicy.NEVER.name,
        HoodieArchivalConfig.AUTO_ARCHIVE.key -> "false"
        )).asJava)

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(path)
      .setConf(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf()))
      .setTimeGeneratorConfig(client.getConfig.getTimeGeneratorConfig)
      .build()

    val commitActionType = CommitUtils.getCommitActionType(WriteOperationType.ALTER_SCHEMA, metaClient.getTableType)
    val instantTime = client.startCommit(commitActionType)
    client.setOperationType(WriteOperationType.ALTER_SCHEMA)

    val hoodieTable = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
    hoodieTable.validateSchema()
    val timeLine = hoodieTable.getActiveTimeline
    val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
    val requested = instantGenerator.createNewInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.ALTER_SCHEMA)
    timeLine.transitionRequestedToInflight(requested, Option.of(metadata))
    val extraMeta = new util.HashMap[String, String]()
    evolutionSchema.setSchemaId(instantTime.toLong)
    extraMeta.put(HoodieSchemaSerDe.LATEST_SCHEMA, HoodieSchemaSerDe.toJson(evolutionSchema))
    val schemaManager = new HoodieSchemaHistoryStorageManager(metaClient)
    schemaManager.persistHistorySchemaStr(instantTime, HoodieSchemaSerDe.inheritHistory(evolutionSchema, historySchemaStr))
    client.commit(instantTime, jsc.emptyRDD, Option.of(extraMeta))
    val existRoTable = sparkSession.catalog.tableExists(table.identifier.unquotedString + "_ro")
    val existRtTable = sparkSession.catalog.tableExists(table.identifier.unquotedString + "_rt")
    try {
      sparkSession.catalog.refreshTable(table.identifier.unquotedString)
      // try to refresh ro/rt table
      if (existRoTable) sparkSession.catalog.refreshTable(table.identifier.unquotedString + "_ro")
      if (existRoTable) sparkSession.catalog.refreshTable(table.identifier.unquotedString + "_rt")
    } catch {
      case NonFatal(e) =>
        log.error(s"Exception when attempting to refresh table ${table.identifier.quotedString}", e)
    }
    // try to sync to hive
    // drop partition field before call alter table
    val fullSparkSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(evolutionSchema)
    val dataSparkSchema = new StructType(fullSparkSchema.fields.filter(p => !table.partitionColumnNames.exists(f => sparkSession.sessionState.conf.resolver(f, p.name))))
    alterTableDataSchema(sparkSession, table.identifier.database.getOrElse("default"), table.identifier.table, dataSparkSchema)
    if (existRoTable) alterTableDataSchema(sparkSession, table.identifier.database.getOrElse("default"), table.identifier.table + "_ro", dataSparkSchema)
    if (existRtTable) alterTableDataSchema(sparkSession, table.identifier.database.getOrElse("default"), table.identifier.table + "_rt", dataSparkSchema)
  }

  def alterTableDataSchema(sparkSession: SparkSession, db: String, tableName: String, dataSparkSchema: StructType): Unit = {
    sparkSession.sessionState.catalog
      .externalCatalog
      .alterTableDataSchema(db, tableName, dataSparkSchema)
  }

  def getTableLocation(table: CatalogTable, sparkSession: SparkSession): String = {
    val uri = if (table.tableType == CatalogTableType.MANAGED) {
      Some(sparkSession.sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }
    val conf = sparkSession.sessionState.newHadoopConf()
    uri.map(makePathQualified(_, conf))
      .map(removePlaceHolder)
      .getOrElse(throw new IllegalArgumentException(s"Missing location for ${table.identifier}"))
  }

  private def removePlaceHolder(path: String): String = {
    if (path == null || path.length == 0) {
      path
    } else if (path.endsWith("-PLACEHOLDER")) {
      path.substring(0, path.length() - 16)
    } else {
      path
    }
  }

  def makePathQualified(path: URI, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath).toUri.toString
  }

  def getParentName(names: Array[String]): String = {
    if (names.size > 1) {
      names.dropRight(1).mkString(".")
    } else ""
  }
}
