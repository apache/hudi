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

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.model.{HoodieCommitMetadata, WriteOperationType}
import org.apache.hudi.{DataSourceOptionsHelper, DataSourceUtils}
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant}
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{CommitUtils, Option}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.action.TableChange.ColumnChangeID
import org.apache.hudi.internal.schema.action.TableChanges
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.{SchemaChangeUtils, SerDeHelper}
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager
import org.apache.hudi.table.HoodieSparkTable
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{TableCatalog, TableChange}
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, DeleteColumn, RemoveProperty, SetProperty}
import org.apache.spark.sql.types.StructType

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
    val (oldSchema, historySchema) = getInternalSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyAddAction2Schema(sparkSession, applyDeleteAction2Schema(sparkSession, oldSchema, deleteChanges), addChanges)
    val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
      SerDeHelper.inheritSchemas(oldSchema, "")
    } else {
      historySchema
    }
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession)
    logInfo("column replace finished")
  }

  def applyAddAction2Schema(sparkSession: SparkSession, oldSchema: InternalSchema, addChanges: Seq[AddColumn]): InternalSchema = {
    val addChange = TableChanges.ColumnAddChange.get(oldSchema)
    addChanges.foreach { addColumn =>
      val names = addColumn.fieldNames()
      val parentName = AlterTableCommand.getParentName(names)
      // add col change
      val colType = SparkInternalSchemaConverter.buildTypeFromStructType(addColumn.dataType(), true, new AtomicInteger(0))
      addChange.addColumns(parentName, names.last, colType, addColumn.comment())
      // add position change
      addColumn.position() match {
        case after: TableChange.After =>
          addChange.addPositionChange(names.mkString("."),
            if (parentName.isEmpty) after.column() else parentName + "." + after.column(), "after")
        case _: TableChange.First =>
          addChange.addPositionChange(names.mkString("."), "", "first")
        case _ =>
      }
    }
    SchemaChangeUtils.applyTableChanges2Schema(oldSchema, addChange)
  }

  def applyDeleteAction2Schema(sparkSession: SparkSession, oldSchema: InternalSchema, deleteChanges: Seq[DeleteColumn]): InternalSchema = {
    val deleteChange = TableChanges.ColumnDeleteChange.get(oldSchema)
    deleteChanges.foreach { c =>
      val originalColName = c.fieldNames().mkString(".")
      checkSchemaChange(Seq(originalColName), table)
      deleteChange.deleteColumn(originalColName)
    }
    SchemaChangeUtils.applyTableChanges2Schema(oldSchema, deleteChange).setSchemaId(oldSchema.getMaxColumnId)
  }


  def applyAddAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getInternalSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyAddAction2Schema(sparkSession, oldSchema, changes.map(_.asInstanceOf[AddColumn]))
    val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
      SerDeHelper.inheritSchemas(oldSchema, "")
    } else {
      historySchema
    }
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession)
    logInfo("column add finished")
  }

  def applyDeleteAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getInternalSchemaAndHistorySchemaStr(sparkSession)
    val newSchema = applyDeleteAction2Schema(sparkSession, oldSchema, changes.map(_.asInstanceOf[DeleteColumn]))
    // delete action should not change the getMaxColumnId field.
    newSchema.setMaxColumnId(oldSchema.getMaxColumnId)
    val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
      SerDeHelper.inheritSchemas(oldSchema, "")
    } else {
      historySchema
    }
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession)
    logInfo("column delete finished")
  }

  def applyUpdateAction(sparkSession: SparkSession): Unit = {
    val (oldSchema, historySchema) = getInternalSchemaAndHistorySchemaStr(sparkSession)
    val updateChange = TableChanges.ColumnUpdateChange.get(oldSchema)
    changes.foreach { change =>
      change match {
        case updateType: TableChange.UpdateColumnType =>
          val newType = SparkInternalSchemaConverter.buildTypeFromStructType(updateType.newDataType(), true, new AtomicInteger(0))
          updateChange.updateColumnType(updateType.fieldNames().mkString("."), newType)
        case updateComment: TableChange.UpdateColumnComment =>
          updateChange.updateColumnComment(updateComment.fieldNames().mkString("."), updateComment.newComment())
        case updateName: TableChange.RenameColumn =>
          val originalColName = updateName.fieldNames().mkString(".")
          checkSchemaChange(Seq(originalColName), table)
          updateChange.renameColumn(originalColName, updateName.newName())
        case updateNullAbility: TableChange.UpdateColumnNullability =>
          updateChange.updateColumnNullability(updateNullAbility.fieldNames().mkString("."), updateNullAbility.nullable())
        case updatePosition: TableChange.UpdateColumnPosition =>
          val names = updatePosition.fieldNames()
          val parentName = AlterTableCommand.getParentName(names)
          updatePosition.position() match {
            case after: TableChange.After =>
              updateChange.addPositionChange(names.mkString("."),
                if (parentName.isEmpty) after.column() else parentName + "." + after.column(), "after")
            case _: TableChange.First =>
              updateChange.addPositionChange(names.mkString("."), "", "first")
            case _ =>
          }
      }
    }
    val newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, updateChange)
    val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
      SerDeHelper.inheritSchemas(oldSchema, "")
    } else {
      historySchema
    }
    AlterTableCommand.commitWithSchema(newSchema, verifiedHistorySchema, table, sparkSession)
    logInfo("column update finished")
  }

  // to do support unset default value to columns, and apply them to internalSchema
  def applyPropertyUnset(sparkSession: SparkSession): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val propKeys = changes.map(_.asInstanceOf[RemoveProperty]).map(_.property())
    // ignore NonExist unset
    propKeys.foreach { k =>
      if (!table.properties.contains(k) && k != TableCatalog.PROP_COMMENT) {
        logWarning(s"find non exist unset property: ${k} , ignore it")
      }
    }
    val tableComment = if (propKeys.contains(TableCatalog.PROP_COMMENT)) None else table.comment
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties, comment = tableComment)
    catalog.alterTable(newTable)
    logInfo("table properties change finished")
  }

  // to do support set default value to columns, and apply them to internalSchema
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

  def getInternalSchemaAndHistorySchemaStr(sparkSession: SparkSession): (InternalSchema, String) = {
    val path = AlterTableCommand.getTableLocation(table, sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(hadoopConf).build()
    val schemaUtil = new TableSchemaResolver(metaClient)

    val schema = schemaUtil.getTableInternalSchemaFromCommitMetadata().orElse {
      AvroInternalSchemaConverter.convert(schemaUtil.getTableAvroSchema)
    }

    val historySchemaStr = schemaUtil.getTableHistorySchemaStrFromCommitMetadata.orElse("")
    (schema, historySchemaStr)
  }

  def checkSchemaChange(colNames: Seq[String], catalogTable: CatalogTable): Unit = {
    val primaryKeys = catalogTable.storage.properties.getOrElse("primaryKey", catalogTable.properties.getOrElse("primaryKey", "keyid")).split(",").map(_.trim)
    val preCombineKey = Seq(catalogTable.storage.properties.getOrElse("preCombineField", catalogTable.properties.getOrElse("preCombineField", "ts"))).map(_.trim)
    val partitionKey = catalogTable.partitionColumnNames.map(_.trim)
    val checkNames = primaryKeys ++ preCombineKey ++ partitionKey
    colNames.foreach { col =>
      if (checkNames.contains(col)) {
        throw new UnsupportedOperationException("cannot support apply changes for primaryKey/CombineKey/partitionKey")
      }
    }
  }
}

object AlterTableCommand extends Logging {

  /**
    * Generate an commit with new schema to change the table's schema.
    *
    * @param internalSchema new schema after change
    * @param historySchemaStr history schemas
    * @param table The hoodie table.
    * @param sparkSession The spark session.
    */
  def commitWithSchema(internalSchema: InternalSchema, historySchemaStr: String, table: CatalogTable, sparkSession: SparkSession): Unit = {
    val schema = AvroInternalSchemaConverter.convert(internalSchema, table.identifier.table)
    val path = getTableLocation(table, sparkSession)
    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    val client = DataSourceUtils.createHoodieClient(jsc, schema.toString,
      path, table.identifier.table, parametersWithWriteDefaults(table.storage.properties).asJava)

    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path).setConf(hadoopConf).build()

    val commitActionType = CommitUtils.getCommitActionType(WriteOperationType.ALTER_SCHEMA, metaClient.getTableType)
    val instantTime = HoodieActiveTimeline.createNewInstantTime
    client.startCommitWithTime(instantTime, commitActionType)

    val hoodieTable = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.ALTER_SCHEMA)
    timeLine.transitionRequestedToInflight(requested, Option.of(metadata.toJsonString.getBytes(StandardCharsets.UTF_8)))
    val extraMeta = new util.HashMap[String, String]()
    extraMeta.put(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(internalSchema.setSchemaId(instantTime.toLong)))
    val schemaManager = new FileBasedInternalSchemaStorageManager(metaClient)
    schemaManager.persistHistorySchemaStr(instantTime, SerDeHelper.inheritSchemas(internalSchema, historySchemaStr))
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
    val fullSparkSchema = SparkInternalSchemaConverter.constructSparkSchemaFromInternalSchema(internalSchema)
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

  def parametersWithWriteDefaults(parameters: Map[String, String]): Map[String, String] = {
    Map(OPERATION.key -> OPERATION.defaultValue,
      TABLE_TYPE.key -> TABLE_TYPE.defaultValue,
      PRECOMBINE_FIELD.key -> PRECOMBINE_FIELD.defaultValue,
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> HoodieWriteConfig.DEFAULT_WRITE_PAYLOAD_CLASS,
      INSERT_DROP_DUPS.key -> INSERT_DROP_DUPS.defaultValue,
      ASYNC_COMPACT_ENABLE.key -> ASYNC_COMPACT_ENABLE.defaultValue,
      INLINE_CLUSTERING_ENABLE.key -> INLINE_CLUSTERING_ENABLE.defaultValue,
      ASYNC_CLUSTERING_ENABLE.key -> ASYNC_CLUSTERING_ENABLE.defaultValue
    ) ++ DataSourceOptionsHelper.translateConfigurations(parameters)
  }
}

