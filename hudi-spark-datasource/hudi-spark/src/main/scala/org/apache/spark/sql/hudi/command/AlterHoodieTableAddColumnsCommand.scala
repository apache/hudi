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

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import org.apache.avro.Schema
import org.apache.hudi.client.utils.SparkSchemaUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant}
import org.apache.hudi.common.util.{CommitUtils, Option}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.action.TableChanges
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.io.FileBaseInternalSchemasManager
import org.apache.hudi.internal.schema.utils.{SchemaChangeUtils, SerDeHelper}
import org.apache.hudi.table.HoodieSparkTable

import scala.collection.JavaConverters._
import org.apache.hudi.{AvroConversionUtils, DataSourceUtils, HoodieWriterUtils}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand}
import org.apache.spark.sql.hudi.HoodieSqlUtils
import org.apache.spark.sql.hudi.HoodieSqlUtils.getTableLocation
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils

import scala.util.control.NonFatal

/**
 * Command for add new columns to the hudi table.
 */
case class AlterHoodieTableAddColumnsCommand(
   tableId: TableIdentifier,
   colsToAdd: Seq[StructField])
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val schemaEvolutionEnabled = sparkSession
      .sessionState.conf.getConfString(HoodieWriteConfig.SCHEMA_EVOLUTION_ENABLE.key(), "false").toBoolean
    if (colsToAdd.nonEmpty) {
      val resolver = sparkSession.sessionState.conf.resolver
      val table = sparkSession.sessionState.catalog.getTableMetadata(tableId)
      val existsColumns =
        colsToAdd.map(_.name).filter(col => table.schema.fieldNames.exists(f => resolver(f, col)))

      if (existsColumns.nonEmpty) {
        throw new AnalysisException(s"Columns: [${existsColumns.mkString(",")}] already exists in the table," +
          s" table columns is: [${HoodieSqlUtils.removeMetaFields(table.schema).fieldNames.mkString(",")}]")
      }
      // Get the new schema
      if (schemaEvolutionEnabled) {
        val (oldSchema, historySchema) = AlterHoodieTableAddColumnsCommand.getInternalSchemaAndHistorySchemaStr(sparkSession, table)
        val addChange = TableChanges.ColumnAddChange.get(oldSchema)
        colsToAdd.foreach { col  =>
          val colType = SparkSchemaUtils.buildTypeFromStructType(col.dataType, true, new AtomicInteger(0))
          addChange.addColumns(col.name, colType, null)
        }
        val newInternalSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, addChange)
        val verifiedHistorySchema = if (historySchema == null || historySchema.isEmpty) {
          SerDeHelper.inheritSchemas(oldSchema, "")
        } else {
          historySchema
        }
        // Commit with new schema to change the table schema
        AlterHoodieTableAddColumnsCommand.commitWithSchema(Some(newInternalSchema), verifiedHistorySchema, None, table, sparkSession)
      } else {
        val newSqlSchema = StructType(table.schema.fields ++ colsToAdd)
        val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableId.table)
        val newSchema = AvroConversionUtils.convertStructTypeToAvroSchema(newSqlSchema, structName, nameSpace)
        AlterHoodieTableAddColumnsCommand.commitWithSchema(None, "", Some(newSchema), table, sparkSession)
      }
      // Refresh the new schema to meta
      val newDataSchema = StructType(table.dataSchema.fields ++ colsToAdd)
      refreshSchemaInMeta(sparkSession, table, newDataSchema)
    }
    Seq.empty[Row]
  }

  private def refreshSchemaInMeta(sparkSession: SparkSession, table: CatalogTable, newSqlSchema: StructType): Unit = {
    try {
      sparkSession.catalog.uncacheTable(tableId.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${tableId.quotedString}", e)
    }
    sparkSession.catalog.refreshTable(table.identifier.unquotedString)

    SchemaUtils.checkColumnNameDuplication(
      newSqlSchema.map(_.name),
      "in the table definition of " + table.identifier,
      conf.caseSensitiveAnalysis)
    DDLUtils.checkDataColNames(table, colsToAdd.map(_.name))

    sparkSession.sessionState.catalog.alterTableDataSchema(tableId, newSqlSchema)
  }
}

object AlterHoodieTableAddColumnsCommand {
  /**
    * Generate an empty commit with new schema to change the table's schema.
    * @param schema The new schema to commit.
    * @param table The hoodie table.
    * @param sparkSession The spark session.
    */
  def commitWithSchema(
      internalSchemaOpt: scala.Option[InternalSchema],
      historySchemaStr: String,
      avroSchemaOpt: scala.Option[Schema],
      table: CatalogTable,
      sparkSession: SparkSession): Unit = {
    if (internalSchemaOpt.isEmpty && avroSchemaOpt.isEmpty) {
      throw new HoodieException("cannot commit schema change, since both internalSchema and avroSchema is empty")
    }
    val schema = avroSchemaOpt.getOrElse(AvroInternalSchemaConverter.convert(internalSchemaOpt.get, table.identifier.table))
    val path = getTableLocation(table, sparkSession)

    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    val client = DataSourceUtils.createHoodieClient(jsc, schema.toString,
      path, table.identifier.table, HoodieWriterUtils.parametersWithWriteDefaults(table.storage.properties).asJava)

    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path).setConf(hadoopConf).build()

    val commitActionType = CommitUtils.getCommitActionType(WriteOperationType.INSERT, metaClient.getTableType)
    val instantTime = HoodieActiveTimeline.createNewInstantTime
    client.startCommitWithTime(instantTime, commitActionType)

    val hoodieTable = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.INSERT)
    timeLine.transitionRequestedToInflight(requested, Option.of(metadata.toJsonString.getBytes(StandardCharsets.UTF_8)))
    if (internalSchemaOpt.isDefined) {
      val internalSchema = internalSchemaOpt.get
      val extraMeta = new java.util.HashMap[String, String]()
      extraMeta.put(SerDeHelper.LATESTSCHEMA, SerDeHelper.toJson(internalSchema.setSchemaId(instantTime.toLong)))
      val schemaManager = new FileBaseInternalSchemasManager(metaClient);
      schemaManager.persistHistorySchemaStr(instantTime, SerDeHelper.inheritSchemas(internalSchema, historySchemaStr))
      client.commit(instantTime, jsc.emptyRDD, Option.of(extraMeta))
    } else {
      client.commit(instantTime, jsc.emptyRDD)
    }
  }

  def getInternalSchemaAndHistorySchemaStr(sparkSession: SparkSession, table: CatalogTable): (InternalSchema, String) = {
    val path = HoodieSqlUtils.getTableLocation(table, sparkSession)
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
}
