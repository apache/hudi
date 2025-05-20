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

import org.apache.hudi.{AvroConversionUtils, DataSourceUtils, HoodieWriterUtils, SparkAdapterSupport}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieFailedWritesCleaningPolicy, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.util.{CommitUtils, Option}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig}
import org.apache.hudi.table.HoodieSparkTable

import org.apache.avro.Schema
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Command for add new columns to the hudi table.
 */
case class AlterHoodieTableAddColumnsCommand(tableId: TableIdentifier,
                                             colsToAdd: Seq[StructField])
  extends HoodieLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (colsToAdd.nonEmpty) {
      val resolver = sparkSession.sessionState.conf.resolver
      val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableId)
      val tableSchema = hoodieCatalogTable.tableSchema
      val existsColumns =
        colsToAdd.map(_.name).filter(col => tableSchema.fieldNames.exists(f => resolver(f, col)))

      if (existsColumns.nonEmpty) {
        throw new AnalysisException(s"Columns: [${existsColumns.mkString(",")}] already exists in the table," +
          s" table columns is: [${hoodieCatalogTable.tableSchemaWithoutMetaFields.fieldNames.mkString(",")}]")
      }
      // Get the new schema
      val rearrangedSchema = hoodieCatalogTable.dataSchema ++ colsToAdd ++ hoodieCatalogTable.partitionSchema
      val newSqlSchema = StructType(rearrangedSchema)
      val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableId.table)
      val newSchema = AvroConversionUtils.convertStructTypeToAvroSchema(newSqlSchema, structName, nameSpace)

      // Commit with new schema to change the table schema
      AlterHoodieTableAddColumnsCommand.commitWithSchema(newSchema, hoodieCatalogTable, sparkSession)

      val newDataSchema = StructType(hoodieCatalogTable.dataSchema.fields ++ colsToAdd)
      validateSchema(newDataSchema)
      // Refresh the new schema to meta
      AlterHoodieTableAddColumnsCommand.refreshSchema(sparkSession, hoodieCatalogTable, newDataSchema)
    }
    Seq.empty[Row]
  }

  private def validateSchema(dataSchema: StructType): Unit = {
    AlterHoodieTableAddColumnsCommand.checkColumnNameDuplication(
      dataSchema.map(_.name),
      "in the table definition of " + tableId.identifier,
      conf.caseSensitiveAnalysis)
  }
}

object AlterHoodieTableAddColumnsCommand extends SparkAdapterSupport with Logging {
  /**
   * Generate an empty commit with new schema to change the table's schema.
   *
   * @param schema             The new schema to commit.
   * @param hoodieCatalogTable The hoodie catalog table.
   * @param sparkSession       The spark session.
   */
  def commitWithSchema(schema: Schema, hoodieCatalogTable: HoodieCatalogTable,
                       sparkSession: SparkSession): Unit = {

    val writeSchema = HoodieAvroUtils.removeMetadataFields(schema);
    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    val client = DataSourceUtils.createHoodieClient(
      jsc,
      writeSchema.toString,
      hoodieCatalogTable.tableLocation,
      hoodieCatalogTable.tableName,
      HoodieWriterUtils.parametersWithWriteDefaults(HoodieOptionConfig.mapSqlOptionsToDataSourceWriteConfigs(
        hoodieCatalogTable.catalogProperties) ++ sparkSession.sqlContext.conf.getAllConfs ++ Map(
        HoodieCleanConfig.AUTO_CLEAN.key -> "false",
        HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key -> HoodieFailedWritesCleaningPolicy.NEVER.name,
        HoodieArchivalConfig.AUTO_ARCHIVE.key -> "false"
      )).asJava
    )

    val commitActionType = CommitUtils.getCommitActionType(WriteOperationType.ALTER_SCHEMA, hoodieCatalogTable.tableType)
    val instantTime = client.startCommit(commitActionType)
    client.preWrite(instantTime, WriteOperationType.ALTER_SCHEMA, hoodieCatalogTable.metaClient)

    val hoodieTable = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val requested = hoodieTable.getInstantGenerator.createNewInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.ALTER_SCHEMA)
    timeLine.transitionRequestedToInflight(requested, Option.of(metadata))

    client.commit(instantTime, jsc.emptyRDD)
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames           column names to check.
   * @param colType               column type name, used in an exception message.
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not.
   */
  def checkColumnNameDuplication(columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    sparkAdapter.getSchemaUtils.checkColumnNameDuplication(columnNames, colType, caseSensitiveAnalysis)
  }

  def refreshSchema(session: SparkSession, catalogTable: HoodieCatalogTable, dataSchema: StructType): Unit = {
    refreshSchemaInMeta(session, catalogTable.table.identifier, dataSchema)
    if (catalogTable.tableType == HoodieTableType.MERGE_ON_READ) {
      val tableId = catalogTable.table.identifier
      val tableName = catalogTable.tableName
      // refresh schema of rt table if exist
      val rtTableId = tableId.copy(table = s"${tableName}_rt")
      if (session.catalog.tableExists(rtTableId.unquotedString)) {
        refreshSchemaInMeta(session, rtTableId, dataSchema)
      }
      // refresh schema of ro table if exist
      val roTableId = tableId.copy(table = s"${tableName}_ro")
      if (session.catalog.tableExists(roTableId.unquotedString)) {
        refreshSchemaInMeta(session, roTableId, dataSchema)
      }
    }
  }

  private def refreshSchemaInMeta(session: SparkSession, tableId: TableIdentifier, dataSchema: StructType): Unit = {
    try {
      session.catalog.uncacheTable(tableId.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${tableId.quotedString}", e)
    }
    session.catalog.refreshTable(tableId.unquotedString)
    session.sessionState.catalog.alterTableDataSchema(tableId, dataSchema)
  }
}
