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

import org.apache.avro.Schema
import org.apache.hudi.common.model.{HoodieCommitMetadata, WriteOperationType}
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant}
import org.apache.hudi.common.util.{CommitUtils, Option}
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.{AvroConversionUtils, DataSourceUtils, HoodieWriterUtils}

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.SchemaUtils

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Command for add new columns to the hudi table.
 */
case class AlterHoodieTableAddColumnsCommand(
   tableId: TableIdentifier,
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
      val newSqlSchema = StructType(tableSchema.fields ++ colsToAdd)
      val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableId.table)
      val newSchema = AvroConversionUtils.convertStructTypeToAvroSchema(newSqlSchema, structName, nameSpace)

      // Commit with new schema to change the table schema
      AlterHoodieTableAddColumnsCommand.commitWithSchema(newSchema, hoodieCatalogTable, sparkSession)

      // Refresh the new schema to meta
      val newDataSchema = StructType(hoodieCatalogTable.dataSchema.fields ++ colsToAdd)
      refreshSchemaInMeta(sparkSession, hoodieCatalogTable.table, newDataSchema)
    }
    Seq.empty[Row]
  }

  private def refreshSchemaInMeta(sparkSession: SparkSession, table: CatalogTable,
      newSqlDataSchema: StructType): Unit = {
    try {
      sparkSession.catalog.uncacheTable(tableId.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${tableId.quotedString}", e)
    }
    sparkSession.catalog.refreshTable(table.identifier.unquotedString)

    SchemaUtils.checkColumnNameDuplication(
      newSqlDataSchema.map(_.name),
      "in the table definition of " + table.identifier,
      conf.caseSensitiveAnalysis)

    sparkSession.sessionState.catalog.alterTableDataSchema(tableId, newSqlDataSchema)
  }
}

object AlterHoodieTableAddColumnsCommand {
  /**
   * Generate an empty commit with new schema to change the table's schema.
   * @param schema The new schema to commit.
   * @param hoodieCatalogTable  The hoodie catalog table.
   * @param sparkSession The spark session.
   */
  def commitWithSchema(schema: Schema, hoodieCatalogTable: HoodieCatalogTable,
      sparkSession: SparkSession): Unit = {

    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    val client = DataSourceUtils.createHoodieClient(
      jsc,
      schema.toString,
      hoodieCatalogTable.tableLocation,
      hoodieCatalogTable.tableName,
      HoodieWriterUtils.parametersWithWriteDefaults(hoodieCatalogTable.catalogProperties).asJava
    )

    val commitActionType = CommitUtils.getCommitActionType(WriteOperationType.INSERT, hoodieCatalogTable.tableType)
    val instantTime = HoodieActiveTimeline.createNewInstantTime
    client.startCommitWithTime(instantTime, commitActionType)

    val hoodieTable = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.INSERT)
    timeLine.transitionRequestedToInflight(requested, Option.of(metadata.toJsonString.getBytes(StandardCharsets.UTF_8)))

    client.commit(instantTime, jsc.emptyRDD)
  }
}
