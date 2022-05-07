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

import org.apache.avro.Schema
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.control.NonFatal

/**
 * Command for alter hudi table's column type.
 */
case class AlterHoodieTableChangeColumnCommand(
    tableIdentifier: TableIdentifier,
    columnName: String,
    newColumn: StructField)
  extends HoodieLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val resolver = sparkSession.sessionState.conf.resolver
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)

    // Find the origin column from dataSchema by column name.
    val originColumn = findColumnByName(hoodieCatalogTable.dataSchema, columnName, resolver).getOrElse(
      throw new AnalysisException(s"Can't find column `$columnName` given table data columns " +
        s"${hoodieCatalogTable.dataSchema.fieldNames.mkString("[`", "`, `", "`]")}")
    )
    // Throw an AnalysisException if the column name/dataType is changed.
    if (!columnEqual(originColumn, newColumn, resolver)) {
      throw new AnalysisException(
        "ALTER TABLE CHANGE COLUMN is not supported for changing column " +
          s"'${originColumn.name}' with type '${originColumn.dataType}' to " +
          s"'${newColumn.name}' with type '${newColumn.dataType}'")
    }

    // Get the new schema
    val newTableSchema = StructType(
      hoodieCatalogTable.tableSchema.fields.map { field =>
      if (field.name == originColumn.name) {
        newColumn
      } else {
        field
      }
    })
    val newDataSchema = StructType(
      hoodieCatalogTable.dataSchema.fields.map { field =>
        if (field.name == columnName) {
          newColumn
        } else {
          field
        }
      })
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableIdentifier.table)
    val newSchema = AvroConversionUtils.convertStructTypeToAvroSchema(newTableSchema, structName, nameSpace)

    // Validate the compatibility between new schema and origin schema.
    validateSchema(newSchema, hoodieCatalogTable.metaClient)
    // Commit new schema to change the table schema
    AlterHoodieTableAddColumnsCommand.commitWithSchema(newSchema, hoodieCatalogTable, sparkSession)

    try {
      sparkSession.catalog.uncacheTable(tableIdentifier.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${tableIdentifier.quotedString}", e)
    }
    sparkSession.catalog.refreshTable(tableIdentifier.unquotedString)
    // Change the schema in the meta using new data schema.
    sparkSession.sessionState.catalog.alterTableDataSchema(tableIdentifier, newDataSchema)

    Seq.empty[Row]
  }

  private def validateSchema(newSchema: Schema, metaClient: HoodieTableMetaClient): Unit = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    val tableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchemaWithoutMetadataFields)
    if (!TableSchemaResolver.isSchemaCompatible(tableSchema, newSchema)) {
      throw new HoodieException("Failed schema compatibility check for newSchema :" + newSchema +
        ", origin table schema :" + tableSchema + ", base path :" + metaClient.getBasePath)
    }
  }
}
