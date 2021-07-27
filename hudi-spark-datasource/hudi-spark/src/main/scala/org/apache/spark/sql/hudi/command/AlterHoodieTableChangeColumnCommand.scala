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
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils.getTableLocation
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.control.NonFatal

/**
 * Command for alter hudi table's column type.
 */
case class AlterHoodieTableChangeColumnCommand(
    tableName: TableIdentifier,
    columnName: String,
    newColumn: StructField)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val resolver = sparkSession.sessionState.conf.resolver

    if (!resolver(columnName, newColumn.name)) {
      throw new AnalysisException(s"Can not support change column name for hudi table currently.")
    }
    // Get the new schema
    val newSqlSchema = StructType(
      table.schema.fields.map { field =>
      if (resolver(field.name, columnName)) {
        newColumn
      } else {
        field
      }
    })
    val newDataSchema = StructType(
      table.dataSchema.fields.map { field =>
        if (resolver(field.name, columnName)) {
          newColumn
        } else {
          field
        }
      })
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableName.table)
    val newSchema = AvroConversionUtils.convertStructTypeToAvroSchema(newSqlSchema, structName, nameSpace)

    val path = getTableLocation(table, sparkSession)
      .getOrElse(s"missing location for ${table.identifier}")
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(hadoopConf).build()
    // Validate the compatibility between new schema and origin schema.
    validateSchema(newSchema, metaClient)
    // Commit new schema to change the table schema
    AlterHoodieTableAddColumnsCommand.commitWithSchema(newSchema, table, sparkSession)

    try {
      sparkSession.catalog.uncacheTable(tableName.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${tableName.quotedString}", e)
    }
    sparkSession.catalog.refreshTable(tableName.unquotedString)
    // Change the schema in the meta using new data schema.
    catalog.alterTableDataSchema(tableName, newDataSchema)

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
