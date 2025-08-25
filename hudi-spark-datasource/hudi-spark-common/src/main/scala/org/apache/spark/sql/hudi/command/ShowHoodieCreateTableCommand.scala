/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.command

import org.apache.hudi.common.util.ConfigUtils

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.util.escapeSingleQuotedString
import org.apache.spark.sql.hudi.HoodieOptionConfig.{SQL_KEY_ORDERING_FIELDS, SQL_KEY_TABLE_PRIMARY_KEY, SQL_KEY_TABLE_TYPE, SQL_PAYLOAD_CLASS, SQL_RECORD_MERGE_STRATEGY_ID}
import org.apache.spark.sql.types.StringType

case class ShowHoodieCreateTableCommand(table: TableIdentifier)
  extends HoodieLeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("createtab_stmt", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbName = table.database.getOrElse("default")
    val tableName = table.table
    val tableExists = sparkSession.sessionState.catalog.tableExists(table)
    if (!tableExists) {
      throw new NoSuchTableException(dbName, tableName)
    }
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, table)
    val stmt = showCreateHoodieTable(hoodieCatalogTable)
    Seq(Row(stmt))
  }

  private def showCreateHoodieTable(metadata: HoodieCatalogTable): String = {
    val builder = StringBuilder.newBuilder
    builder ++= s"CREATE TABLE IF NOT EXISTS ${table.quotedString}"
    showHoodieTableHeader(metadata, builder)
    showHoodieTableNonDataColumns(metadata, builder)
    showHoodieTableProperties(metadata, builder)
    builder.toString()
  }

  private def showHoodieTableHeader(metadata: HoodieCatalogTable, builder: StringBuilder): Unit = {
    val columns = metadata.tableSchemaWithoutMetaFields.map(_.toDDL)
    if (columns.nonEmpty) {
      builder ++= columns.mkString(" (\n", ",\n ", ")\n")
    }

    metadata.table
      .comment
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }


  private def showHoodieTableNonDataColumns(metadata: HoodieCatalogTable, builder: StringBuilder): Unit = {
    builder ++= s"USING ${metadata.table.provider.get}\n"
    if (metadata.partitionFields.nonEmpty) {
      val partCols = metadata.partitionFields
      builder ++= partCols.mkString("PARTITIONED BY (", ", ", ")\n")
    }
  }

  private def showHoodieTableProperties(metadata: HoodieCatalogTable, builder: StringBuilder): Unit = {
    val standardOptions = Seq(SQL_KEY_TABLE_PRIMARY_KEY, SQL_KEY_ORDERING_FIELDS,
      SQL_KEY_TABLE_TYPE, SQL_PAYLOAD_CLASS, SQL_RECORD_MERGE_STRATEGY_ID)
      .flatMap(key => (List(key.sqlKeyName) ++ key.alternatives).toStream)
    val props = metadata.catalogProperties.filter(key => standardOptions.contains(key._1)).map {
      case (key, value) => s"$key='${escapeSingleQuotedString(value)}'"
    } ++ metadata.catalogProperties.filterNot(_._1.equals(ConfigUtils.IS_QUERY_AS_RO_TABLE)).map {
      case (key, value) => s"'${escapeSingleQuotedString(key)}'='${escapeSingleQuotedString(value)}'"
    }

    if (props.nonEmpty) {
      builder ++= props.mkString("TBLPROPERTIES (\n  ", ",\n  ", "\n)\n")
    }
  }
}
