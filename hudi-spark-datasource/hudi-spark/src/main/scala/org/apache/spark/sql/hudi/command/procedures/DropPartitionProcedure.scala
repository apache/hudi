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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.HoodieSparkSqlWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class DropPartitionProcedure extends BaseProcedure
  with ProcedureBuilder
  with Logging
  with ProvidesHoodieConfig {
  override def build: Procedure = new DropPartitionProcedure

  val PARAMETERS: Array[ProcedureParameter] = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "partition", DataTypes.StringType)
  )

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = new StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val partitions = getArgValueOrDefault(args, PARAMETERS(1))
    val tableNameStr = tableName.get.asInstanceOf[String]
    val partitionsStr = partitions.get.asInstanceOf[String]

    val (db, table) = getDbAndTableName(tableNameStr)

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, TableIdentifier(table, Some(db)))

    val parameters = buildHoodieDropPartitionsConfig(sparkSession, hoodieCatalogTable, partitionsStr)
    val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(
      sparkSession.sqlContext,
      SaveMode.Append,
      parameters,
      sparkSession.emptyDataFrame)

    if (!success) {
      throw new RuntimeException(s"Failed to drop partition $partitionsStr for table $tableNameStr")
    }

    sparkSession.catalog.refreshTable(tableNameStr)
    logInfo(s"Finish execute alter table drop partition procedure for $tableNameStr")
    Seq(Row("Success"))
  }
}

object DropPartitionProcedure {
  val NAME: String = "drop_partition"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new DropPartitionProcedure
  }
}
