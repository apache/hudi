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

import org.apache.hudi.cli.HDFSParquetImporterUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.language.higherKinds

class HdfsParquetImportProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.required(1, "table_type", DataTypes.StringType, None),
    ProcedureParameter.required(2, "src_path", DataTypes.StringType, None),
    ProcedureParameter.required(3, "target_path", DataTypes.StringType, None),
    ProcedureParameter.required(4, "row_key", DataTypes.StringType, None),
    ProcedureParameter.required(5, "partition_key", DataTypes.StringType, None),
    ProcedureParameter.required(6, "schema_file_path", DataTypes.StringType, None),
    ProcedureParameter.optional(7, "format", DataTypes.StringType, "parquet"),
    ProcedureParameter.optional(8, "command", DataTypes.StringType, "insert"),
    ProcedureParameter.optional(9, "retry", DataTypes.IntegerType, 0),
    ProcedureParameter.optional(10, "parallelism", DataTypes.IntegerType, jsc.defaultParallelism),
    ProcedureParameter.optional(11, "props_file_path", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("import_result", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val tableType = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val srcPath = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val targetPath = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    val rowKey = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val partitionKey = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val schemaFilePath = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val format = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[String]
    val command = getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[String]
    val retry = getArgValueOrDefault(args, PARAMETERS(9)).get.asInstanceOf[Int]
    val parallelism = getArgValueOrDefault(args, PARAMETERS(10)).get.asInstanceOf[Int]
    val propsFilePath = getArgValueOrDefault(args, PARAMETERS(11)).get.asInstanceOf[String]

    val parquetImporterUtils: HDFSParquetImporterUtils = new HDFSParquetImporterUtils(command, srcPath, targetPath,
      tableName, tableType, rowKey, partitionKey, parallelism, schemaFilePath, retry, propsFilePath)

    Seq(Row(parquetImporterUtils.dataImport(jsc)))
  }

  override def build = new HdfsParquetImportProcedure()
}

object HdfsParquetImportProcedure {
  val NAME = "hdfs_parquet_import"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new HdfsParquetImportProcedure()
  }
}


