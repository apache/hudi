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

import org.apache.hudi.DataSourceReadOptions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class CopyToTableProcedure extends BaseProcedure with ProcedureBuilder with Logging {


  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "query_type", DataTypes.StringType, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL),
    ProcedureParameter.required(2, "new_table", DataTypes.StringType),
    ProcedureParameter.optional(3, "begin_instance_time", DataTypes.StringType, ""),
    ProcedureParameter.optional(4, "end_instance_time", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "as_of_instant", DataTypes.StringType, ""),
    ProcedureParameter.optional(6, "save_mode", DataTypes.StringType, "overwrite"),
    ProcedureParameter.optional(7, "columns", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("status", DataTypes.IntegerType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val queryType = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val newTableName = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val startInstance = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    val endInstance = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val asOfInstant = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val saveModeStr = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]
    val columns = getArgValueOrDefault(args, PARAMETERS(7)).get.asInstanceOf[String]

    assert(saveModeStr.nonEmpty, "save_mode(append,overwrite) can not be null.")

    val saveMode: Any = saveModeStr.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case _ => assert(assertion = false, s"save_mode not support $saveModeStr.")
    }


    val tablePath = getBasePath(tableName)

    val sourceDataFrame = queryType match {
      case DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL => if (asOfInstant.nonEmpty) {
        sparkSession.read
          .format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
          .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, asOfInstant)
          .load(tablePath)
      } else {
        sparkSession.read
          .format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
          .load(tablePath)
      }
      case DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL =>
        assert(startInstance.nonEmpty && endInstance.nonEmpty, "when the query_type is incremental, begin_instance_time and end_instance_time can not be null.")
        sparkSession.read
          .format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.START_COMMIT.key, startInstance)
          .option(DataSourceReadOptions.END_COMMIT.key, endInstance)
          .load(tablePath)
      case DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL =>
        sparkSession.read
          .format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
          .load(tablePath)
    }
    if (sparkSession.catalog.tableExists(newTableName)) {
      val schema = sparkSession.read.table(newTableName).schema
      val selectColumns = schema.fields.toStream.map(_.name)
      sourceDataFrame.selectExpr(selectColumns: _*)
        .write
        .mode(saveMode.toString)
        .saveAsTable(newTableName)
    } else {
      var selectColumns = Seq[String]()
      if (!columns.eq("")) {
        selectColumns = columns.split(",").toStream
      } else {
        selectColumns = sparkSession.read.table(tableName.get.asInstanceOf[String]).schema.fields.toStream.map(_.name)
      }
      sourceDataFrame.selectExpr(selectColumns: _*)
        .write
        .mode(saveMode.toString)
        .saveAsTable(newTableName)
    }

    Seq(Row(0))
  }

  override def build = new CopyToTableProcedure()
}

object CopyToTableProcedure {
  val NAME = "copy_to_table"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new CopyToTableProcedure()
  }
}
