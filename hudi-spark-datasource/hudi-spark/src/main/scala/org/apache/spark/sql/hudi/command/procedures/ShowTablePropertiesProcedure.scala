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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowTablePropertiesProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(3, "filter", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("key", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("value", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val filter = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]

    validateFilter(filter, outputType)
    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)
    val tableProps = metaClient.getTableConfig.getProps

    val rows = new util.ArrayList[Row]
    tableProps.asScala.foreach(p => rows.add(Row(p._1, p._2)))
    val results = rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
    applyFilter(results, filter, outputType)
  }

  override def build: Procedure = new ShowTablePropertiesProcedure()

}

object ShowTablePropertiesProcedure {
  val NAME = "show_table_properties"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowTablePropertiesProcedure()
  }
}
