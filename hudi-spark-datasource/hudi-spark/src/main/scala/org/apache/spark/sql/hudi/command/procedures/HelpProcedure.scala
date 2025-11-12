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

import org.apache.hudi.exception.HoodieException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class HelpProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "cmd", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  /**
   * Returns the description of this procedure.
   */
  override def description: String = s"The procedure help command allows you to view all the commands currently provided, as well as their parameters and output fields."

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)
    val line = "\n"
    val tab = "\t"
    if (args.map.isEmpty) {
      val procedures: Map[String, Supplier[ProcedureBuilder]] = HoodieProcedures.procedures()
      val result = new StringBuilder
      result.append("synopsis").append(line)
        .append(tab).append("call [command]([key1]=>[value1],[key2]=>[value2])").append(line)
      result.append("commands and description").append(line)
      procedures.toSeq.sortBy(_._1).foreach(procedure => {
        val name = procedure._1
        val builderSupplier: Option[Supplier[ProcedureBuilder]] = Option.apply(procedure._2)
        if (builderSupplier.isDefined) {
          val procedure: Procedure = builderSupplier.get.get().build
          result.append(tab)
            .append(name).append(tab)
            .append(procedure.description).append(line)
        }
      })
      result.append("You can use 'call help(cmd=>[command])' to view the detailed parameters of the command").append(line)
      Seq(Row(result.toString()))
    } else {
      val cmdOpt: Option[Any] = getArgValueOrDefault(args, PARAMETERS(0))
      assert(cmdOpt.isDefined, "The cmd parameter is required")
      val cmd: String = cmdOpt.get.asInstanceOf[String]
      val procedures: Map[String, Supplier[ProcedureBuilder]] = HoodieProcedures.procedures()
      val builderSupplier: Option[Supplier[ProcedureBuilder]] = procedures.get(cmd.trim)
      if (builderSupplier.isEmpty) {
        throw new HoodieException(s"can not find $cmd command in procedures.")
      }
      val procedure: Procedure = builderSupplier.get.get().build
      val result = new StringBuilder

      result.append("parameters:").append(line)
      // set parameters header
      result.append(tab)
        .append(lengthFormat("param")).append(tab)
        .append(lengthFormat("type_name")).append(tab)
        .append(lengthFormat("default_value")).append(tab)
        .append(lengthFormat("required")).append(line)
      procedure.parameters.foreach(param => {
        result.append(tab)
          .append(lengthFormat(param.name)).append(tab)
          .append(lengthFormat(param.dataType.typeName)).append(tab)
          .append(lengthFormat(String.valueOf(param.default))).append(tab)
          .append(lengthFormat(param.required.toString)).append(line)
      })
      result.append("outputType:").append(line)
      // set outputType header
      result.append(tab)
        .append(lengthFormat("name")).append(tab)
        .append(lengthFormat("type_name")).append(tab)
        .append(lengthFormat("nullable")).append(tab)
        .append(lengthFormat("metadata")).append(line)
      procedure.outputType.map(field => {
        result.append(tab)
          .append(lengthFormat(field.name)).append(tab)
          .append(lengthFormat(field.dataType.typeName)).append(tab)
          .append(lengthFormat(field.nullable.toString)).append(tab)
          .append(lengthFormat(field.metadata.toString())).append(line)
      })
      Seq(Row(result.toString()))
    }
  }

  def lengthFormat(string: String): String = {
    String.format("%-30s", string)
  }

  override def build = new HelpProcedure()
}

object HelpProcedure {
  val NAME = "help"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new HelpProcedure()
  }
}
