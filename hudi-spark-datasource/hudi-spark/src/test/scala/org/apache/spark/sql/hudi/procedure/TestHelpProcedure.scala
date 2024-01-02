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

package org.apache.spark.sql.hudi.procedure

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.command.procedures.{HoodieProcedures, Procedure, ProcedureBuilder}

import java.util
import java.util.function.Supplier

class TestHelpProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call help Procedure with no params") {
    val help: util.List[Row] = spark.sql("call help").collectAsList()
    assert(help.size() == 1)

    val help2: util.List[Row] = spark.sql("call help()").collectAsList()
    assert(help2.size() == 1)

    assert(help.get(0).toString().equals(help2.get(0).toString()))


    val helpStr: String = help.get(0).toString()
    val procedures: Map[String, Supplier[ProcedureBuilder]] = HoodieProcedures.procedures()

    // check all procedures
    procedures.keySet.foreach(name => {
      // check cmd contains all procedure name
      assert(helpStr.contains(name))
      // check cmd contains all procedure description
      val builderSupplier: Option[Supplier[ProcedureBuilder]] = procedures.get(name)
      assert(builderSupplier.isDefined)
      val procedure: Procedure = builderSupplier.get.get().build
      assert(helpStr.contains(procedure.description))
    })
  }


  test("Test Call help Procedure with params") {

    // check not valid params
    checkExceptionContain("call help(not_valid=>true)")("The cmd parameter is required")

    val procedures: Map[String, Supplier[ProcedureBuilder]] = HoodieProcedures.procedures()

    // check all procedures
    procedures.keySet.foreach(name => {
      val help: util.List[Row] = spark.sql(s"call help(cmd=>'$name')").collectAsList()
      assert(help.size() == 1)

      val helpStr: String = help.get(0).toString()
      val builderSupplier: Option[Supplier[ProcedureBuilder]] = procedures.get(name)

      assert(builderSupplier.isDefined)

      // check result contains params
      val procedure: Procedure = builderSupplier.get.get().build
      procedure.parameters.foreach(params => {
        assert(helpStr.contains(params.name))
      })

      // check result contains outputType
      procedure.outputType.foreach(output => {
        assert(helpStr.contains(output.name))
      })
    })
  }

  test("Test whether the result of Call help Procedure are in order") {
    val help: util.List[Row] = spark.sql("call help").collectAsList()
    assert(help.size() == 1)

    var helpStr: String = help.get(0).toString()
    val line = "\n"
    val tab = "\t"
    val result = new StringBuilder
    result.append("synopsis").append(line)
      .append(tab).append("call [command]([key1]=>[value1],[key2]=>[value2])").append(line)
    result.append("commands and description").append(line)
    // get ordered result from help result
    helpStr = helpStr.replace(result.toString(), "")
    result.clear()
    result.append("You can use 'call help(cmd=>[command])' to view the detailed parameters of the command").append(line)
    helpStr = helpStr.replace(result.toString(), "")

    val orderedResult = helpStr.split(line).toList
    val procedures: Map[String, Supplier[ProcedureBuilder]] = HoodieProcedures.procedures()

    // check all procedures
    procedures.keySet.toList.sortWith(_ < _).zipWithIndex.foreach { case (name, index) =>
      // check whether name is ordering
      assert(orderedResult(index).contains(name))
    }
  }
}
