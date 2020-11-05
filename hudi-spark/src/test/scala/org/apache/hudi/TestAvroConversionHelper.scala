/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import java.time.LocalDate

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{FunSuite, Matchers}

class TestAvroConversionHelper extends FunSuite with Matchers {

  val dateSchema = s"""
      {
        "namespace": "logical",
        "type": "record",
        "name": "test",
        "fields": [
          {"name": "date", "type": {"type": "int", "logicalType": "date"}}
        ]
      }
    """

  val dateInputData = Seq(7, 365, 0)

  test("Logical type: date") {
    val schema = new Schema.Parser().parse(dateSchema)
    val convertor = AvroConversionHelper.createConverterToRow(schema, AvroConversionUtils.convertAvroSchemaToStructType(schema))

    val dateOutputData = dateInputData.map(x => {
      val record = new GenericData.Record(schema) {{ put("date", x) }}
      convertor(record).asInstanceOf[GenericRow].get(0)
    })

    println(s"trans data from int [ ${dateInputData.mkString(", ")} ] to date [ ${dateOutputData.mkString(", ")} ]")

    assert(dateOutputData(0).toString === LocalDate.ofEpochDay(dateInputData(0)).toString)
    assert(dateOutputData(1).toString === LocalDate.ofEpochDay(dateInputData(1)).toString)
    assert(dateOutputData(2).toString === LocalDate.ofEpochDay(dateInputData(2)).toString)
  }
}
