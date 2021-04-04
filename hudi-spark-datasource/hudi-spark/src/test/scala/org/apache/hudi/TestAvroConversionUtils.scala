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

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import org.scalatest.{FunSuite, Matchers}

class TestAvroConversionUtils extends FunSuite with Matchers {


  test("test convertStructTypeToAvroSchema") {
    val innerStruct = new StructType().add("innerKey","string",false).add("value", "long", true)
    val struct = new StructType().add("key", "string", false).add("version", "string", true)
      .add("data1",innerStruct,false).add("data2",innerStruct,true)

    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(struct, "SchemaName", "SchemaNS")

    val expectedSchemaStr = s"""
       {
         "type" : "record",
         "name" : "SchemaName",
         "namespace" : "SchemaNS",
         "fields" : [ {
           "name" : "key",
           "type" : "string"
         }, {
           "name" : "version",
           "type" : [ "null", "string" ],
           "default" : null
         }, {
           "name" : "data1",
           "type" : {
             "type" : "record",
             "name" : "data1",
             "namespace" : "SchemaNS.SchemaName",
             "fields" : [ {
               "name" : "innerKey",
               "type" : "string"
             }, {
               "name" : "value",
               "type" : [ "null", "long" ],
               "default" : null
             } ]
           }
         }, {
           "name" : "data2",
           "type" : [ "null", {
             "type" : "record",
             "name" : "data2",
             "namespace" : "SchemaNS.SchemaName",
             "fields" : [ {
               "name" : "innerKey",
               "type" : "string"
             }, {
               "name" : "value",
               "type" : [ "null", "long" ],
               "default" : null
             } ]
           } ],
           "default" : null
         } ]
       }
    """
    val expectedAvroSchema = new Schema.Parser().parse(expectedSchemaStr)

    assert(avroSchema.equals(expectedAvroSchema))
  }
}
