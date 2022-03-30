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
import org.apache.spark.sql.types.{DataTypes, StructType, StringType, ArrayType}
import org.scalatest.{FunSuite, Matchers}

class TestAvroConversionUtils extends FunSuite with Matchers {


  test("test convertStructTypeToAvroSchema") {
    val mapType = DataTypes.createMapType(StringType, new StructType().add("mapKey", "string", false).add("mapVal", "integer", true))
    val arrayType =  ArrayType(new StructType().add("arrayKey", "string", false).add("arrayVal", "integer", true))
    val innerStruct = new StructType().add("innerKey","string",false).add("value", "long", true)

    val struct = new StructType().add("key", "string", false).add("version", "string", true)
      .add("data1",innerStruct,false).add("data2",innerStruct,true)
      .add("nullableMap", mapType, true).add("map",mapType,false)
      .add("nullableArray", arrayType, true).add("array",arrayType,false)

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
         }, {
           "name" : "nullableMap",
           "type" : [ "null", {
             "type" : "map",
             "values" : [ {
               "type" : "record",
               "name" : "nullableMap",
               "namespace" : "SchemaNS.SchemaName",
               "fields" : [ {
                 "name" : "mapKey",
                 "type" : "string"
               }, {
                 "name" : "mapVal",
                 "type" : [ "null", "int" ],
                 "default" : null
               } ]
             }, "null" ]
           } ],
           "default" : null
         }, {
           "name" : "map",
           "type" : {
             "type" : "map",
             "values" : [ {
               "type" : "record",
               "name" : "map",
               "namespace" : "SchemaNS.SchemaName",
               "fields" : [ {
                 "name" : "mapKey",
                 "type" : "string"
               }, {
                 "name" : "mapVal",
                 "type" : [ "null", "int" ],
                 "default" : null
               } ]
             }, "null" ]
           }
         }, {
           "name" : "nullableArray",
           "type" : [ "null", {
             "type" : "array",
             "items" : [ {
               "type" : "record",
               "name" : "nullableArray",
               "namespace" : "SchemaNS.SchemaName",
               "fields" : [ {
                 "name" : "arrayKey",
                 "type" : "string"
               }, {
                 "name" : "arrayVal",
                 "type" : [ "null", "int" ],
                 "default" : null
               } ]
             }, "null" ]
           } ],
           "default" : null
         }, {
           "name" : "array",
           "type" : {
             "type" : "array",
             "items" : [ {
               "type" : "record",
               "name" : "array",
               "namespace" : "SchemaNS.SchemaName",
               "fields" : [ {
                 "name" : "arrayKey",
                 "type" : "string"
               }, {
                 "name" : "arrayVal",
                 "type" : [ "null", "int" ],
                 "default" : null
               } ]
             }, "null" ]
           }
         } ]
       }
    """
    val expectedAvroSchema = new Schema.Parser().parse(expectedSchemaStr)

    assert(avroSchema.equals(expectedAvroSchema))
  }

  test("test convertStructTypeToAvroSchema with Nested StructField comment") {
    val mapType = DataTypes.createMapType(StringType, new StructType().add("mapKey", "string", false, "mapKeyComment").add("mapVal", "integer", true))
    val arrayType =  ArrayType(new StructType().add("arrayKey", "string", false).add("arrayVal", "integer", true, "arrayValComment"))
    val innerStruct = new StructType().add("innerKey","string",false, "innerKeyComment").add("value", "long", true, "valueComment")

    val struct = new StructType().add("key", "string", false).add("version", "string", true, "versionComment")
      .add("data1",innerStruct,false).add("data2",innerStruct,true)
      .add("nullableMap", mapType, true).add("map",mapType,false)
      .add("nullableArray", arrayType, true).add("array",arrayType,false)

    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(struct, "SchemaName", "SchemaNS")

    val expectedSchemaStr = s"""
        {
          "type": "record",
          "name": "SchemaName",
          "namespace": "SchemaNS",
          "fields": [
            {
              "name": "key",
              "type": "string"
            },
            {
              "name": "version",
              "type": [
                "null",
                "string"
              ],
              "doc": "versionComment",
              "default": null
            },
            {
              "name": "data1",
              "type": {
                "type": "record",
                "name": "data1",
                "namespace": "SchemaNS.SchemaName",
                "fields": [
                  {
                    "name": "innerKey",
                    "type": "string",
                    "doc": "innerKeyComment"
                  },
                  {
                    "name": "value",
                    "type": [
                      "null",
                      "long"
                    ],
                    "doc": "valueComment",
                    "default": null
                  }
                ]
              }
            },
            {
              "name": "data2",
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "data2",
                  "namespace": "SchemaNS.SchemaName",
                  "fields": [
                    {
                      "name": "innerKey",
                      "type": "string",
                      "doc": "innerKeyComment"
                    },
                    {
                      "name": "value",
                      "type": [
                        "null",
                        "long"
                      ],
                      "doc": "valueComment",
                      "default": null
                    }
                  ]
                }
              ],
              "default": null
            },
            {
              "name": "nullableMap",
              "type": [
                "null",
                {
                  "type": "map",
                  "values": [
                    {
                      "type": "record",
                      "name": "nullableMap",
                      "namespace": "SchemaNS.SchemaName",
                      "fields": [
                        {
                          "name": "mapKey",
                          "type": "string",
                          "doc": "mapKeyComment"
                        },
                        {
                          "name": "mapVal",
                          "type": [
                            "null",
                            "int"
                          ],
                          "default": null
                        }
                      ]
                    },
                    "null"
                  ]
                }
              ],
              "default": null
            },
            {
              "name": "map",
              "type": {
                "type": "map",
                "values": [
                  {
                    "type": "record",
                    "name": "map",
                    "namespace": "SchemaNS.SchemaName",
                    "fields": [
                      {
                        "name": "mapKey",
                        "type": "string",
                        "doc": "mapKeyComment"
                      },
                      {
                        "name": "mapVal",
                        "type": [
                          "null",
                          "int"
                        ],
                        "default": null
                      }
                    ]
                  },
                  "null"
                ]
              }
            },
            {
              "name": "nullableArray",
              "type": [
                "null",
                {
                  "type": "array",
                  "items": [
                    {
                      "type": "record",
                      "name": "nullableArray",
                      "namespace": "SchemaNS.SchemaName",
                      "fields": [
                        {
                          "name": "arrayKey",
                          "type": "string"
                        },
                        {
                          "name": "arrayVal",
                          "type": [
                            "null",
                            "int"
                          ],
                          "doc": "arrayValComment",
                          "default": null
                        }
                      ]
                    },
                    "null"
                  ]
                }
              ],
              "default": null
            },
            {
              "name": "array",
              "type": {
                "type": "array",
                "items": [
                  {
                    "type": "record",
                    "name": "array",
                    "namespace": "SchemaNS.SchemaName",
                    "fields": [
                      {
                        "name": "arrayKey",
                        "type": "string"
                      },
                      {
                        "name": "arrayVal",
                        "type": [
                          "null",
                          "int"
                        ],
                        "doc": "arrayValComment",
                        "default": null
                      }
                    ]
                  },
                  "null"
                ]
              }
            }
          ]
        }}
    """

    val expectedAvroSchema = new Schema.Parser().parse(expectedSchemaStr)

    assert(avroSchema.equals(expectedAvroSchema))
  }
}
