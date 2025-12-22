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

import java.nio.ByteBuffer
import java.util.Objects
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hudi.internal.schema.HoodieSchemaException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, DataTypes, MapType, StringType, StructField, StructType}
import org.scalatest.{FunSuite, Matchers}

class TestAvroConversionUtils extends FunSuite with Matchers {


  val complexSchemaStr =
    s"""
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
           "values" : [
           "null",
           {
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
           } ]
         } ],
         "default" : null
       }, {
         "name" : "map",
         "type" : {
           "type" : "map",
           "values" : [
           "null",
           {
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
           } ]
         }
       }, {
         "name" : "nullableArray",
         "type" : [ "null", {
           "type" : "array",
           "items" : [
           "null",
           {
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
           } ]
         } ],
         "default" : null
       }, {
         "name" : "array",
         "type" : {
           "type" : "array",
           "items" : [
           "null",
           {
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
           } ]
         }
       } ]
     }
    """


  test("test convertStructTypeToAvroSchema_orig") {
    val mapType = DataTypes.createMapType(StringType, new StructType().add("mapKey", "string", false).add("mapVal", "integer", true))
    val arrayType = ArrayType(new StructType().add("arrayKey", "string", false).add("arrayVal", "integer", true))
    val innerStruct = new StructType().add("innerKey", "string", false).add("value", "long", true)

    val struct = new StructType().add("key", "string", false).add("version", "string", true)
      .add("data1", innerStruct, false).add("data2", innerStruct, true)
      .add("nullableMap", mapType, true).add("map", mapType, false)
      .add("nullableArray", arrayType, true).add("array", arrayType, false)

    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(struct, "SchemaName", "SchemaNS")

    val expectedSchemaStr = complexSchemaStr
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
                    "null",
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
                    }
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
                  "null",
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
                  }
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
                    "null",
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
                    }
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
                  "null",
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
                  }
                ]
              }
            }
          ]
        }
    """

    val expectedAvroSchema = new Schema.Parser().parse(expectedSchemaStr)

    assert(avroSchema.equals(expectedAvroSchema))
  }

  test("test converter with binary") {
    val avroSchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"h0_record\",\"namespace\":\"hoodie.h0\",\"fields\""
      + ":[{\"name\":\"col9\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}")
    val sparkSchema = StructType(List(StructField("col9", BinaryType, nullable = true)))
    // create a test record with avroSchema
    val avroRecord = new GenericData.Record(avroSchema)
    val bb = ByteBuffer.wrap(Array[Byte](97, 48, 53))
    avroRecord.put("col9", bb)
    val row1 = AvroConversionUtils.createAvroToInternalRowConverter(avroSchema, sparkSchema).apply(avroRecord).get
    val row2 = AvroConversionUtils.createAvroToInternalRowConverter(avroSchema, sparkSchema).apply(avroRecord).get
    internalRowCompare(row1, row2, sparkSchema)
  }

  private def internalRowCompare(expected: Any, actual: Any, schema: DataType): Unit = {
    schema match {
      case StructType(fields) =>
        val expectedRow = expected.asInstanceOf[InternalRow]
        val actualRow = actual.asInstanceOf[InternalRow]
        fields.zipWithIndex.foreach { case (field, i) => internalRowCompare(expectedRow.get(i, field.dataType), actualRow.get(i, field.dataType), field.dataType) }
      case ArrayType(elementType, _) =>
        val expectedArray = expected.asInstanceOf[ArrayData].toSeq[Any](elementType)
        val actualArray = actual.asInstanceOf[ArrayData].toSeq[Any](elementType)
        if (expectedArray.size != actualArray.size) {
          throw new AssertionError()
        } else {
          expectedArray.zip(actualArray).foreach { case (e1, e2) => internalRowCompare(e1, e2, elementType) }
        }
      case MapType(keyType, valueType, _) =>
        val expectedKeyArray = expected.asInstanceOf[MapData].keyArray()
        val expectedValueArray = expected.asInstanceOf[MapData].valueArray()
        val actualKeyArray = actual.asInstanceOf[MapData].keyArray()
        val actualValueArray = actual.asInstanceOf[MapData].valueArray()
        internalRowCompare(expectedKeyArray, actualKeyArray, ArrayType(keyType))
        internalRowCompare(expectedValueArray, actualValueArray, ArrayType(valueType))
      case StringType => if (checkNull(expected, actual) || !expected.toString.equals(actual.toString)) {
        throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
      }
      case BinaryType => if (checkNull(expected, actual) || !expected.asInstanceOf[Array[Byte]].sameElements(actual.asInstanceOf[Array[Byte]])) {
        throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
      }
      case _ => if (!Objects.equals(expected, actual)) {
        throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
      }
    }
  }

  private def checkNull(left: Any, right: Any): Boolean = {
    (left == null && right != null) || (left == null && right != null)
  }

  test("convert struct type with duplicate column names") {
    val struct = new StructType().add("id", DataTypes.LongType, true)
      .add("name", DataTypes.StringType, true)
      .add("name", DataTypes.StringType, true)
    the[HoodieSchemaException] thrownBy {
      AvroConversionUtils.convertStructTypeToAvroSchema(struct, "SchemaName", "SchemaNS")
    } should have message "Duplicate field name in record SchemaNS.SchemaName: name type:UNION pos:2 and name type:UNION pos:1."
  }
}
