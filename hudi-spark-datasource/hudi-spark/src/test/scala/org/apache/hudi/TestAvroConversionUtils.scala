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

import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.internal.schema.HoodieSchemaException

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, DataTypes, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.scalatest.{FunSuite, Matchers}

import java.nio.ByteBuffer
import java.util.Objects

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
    val expectedAvroSchema = HoodieSchema.parse(expectedSchemaStr).toAvroSchema

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

    val expectedAvroSchema = HoodieSchema.parse(expectedSchemaStr).toAvroSchema

    assert(avroSchema.equals(expectedAvroSchema))
  }

  test("test converter with binary") {
    val hoodieSchema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"h0_record\",\"namespace\":\"hoodie.h0\",\"fields\""
      + ":[{\"name\":\"col9\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}")
    val avroSchema = hoodieSchema.toAvroSchema
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

  test("test alignFieldsNullability function") {
    val sourceTableSchema: StructType =
      StructType(
        Seq(
          StructField("intType", IntegerType, nullable = false),
          StructField("longType", LongType),
          StructField("stringType", StringType, nullable = false),
          StructField("doubleType", DoubleType),
          StructField("floatType", FloatType, nullable = true),
          StructField("structType", new StructType(
            Array(StructField("structType_1", StringType), StructField("structType_2", StringType)))),
          StructField("dateType", DateType),
          StructField("listType", new ArrayType(StringType, true)),
          StructField("decimalType", new DecimalType(7, 3), nullable = false),
          StructField("timeStampType", TimestampType),
          StructField("mapType", new MapType(StringType, IntegerType, true))
        )
      )

    val writeStructSchema: StructType =
      StructType(
        Seq(
          StructField("intType", IntegerType, nullable = false),
          StructField("longType", LongType),
          StructField("stringType", StringType, nullable = true),
          StructField("doubleType", DoubleType),
          StructField("floatType", FloatType, nullable = false),
          StructField("structType", new StructType(
            Array(StructField("structType_1", StringType, nullable = false), StructField("structType_2", StringType)))),
          StructField("dateType", DateType, nullable = false),
          StructField("listType", new ArrayType(StringType, true)),
          StructField("decimalType", new DecimalType(7, 3)),
          StructField("timeStampType", TimestampType),
          StructField("mapType", new MapType(StringType, IntegerType, true)),
          StructField("notInTableSchemaTimeStampType_1", TimestampType),
          StructField("notInTableSchemaIntType", IntegerType, nullable = false),
          StructField("notInTableSchemaMapType", new MapType(StringType, IntegerType, true))
        )
      )
    val tableAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(sourceTableSchema, "data")

    val alignedSchema = AvroConversionUtils.alignFieldsNullability(writeStructSchema, tableAvroSchema)

    val nameToNullableSourceSchema = sourceTableSchema.fields.map(item => (item.name, item.nullable)).toMap

    val nameToNullableWriteSchema = writeStructSchema.fields.map(item => (item.name, item.nullable)).toMap

    // Validate alignment rules:
    // 1. For fields existing in both schemas: use source table's nullability
    // 2. For fields only in write schema: retain original nullability
    for (field <- alignedSchema.fields) {
      if (nameToNullableSourceSchema.contains(field.name) && nameToNullableWriteSchema.contains(field.name)) {
        assertTrue(field.nullable == nameToNullableSourceSchema(field.name))
      }
      if (!nameToNullableSourceSchema.contains(field.name) && nameToNullableWriteSchema.contains(field.name)) {
        assertTrue(field.nullable == nameToNullableWriteSchema(field.name))
      }
    }

    for (field <- alignedSchema.fields) {
      if (field.name.equals("intType")) {
        // Common field: both schemas specify nullable=false → aligned nullable=false
        assertFalse(field.nullable)
      }
      if (field.name.equals("longType")) {
        // Common field: both schemas default to nullable=true → aligned nullable=true
        assertTrue(field.nullable)
      }
      if (field.name.equals("stringType")) {
        // Conflicting case:
        // Write schema (nullable=true) overridden by table schema (nullable=false) → aligned nullable=false
        assertFalse(field.nullable)
      }

      if (field.name.equals("structType")) {
        val fields = field.dataType.asInstanceOf[StructType].fields
        assertTrue(fields.apply(0).nullable)
        assertTrue(fields.apply(1).nullable)
      }

      if (field.name.equals("dateType")) {
        // Conflicting case:
        // Write schema specifies nullable=false but table schema defaults to true → aligned nullable=true
        assertTrue(field.nullable)
      }

      if (field.name.equals("notInTableSchemaIntType")) {
        // Write-exclusive field: retains original nullability=false
        assertFalse(field.nullable)
      }

      if (field.name.equals("notInTableSchemaMapType")) {
        // Write-exclusive field: retains original nullability=true
        assertTrue(field.nullable)
      }
    }
  }
}
