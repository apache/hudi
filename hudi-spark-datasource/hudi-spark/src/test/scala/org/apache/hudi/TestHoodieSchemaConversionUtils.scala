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

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}
import org.apache.hudi.internal.schema.HoodieSchemaException

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.avro.HoodieSparkSchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.assertEquals
import org.scalatest.{FunSuite, Matchers}

import java.nio.ByteBuffer
import java.util.{Arrays, Objects}

class TestHoodieSchemaConversionUtils extends FunSuite with Matchers {

  test("test all primitive types conversion") {
    val struct = new StructType()
      .add("bool_field", BooleanType, false)
      .add("byte_field", ByteType, false)
      .add("short_field", ShortType, false)
      .add("int_field", IntegerType, false)
      .add("long_field", LongType, false)
      .add("float_field", FloatType, false)
      .add("double_field", DoubleType, false)
      .add("string_field", StringType, false)
      .add("binary_field", BinaryType, false)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "PrimitiveTypes", "test")

    // Verify all primitive type conversions
    assert(hoodieSchema.getField("bool_field").get().schema().getType == HoodieSchemaType.BOOLEAN)
    assert(hoodieSchema.getField("byte_field").get().schema().getType == HoodieSchemaType.INT)
    assert(hoodieSchema.getField("short_field").get().schema().getType == HoodieSchemaType.INT)
    assert(hoodieSchema.getField("int_field").get().schema().getType == HoodieSchemaType.INT)
    assert(hoodieSchema.getField("long_field").get().schema().getType == HoodieSchemaType.LONG)
    assert(hoodieSchema.getField("float_field").get().schema().getType == HoodieSchemaType.FLOAT)
    assert(hoodieSchema.getField("double_field").get().schema().getType == HoodieSchemaType.DOUBLE)
    assert(hoodieSchema.getField("string_field").get().schema().getType == HoodieSchemaType.STRING)
    assert(hoodieSchema.getField("binary_field").get().schema().getType == HoodieSchemaType.BYTES)

    // Verify roundtrip
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)
    assert(convertedStruct.fields.length == 9)
    assert(convertedStruct.fields(0).dataType == BooleanType)
    assert(convertedStruct.fields(1).dataType == IntegerType) // Byte → Int
    assert(convertedStruct.fields(2).dataType == IntegerType) // Short → Int
    assert(convertedStruct.fields(3).dataType == IntegerType)
    assert(convertedStruct.fields(4).dataType == LongType)
    assert(convertedStruct.fields(5).dataType == FloatType)
    assert(convertedStruct.fields(6).dataType == DoubleType)
    assert(convertedStruct.fields(7).dataType == StringType)
    assert(convertedStruct.fields(8).dataType == BinaryType)
  }

  test("test HoodieSchema to Spark conversion for all primitive types and enum") {
    // Create HoodieSchema with all primitive types and enum
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("bool", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
      HoodieSchemaField.of("int", HoodieSchema.create(HoodieSchemaType.INT)),
      HoodieSchemaField.of("long", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("float", HoodieSchema.create(HoodieSchemaType.FLOAT)),
      HoodieSchemaField.of("double", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
      HoodieSchemaField.of("string", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("bytes", HoodieSchema.create(HoodieSchemaType.BYTES)),
      HoodieSchemaField.of("fixed", HoodieSchema.createFixed("MD5", "com.example", "MD5 hash", 16)),
      HoodieSchemaField.of("enum", HoodieSchema.createEnum("Color", "com.example", "Color enum", Arrays.asList("RED", "GREEN", "BLUE"))),
      HoodieSchemaField.of("null", HoodieSchema.create(HoodieSchemaType.NULL))
    )
    val hoodieSchema = HoodieSchema.createRecord("AllPrimitives", "test", null, fields)

    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    assert(structType.fields.length == 10)
    assert(structType.fields(0).dataType == BooleanType)
    assert(structType.fields(1).dataType == IntegerType)
    assert(structType.fields(2).dataType == LongType)
    assert(structType.fields(3).dataType == FloatType)
    assert(structType.fields(4).dataType == DoubleType)
    assert(structType.fields(5).dataType == StringType)
    assert(structType.fields(6).dataType == BinaryType)
    assert(structType.fields(7).dataType == BinaryType)
    assert(structType.fields(8).dataType == StringType)
    assert(structType.fields(9).dataType == NullType)
    assert(structType.fields(9).nullable) // Null type is always nullable
  }

  test("test logical types conversion - date, timestamp, decimal") {
    val struct = new StructType()
      .add("date_field", DateType, false)
      .add("timestamp_field", TimestampType, true)
      .add("decimal_field", DecimalType(10, 2), false)
      .add("decimal_field2", DecimalType(20, 5), true)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "LogicalTypes", "test")

    // Verify DATE logical type
    val dateField = hoodieSchema.getField("date_field").get()
    assert(dateField.schema().getType == HoodieSchemaType.DATE)
    assert(!dateField.isNullable())

    // Verify TIMESTAMP logical type
    val timestampField = hoodieSchema.getField("timestamp_field").get()
    assert(timestampField.isNullable())
    val timestampSchema = timestampField.schema().getNonNullType()
    assert(timestampSchema.getType == HoodieSchemaType.TIMESTAMP)
    assert(timestampSchema.isInstanceOf[HoodieSchema.Timestamp])
    assert(timestampSchema.asInstanceOf[HoodieSchema.Timestamp].isUtcAdjusted)

    // Verify DECIMAL logical type
    val decimalField = hoodieSchema.getField("decimal_field").get()
    assert(decimalField.schema().getType == HoodieSchemaType.DECIMAL)
    assert(decimalField.schema().isInstanceOf[HoodieSchema.Decimal])
    val decimalSchema = decimalField.schema().asInstanceOf[HoodieSchema.Decimal]
    assert(decimalSchema.getPrecision == 10)
    assert(decimalSchema.getScale == 2)

    // Verify roundtrip preserves logical types
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)
    assert(convertedStruct.fields(0).dataType == DateType)
    assert(convertedStruct.fields(1).dataType == TimestampType)
    assert(convertedStruct.fields(2).dataType == DecimalType(10, 2))
    assert(convertedStruct.fields(3).dataType == DecimalType(20, 5))
  }

  test("test HoodieSchema to Spark conversion for logical types") {
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("date", HoodieSchema.createDate()),
      HoodieSchemaField.of("timestamp_micros", HoodieSchema.createTimestampMicros()),
      HoodieSchemaField.of("timestamp_ntz", HoodieSchema.createLocalTimestampMicros()),
      HoodieSchemaField.of("decimal", HoodieSchema.createDecimal(15, 3)),
      HoodieSchemaField.of("time_millis", HoodieSchema.createTimeMillis()),
      HoodieSchemaField.of("time_micros", HoodieSchema.createTimeMicros()),
      HoodieSchemaField.of("uuid", HoodieSchema.createUUID())
    )
    val hoodieSchema = HoodieSchema.createRecord("LogicalTypes", "test", null, fields)

    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    assert(structType.fields.length == 7)
    assert(structType.fields(0).dataType == DateType)
    assert(structType.fields(1).dataType == TimestampType)
    assert(structType.fields(3).dataType == DecimalType(15, 3))
    assert(structType.fields(4).dataType == IntegerType)  // time_millis -> INT
    assert(structType.fields(5).dataType == LongType)     // time_micros -> LONG
    assert(structType.fields(6).dataType == StringType)   // uuid -> STRING
  }

  test("test binary type handling") {
    val struct = new StructType()
      .add("binary_field", BinaryType, false)
      .add("nullable_binary", BinaryType, true)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "BinaryTypes", "test")

    val binaryField = hoodieSchema.getField("binary_field").get()
    assert(binaryField.schema().getType == HoodieSchemaType.BYTES)
    assert(!binaryField.isNullable())

    val nullableBinaryField = hoodieSchema.getField("nullable_binary").get()
    assert(nullableBinaryField.isNullable())

    // Verify roundtrip
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)
    assert(convertedStruct.fields(0).dataType == BinaryType)
    assert(!convertedStruct.fields(0).nullable)
    assert(convertedStruct.fields(1).dataType == BinaryType)
    assert(convertedStruct.fields(1).nullable)
  }

  test("test CharType and VarcharType conversion to STRING") {
    val struct = new StructType()
      .add("char_field", CharType(10), false)
      .add("varchar_field", VarcharType(255), false)
      .add("string_field", StringType, false)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "CharTypes", "test")

    // All should map to STRING
    assert(hoodieSchema.getField("char_field").get().schema().getType == HoodieSchemaType.STRING)
    assert(hoodieSchema.getField("varchar_field").get().schema().getType == HoodieSchemaType.STRING)
    assert(hoodieSchema.getField("string_field").get().schema().getType == HoodieSchemaType.STRING)
  }

  test("test SchemaType enum values for logical types") {
    // Verify that DATE, TIMESTAMP, DECIMAL are properly recognized as distinct types
    val dateSchema = HoodieSchema.createDate()
    assertEquals(dateSchema.getType, HoodieSchemaType.DATE)

    val timestampSchema = HoodieSchema.createTimestampMicros()
    assertEquals(timestampSchema.getType, HoodieSchemaType.TIMESTAMP)

    val decimalSchema = HoodieSchema.createDecimal(10, 2)
    assertEquals(decimalSchema.getType, HoodieSchemaType.DECIMAL)

    // Verify conversion to Spark types
    val dateType = HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(dateSchema)
    assertEquals(dateType, DateType)

    val timestampType = HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(timestampSchema)
    assertEquals(timestampType, TimestampType)

    val decimalType = HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(decimalSchema)
    assertEquals(decimalType, DecimalType(10, 2))
  }

  test("test conversion error handling with duplicate field names") {
    val invalidStruct = new StructType()
      .add("field1", "string", false)
      .add("field1", "int", false)  // Duplicate field name

    the[HoodieSchemaException] thrownBy {
      HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        invalidStruct, "InvalidSchema", "test")
    }
  }

  test("test empty namespace handling") {
    val struct = new StructType().add("field", "string", false)

    // Convert with empty namespace
    val hoodieSchema1 = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "Test", "")

    assert(hoodieSchema1.getName() == "Test")
    assert(!hoodieSchema1.getNamespace().isPresent || hoodieSchema1.getNamespace().get() == "")

    // Convert with no namespace just qualifiedName
    val hoodieSchema2 = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "SimpleTest")

    assert(hoodieSchema2.getName() == "SimpleTest")
  }

  test("test qualified name parsing") {
    val struct = new StructType().add("field", "string", false)

    // Test multi-part qualified name
    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "com.example.database.Table")

    assert(hoodieSchema.getName() == "Table")
    assert(hoodieSchema.getNamespace().get() == "com.example.database")
  }

  test("test field with no comment preserves existing doc") {
    val struct = new StructType()
      .add("field_with_comment", "string", false, "User provided comment")
      .add("field_without_comment", "int", false)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "Comments", "test")

    val field1 = hoodieSchema.getField("field_with_comment").get()
    assert(field1.doc().get() == "User provided comment")

    val field2 = hoodieSchema.getField("field_without_comment").get()
    assert(!field2.doc().isPresent)
  }

  test("test convertHoodieSchemaToStructType using hoodie schema field") {
    val innerFields = java.util.Arrays.asList(
      HoodieSchemaField.of("innerKey", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("value", HoodieSchema.createNullable(HoodieSchemaType.LONG))
    )
    val innerRecord = HoodieSchema.createRecord("InnerRecord", "test", "Test inner record", innerFields)

    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("nested", innerRecord)
    )
    val hoodieSchema = HoodieSchema.createRecord("TestRecord", "test", "Test record", fields)

    // Convert to Spark StructType
    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    // Verify structure
    assert(structType.fields.length == 2)
    assert(structType.fields(0).name == "key")
    assert(structType.fields(0).dataType == StringType)
    assert(!structType.fields(0).nullable)

    assert(structType.fields(1).name == "nested")
    assert(structType.fields(1).dataType.isInstanceOf[StructType])

    val nestedStruct = structType.fields(1).dataType.asInstanceOf[StructType]
    assert(nestedStruct.fields.length == 2)
    assert(nestedStruct.fields(0).name == "innerKey")
    assert(nestedStruct.fields(0).dataType == StringType)
    assert(!nestedStruct.fields(0).nullable)

    assert(nestedStruct.fields(1).name == "value")
    assert(nestedStruct.fields(1).dataType == LongType)
    assert(nestedStruct.fields(1).nullable)
  }

  test("test roundtrip conversion preserves schema structure") {
    val originalStruct = new StructType()
      .add("id", "long", false)
      .add("name", "string", true)
      .add("scores", ArrayType(IntegerType, containsNull = true), false)
      .add("metadata", MapType(StringType, StringType, valueContainsNull = true), true)
      .add("timestamp", TimestampType, false)
      .add("date", DateType, true)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      originalStruct, "TestSchema", "test.namespace")
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    // Should be equivalent (comparing field names, types, and nullability)
    assert(originalStruct.fields.length == convertedStruct.fields.length)
    originalStruct.fields.zip(convertedStruct.fields).foreach { case (orig, converted) =>
      assert(orig.name == converted.name, s"Field name mismatch: ${orig.name} vs ${converted.name}")
      assert(orig.dataType == converted.dataType, s"Field ${orig.name} type mismatch: ${orig.dataType} vs ${converted.dataType}")
      assert(orig.nullable == converted.nullable, s"Field ${orig.name} nullability mismatch: ${orig.nullable} vs ${converted.nullable}")
    }
  }

  test("test convertStructTypeToHoodieSchema preserves field comments") {
    val struct = new StructType()
      .add("id", "long", false, "Primary identifier")
      .add("name", "string", true, "User display name")
      .add("nested", new StructType()
        .add("field1", "int", false, "Nested field comment")
        .add("field2", "string", true), false, "Nested struct comment")

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "TestSchema", "test.namespace")

    // Verify comments are preserved
    val idField = hoodieSchema.getField("id").get()
    assert(idField.doc().get() == "Primary identifier")

    val nameField = hoodieSchema.getField("name").get()
    assert(nameField.doc().get() == "User display name")

    val nestedField = hoodieSchema.getField("nested").get()
    assert(nestedField.doc().get() == "Nested struct comment")

    // Verify nested field comments
    val nestedSchema = nestedField.schema()
    val field1 = nestedSchema.getField("field1").get()
    assert(field1.doc().get() == "Nested field comment")
  }

  test("test complex types - arrays with nullability") {
    val struct = new StructType()
      .add("array_non_null_elements", ArrayType(StringType, containsNull = false), false)
      .add("array_nullable_elements", ArrayType(StringType, containsNull = true), false)
      .add("nullable_array", ArrayType(IntegerType, containsNull = false), true)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "ArrayTypes", "test")

    // Verify array nullability handling
    val field1 = hoodieSchema.getField("array_non_null_elements").get()
    assert(field1.schema().getType == HoodieSchemaType.ARRAY)
    assert(!field1.schema().getElementType.isNullable)
    assert(!field1.isNullable())

    val field2 = hoodieSchema.getField("array_nullable_elements").get()
    assert(field2.schema().getType == HoodieSchemaType.ARRAY)
    assert(field2.schema().getElementType.isNullable)

    val field3 = hoodieSchema.getField("nullable_array").get()
    assert(field3.isNullable())

    // Verify roundtrip
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)
    assert(!convertedStruct.fields(0).dataType.asInstanceOf[ArrayType].containsNull)
    assert(convertedStruct.fields(1).dataType.asInstanceOf[ArrayType].containsNull)
    assert(convertedStruct.fields(2).nullable)
  }

  test("test complex types - maps with nullability") {
    val struct = new StructType()
      .add("map_non_null_values", MapType(StringType, IntegerType, valueContainsNull = false), false)
      .add("map_nullable_values", MapType(StringType, IntegerType, valueContainsNull = true), false)
      .add("nullable_map", MapType(StringType, StringType, valueContainsNull = false), true)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "MapTypes", "test")

    // Verify map value nullability
    val field1 = hoodieSchema.getField("map_non_null_values").get()
    assert(field1.schema().getType == HoodieSchemaType.MAP)
    assert(!field1.schema().getValueType.isNullable)

    val field2 = hoodieSchema.getField("map_nullable_values").get()
    assert(field2.schema().getType == HoodieSchemaType.MAP)
    assert(field2.schema().getValueType.isNullable)

    val field3 = hoodieSchema.getField("nullable_map").get()
    assert(field3.isNullable())

    // Verify roundtrip
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)
    assert(!convertedStruct.fields(0).dataType.asInstanceOf[MapType].valueContainsNull)
    assert(convertedStruct.fields(1).dataType.asInstanceOf[MapType].valueContainsNull)
    assert(convertedStruct.fields(2).nullable)
  }

  test("test arrays of complex types") {
    val elementStruct = new StructType()
      .add("id", "int", false)
      .add("name", "string", true)

    val struct = new StructType()
      .add("array_of_structs", ArrayType(elementStruct, containsNull = true), false)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "ArrayOfStructs", "test")

    val arrayField = hoodieSchema.getField("array_of_structs").get()
    assert(arrayField.schema().getType == HoodieSchemaType.ARRAY)

    val elementType = arrayField.schema().getElementType
    assert(elementType.isNullable) // Elements are nullable
    val elementRecord = elementType.getNonNullType()
    assert(elementRecord.getType == HoodieSchemaType.RECORD)
    assert(elementRecord.getFields.size() == 2)

    // Verify roundtrip
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)
    val convertedArrayType = convertedStruct.fields(0).dataType.asInstanceOf[ArrayType]
    assert(convertedArrayType.containsNull)
    assert(convertedArrayType.elementType.isInstanceOf[StructType])
  }

  test("test maps of complex types") {
    val valueStruct = new StructType()
      .add("count", "long", false)
      .add("metadata", "string", true)

    val struct = new StructType()
      .add("map_of_structs", MapType(StringType, valueStruct, valueContainsNull = true), false)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "MapOfStructs", "test")

    val mapField = hoodieSchema.getField("map_of_structs").get()
    assert(mapField.schema().getType == HoodieSchemaType.MAP)

    val valueType = mapField.schema().getValueType
    assert(valueType.isNullable) // Values are nullable
    val valueRecord = valueType.getNonNullType()
    assert(valueRecord.getType == HoodieSchemaType.RECORD)
    assert(valueRecord.getFields.size() == 2)

    // Verify roundtrip
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)
    val convertedMapType = convertedStruct.fields(0).dataType.asInstanceOf[MapType]
    assert(convertedMapType.valueContainsNull)
    assert(convertedMapType.valueType.isInstanceOf[StructType])
  }

  test("test namespace hierarchy for nested records") {
    val level2 = new StructType().add("field2", "string", false)
    val level1 = new StructType().add("field1", "int", false).add("nested2", level2, false)
    val struct = new StructType().add("field0", "long", false).add("nested1", level1, false)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "Root", "com.example")

    // Verify namespace hierarchy
    assert(hoodieSchema.getNamespace().orElse(null) == "com.example")
    assert(hoodieSchema.getName() == "Root")

    val nested1 = hoodieSchema.getField("nested1").get().schema()
    assert(nested1.getNamespace().orElse(null) == "com.example.Root")

    val nested2 = nested1.getField("nested2").get().schema()
    assert(nested2.getNamespace().orElse(null) == "com.example.Root.nested1")
  }

  test("test alignFieldsNullability with HoodieSchema") {
    val sourceStruct = new StructType()
      .add("field1", "string", false)  // Non-nullable in source
      .add("field2", "int", true)      // Nullable in source
      .add("nested", new StructType()
        .add("inner1", "long", false)
        .add("inner2", "string", true), false)

    // Create HoodieSchema with different nullability
    val nestedFields = java.util.Arrays.asList(
      HoodieSchemaField.of("inner1", HoodieSchema.createNullable(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("inner2", HoodieSchema.create(HoodieSchemaType.STRING))
    )
    val nestedSchema = HoodieSchema.createRecord("nested", "test", null, nestedFields)

    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("field1", HoodieSchema.createNullable(HoodieSchemaType.STRING)), // Nullable in target
      HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.INT)),            // Non-nullable in target
      HoodieSchemaField.of("nested", nestedSchema)
    )
    val hoodieSchema = HoodieSchema.createRecord("TestSchema", "test", null, fields)

    // Align nullability
    val alignedStruct = HoodieSchemaConversionUtils.alignFieldsNullability(sourceStruct, hoodieSchema)

    // Verify alignment (should match HoodieSchema nullability)
    assert(alignedStruct.fields(0).nullable)  // Aligned to HoodieSchema
    assert(!alignedStruct.fields(1).nullable) // Aligned to HoodieSchema

    val nestedStruct = alignedStruct.fields(2).dataType.asInstanceOf[StructType]
    assert(nestedStruct.fields(0).nullable)   // Aligned to HoodieSchema
    assert(!nestedStruct.fields(1).nullable)  // Aligned to HoodieSchema
  }

  test("test alignFieldsNullability with complex types") {
    val sourceStruct = new StructType()
      .add("arrayField", ArrayType(StringType, containsNull = false), false)
      .add("mapField", MapType(StringType, IntegerType, valueContainsNull = false), false)

    // Create HoodieSchema where element/value types are nullable
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("arrayField", HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchemaType.STRING))),
      HoodieSchemaField.of("mapField", HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchemaType.INT)))
    )
    val hoodieSchema = HoodieSchema.createRecord("ComplexNullability", "test", null, fields)

    val alignedStruct = HoodieSchemaConversionUtils.alignFieldsNullability(sourceStruct, hoodieSchema)

    // Verify that array element and map value nullability is aligned
    val arrayType = alignedStruct.fields(0).dataType.asInstanceOf[ArrayType]
    assert(arrayType.containsNull) // Aligned to nullable elements

    val mapType = alignedStruct.fields(1).dataType.asInstanceOf[MapType]
    assert(mapType.valueContainsNull) // Aligned to nullable values
  }

  test("test createGenericRecordToInternalRowConverter with binary") {
    val hoodieSchema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"h0_record\",\"namespace\":\"hoodie.h0\",\"fields\""
      + ":[{\"name\":\"col9\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}")
    val avroSchema = hoodieSchema.toAvroSchema
    val sparkSchema = StructType(List(StructField("col9", BinaryType, nullable = true)))

    // Create test record using Avro schema
    val avroRecord = new GenericData.Record(avroSchema)
    val bb = ByteBuffer.wrap(Array[Byte](97, 48, 53))
    avroRecord.put("col9", bb)

    // Test the HoodieSchema-based converter
    val row1 = HoodieSchemaConversionUtils.createGenericRecordToInternalRowConverter(hoodieSchema, sparkSchema).apply(avroRecord).get
    val row2 = HoodieSchemaConversionUtils.createGenericRecordToInternalRowConverter(hoodieSchema, sparkSchema).apply(avroRecord).get

    internalRowCompare(row1, row2, sparkSchema)
  }

  test("test VECTOR type conversion - Spark to HoodieSchema") {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(128)")
      .build()
    val struct = new StructType()
      .add("id", IntegerType, false)
      .add("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "VectorTest", "test")

    // Verify the embedding field is VECTOR type
    val embeddingField = hoodieSchema.getField("embedding").get()
    assert(embeddingField.schema().getType == HoodieSchemaType.VECTOR)
    assert(embeddingField.schema().isInstanceOf[HoodieSchema.Vector])

    val vectorSchema = embeddingField.schema().asInstanceOf[HoodieSchema.Vector]
    assert(vectorSchema.getDimension == 128)
    assert(vectorSchema.getVectorElementType == HoodieSchema.Vector.VectorElementType.FLOAT)
    assert(!embeddingField.isNullable())
  }

  test("test VECTOR type conversion - Spark to HoodieSchema for DOUBLE and INT8") {
    val doubleMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(64, DOUBLE)")
      .build()
    val int8Metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(32, INT8)")
      .build()

    val struct = new StructType()
      .add("embedding_double", ArrayType(DoubleType, containsNull = false), nullable = false, doubleMetadata)
      .add("embedding_int8", ArrayType(ByteType, containsNull = false), nullable = false, int8Metadata)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      struct, "VectorTypedTest", "test")

    val doubleField = hoodieSchema.getField("embedding_double").get()
    assert(doubleField.schema().getType == HoodieSchemaType.VECTOR)
    val doubleVector = doubleField.schema().asInstanceOf[HoodieSchema.Vector]
    assert(doubleVector.getDimension == 64)
    assert(doubleVector.getVectorElementType == HoodieSchema.Vector.VectorElementType.DOUBLE)
    assert(!doubleField.isNullable())

    val int8Field = hoodieSchema.getField("embedding_int8").get()
    assert(int8Field.schema().getType == HoodieSchemaType.VECTOR)
    val int8Vector = int8Field.schema().asInstanceOf[HoodieSchema.Vector]
    assert(int8Vector.getDimension == 32)
    assert(int8Vector.getVectorElementType == HoodieSchema.Vector.VectorElementType.INT8)
    assert(!int8Field.isNullable())
  }

  test("test VECTOR type conversion - HoodieSchema to Spark") {
    val vectorSchema = HoodieSchema.createVector(256, HoodieSchema.Vector.VectorElementType.FLOAT)
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
      HoodieSchemaField.of("embedding", vectorSchema)
    )
    val hoodieSchema = HoodieSchema.createRecord("VectorTest", "test", null, fields)

    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    // Verify the embedding field is ArrayType(FloatType)
    assert(structType.fields.length == 2)
    val embeddingField = structType.fields(1)
    assert(embeddingField.name == "embedding")
    assert(embeddingField.dataType.isInstanceOf[ArrayType])
    assert(!embeddingField.nullable)

    val arrayType = embeddingField.dataType.asInstanceOf[ArrayType]
    assert(arrayType.elementType == FloatType)
    assert(!arrayType.containsNull)

    // Verify metadata contains type descriptor with dimension
    assert(embeddingField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsedVector = HoodieSchema.parseTypeDescriptor(
      embeddingField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assert(parsedVector.getType == HoodieSchemaType.VECTOR)
    assert(parsedVector.getDimension == 256)
  }

  test("test VECTOR type conversion - HoodieSchema to Spark for DOUBLE and INT8") {
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("embedding_double", HoodieSchema.createVector(64, HoodieSchema.Vector.VectorElementType.DOUBLE)),
      HoodieSchemaField.of("embedding_int8", HoodieSchema.createVector(32, HoodieSchema.Vector.VectorElementType.INT8))
    )
    val hoodieSchema = HoodieSchema.createRecord("VectorTypedTest", "test", null, fields)

    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    val doubleField = structType.fields(0)
    assert(doubleField.dataType == ArrayType(DoubleType, containsNull = false))
    assert(doubleField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsedDoubleVector = HoodieSchema.parseTypeDescriptor(
      doubleField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assert(parsedDoubleVector.getType == HoodieSchemaType.VECTOR)
    assert(parsedDoubleVector.getDimension == 64)
    assert(parsedDoubleVector.getVectorElementType == HoodieSchema.Vector.VectorElementType.DOUBLE)

    val int8Field = structType.fields(1)
    assert(int8Field.dataType == ArrayType(ByteType, containsNull = false))
    assert(int8Field.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsedInt8Vector = HoodieSchema.parseTypeDescriptor(
      int8Field.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assert(parsedInt8Vector.getType == HoodieSchemaType.VECTOR)
    assert(parsedInt8Vector.getDimension == 32)
    assert(parsedInt8Vector.getVectorElementType == HoodieSchema.Vector.VectorElementType.INT8)
  }

  test("test VECTOR round-trip conversion - Spark to HoodieSchema to Spark") {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(512)")
      .build()
    val originalStruct = new StructType()
      .add("id", LongType, false)
      .add("vector_field", ArrayType(FloatType, containsNull = false), nullable = false, metadata)
      .add("name", StringType, true)

    // Convert Spark -> HoodieSchema -> Spark
    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      originalStruct, "RoundTripTest", "test")
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    // Verify structure is preserved
    assert(convertedStruct.fields.length == originalStruct.fields.length)

    // Verify vector field properties
    val originalVectorField = originalStruct.fields(1)
    val convertedVectorField = convertedStruct.fields(1)

    assert(convertedVectorField.name == originalVectorField.name)
    assert(convertedVectorField.dataType == originalVectorField.dataType)
    assert(convertedVectorField.nullable == originalVectorField.nullable)

    // Verify metadata is preserved
    assert(convertedVectorField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val roundTripVector = HoodieSchema.parseTypeDescriptor(
      convertedVectorField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assert(roundTripVector.getType == HoodieSchemaType.VECTOR)
    assert(roundTripVector.getDimension == 512)

    // Verify array properties
    val convertedArrayType = convertedVectorField.dataType.asInstanceOf[ArrayType]
    assert(convertedArrayType.elementType == FloatType)
    assert(!convertedArrayType.containsNull)
  }

  test("test VECTOR round-trip conversion - Spark to HoodieSchema to Spark for DOUBLE and INT8") {
    val doubleMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(64, DOUBLE)")
      .build()
    val int8Metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(32, INT8)")
      .build()
    val originalStruct = new StructType()
      .add("id", LongType, false)
      .add("vector_double", ArrayType(DoubleType, containsNull = false), nullable = false, doubleMetadata)
      .add("vector_int8", ArrayType(ByteType, containsNull = false), nullable = false, int8Metadata)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      originalStruct, "VectorTypedRoundTripTest", "test")
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    val originalDouble = originalStruct.fields(1)
    val convertedDouble = convertedStruct.fields(1)
    assert(convertedDouble.name == originalDouble.name)
    assert(convertedDouble.dataType == originalDouble.dataType)
    assert(convertedDouble.nullable == originalDouble.nullable)
    val convertedDoubleVector = HoodieSchema.parseTypeDescriptor(
      convertedDouble.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assert(convertedDoubleVector.getType == HoodieSchemaType.VECTOR)
    assert(convertedDoubleVector.getDimension == 64)
    assert(convertedDoubleVector.getVectorElementType == HoodieSchema.Vector.VectorElementType.DOUBLE)

    val originalInt8 = originalStruct.fields(2)
    val convertedInt8 = convertedStruct.fields(2)
    assert(convertedInt8.name == originalInt8.name)
    assert(convertedInt8.dataType == originalInt8.dataType)
    assert(convertedInt8.nullable == originalInt8.nullable)
    val convertedInt8Vector = HoodieSchema.parseTypeDescriptor(
      convertedInt8.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assert(convertedInt8Vector.getType == HoodieSchemaType.VECTOR)
    assert(convertedInt8Vector.getDimension == 32)
    assert(convertedInt8Vector.getVectorElementType == HoodieSchema.Vector.VectorElementType.INT8)
  }

  test("test VECTOR round-trip conversion - HoodieSchema to Spark to HoodieSchema") {
    val originalVectorSchema = HoodieSchema.createVector(1024, HoodieSchema.Vector.VectorElementType.FLOAT)
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("embedding", originalVectorSchema),
      HoodieSchemaField.of("metadata", HoodieSchema.createNullable(HoodieSchemaType.STRING))
    )
    val originalHoodieSchema = HoodieSchema.createRecord("RoundTripTest", "test", null, fields)

    // Convert HoodieSchema -> Spark -> HoodieSchema
    val sparkStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(originalHoodieSchema)
    val convertedHoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      sparkStruct, "RoundTripTest", "test")

    // Verify the vector field is preserved
    val convertedEmbeddingField = convertedHoodieSchema.getField("embedding").get()
    assert(convertedEmbeddingField.schema().getType == HoodieSchemaType.VECTOR)
    assert(convertedEmbeddingField.schema().isInstanceOf[HoodieSchema.Vector])

    val convertedVectorSchema = convertedEmbeddingField.schema().asInstanceOf[HoodieSchema.Vector]
    assert(convertedVectorSchema.getDimension == 1024)
    assert(convertedVectorSchema.getVectorElementType == HoodieSchema.Vector.VectorElementType.FLOAT)
    assert(!convertedEmbeddingField.isNullable())

    // Verify other fields are preserved
    assert(convertedHoodieSchema.getFields.size() == 3)
    assert(convertedHoodieSchema.getField("id").get().schema().getType == HoodieSchemaType.LONG)
    assert(convertedHoodieSchema.getField("metadata").get().isNullable())
  }

  test("test nullable VECTOR round-trip conversion") {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(64)")
      .build()
    val originalStruct = new StructType()
      .add("id", LongType, false)
      .add("embedding", ArrayType(FloatType, containsNull = false), nullable = true, metadata)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      originalStruct, "NullableVectorTest", "test")

    // Verify the vector field is nullable in HoodieSchema
    val embeddingField = hoodieSchema.getField("embedding").get()
    assert(embeddingField.isNullable())
    val vectorSchema = embeddingField.schema().getNonNullType().asInstanceOf[HoodieSchema.Vector]
    assert(vectorSchema.getDimension == 64)
    assert(vectorSchema.getVectorElementType == HoodieSchema.Vector.VectorElementType.FLOAT)

    // Convert back to Spark
    val convertedStruct = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)
    val convertedField = convertedStruct.fields(1)
    assert(convertedField.nullable)
    assert(convertedField.dataType == ArrayType(FloatType, containsNull = false))
    assert(convertedField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsedVector = HoodieSchema.parseTypeDescriptor(
      convertedField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assert(parsedVector.getDimension == 64)
  }

  test("test nullable VECTOR via UNION preserves metadata in HoodieSchema to Spark conversion") {
    val vectorSchema = HoodieSchema.createVector(256, HoodieSchema.Vector.VectorElementType.FLOAT)
    val nullableVectorSchema = HoodieSchema.createNullable(vectorSchema)
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
      HoodieSchemaField.of("embedding", nullableVectorSchema)
    )
    val hoodieSchema = HoodieSchema.createRecord("NullableVectorUnionTest", "test", null, fields)

    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    val embeddingField = structType.fields(1)
    assert(embeddingField.name == "embedding")
    assert(embeddingField.nullable)
    assert(embeddingField.dataType == ArrayType(FloatType, containsNull = false))

    // The key assertion: metadata must survive the UNION unwrapping (.copy(nullable = true))
    assert(embeddingField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsedVector = HoodieSchema.parseTypeDescriptor(
      embeddingField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assert(parsedVector.getDimension == 256)
    assert(parsedVector.getVectorElementType == HoodieSchema.Vector.VectorElementType.FLOAT)
  }

  test("alignSchemaWithCatalog narrows nullability when alignNullability = true") {
    val sourceSchema = new StructType()
      .add("id", LongType, nullable = true)
      .add("payload", new StructType()
        .add("inner", StringType, nullable = true), nullable = true)
      .add("items", ArrayType(new StructType()
        .add("x", IntegerType, nullable = true), containsNull = true), nullable = true)
      .add("lookup", MapType(StringType, new StructType()
        .add("y", IntegerType, nullable = true), valueContainsNull = true), nullable = true)

    val targetSchema = new StructType()
      .add("id", LongType, nullable = false)
      .add("payload", new StructType()
        .add("inner", StringType, nullable = false), nullable = false)
      .add("items", ArrayType(new StructType()
        .add("x", IntegerType, nullable = false), containsNull = false), nullable = false)
      .add("lookup", MapType(StringType, new StructType()
        .add("y", IntegerType, nullable = false), valueContainsNull = false), nullable = false)

    val aligned = HoodieSchemaConversionUtils.alignSchemaWithCatalog(
      sourceSchema, targetSchema, caseSensitive = false, alignNullability = true)

    assert(!aligned("id").nullable)
    assert(!aligned("payload").nullable)
    assert(!aligned("payload").dataType.asInstanceOf[StructType]("inner").nullable)
    val itemsArray = aligned("items").dataType.asInstanceOf[ArrayType]
    assert(!aligned("items").nullable && !itemsArray.containsNull)
    assert(!itemsArray.elementType.asInstanceOf[StructType]("x").nullable)
    val lookupMap = aligned("lookup").dataType.asInstanceOf[MapType]
    assert(!aligned("lookup").nullable && !lookupMap.valueContainsNull)
    assert(!lookupMap.valueType.asInstanceOf[StructType]("y").nullable)
  }

  test("alignSchemaWithCatalog preserves source nullability when alignNullability = false") {
    val sourceSchema = new StructType()
      .add("id", LongType, nullable = true)
      .add("payload", new StructType()
        .add("inner", StringType, nullable = true), nullable = true)
      .add("items", ArrayType(new StructType()
        .add("x", IntegerType, nullable = true), containsNull = true), nullable = true)
      .add("lookup", MapType(StringType, new StructType()
        .add("y", IntegerType, nullable = true), valueContainsNull = true), nullable = true)

    val targetSchema = new StructType()
      .add("id", LongType, nullable = false)
      .add("payload", new StructType()
        .add("inner", StringType, nullable = false), nullable = false)
      .add("items", ArrayType(new StructType()
        .add("x", IntegerType, nullable = false), containsNull = false), nullable = false)
      .add("lookup", MapType(StringType, new StructType()
        .add("y", IntegerType, nullable = false), valueContainsNull = false), nullable = false)

    val aligned = HoodieSchemaConversionUtils.alignSchemaWithCatalog(
      sourceSchema, targetSchema, caseSensitive = false, alignNullability = false)

    assert(aligned("id").nullable)
    assert(aligned("payload").nullable)
    assert(aligned("payload").dataType.asInstanceOf[StructType]("inner").nullable)
    val itemsArray = aligned("items").dataType.asInstanceOf[ArrayType]
    assert(aligned("items").nullable && itemsArray.containsNull)
    assert(itemsArray.elementType.asInstanceOf[StructType]("x").nullable)
    val lookupMap = aligned("lookup").dataType.asInstanceOf[MapType]
    assert(aligned("lookup").nullable && lookupMap.valueContainsNull)
    assert(lookupMap.valueType.asInstanceOf[StructType]("y").nullable)
  }

  test("alignSchemaWithCatalog reattaches custom-type metadata regardless of alignNullability") {
    val vectorMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(4, FLOAT)")
      .build()
    val sourceSchema = new StructType()
      .add("embedding", ArrayType(FloatType, containsNull = false), nullable = true)
    val targetSchema = new StructType()
      .add("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vectorMetadata)

    Seq(true, false).foreach { flag =>
      val aligned = HoodieSchemaConversionUtils.alignSchemaWithCatalog(
        sourceSchema, targetSchema, caseSensitive = false, alignNullability = flag)
      assert(aligned("embedding").metadata.contains(HoodieSchema.TYPE_METADATA_FIELD),
        s"metadata missing when alignNullability=$flag")
      assertEquals("VECTOR(4, FLOAT)",
        aligned("embedding").metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
    }
  }

  test("BLOB structure validator accepts mixed-case field names under case-insensitive analysis") {
    // Matches RFC-100 BLOB layout (type, data, reference) but with mixed-case field names.
    // Under Spark's default case-insensitive analysis, this should validate successfully.
    val referenceStruct = new StructType()
      .add("External_Path", StringType, nullable = true)
      .add("OFFSET", LongType, nullable = true)
      .add("Length", LongType, nullable = true)
      .add("managed", BooleanType, nullable = true)
    val mixedCaseBlob = new StructType()
      .add("TYPE", StringType, nullable = true)
      .add("Data", BinaryType, nullable = true)
      .add("REFERENCE", referenceStruct, nullable = true)
    val blobMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchema.Blob.TYPE_DESCRIPTOR)
      .build()
    val outerStruct = new StructType()
      .add("id", LongType, nullable = false)
      .add("payload", mixedCaseBlob, nullable = true, blobMetadata)

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
      outerStruct, "MixedCaseBlobTest", "test")
    val payload = hoodieSchema.getField("payload").get().schema().getNonNullType
    assertEquals(HoodieSchemaType.BLOB, payload.getType)
  }

  test("BLOB structure validator rejects wrong field ordering even under case-insensitive analysis") {
    // Correct field names but reversed ordering - matchesStructure is positional and must reject.
    val referenceStruct = new StructType()
      .add("external_path", StringType, nullable = true)
      .add("offset", LongType, nullable = true)
      .add("length", LongType, nullable = true)
      .add("managed", BooleanType, nullable = true)
    val wrongOrderBlob = new StructType()
      .add("reference", referenceStruct, nullable = true)
      .add("data", BinaryType, nullable = true)
      .add("type", StringType, nullable = true)
    val blobMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchema.Blob.TYPE_DESCRIPTOR)
      .build()
    val outerStruct = new StructType()
      .add("id", LongType, nullable = false)
      .add("payload", wrongOrderBlob, nullable = true, blobMetadata)

    val ex = intercept[IllegalArgumentException] {
      HoodieSparkSchemaConverters.validateCustomTypeStructures(outerStruct)
    }
    assert(ex.getMessage.startsWith("Invalid blob schema structure"),
      s"unexpected exception: ${ex.getMessage}")
  }

  test("test VECTOR element type mismatch throws error") {
    // Metadata says DOUBLE, but Spark array element type is Float
    val mismatchMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(128, DOUBLE)")
      .build()
    val struct = new StructType()
      .add("embedding", ArrayType(FloatType, containsNull = false), nullable = false, mismatchMetadata)

    the[Exception] thrownBy {
      HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        struct, "MismatchTest", "test")
    } should have message (
      "VECTOR element type mismatch for field embedding: metadata requires DOUBLE, Spark array has FloatType")
  }

  private def internalRowCompare(expected: Any, actual: Any, schema: DataType): Unit = {
    schema match {
      case StructType(fields) =>
        val expectedRow = expected.asInstanceOf[InternalRow]
        val actualRow = actual.asInstanceOf[InternalRow]
        fields.zipWithIndex.foreach { case (field, i) =>
          internalRowCompare(expectedRow.get(i, field.dataType), actualRow.get(i, field.dataType), field.dataType)
        }
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
      case StringType =>
        if (checkNull(expected, actual) || !expected.toString.equals(actual.toString)) {
          throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
        }
      case BinaryType =>
        if (checkNull(expected, actual) || !expected.asInstanceOf[Array[Byte]].sameElements(actual.asInstanceOf[Array[Byte]])) {
          throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
        }
      case _ =>
        if (!Objects.equals(expected, actual)) {
          throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
        }
    }
  }

  private def checkNull(left: Any, right: Any): Boolean = {
    (left == null && right != null) || (left != null && right == null)
  }
}
