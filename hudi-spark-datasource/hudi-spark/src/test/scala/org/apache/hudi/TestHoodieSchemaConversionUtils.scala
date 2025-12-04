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

import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

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

  test("test HoodieSchema to Spark conversion for all primitive types") {
    // Create HoodieSchema with all primitive types
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("bool", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
      HoodieSchemaField.of("int", HoodieSchema.create(HoodieSchemaType.INT)),
      HoodieSchemaField.of("long", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("float", HoodieSchema.create(HoodieSchemaType.FLOAT)),
      HoodieSchemaField.of("double", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
      HoodieSchemaField.of("string", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("bytes", HoodieSchema.create(HoodieSchemaType.BYTES)),
      HoodieSchemaField.of("null", HoodieSchema.create(HoodieSchemaType.NULL))
    )
    val hoodieSchema = HoodieSchema.createRecord("AllPrimitives", "test", null, fields)

    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    assert(structType.fields.length == 8)
    assert(structType.fields(0).dataType == BooleanType)
    assert(structType.fields(1).dataType == IntegerType)
    assert(structType.fields(2).dataType == LongType)
    assert(structType.fields(3).dataType == FloatType)
    assert(structType.fields(4).dataType == DoubleType)
    assert(structType.fields(5).dataType == StringType)
    assert(structType.fields(6).dataType == BinaryType)
    assert(structType.fields(7).dataType == NullType)
    assert(structType.fields(7).nullable) // Null type is always nullable
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
    assert(convertedStruct.fields(3).dataType == DecimalType(10, 2))
    assert(convertedStruct.fields(4).dataType == DecimalType(20, 5))
  }

  test("test HoodieSchema to Spark conversion for logical types") {
    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("date", HoodieSchema.createDate()),
      HoodieSchemaField.of("timestamp_micros", HoodieSchema.createTimestampMicros()),
      HoodieSchemaField.of("timestamp_ntz", HoodieSchema.createLocalTimestampMicros()),
      HoodieSchemaField.of("decimal", HoodieSchema.createDecimal(15, 3))
    )
    val hoodieSchema = HoodieSchema.createRecord("LogicalTypes", "test", null, fields)

    val structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(hoodieSchema)

    assert(structType.fields.length == 4)
    assert(structType.fields(0).dataType == DateType)
    assert(structType.fields(1).dataType == TimestampType)
    assert(structType.fields(3).dataType == DecimalType(15, 3))
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
    assert(dateSchema.getType == HoodieSchemaType.DATE)

    val timestampSchema = HoodieSchema.createTimestampMicros()
    assert(timestampSchema.getType == HoodieSchemaType.TIMESTAMP)

    val decimalSchema = HoodieSchema.createDecimal(10, 2)
    assert(decimalSchema.getType == HoodieSchemaType.DECIMAL)

    // Verify conversion to Spark types
    val dateType = HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(dateSchema)
    assert(dateType == DateType)

    val timestampType = HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(timestampSchema)
    assert(timestampType == TimestampType)

    val decimalType = HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(decimalSchema)
    assert(decimalType == DecimalType(10, 2))
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
}
