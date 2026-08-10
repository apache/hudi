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

package org.apache.spark.sql.adapter

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}

import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type.Repetition
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

/**
 * Tests for the variant-related methods in [[BaseSpark4Adapter]].
 * These tests cover the Spark 4.x implementations that handle VariantType.
 */
class TestBaseSpark4AdapterVariantMethods extends SparkAdapterSupport {

  @Test
  def testGetVariantDataTypeReturnsSome(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    assertTrue(sparkAdapter.getVariantDataType.isDefined,
      "getVariantDataType should return Some on Spark 4.x")
  }

  @Test
  def testIsVariantTypeReturnsTrueForVariantType(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get
    assertTrue(sparkAdapter.isVariantType(variantType),
      "isVariantType should return true for VariantType on Spark 4.x")
  }

  @Test
  def testIsVariantTypeReturnsFalseForOtherTypes(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    assertFalse(sparkAdapter.isVariantType(StringType),
      "isVariantType should return false for StringType")
    assertFalse(sparkAdapter.isVariantType(IntegerType),
      "isVariantType should return false for IntegerType")
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaVariantAndValidStruct(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get

    // Valid physical schema: struct with "value" (BinaryType) and "metadata" (BinaryType)
    val validStruct = StructType(Seq(
      StructField("value", BinaryType),
      StructField("metadata", BinaryType)
    ))

    // VariantType as requiredType, StructType as fileType
    val result1 = sparkAdapter.isDataTypeEqualForPhysicalSchema(variantType, validStruct)
    assertTrue(result1.isDefined && result1.get, "Should return Some(true) for VariantType vs valid physical struct")

    // StructType as requiredType, VariantType as fileType
    val result2 = sparkAdapter.isDataTypeEqualForPhysicalSchema(validStruct, variantType)
    assertTrue(result2.isDefined && result2.get, "Should return Some(true) for valid physical struct vs VariantType")
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaStructWithWrongFieldCount(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get

    // Struct with 3 fields - should fail isVariantPhysicalSchema (line 212-213)
    val threeFieldStruct = StructType(Seq(
      StructField("value", BinaryType),
      StructField("metadata", BinaryType),
      StructField("extra", StringType)
    ))

    // The guard `isVariantPhysicalSchema(s)` fails, so the match falls through to None
    val result = sparkAdapter.isDataTypeEqualForPhysicalSchema(variantType, threeFieldStruct)
    assertTrue(result.isEmpty, "Should return None for struct with wrong field count")
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaStructWithWrongFieldNames(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get

    // Struct with 2 fields but wrong names
    val wrongNameStruct = StructType(Seq(
      StructField("val", BinaryType),
      StructField("meta", BinaryType)
    ))

    val result = sparkAdapter.isDataTypeEqualForPhysicalSchema(variantType, wrongNameStruct)
    assertTrue(result.isEmpty, "Should return None for struct with wrong field names")
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaStructWithWrongFieldTypes(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get

    // Struct with correct names but wrong types
    val wrongTypeStruct = StructType(Seq(
      StructField("value", StringType),
      StructField("metadata", BinaryType)
    ))

    val result = sparkAdapter.isDataTypeEqualForPhysicalSchema(variantType, wrongTypeStruct)
    assertTrue(result.isEmpty, "Should return None for struct with wrong field types")
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaNoVariantType(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    // Neither type is VariantType - should return None
    val result = sparkAdapter.isDataTypeEqualForPhysicalSchema(StringType, IntegerType)
    assertTrue(result.isEmpty, "Should return None when neither type is VariantType")
  }

  @Test
  def testCreateVariantValueWriterWithNonVariantTypeThrows(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    // Passing a non-VariantType should throw IllegalArgumentException (line 240-241)
    assertThrows(classOf[IllegalArgumentException], () => {
      sparkAdapter.createVariantValueWriter(StringType, _ => (), _ => ())
    })
  }

  @Test
  def testConvertVariantFieldToParquetTypeWithNonVariantTypeThrows(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val schema = HoodieSchema.create(HoodieSchemaType.BYTES)
    // Passing a non-VariantType should throw IllegalArgumentException (line 257-258)
    assertThrows(classOf[IllegalArgumentException], () => {
      sparkAdapter.convertVariantFieldToParquetType(StringType, "test_field", schema, Repetition.REQUIRED)
    })
  }

  @Test
  def testConvertVariantFieldToParquetTypeUnshredded(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get
    val variantSchema = HoodieSchema.createVariant()

    val parquetType = sparkAdapter.convertVariantFieldToParquetType(
      variantType, "variant_col", variantSchema, Repetition.OPTIONAL)

    // Should be a group type with the given name
    assertEquals("variant_col", parquetType.getName)
    assertEquals(Repetition.OPTIONAL, parquetType.getRepetition)
    assertTrue(parquetType.isInstanceOf[org.apache.parquet.schema.GroupType])

    val groupType = parquetType.asGroupType()
    assertEquals(2, groupType.getFieldCount)

    // Value field should be REQUIRED for unshredded
    val valueField = groupType.getType("value")
    assertEquals(Repetition.REQUIRED, valueField.getRepetition)
    assertTrue(valueField.isPrimitive)
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
      valueField.asPrimitiveType().getPrimitiveTypeName)

    // Metadata field should be REQUIRED
    val metadataField = groupType.getType("metadata")
    assertEquals(Repetition.REQUIRED, metadataField.getRepetition)
    assertTrue(metadataField.isPrimitive)
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
      metadataField.asPrimitiveType().getPrimitiveTypeName)
  }

  @Test
  def testConvertVariantFieldToParquetTypeShredded(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get
    // Create a shredded variant schema (line 263 - the HoodieSchema.Variant match case)
    val shreddedSchema = HoodieSchema.createVariantShredded(null)

    val parquetType = sparkAdapter.convertVariantFieldToParquetType(
      variantType, "shredded_col", shreddedSchema, Repetition.REQUIRED)

    assertEquals("shredded_col", parquetType.getName)
    assertEquals(Repetition.REQUIRED, parquetType.getRepetition)

    val groupType = parquetType.asGroupType()
    assertEquals(2, groupType.getFieldCount)

    // Value field should be OPTIONAL for shredded
    val valueField = groupType.getType("value")
    assertEquals(Repetition.OPTIONAL, valueField.getRepetition)
  }

  @Test
  def testConvertVariantFieldToParquetTypeWithNonVariantSchema(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get
    // Pass a non-Variant HoodieSchema - should fall through to `case _ => false` (line 264)
    val bytesSchema = HoodieSchema.create(HoodieSchemaType.BYTES)

    val parquetType = sparkAdapter.convertVariantFieldToParquetType(
      variantType, "test_field", bytesSchema, Repetition.REQUIRED)

    val groupType = parquetType.asGroupType()
    // Value field should be REQUIRED when isShredded is false (non-Variant schema)
    val valueField = groupType.getType("value")
    assertEquals(Repetition.REQUIRED, valueField.getRepetition)
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaStructMissingMetadataField(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get
    // Has "value" but not "metadata" — covers line 217 false branch
    val struct = StructType(Seq(
      StructField("value", BinaryType),
      StructField("other", BinaryType)
    ))
    val result = sparkAdapter.isDataTypeEqualForPhysicalSchema(variantType, struct)
    assertTrue(result.isEmpty, "Should return None when metadata field is missing")
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaStructWithWrongMetadataType(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get
    // Both names correct, value is BinaryType, but metadata is StringType — covers line 219 false branch
    val struct = StructType(Seq(
      StructField("value", BinaryType),
      StructField("metadata", StringType)
    ))
    val result = sparkAdapter.isDataTypeEqualForPhysicalSchema(variantType, struct)
    assertTrue(result.isEmpty, "Should return None when metadata field has wrong type")
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaReversedWithInvalidStruct(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark4, "Only applies to Spark 4.x")
    val variantType = sparkAdapter.getVariantDataType.get
    // Invalid struct as requiredType, VariantType as fileType — covers line 226 guard-false
    val invalidStruct = StructType(Seq(
      StructField("val", BinaryType),
      StructField("meta", BinaryType)
    ))
    val result = sparkAdapter.isDataTypeEqualForPhysicalSchema(invalidStruct, variantType)
    assertTrue(result.isEmpty, "Should return None when reversed struct doesn't match variant physical schema")
  }
}
