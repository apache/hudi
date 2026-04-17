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

package org.apache.spark.sql.avro

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.avro.model.HoodieMetadataColumnStats
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}
import org.apache.hudi.internal.schema.HoodieSchemaException

import org.apache.avro.JsonProperties
import org.apache.spark.sql.types.{ArrayType, DataTypes, FloatType, MetadataBuilder, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

import java.util

class TestSchemaConverters extends SparkAdapterSupport {

  @Test
  def testAvroUnionConversion(): Unit = {
    val originalSchema = HoodieSchema.fromAvroSchema(HoodieMetadataColumnStats.SCHEMA$)

    val (convertedStructType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    val convertedSchema = HoodieSparkSchemaConverters.toHoodieType(convertedStructType)

    // NOTE: Here we're validating that converting Avro -> Catalyst and Catalyst -> Avro are inverse
    //       transformations, but since it's not an easy endeavor to match Avro schemas, we match
    //       derived Catalyst schemas instead
    assertEquals(convertedStructType, HoodieSparkSchemaConverters.toSqlType(convertedSchema)._1)
    // validate that the doc string and default null value are set
    originalSchema.getFields.forEach { field =>
      val convertedField = convertedSchema.getField(field.name()).get()
      assertEquals(field.doc(), convertedField.doc())
      if (field.schema().isNullable) {
        assertEquals(JsonProperties.NULL_VALUE, convertedField.defaultVal().get())
      }
    }
  }

  @Test
  def testSchemaWithBlobsRoundTrip(): Unit = {
    val originalSchema = HoodieSchema.createRecord("document", "test", null, util.Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("metadata", HoodieSchema.createRecord("meta", null, null, util.Arrays.asList(
        HoodieSchemaField.of("image", HoodieSchema.createBlob()),
        HoodieSchemaField.of("thumbnail", HoodieSchema.createNullable(HoodieSchema.createBlob()))
      )))))

    // Hudi -> Spark
    val sparkType = HoodieSparkSchemaConverters.toSqlType(originalSchema)._1
    // validate the metadata is set on the blob fields and nullability is preserved
    val metadataSparkField = sparkType.asInstanceOf[StructType].fields.find(_.name == "metadata").get.dataType.asInstanceOf[StructType]
    val thumbNailSparkField = metadataSparkField.fields.find(_.name == "thumbnail").get
    assertEquals(HoodieSchemaType.BLOB.name(), thumbNailSparkField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertTrue(thumbNailSparkField.nullable)
    validateBlobFields(thumbNailSparkField.dataType.asInstanceOf[StructType])
    val imageSparkField = metadataSparkField.fields.find(_.name == "image").get
    assertEquals(HoodieSchemaType.BLOB.name(), imageSparkField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertFalse(imageSparkField.nullable)
    validateBlobFields(imageSparkField.dataType.asInstanceOf[StructType])

    // Spark -> Hudi
    val reconstructed = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "document", nameSpace = "test")
    // Verify the blob type and nullability are preserved in the reconstructed schema
    assertTrue(reconstructed.getField("id").isPresent)
    val metadataField = reconstructed.getField("metadata").get()
    val thumbnailField = metadataField.schema().getField("thumbnail").get()
    assertTrue(thumbnailField.schema().isNullable)
    assertEquals(HoodieSchemaType.BLOB, thumbnailField.schema().getNonNullType.getType)
    val imageField = metadataField.schema().getField("image").get()
    assertFalse(imageField.schema().isNullable)
    assertEquals(HoodieSchemaType.BLOB, imageField.schema().getType)
  }

  @Test
  def testInvalidBlobSchema(): Unit = {
    // Struct with only 2 fields marked as blob
    val invalidStruct = new StructType(Array[StructField](
      StructField(HoodieSchema.Blob.TYPE, DataTypes.StringType, nullable = false),
      StructField(HoodieSchema.Blob.INLINE_DATA_FIELD, DataTypes.BinaryType, nullable = true)
    ))

    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchemaType.BLOB.name())
      .build()

    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      HoodieSparkSchemaConverters.toHoodieType(invalidStruct, nullable = false, metadata = metadata)
    })
    assertTrue(exception.getMessage.startsWith("Invalid blob schema structure"))
  }

  @Test
  def testBlobArrayRoundtrip(): Unit = {
    // Test array containing blobs at various nesting levels
    val innerSchema = HoodieSchema.createRecord("nested", null, null, util.Arrays.asList(
      HoodieSchemaField.of("nested_long", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("nested_blob", HoodieSchema.createBlob())))
    val outerArray = HoodieSchema.createArray(innerSchema)

    val fields = util.Arrays.asList(
      HoodieSchemaField.of("simple_blobs", HoodieSchema.createArray(HoodieSchema.createBlob())),
      HoodieSchemaField.of("simple_nullable_blobs", HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchema.createBlob()))),
      HoodieSchemaField.of("nested_blobs", outerArray)
    )
    val originalSchema = HoodieSchema.createRecord("BlobArrays", "test", null, fields)

    // Roundtrip
    val (sparkType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    val reconstructed = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "BlobArrays", nameSpace = "test")

    // Verify simple array
    val simpleField = reconstructed.getField("simple_blobs").get()
    assertEquals(HoodieSchemaType.ARRAY, simpleField.schema.getType)
    assertFalse(simpleField.schema.getElementType.isNullable)
    assertEquals(HoodieSchemaType.BLOB, simpleField.schema.getElementType.getType)

    // Verify simple nullable array
    val nullableField = reconstructed.getField("simple_nullable_blobs").get()
    assertEquals(HoodieSchemaType.ARRAY, nullableField.schema.getType)
    assertTrue(nullableField.schema.getElementType.isNullable)
    assertEquals(HoodieSchemaType.BLOB, nullableField.schema.getElementType.getNonNullType.getType)

    // Verify nested array
    val nestedField = reconstructed.getField("nested_blobs").get()
    assertEquals(HoodieSchemaType.ARRAY, nestedField.schema.getType)
    val nestedArrayType = nestedField.schema.getElementType
    assertEquals(HoodieSchemaType.RECORD, nestedArrayType.getType)
    assertEquals(HoodieSchemaType.BLOB, nestedArrayType.getField("nested_blob").get.schema.getType)
  }

  @Test
  def testBlobMapRoundtrip(): Unit = {
    // Test map containing blobs at various nesting levels
    val innerSchema = HoodieSchema.createRecord("nested", null, null, util.Arrays.asList(
      HoodieSchemaField.of("nested_long", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("nested_blob", HoodieSchema.createBlob())))
    val outerMap = HoodieSchema.createMap(innerSchema)

    val fields = util.Arrays.asList(
      HoodieSchemaField.of("simple_blobs_map", HoodieSchema.createMap(HoodieSchema.createBlob())),
      HoodieSchemaField.of("simple_nullable_blobs_map", HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchema.createBlob()))),
      HoodieSchemaField.of("nested_blobs_map", outerMap)
    )
    val originalSchema = HoodieSchema.createRecord("BlobMaps", "test", null, fields)

    // Roundtrip
    val (sparkType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    val reconstructed = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "BlobMaps")

    // Verify simple map
    val simpleField = reconstructed.getField("simple_blobs_map").get()
    assertEquals(HoodieSchemaType.MAP, simpleField.schema.getType)
    assertFalse(simpleField.schema.getValueType.isNullable)
    assertEquals(HoodieSchemaType.BLOB, simpleField.schema.getValueType.getNonNullType.getType)

    // Verify simple nullable map
    val nullableField = reconstructed.getField("simple_nullable_blobs_map").get()
    assertEquals(HoodieSchemaType.MAP, nullableField.schema.getType)
    assertTrue(nullableField.schema.getValueType.isNullable)
    assertEquals(HoodieSchemaType.BLOB, nullableField.schema.getValueType.getNonNullType.getType)

    // Verify nested map
    val nestedField = reconstructed.getField("nested_blobs_map").get()
    assertEquals(HoodieSchemaType.MAP, nestedField.schema.getType)
    val nestedMapType = nestedField.schema.getValueType
    assertEquals(HoodieSchemaType.RECORD, nestedMapType.getType)
    assertEquals(HoodieSchemaType.BLOB, nestedMapType.getField("nested_blob").get.schema.getType)
  }

  @Test
  def testVectorContainsNullThrows(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(4)")
      .build()
    val sparkType = ArrayType(FloatType, containsNull = true)
    assertThrows(classOf[HoodieSchemaException], () => {
      HoodieSparkSchemaConverters.toHoodieType(sparkType, nullable = false, metadata = metadata)
    })
  }

  @Test
  def testVariantToSqlTypeThrowsOnSpark3(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark3, "Only applies to Spark 3.x")
    val variantSchema = HoodieSchema.createVariant()
    val exception = assertThrows(classOf[IncompatibleSchemaException], () => {
      HoodieSparkSchemaConverters.toSqlType(variantSchema)
    })
    assertTrue(exception.getMessage.contains("VARIANT type is only supported in Spark 4.0+"))
  }

  @Test
  def testVariantToSqlTypeOnSpark4(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_0, "Only applies to Spark 4.x+")
    val variantSchema = HoodieSchema.createVariant()
    val (dataType, nullable) = HoodieSparkSchemaConverters.toSqlType(variantSchema)
    assertEquals(sparkAdapter.getVariantDataType.get, dataType)
    assertFalse(nullable)
  }

  @Test
  def testVariantSparkToHoodieOnSpark4(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_0, "Only applies to Spark 4.x+")
    val variantDataType = sparkAdapter.getVariantDataType.get
    val hoodieSchema = HoodieSparkSchemaConverters.toHoodieType(variantDataType)
    assertEquals(HoodieSchemaType.VARIANT, hoodieSchema.getType)
  }

  @Test
  def testVariantInRecordRoundTripOnSpark4(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_0, "Only applies to Spark 4.x+")
    val originalSchema = HoodieSchema.createRecord("variantRecord", "test", null, util.Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("data", HoodieSchema.createVariant()),
      HoodieSchemaField.of("nullable_data", HoodieSchema.createNullable(HoodieSchema.createVariant()))
    ))

    // Hudi -> Spark
    val (sparkType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    val structType = sparkType.asInstanceOf[StructType]
    val expectedVariantType = sparkAdapter.getVariantDataType.get
    assertEquals(expectedVariantType, structType("data").dataType)
    assertFalse(structType("data").nullable)
    assertEquals(expectedVariantType, structType("nullable_data").dataType)
    assertTrue(structType("nullable_data").nullable)

    // Spark -> Hudi
    val reconstructed = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "variantRecord", nameSpace = "test")
    assertEquals(HoodieSchemaType.VARIANT, reconstructed.getField("data").get().schema().getType)
    val nullableField = reconstructed.getField("nullable_data").get()
    assertTrue(nullableField.schema().isNullable)
    assertEquals(HoodieSchemaType.VARIANT, nullableField.schema().getNonNullType.getType)
  }

  @Test
  def testInvalidVariantSchemaWrongFieldCount(): Unit = {
    val invalidStruct = new StructType(Array[StructField](
      StructField(HoodieSchema.Variant.VARIANT_METADATA_FIELD, DataTypes.BinaryType, nullable = false)
    ))
    assertInvalidVariantSchema(invalidStruct)
  }

  @Test
  def testInvalidVariantSchemaNullableMetadataField(): Unit = {
    val invalidStruct = new StructType(Array[StructField](
      StructField(HoodieSchema.Variant.VARIANT_METADATA_FIELD, DataTypes.BinaryType, nullable = true),
      StructField(HoodieSchema.Variant.VARIANT_VALUE_FIELD, DataTypes.BinaryType, nullable = false)
    ))
    assertInvalidVariantSchema(invalidStruct)
  }

  @Test
  def testInvalidVariantSchemaWrongValueFieldType(): Unit = {
    val invalidStruct = new StructType(Array[StructField](
      StructField(HoodieSchema.Variant.VARIANT_METADATA_FIELD, DataTypes.BinaryType, nullable = false),
      StructField(HoodieSchema.Variant.VARIANT_VALUE_FIELD, DataTypes.StringType, nullable = false)
    ))
    assertInvalidVariantSchema(invalidStruct)
  }

  @Test
  def testInvalidVariantSchemaWrongFieldNames(): Unit = {
    val invalidStruct = new StructType(Array[StructField](
      StructField("foo", DataTypes.BinaryType, nullable = false),
      StructField("bar", DataTypes.BinaryType, nullable = false)
    ))
    assertInvalidVariantSchema(invalidStruct)
  }

  private def assertInvalidVariantSchema(invalidStruct: StructType): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchemaType.VARIANT.name())
      .build()
    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      HoodieSparkSchemaConverters.toHoodieType(invalidStruct, metadata = metadata)
    })
    assertTrue(exception.getMessage.startsWith("Invalid variant schema structure"))
  }

  @Test
  def testTopLevelVectorStillAllowed(): Unit = {
    val vectorMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(4)")
      .build()
    val sparkType = new StructType(Array[StructField](
      StructField("id", DataTypes.LongType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata = vectorMetadata)
    ))
    val hoodieSchema = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "record")
    assertEquals(HoodieSchemaType.VECTOR, hoodieSchema.getField("embedding").get().schema().getType)
  }

  @Test
  def testVectorInNestedStructThrows(): Unit = {
    val vectorMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(4)")
      .build()
    // Outer struct has a nested struct whose field is a VECTOR
    val innerStruct = new StructType(Array[StructField](
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata = vectorMetadata)
    ))
    val outerStruct = new StructType(Array[StructField](
      StructField("id", DataTypes.LongType, nullable = false),
      StructField("data", innerStruct, nullable = false)
    ))
    val exception = assertThrows(classOf[HoodieSchemaException], () => {
      HoodieSparkSchemaConverters.toHoodieType(outerStruct, recordName = "record")
    })
    assertEquals("VECTOR column 'embedding' must be a top-level field. Nested VECTOR columns (inside STRUCT, ARRAY, or MAP) are not supported.", exception.getMessage)
  }

  @Test
  def testVectorInsideArrayOfStructsThrows(): Unit = {
    // VECTOR nested inside an array of structs: ARRAY<STRUCT<embedding VECTOR(4)>>
    val vectorMetadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(4)")
      .build()
    val innerStruct = new StructType(Array[StructField](
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata = vectorMetadata)
    ))
    val outerStruct = new StructType(Array[StructField](
      StructField("items", ArrayType(innerStruct, containsNull = false), nullable = false)
    ))
    val exception = assertThrows(classOf[HoodieSchemaException], () => {
      HoodieSparkSchemaConverters.toHoodieType(outerStruct, recordName = "record")
    })
    assertEquals("VECTOR column 'embedding' must be a top-level field. Nested VECTOR columns (inside STRUCT, ARRAY, or MAP) are not supported.", exception.getMessage)
  }

  /**
   * Validates the content of the blob fields to ensure the fields match our expectations.
   * @param dataType the StructType containing the blob fields to validate
   */
  private def validateBlobFields(dataType: StructType): Unit = {
    // storage_type is a non-null string field
    val storageTypeField = dataType.fields.find(_.name == HoodieSchema.Blob.TYPE).get
    assertEquals(DataTypes.StringType, storageTypeField.dataType)
    assertFalse(storageTypeField.nullable)
    // data is a nullable binary field
    val dataField = dataType.fields.find(_.name == HoodieSchema.Blob.INLINE_DATA_FIELD).get
    assertEquals(DataTypes.BinaryType, dataField.dataType)
    assertTrue(dataField.nullable)
    // reference is a nullable struct field
    val referenceField = dataType.fields.find(_.name == HoodieSchema.Blob.EXTERNAL_REFERENCE).get
    assertEquals(new StructType(Array[StructField](
      StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, DataTypes.StringType, nullable = false),
      StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, DataTypes.LongType, nullable = true),
      StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, DataTypes.LongType, nullable = true),
      StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, DataTypes.BooleanType, nullable = false)
    )), referenceField.dataType)
    assertTrue(referenceField.nullable)
  }
}
