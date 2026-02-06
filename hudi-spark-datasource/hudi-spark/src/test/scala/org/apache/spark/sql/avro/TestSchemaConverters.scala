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

import org.apache.hudi.avro.model.HoodieMetadataColumnStats
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}

import org.apache.avro.JsonProperties
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.util

class TestSchemaConverters {

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
  def testSchemaWithBlobsRoundtrip(): Unit = {
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
  def testBlobArrayRoundtrip(): Unit = {
    // Test array containing blobs at various nesting levels
    val innerArray = HoodieSchema.createArray(HoodieSchema.createBlob())
    val outerArray = HoodieSchema.createArray(innerArray)

    val fields = util.Arrays.asList(
      HoodieSchemaField.of("simple_blobs", HoodieSchema.createArray(HoodieSchema.createBlob())),
      HoodieSchemaField.of("nested_blobs", outerArray)
    )
    val originalSchema = HoodieSchema.createRecord("BlobArrays", "test", null, fields)

    // Roundtrip
    val (sparkType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    val reconstructed = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "BlobArrays", nameSpace = "test")

    // Verify simple array
    val simpleField = reconstructed.getField("simple_blobs").get()
    assertEquals(HoodieSchemaType.ARRAY, simpleField.schema().getType)
    assertEquals(HoodieSchemaType.BLOB, simpleField.schema().getElementType.getType)

    // Verify nested array
    val nestedField = reconstructed.getField("nested_blobs").get()
    assertEquals(HoodieSchemaType.ARRAY, nestedField.schema().getType)
    val nestedArrayType = nestedField.schema().getElementType
    assertEquals(HoodieSchemaType.ARRAY, nestedArrayType.getType)
    assertEquals(HoodieSchemaType.BLOB, nestedArrayType.getElementType.getType)
  }

  @Test
  def testBlobMapRoundtrip(): Unit = {
    // Test map containing blobs at various nesting levels
    val innerMap = HoodieSchema.createMap(HoodieSchema.createBlob())
    val outerMap = HoodieSchema.createMap(innerMap)

    val fields = util.Arrays.asList(
      HoodieSchemaField.of("simple_blobs_map", HoodieSchema.createMap(HoodieSchema.createBlob())),
      HoodieSchemaField.of("nested_blobs_map", outerMap)
    )
    val originalSchema = HoodieSchema.createRecord("BlobMaps", "test", null, fields)

    // Roundtrip
    val (sparkType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    val reconstructed = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "BlobMaps")

    // Verify simple map
    val simpleField = reconstructed.getField("simple_blobs_map").get()
    assertEquals(HoodieSchemaType.MAP, simpleField.schema().getType)
    assertEquals(HoodieSchemaType.BLOB, simpleField.schema().getValueType.getType)

    // Verify nested map
    val nestedField = reconstructed.getField("nested_blobs_map").get()
    assertEquals(HoodieSchemaType.MAP, nestedField.schema().getType)
    val nestedMapType = nestedField.schema().getValueType
    assertEquals(HoodieSchemaType.MAP, nestedMapType.getType)
    assertEquals(HoodieSchemaType.BLOB, nestedMapType.getValueType.getType)
  }

  /**
   * Validates the content of the blob fields to ensure the fields match our expectations.
   * @param dataType the StructType containing the blob fields to validate
   */
  private def validateBlobFields(dataType: StructType): Unit = {
    // storage_type is a non-null string field
    val storageTypeField = dataType.fields.find(_.name == HoodieSchema.Blob.STORAGE_TYPE).get
    assertEquals(DataTypes.StringType, storageTypeField.dataType)
    assertFalse(storageTypeField.nullable)
    // data is a nullable binary field
    val dataField = dataType.fields.find(_.name == HoodieSchema.Blob.INLINE_DATA_FIELD).get
    assertEquals(DataTypes.BinaryType, dataField.dataType)
    assertTrue(dataField.nullable)
    // reference is a nullable struct field
    val referenceField = dataType.fields.find(_.name == HoodieSchema.Blob.EXTERNAL_FILE_REFERENCE).get
    assertEquals(new StructType(Array[StructField](
      StructField(HoodieSchema.Blob.EXTERNAL_PATH, DataTypes.StringType, nullable = false),
      StructField(HoodieSchema.Blob.EXTERNAL_PATH_OFFSET, DataTypes.LongType, nullable = true),
      StructField(HoodieSchema.Blob.EXTERNAL_PATH_LENGTH, DataTypes.LongType, nullable = true),
      StructField(HoodieSchema.Blob.EXTERNAL_PATH_IS_MANAGED, DataTypes.BooleanType, nullable = false)
    )), referenceField.dataType)
    assertTrue(referenceField.nullable)
  }
}
