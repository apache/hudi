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
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
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

    // Hudi -> Spark -> Hudi
    val (sparkType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    // validate the metadata is set on the blob fields and nullability is preserved
    val metadataSparkField: StructType = sparkType.asInstanceOf[StructType].fields.find(_.name == "metadata").get.asInstanceOf[StructType]
    val thumbNailSparkField = metadataSparkField.fields.find(_.name == "thumbnail").get
    assertEquals(HoodieSchemaType.BLOB.name(), thumbNailSparkField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertTrue(thumbNailSparkField.nullable)
    val imageSparkField = metadataSparkField.fields.find(_.name == "image").get
    assertEquals(HoodieSchemaType.BLOB.name(), imageSparkField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertTrue(!imageSparkField.nullable)

    val reconstructed = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "document")

    // Deep comparison
    assertEquals(reconstructed.toAvroSchema().toString(), originalSchema.toAvroSchema().toString())

    // Verify the blob type and nullability are preserved in the reconstructed schema
    val metadataField = reconstructed.getField("metadata").get()
    val thumbnailField = metadataField.schema().getField("thumbnail").get()
    assert(thumbnailField.schema().isNullable())
    assertEquals(HoodieSchemaType.BLOB, thumbnailField.schema().getNonNullType().getType)
    val imageField = metadataField.schema().getField("image").get()
    assert(!imageField.schema().isNullable())
    assertEquals(HoodieSchemaType.BLOB, imageField.schema().getType)
  }

  @Test
  def testBlobArrayRoundtrip(): Unit = {
    // Test array containing blobs at various nesting levels
    val innerArray = HoodieSchema.createArray(HoodieSchema.createBlob())
    val outerArray = HoodieSchema.createArray(innerArray)

    val fields = java.util.Arrays.asList(
      HoodieSchemaField.of("simple_blobs", HoodieSchema.createArray(HoodieSchema.createBlob())),
      HoodieSchemaField.of("nested_blobs", outerArray)
    )
    val originalSchema = HoodieSchema.createRecord("BlobArrays", "test", null, fields)

    // Roundtrip
    val (sparkType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    val reconstructed = HoodieSparkSchemaConverters.toHoodieType(sparkType, recordName = "BlobArrays")

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

    // Verify full equivalence
    assertEquals(originalSchema.toAvroSchema().toString(), reconstructed.toAvroSchema().toString())
  }
}
