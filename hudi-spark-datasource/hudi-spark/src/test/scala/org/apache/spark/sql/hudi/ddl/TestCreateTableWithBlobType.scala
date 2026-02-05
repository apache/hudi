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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

class TestCreateTableWithBlobType extends HoodieSparkSqlTestBase {

  test("test create table with BLOB column") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id BIGINT,
           |  video BLOB
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id'
           |)
           """.stripMargin)

      // Verify schema has hudi_blob metadata
      val schema = spark.table(tableName).schema
      val videoField = schema.find(_.name == "video").get
      assertTrue(videoField.metadata.contains(HoodieSchema.Blob.HUDI_BLOB))
      assertTrue(videoField.metadata.getBoolean(HoodieSchema.Blob.HUDI_BLOB))

      // Verify structure matches blob schema
      assertTrue(videoField.dataType.isInstanceOf[StructType])
      val blobStruct = videoField.dataType.asInstanceOf[StructType]
      assertEquals(Seq("storage_type", "bytes", "reference"), blobStruct.fieldNames.toSeq)

      // Verify field types
      assertEquals(StringType, blobStruct("storage_type").dataType)
      assertEquals(BinaryType, blobStruct("bytes").dataType)
      assertTrue(blobStruct("reference").dataType.isInstanceOf[StructType])

      val refStruct = blobStruct("reference").dataType.asInstanceOf[StructType]
      assertEquals(Seq("file", "position", "length", "managed"), refStruct.fieldNames.toSeq)
      assertEquals(StringType, refStruct("file").dataType)
      assertEquals(LongType, refStruct("position").dataType)
      assertEquals(LongType, refStruct("length").dataType)
      assertEquals(BooleanType, refStruct("managed").dataType)
    }
  }

  test("test create table with multiple BLOB columns") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id BIGINT,
           |  video BLOB,
           |  thumbnail BLOB,
           |  audio BLOB
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id'
           |)
           """.stripMargin)

      val schema = spark.table(tableName).schema

      // Verify all BLOB columns have the metadata
      val blobColumns = Seq("video", "thumbnail", "audio")
      blobColumns.foreach { colName =>
        val field = schema.find(_.name == colName).get
        assertTrue(field.metadata.contains(HoodieSchema.Blob.HUDI_BLOB))
        assertTrue(field.metadata.getBoolean(HoodieSchema.Blob.HUDI_BLOB))
        assertTrue(field.dataType.isInstanceOf[StructType])

        val blobStruct = field.dataType.asInstanceOf[StructType]
        assertEquals(Seq("storage_type", "bytes", "reference"), blobStruct.fieldNames.toSeq)
      }
    }
  }

  test("test create table with BLOB and comment") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id BIGINT,
           |  video BLOB COMMENT 'Product demonstration video'
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id'
           |)
           """.stripMargin)

      val schema = spark.table(tableName).schema
      val videoField = schema.find(_.name == "video").get

      // Verify both comment and hudi_blob metadata are preserved
      assertTrue(videoField.metadata.contains("comment"))
      assertEquals("Product demonstration video", videoField.metadata.getString("comment"))
      assertTrue(videoField.metadata.contains(HoodieSchema.Blob.HUDI_BLOB))
      assertTrue(videoField.metadata.getBoolean(HoodieSchema.Blob.HUDI_BLOB))
    }
  }

  test("test create table with mixed column types including BLOB") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id BIGINT,
           |  product_name STRING,
           |  price DOUBLE,
           |  created_at TIMESTAMP,
           |  video BLOB,
           |  metadata MAP<STRING, STRING>,
           |  tags ARRAY<STRING>
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id'
           |)
           """.stripMargin)

      val schema = spark.table(tableName).schema

      // Verify primitive types
      assertEquals(LongType, schema.find(_.name == "id").get.dataType)
      assertEquals(StringType, schema.find(_.name == "product_name").get.dataType)
      assertEquals(DoubleType, schema.find(_.name == "price").get.dataType)

      // Verify complex types
      assertTrue(schema.find(_.name == "metadata").get.dataType.isInstanceOf[MapType])
      assertTrue(schema.find(_.name == "tags").get.dataType.isInstanceOf[ArrayType])

      // Verify BLOB column has metadata but others don't
      val videoField = schema.find(_.name == "video").get
      assertTrue(videoField.metadata.contains(HoodieSchema.Blob.HUDI_BLOB))

      val nameField = schema.find(_.name == "product_name").get
      assertFalse(nameField.metadata.contains(HoodieSchema.Blob.HUDI_BLOB))
    }
  }

  test("test BLOB type case insensitivity") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Test with uppercase BLOB
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id BIGINT,
           |  video BLOB,
           |  image blob
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id'
           |)
           """.stripMargin)

      val schema = spark.table(tableName).schema
      val videoField = schema.find(_.name == "video").get
      val imageField = schema.find(_.name == "image").get

      // Both should be recognized as BLOBs
      assertTrue(videoField.metadata.getBoolean(HoodieSchema.Blob.HUDI_BLOB))
      assertTrue(imageField.metadata.getBoolean(HoodieSchema.Blob.HUDI_BLOB))
    }
  }

  test("test BLOB column nullability") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id BIGINT,
           |  required_video BLOB NOT NULL,
           |  optional_video BLOB
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id'
           |)
           """.stripMargin)

      val schema = spark.table(tableName).schema
      val requiredField = schema.find(_.name == "required_video").get
      val optionalField = schema.find(_.name == "optional_video").get

      // Verify nullability
      assertFalse(requiredField.nullable)
      assertTrue(optionalField.nullable)

      // Both should still be BLOB types
      assertTrue(requiredField.metadata.getBoolean(HoodieSchema.Blob.HUDI_BLOB))
      assertTrue(optionalField.metadata.getBoolean(HoodieSchema.Blob.HUDI_BLOB))
    }
  }

  test("test BLOB in nested struct") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id BIGINT,
           |  media STRUCT<title: STRING, content: BLOB>
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id'
           |)
           """.stripMargin)

      val schema = spark.table(tableName).schema
      val mediaField = schema.find(_.name == "media").get
      assertTrue(mediaField.dataType.isInstanceOf[StructType])

      val mediaStruct = mediaField.dataType.asInstanceOf[StructType]
      val contentField = mediaStruct.find(_.name == "content").get

      // Verify nested BLOB has metadata
      assertTrue(contentField.metadata.contains(HoodieSchema.Blob.HUDI_BLOB))
      assertTrue(contentField.metadata.getBoolean(HoodieSchema.Blob.HUDI_BLOB))

      // Verify structure
      assertTrue(contentField.dataType.isInstanceOf[StructType])
      val blobStruct = contentField.dataType.asInstanceOf[StructType]
      assertEquals(Seq("storage_type", "bytes", "reference"), blobStruct.fieldNames.toSeq)
    }
  }
}
