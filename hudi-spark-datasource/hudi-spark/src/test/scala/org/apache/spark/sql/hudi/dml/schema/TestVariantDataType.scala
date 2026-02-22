/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.dml.schema

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.StringUtils

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{GroupType, MessageType, Type}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase


class TestVariantDataType extends HoodieSparkSqlTestBase {

  test(s"Test Table with Variant Data Type") {
    // Variant type is only supported in Spark 4.0+
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    Seq("cow", "mor").foreach { tableType =>
      withRecordType()(withTempDir { tmp =>
        val tableName = generateTableName
        // Create a table with a Variant column
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  v variant,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
         """.stripMargin)

        // Insert data with Variant values using parse_json (Spark 4.0+)
        spark.sql(
          s"""
             |insert into $tableName
             |values
             |  (1, 'row1', parse_json('{"key": "value1", "num": 1}'), 1000),
             |  (2, 'row2', parse_json('{"key": "value2", "list": [1, 2, 3]}'), 1000)
         """.stripMargin)

        // Verify the data by casting Variant to String for deterministic comparison
        checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
          Seq(1, "row1", "{\"key\":\"value1\",\"num\":1}", 1000),
          Seq(2, "row2", "{\"key\":\"value2\",\"list\":[1,2,3]}", 1000)
        )

        // Test Updates on Variant column, MOR will generate logs
        spark.sql(
          s"""
             |update $tableName
             |set v = parse_json('{"updated": true, "new_field": 123}')
             |where id = 1
         """.stripMargin)

        checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
          Seq(1, "row1", "{\"new_field\":123,\"updated\":true}", 1000),
          Seq(2, "row2", "{\"key\":\"value2\",\"list\":[1,2,3]}", 1000)
        )

        // Test Delete
        spark.sql(s"delete from $tableName where id = 2")

        checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
          Seq(1, "row1", "{\"new_field\":123,\"updated\":true}", 1000)
        )
      })
    }
  }

  test(s"Test Backward Compatibility: Read Spark 4.0 Variant Table in Spark 3.x") {
    // This test only runs on Spark 3.x to verify backward compatibility
    assume(HoodieSparkUtils.isSpark3, "This test verifies Spark 3.x can read Spark 4.0 Variant tables")

    withTempDir { tmpDir =>
      // Test COW table - record type does not affect file metadata for COW, only need one test
      HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_cow.zip", tmpDir.toPath, getClass)
      val cowPath = tmpDir.toPath.resolve("variant_cow").toString
      verifyVariantBackwardCompatibility(cowPath, "cow", "COW table")

      // Test MOR table with AVRO record type
      HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_mor_avro.zip", tmpDir.toPath, getClass)
      val morAvroPath = tmpDir.toPath.resolve("variant_mor_avro").toString
      verifyVariantBackwardCompatibility(morAvroPath, "mor", "MOR table with AVRO record type")

      // Test MOR table with SPARK record type
      HoodieTestUtils.extractZipToDirectory("variant_backward_compat/variant_mor_spark.zip", tmpDir.toPath, getClass)
      val morSparkPath = tmpDir.toPath.resolve("variant_mor_spark").toString
      verifyVariantBackwardCompatibility(morSparkPath, "mor", "MOR table with SPARK record type")
    }
  }

  /**
   * Helper method to verify backward compatibility of reading Spark 4.0 Variant tables in Spark 3.x
   */
  private def verifyVariantBackwardCompatibility(resourcePath: String, tableType: String, testDescription: String): Unit = {
    val tableName = generateTableName

    // Create a Hudi table pointing to the saved data location
    // In Spark 3.x, we define the Variant column as a struct with binary fields since Variant type is not available
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  v struct<value: binary, metadata: binary>,
         |  ts long
         |) using hudi
         |location '$resourcePath'
         |tblproperties (
         |  primaryKey = 'id',
         |  tableType = '$tableType',
         |  preCombineField = 'ts'
         |)
       """.stripMargin)

    // Verify we can read the basic columns
    checkAnswer(s"select id, name, ts from $tableName order by id")(Seq(1, "row1", 1000))

    // Read and verify the variant column as a struct with binary fields
    val rows = spark.sql(s"select id, v from $tableName order by id").collect()
    assert(rows.length == 1, s"Should have 1 row after delete operation in Spark 4.0 ($testDescription)")
    assert(rows(0).getInt(0) == 1, "First column should be id=1")
    assert(!rows(0).isNullAt(1), "Variant column should not be null")

    val variantStruct = rows(0).getStruct(1)
    assert(variantStruct.size == 2, "Variant struct should have 2 fields: value and metadata")

    val valueBytes = variantStruct.getAs[Array[Byte]](0)
    val metadataBytes = variantStruct.getAs[Array[Byte]](1)

    // Expected byte values from Spark 4.0 Variant representation: {"updated": true, "new_field": 123}
    val expectedValueBytes = Array[Byte](0x02, 0x02, 0x01, 0x00, 0x01, 0x00, 0x03, 0x04, 0x0C, 0x7B)
    val expectedMetadataBytes = Array[Byte](0x01, 0x02, 0x00, 0x07, 0x10, 0x75, 0x70, 0x64, 0x61,
      0x74, 0x65, 0x64, 0x6E, 0x65, 0x77, 0x5F, 0x66, 0x69, 0x65, 0x6C, 0x64)

    assert(valueBytes.sameElements(expectedValueBytes),
      s"Variant value bytes mismatch ($testDescription). " +
        s"Expected: ${StringUtils.encodeHex(expectedValueBytes).mkString("Array(", ", ", ")")}, " +
        s"Got: ${StringUtils.encodeHex(valueBytes).mkString("Array(", ", ", ")")}")

    assert(metadataBytes.sameElements(expectedMetadataBytes),
      s"Variant metadata bytes mismatch ($testDescription). " +
        s"Expected: ${StringUtils.encodeHex(expectedMetadataBytes).mkString("Array(", ", ", ")")}, " +
        s"Got: ${StringUtils.encodeHex(metadataBytes).mkString("Array(", ", ", ")")}")

    // Verify we can select all columns without errors
    assert(spark.sql(s"select * from $tableName").count() == 1, "Should be able to read all columns including variant")

    spark.sql(s"drop table $tableName")
  }

  test("Test Shredded Variant Write and Read + Validate Parquet Schema after Write") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")

    // Test 1: Shredding enabled with forced schema → parquet should have typed_value
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  v variant,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'cow',
           |  preCombineField = 'ts'
           | )
        """.stripMargin)

      spark.sql("set hoodie.parquet.variant.write.shredding.enabled = true")
      spark.sql("set hoodie.parquet.variant.allow.reading.shredded = true")
      spark.sql("set hoodie.parquet.variant.force.shredding.schema.for.test = a int, b string")

      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, 'row1', parse_json('{"a": 1, "b": "hello"}'), 1000)
        """.stripMargin)
      checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
        Seq(1, "row1", "{\"a\":1,\"b\":\"hello\"}", 1000)
      )

      // Verify parquet schema has shredded structure with typed_value
      val parquetFiles = listDataParquetFiles(tmp.getCanonicalPath)
      assert(parquetFiles.nonEmpty, "Should have at least one data parquet file")

      parquetFiles.foreach { filePath =>
        val schema = readParquetSchema(filePath)
        val variantGroup = getFieldAsGroup(schema, "v")
        assert(groupContainsField(variantGroup, "typed_value"),
          s"Shredded variant should have typed_value field. Schema:\n$variantGroup")
        val valueField = variantGroup.getType(variantGroup.getFieldIndex("value"))
        assert(valueField.getRepetition == Type.Repetition.OPTIONAL,
          "Shredded variant value field should be OPTIONAL")
        val metadataField = variantGroup.getType(variantGroup.getFieldIndex("metadata"))
        assert(metadataField.getRepetition == Type.Repetition.REQUIRED,
          "Shredded variant metadata field should be REQUIRED")
      }
    })
  }

  test("Test Unshredded Variant Write and Read + Validate Parquet Schema after Write") {
    assume(HoodieSparkUtils.gteqSpark4_0, "Variant type requires Spark 4.0 or higher")
    // Shredding disabled parquet should NOT have typed_value
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  v variant,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'cow',
           |  preCombineField = 'ts'
           | )
              """.stripMargin)

      spark.sql(s"set hoodie.parquet.variant.write.shredding.enabled = false")

      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, 'row1', parse_json('{"a": 1, "b": "hello"}'), 1000)
              """.stripMargin)

      checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
        Seq(1, "row1", "{\"a\":1,\"b\":\"hello\"}", 1000)
      )

      // Verify parquet schema does NOT have typed_value
      val parquetFiles = listDataParquetFiles(tmp.getCanonicalPath)
      assert(parquetFiles.nonEmpty, "Should have at least one data parquet file")

      parquetFiles.foreach { filePath =>
        val schema = readParquetSchema(filePath)
        val variantGroup = getFieldAsGroup(schema, "v")
        assert(!groupContainsField(variantGroup, "typed_value"),
          s"Non-shredded variant should NOT have typed_value field. Schema:\n$variantGroup")
        val valueField = variantGroup.getType(variantGroup.getFieldIndex("value"))
        assert(valueField.getRepetition == Type.Repetition.REQUIRED,
          "Non-shredded variant value field should be REQUIRED")
      }

      // Verify data can still be read back for the non-shredded case
      checkAnswer(s"select id, name, cast(v as string), ts from $tableName order by id")(
        Seq(1, "row1", "{\"a\":1,\"b\":\"hello\"}", 1000)
      )
    })
  }

  /**
   * Lists data parquet files in the table directory, excluding Hudi metadata files.
   */
  private def listDataParquetFiles(tablePath: String): Seq[String] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(new HadoopPath(tablePath).toUri, conf)
    val iter = fs.listFiles(new HadoopPath(tablePath), true)
    val files = scala.collection.mutable.ArrayBuffer[String]()
    while (iter.hasNext) {
      val file = iter.next()
      val path = file.getPath.toString
      if (path.endsWith(".parquet") && !path.contains(".hoodie")) {
        files += path
      }
    }
    files.toSeq
  }

  /**
   * Reads the Parquet schema (MessageType) from a parquet file.
   */
  private def readParquetSchema(filePath: String): MessageType = {
    val conf = spark.sparkContext.hadoopConfiguration
    val inputFile = HadoopInputFile.fromPath(new HadoopPath(filePath), conf)
    val reader = ParquetFileReader.open(inputFile)
    try {
      reader.getFooter.getFileMetaData.getSchema
    } finally {
      reader.close()
    }
  }

  /**
   * Gets a named field from a GroupType (MessageType) and returns it as a GroupType.
   * Uses getFieldIndex(String) + getType(int) to avoid Scala overload resolution issues.
   */
  private def getFieldAsGroup(parent: GroupType, fieldName: String): GroupType = {
    val idx: Int = parent.getFieldIndex(fieldName)
    parent.getType(idx).asGroupType()
  }

  /**
   * Checks whether a GroupType contains a field with the given name.
   * Uses try/catch on getFieldIndex to avoid Scala-Java collection converter dependencies.
   */
  private def groupContainsField(group: GroupType, fieldName: String): Boolean = {
    try {
      group.getFieldIndex(fieldName)
      true
    } catch {
      case _: Exception => false
    }
  }
}
