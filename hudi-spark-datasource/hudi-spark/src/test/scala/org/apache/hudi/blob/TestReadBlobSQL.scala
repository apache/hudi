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

package org.apache.hudi.blob

import org.apache.hudi.blob.BlobTestHelpers._
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.exception.HoodieIOException
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.Collections

/**
 * Tests for the read_blob() SQL function.
 *
 * This test suite verifies:
 * <ul>
 *   <li>Basic SQL integration with read_blob()</li>
 *   <li>Integration with WHERE clauses, JOINs</li>
 *   <li>Configuration parameter handling</li>
 *   <li>Error handling for invalid inputs</li>
 * </ul>
 */
class TestReadBlobSQL extends HoodieClientTestBase {

  @Test
  def testReadOutOfLineBlobOnHudiBackedTable(): Unit = {
    // Verifies read_blob()'s logical plan is task-serializable over a
    // HoodieFileIndex-backed relation (DataFrame write path).
    val extFile = createTestFile(tempDir, "basic.bin", 10000)
    val tablePath = s"$tempDir/hudi_blob_table"

    val rawDf = sparkSession.createDataFrame(Seq(
        (1, "rec1", extFile, 0L, 100L),
        (2, "rec2", extFile, 100L, 100L),
        (3, "rec3", extFile, 200L, 100L)
      )).toDF("id", "name", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"),
          col("length")))
      .select("id", "name", "file_info")

    // Coerce to the canonical BlobType schema. blobStructCol produces a
    // non-null reference struct, but HoodieSparkSchemaConverters
    // .validateBlobStructure rejects that on write — it demands the
    // reference field be nullable. Rebuild the DataFrame on its RDD
    // against the canonical shape so .save() doesn't fail early.
    val canonicalSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("file_info", BlobType().asInstanceOf[StructType],
        nullable = true, blobMetadata)
    ))
    val df = sparkSession.createDataFrame(rawDf.rdd, canonicalSchema)

    df.write.format("hudi")
      .option("hoodie.table.name", "blob_test")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite")
      .save(tablePath)

    // Hudi read pulls HoodieFileIndex into the plan that BatchedBlobReadExec
    // serializes to executors — the scenario the exec-node fix must handle.
    sparkSession.read.format("hudi").load(tablePath)
      .createOrReplaceTempView("hudi_blob_view")

    val result = sparkSession.sql("""
      SELECT id, read_blob(file_info) AS data
      FROM hudi_blob_view
      ORDER BY id
    """).collect()

    assertEquals(3, result.length)
    // Verify the bytes read from the external file match the recorded offsets.
    result.zipWithIndex.foreach { case (row, idx) =>
      assertEquals(idx + 1, row.getInt(0))
      val bytes = row.getAs[Array[Byte]]("data")
      assertEquals(100, bytes.length)
      assertBytesContent(bytes, expectedOffset = idx * 100)
    }
  }

  @Test
  def testBasicReadBlobSQL(): Unit = {
    val filePath = createTestFile(tempDir, "basic.bin", 10000)

    // Main DataFrame with blobStructCol
    val df = sparkSession.createDataFrame(Seq(
      (1, "record1", filePath, 0L, 100L),
      (3, "record3", filePath, 100L, 100L),
      (4, "record4", filePath, 200L, 100L)
    )).toDF("id", "name", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "name", "file_info")

    // Ensure file_info is nullable in the schema
    val schema = StructType(df.schema.map {
      case StructField("file_info", dt, _, md) => StructField("file_info", dt, nullable = true, md)
      case other => other
    })
    val dfWithNullable = sparkSession.createDataFrame(df.rdd, schema)

    // DataFrame with a null blob value
    val nullRow = Row(2, "record2", null)
    val nullDf = sparkSession.createDataFrame(Collections.singletonList(nullRow), schema)

    // Union the null row
    val fullDf = dfWithNullable.unionByName(nullDf)
    fullDf.createOrReplaceTempView("test_table")

    // Use SQL with read_blob
    val result = sparkSession.sql("""
      SELECT id, name, read_blob(file_info) as data
      FROM test_table
      WHERE id <= 3
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)

    // Verify data is binary for non-null rows
    val data1 = rows(0).getAs[Array[Byte]]("data")
    assertEquals(100, data1.length)
    assertBytesContent(data1)

    // The null_blob row should have null data
    assertTrue(rows(1).isNullAt(2))

    val data3 = rows(2).getAs[Array[Byte]]("data")
    assertEquals(100, data3.length)
    assertBytesContent(data3, expectedOffset = 100)
  }

  @Test
  def testReadBlobWithJoin(): Unit = {
    val filePath = createTestFile(tempDir, "join.bin", 10000)

    // Create blob table
    val blobDF = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 100L, 100L)
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")

    blobDF.createOrReplaceTempView("blob_table")

    // Create metadata table
    val metaDF = sparkSession.createDataFrame(Seq(
      (1, "Alice"),
      (2, "Bob")
    )).toDF("id", "name")

    metaDF.createOrReplaceTempView("meta_table")

    // SQL with JOIN
    val result = sparkSession.sql("""
      SELECT m.id, m.name, read_blob(b.file_info) as data
      FROM meta_table m
      JOIN blob_table b ON m.id = b.id
      ORDER BY m.id
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)
    assertEquals("Alice", rows(0).getAs[String]("name"))
    assertEquals(100, rows(0).getAs[Array[Byte]]("data").length)
    assertEquals("Bob", rows(1).getAs[String]("name"))
    assertEquals(100, rows(1).getAs[Array[Byte]]("data").length)

    // Verify data content
    val data1 = rows(0).getAs[Array[Byte]]("data")
    assertBytesContent(data1)
  }

  @Test
  def testReadBlobInSubquery(): Unit = {
    val filePath = createTestFile(tempDir, "subquery.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, "A", filePath, 0L, 100L),
      (2, "A", filePath, 100L, 100L),
      (3, "B", filePath, 200L, 100L)
    )).toDF("id", "category", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "category", "file_info")

    df.createOrReplaceTempView("subquery_table")

    // SQL with subquery
    val result = sparkSession.sql("""
      SELECT * FROM (
        SELECT id, category, read_blob(file_info) as data
        FROM subquery_table
      ) WHERE category = 'A'
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)
    rows.foreach { row =>
      assertEquals("A", row.getAs[String]("category"))
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
    }
  }

  @Test
  def testConfigurationParameters(): Unit = {
    val filePath = createTestFile(tempDir, "config.bin", 50000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 5000L, 100L),  // 4.9KB gap
      (3, filePath, 10000L, 100L)
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")

    df.createOrReplaceTempView("config_table")

    // Use withSparkConfig to automatically manage configuration
    withSparkConfig(sparkSession, Map(
      "hoodie.blob.batching.max.gap.bytes" -> "10000",
      "hoodie.blob.batching.lookahead.size" -> "100"
    )) {
      val result = sparkSession.sql("""
        SELECT id, read_blob(file_info) as data
        FROM config_table
      """)

      val rows = result.collect()
      assertEquals(3, rows.length)

      // Verify all reads completed successfully
      rows.foreach { row =>
        assertEquals(100, row.getAs[Array[Byte]]("data").length)
      }
    }
  }

  @Test
  def testMultipleReadBlobInSameQuery(): Unit = {
    val filePath1 = createTestFile(tempDir, "multi1.bin", 10000)
    val filePath2 = createTestFile(tempDir, "multi2.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath1, 0L, 50L, filePath2, 500L, 50L),
      (2, filePath1, 100L, 50L, filePath2, 600L, 50L)
    )).toDF("id", "external_path1", "offset1", "length1", "external_path2", "offset2", "length2")
      .withColumn("file_info1",
        blobStructCol("file_info1", col("external_path1"), col("offset1"), col("length1")))
      .withColumn("file_info2",
        blobStructCol("file_info2", col("external_path2"), col("offset2"), col("length2")))
      .select("id", "file_info1", "file_info2")

    df.createOrReplaceTempView("multi_table")

    // SQL with multiple read_blob calls
    val result = sparkSession.sql("""
      SELECT
        id,
        read_blob(file_info1) as data1,
        read_blob(file_info2) as data2
      FROM multi_table
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)

    // Row 1: data1 = file1 at offset 0, data2 = file2 at offset 500
    val data1_row1 = rows(0).getAs[Array[Byte]]("data1")
    val data2_row1 = rows(0).getAs[Array[Byte]]("data2")
    assertEquals(50, data1_row1.length)
    assertEquals(50, data2_row1.length)
    assertBytesContent(data1_row1, expectedOffset = 0)
    assertBytesContent(data2_row1, expectedOffset = 500)

    // Row 2: data1 = file1 at offset 100, data2 = file2 at offset 600
    val data1_row2 = rows(1).getAs[Array[Byte]]("data1")
    val data2_row2 = rows(1).getAs[Array[Byte]]("data2")
    assertBytesContent(data1_row2, expectedOffset = 100)
    assertBytesContent(data2_row2, expectedOffset = 600)
  }

  @Test
  def testReadBlobWithEmptyResult(): Unit = {
    val filePath = createTestFile(tempDir, "empty.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 100L, 100L)
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")

    df.createOrReplaceTempView("empty_table")

    // SQL that returns no rows
    val result = sparkSession.sql("""
      SELECT id, read_blob(file_info) as data
      FROM empty_table
      WHERE id > 100
    """)

    val rows = result.collect()
    assertEquals(0, rows.length)
  }

  @Test
  def testReadBlobMultipleFiles(): Unit = {
    val filePath1 = createTestFile(tempDir, "file1.bin", 10000)
    val filePath2 = createTestFile(tempDir, "file2.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath1, 0L, 100L),
      (2, filePath2, 0L, 100L),
      (3, filePath1, 100L, 100L),
      (4, filePath2, 100L, 100L)
    )).toDF("id", "external_path", "offset", "length")
    .withColumn("file_info",
      blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")

    df.createOrReplaceTempView("multi_file_table")

    // SQL reading from multiple files
    val result = sparkSession.sql("""
      SELECT id, read_blob(file_info) as data
      FROM multi_file_table
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(4, rows.length)

    // Verify all data was read correctly
    rows.foreach { row =>
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
    }
  }

  @Test
  def testReadBlobInWhereClause(): Unit = {
    val filePath = createTestFile(tempDir, "where.bin", 10000)
    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 50L),   // 50 bytes — filtered out
      (2, filePath, 100L, 100L), // 100 bytes — passes filter
      (3, filePath, 200L, 200L)  // 200 bytes — passes filter
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")
    df.createOrReplaceTempView("where_table")

    val result = sparkSession.sql("""
      SELECT id, read_blob(file_info) AS data
      FROM where_table
      WHERE length(read_blob(file_info)) > 50
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)

    // validate that rows with IDs 2 and 3 are returned
    assertEquals(2, rows(0).getInt(0))
    assertEquals(3, rows(1).getInt(0))
  }

  @Test
  def testReadBlobWithCaseWhen(): Unit = {
    val filePath = createTestFile(tempDir, "case.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, true, filePath, 0L, 100L),
      (2, false, filePath, 100L, 100L),
      (3, true, filePath, 200L, 100L)
    )).toDF("id", "should_resolve", "external_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "should_resolve", "file_info")

    df.createOrReplaceTempView("case_table")

    // SQL with CASE WHEN - note: this tests that the expression is handled
    // even in conditional contexts
    val result = sparkSession.sql("""
      SELECT
        id,
        should_resolve,
        CASE
          WHEN should_resolve THEN read_blob(file_info)
          ELSE NULL
        END as data
      FROM case_table
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)

    // Row 1 should have data
    assertTrue(rows(0).getAs[Boolean]("should_resolve"))
    assertNotNull(rows(0).get(2))

    // Row 2 should have null
    assertFalse(rows(1).getAs[Boolean]("should_resolve"))
    assertTrue(rows(1).isNullAt(2))

    // Row 3 should have data
    assertTrue(rows(2).getAs[Boolean]("should_resolve"))
    assertNotNull(rows(2).get(2))
  }

  @Test
  def testReadBlobWithMissingFile(): Unit = {
    val missingPath = tempDir.resolve("does_not_exist.bin").toString
    val df = sparkSession.createDataFrame(Seq(
      (1, missingPath, 0L, 10L)
    )).toDF("id", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")
    df.createOrReplaceTempView("missing_file_table")
    val thrown = assertThrows(classOf[Exception], () => {
      sparkSession.sql("SELECT id, read_blob(file_info) as data FROM missing_file_table").collect()
    })
    assertTrue(thrown.getCause.isInstanceOf[HoodieIOException])
  }

  /**
   * INLINE BLOB writes accept the minimal `{type, data}` struct (no `reference` field) at
   * the DataFrame entry. The writer pads each row to the canonical 3-field shape with
   * `reference = null`. Round-trips the bytes through `read_blob()` to confirm the padded
   * row materializes correctly.
   */
  @Test
  def testInlineBlobWriteAcceptsMinimalStruct(): Unit = {
    val tablePath = s"$tempDir/hudi_inline_min_struct_table"
    val payload = Array[Byte](0xA, 0xB, 0xC, 0xD)

    val minimalInlineSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("payload", StructType(Seq(
        StructField(HoodieSchema.Blob.TYPE, StringType, nullable = true),
        StructField(HoodieSchema.Blob.INLINE_DATA_FIELD, BinaryType, nullable = true)
      )), nullable = true, blobMetadata)
    ))
    val minimalRow = Row(1, Row(HoodieSchema.Blob.INLINE, payload))
    val minimalDf = sparkSession.createDataFrame(
      Collections.singletonList(minimalRow), minimalInlineSchema)

    minimalDf.write.format("hudi")
      .option("hoodie.table.name", "blob_inline_min_struct")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite")
      .save(tablePath)

    val readBack = sparkSession.read.format("hudi").load(tablePath)
      .select("id", "payload")
      .collect()
    assertEquals(1, readBack.length)
    val payloadStruct = readBack(0).getStruct(readBack(0).fieldIndex("payload"))
    assertEquals(HoodieSchema.Blob.INLINE,
      payloadStruct.getString(payloadStruct.fieldIndex(HoodieSchema.Blob.TYPE)))
    assertArrayEquals(payload,
      payloadStruct.getAs[Array[Byte]](HoodieSchema.Blob.INLINE_DATA_FIELD))
    // The padder filled `reference` with null on the way in.
    assertTrue(payloadStruct.isNullAt(
      payloadStruct.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)))

    // read_blob() resolves INLINE bytes directly.
    sparkSession.read.format("hudi").load(tablePath)
      .createOrReplaceTempView("blob_min_inline_view")
    val readBlobBytes = sparkSession.sql(
      "SELECT id, read_blob(payload) AS bytes FROM blob_min_inline_view")
      .collect().head.getAs[Array[Byte]]("bytes")
    assertArrayEquals(payload, readBlobBytes)
  }

  /**
   * OUT_OF_LINE BLOB writes accept the minimal `{type, reference}` struct (no `data`
   * field) at the DataFrame entry. The writer pads each row to the canonical 3-field
   * shape with `data = null`.
   */
  @Test
  def testOutOfLineBlobWriteAcceptsMinimalStruct(): Unit = {
    val extFile = createTestFile(tempDir, "minstruct_ool.bin", 1000)
    val tablePath = s"$tempDir/hudi_oolline_min_struct_table"

    // Minimal {type, reference} schema — no `data` field.
    val refStructType = StructType(Seq(
      StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, StringType, nullable = true),
      StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, LongType, nullable = true),
      StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, LongType, nullable = true),
      StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, BooleanType, nullable = true)
    ))
    val minimalSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("payload", StructType(Seq(
        StructField(HoodieSchema.Blob.TYPE, StringType, nullable = true),
        StructField(HoodieSchema.Blob.EXTERNAL_REFERENCE, refStructType, nullable = true)
      )), nullable = true, blobMetadata)
    ))
    val refRow = Row(extFile, 0L, 100L, false)
    val minimalRow = Row(1, Row(HoodieSchema.Blob.OUT_OF_LINE, refRow))
    val minimalDf = sparkSession.createDataFrame(
      Collections.singletonList(minimalRow), minimalSchema)

    minimalDf.write.format("hudi")
      .option("hoodie.table.name", "blob_oolline_min_struct")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite")
      .save(tablePath)

    val readBack = sparkSession.read.format("hudi").load(tablePath)
      .select("id", "payload")
      .collect()
    assertEquals(1, readBack.length)
    val payloadStruct = readBack(0).getStruct(readBack(0).fieldIndex("payload"))
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE,
      payloadStruct.getString(payloadStruct.fieldIndex(HoodieSchema.Blob.TYPE)))
    // The padder filled `data` with null on the way in.
    assertTrue(payloadStruct.isNullAt(
      payloadStruct.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)))
    val ref = payloadStruct.getStruct(
      payloadStruct.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE))
    assertTrue(ref.getString(ref.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH))
      .endsWith("minstruct_ool.bin"))

    // read_blob() dereferences the external file and reads the byte range.
    sparkSession.read.format("hudi").load(tablePath)
      .createOrReplaceTempView("blob_min_oolline_view")
    val readBlobBytes = sparkSession.sql(
      "SELECT id, read_blob(payload) AS bytes FROM blob_min_oolline_view")
      .collect().head.getAs[Array[Byte]]("bytes")
    assertEquals(100, readBlobBytes.length)
    assertBytesContent(readBlobBytes, expectedOffset = 0)
  }

  /**
   * Partial INLINE blob structs nested inside a non-blob struct, an array of structs, and
   * a map of string -> struct are all padded by the writer. Exercises the recursive pad
   * paths in `padPartialBlobColumns` (struct rebuild, `transform`, `transform_values`).
   */
  @Test
  def testNestedPartialBlobWritesAcceptedThroughStructArrayAndMap(): Unit = {
    val tablePath = s"$tempDir/hudi_nested_partial_blob_table"

    val payloadStruct = StructType(Seq(
      StructField(HoodieSchema.Blob.TYPE, StringType, nullable = true),
      StructField(HoodieSchema.Blob.INLINE_DATA_FIELD, BinaryType, nullable = true)
    ))
    val nestedStructField = StructField(
      "doc", StructType(Seq(
        StructField("doc_id", IntegerType, nullable = false),
        StructField("payload", payloadStruct, nullable = true, blobMetadata)
      )), nullable = true)
    val nestedArrayField = StructField(
      "items", ArrayType(StructType(Seq(
        StructField("idx", IntegerType, nullable = false),
        StructField("payload", payloadStruct, nullable = true, blobMetadata)
      )), containsNull = true), nullable = true)
    val nestedMapField = StructField(
      "by_label", MapType(StringType, StructType(Seq(
        StructField("payload", payloadStruct, nullable = true, blobMetadata)
      )), valueContainsNull = true), nullable = true)

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      nestedStructField,
      nestedArrayField,
      nestedMapField
    ))

    val structPayload = Array[Byte](0x1, 0x2)
    val arrPayload0 = Array[Byte](0x3, 0x4)
    val arrPayload1 = Array[Byte](0x5, 0x6)
    val mapPayload = Array[Byte](0x7, 0x8)

    val docRow = Row(101, Row(HoodieSchema.Blob.INLINE, structPayload))
    val itemsRow = Seq(
      Row(0, Row(HoodieSchema.Blob.INLINE, arrPayload0)),
      Row(1, Row(HoodieSchema.Blob.INLINE, arrPayload1))
    )
    val byLabelRow = Map("a" -> Row(Row(HoodieSchema.Blob.INLINE, mapPayload)))
    val row = Row(1, docRow, itemsRow, byLabelRow)

    val df = sparkSession.createDataFrame(Collections.singletonList(row), schema)

    df.write.format("hudi")
      .option("hoodie.table.name", "blob_nested_partial")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite")
      .save(tablePath)

    val rows = sparkSession.read.format("hudi").load(tablePath)
      .select("id", "doc", "items", "by_label").collect()
    assertEquals(1, rows.length)
    val readBack = rows(0)

    // Struct-nested blob: padded `reference` is null, bytes round-trip.
    val docReadBack = readBack.getStruct(readBack.fieldIndex("doc"))
    val docPayload = docReadBack.getStruct(docReadBack.fieldIndex("payload"))
    assertEquals(HoodieSchema.Blob.INLINE,
      docPayload.getString(docPayload.fieldIndex(HoodieSchema.Blob.TYPE)))
    assertArrayEquals(structPayload,
      docPayload.getAs[Array[Byte]](HoodieSchema.Blob.INLINE_DATA_FIELD))
    assertTrue(docPayload.isNullAt(docPayload.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)))

    // Array-nested blob: each element's blob is independently padded.
    val items = readBack.getList[Row](readBack.fieldIndex("items"))
    assertEquals(2, items.size())
    val itemPayloads = (0 until 2).map { i =>
      val itemBlob = items.get(i).getStruct(items.get(i).fieldIndex("payload"))
      assertEquals(HoodieSchema.Blob.INLINE,
        itemBlob.getString(itemBlob.fieldIndex(HoodieSchema.Blob.TYPE)))
      assertTrue(itemBlob.isNullAt(itemBlob.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)))
      itemBlob.getAs[Array[Byte]](HoodieSchema.Blob.INLINE_DATA_FIELD)
    }
    assertArrayEquals(arrPayload0, itemPayloads(0))
    assertArrayEquals(arrPayload1, itemPayloads(1))

    // Map-nested blob: padded value's blob is canonical.
    val byLabel = readBack.getJavaMap[String, Row](readBack.fieldIndex("by_label"))
    val mapVal = byLabel.get("a")
    val mapBlob = mapVal.getStruct(mapVal.fieldIndex("payload"))
    assertEquals(HoodieSchema.Blob.INLINE,
      mapBlob.getString(mapBlob.fieldIndex(HoodieSchema.Blob.TYPE)))
    assertTrue(mapBlob.isNullAt(mapBlob.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)))
    assertArrayEquals(mapPayload,
      mapBlob.getAs[Array[Byte]](HoodieSchema.Blob.INLINE_DATA_FIELD))

    // read_blob() resolves the struct-nested blob directly: the rule lifts the
    // nested expression to a synthetic top-level alias before BatchedBlobRead.
    sparkSession.read.format("hudi").load(tablePath)
      .createOrReplaceTempView("blob_nested_view")
    val sqlBytes = sparkSession.sql(
      "SELECT read_blob(doc.payload) AS bytes FROM blob_nested_view")
      .collect().head.getAs[Array[Byte]]("bytes")
    assertArrayEquals(structPayload, sqlBytes)
  }

  @Test
  def testReadBlobOnNonBlobColumn(): Unit = {
    val df = sparkSession.createDataFrame(Seq(
      (1, "not_a_blob")
    )).toDF("id", "not_blob")
    df.createOrReplaceTempView("non_blob_table")
    val thrown = assertThrows(classOf[Exception], () => {
      sparkSession.sql("SELECT id, read_blob(not_blob) as data FROM non_blob_table").collect()
    })
    assertTrue(thrown.isInstanceOf[IllegalArgumentException])
    assertTrue(thrown.getMessage.contains("must be compatible with BlobType"))
  }

  @Test
  def testReadBlobInProjectAndFilter(): Unit = {
    val filePath = createTestFile(tempDir, "project_and_filter.bin", 10000)

    // DataFrame with blobStructCol
    val df = sparkSession.createDataFrame(Seq(
      (1, "record1", filePath, 0L, 100L),
      (2, "record2", filePath, 100L, 100L),
      (3, "record3", filePath, 200L, 100L)
    )).toDF("id", "name", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "name", "file_info")

    df.createOrReplaceTempView("project_and_filter_table")

    // Query with read_blob in both SELECT and WHERE
    val result = sparkSession.sql("""
      SELECT id, name, read_blob(file_info) as data
      FROM project_and_filter_table
      WHERE length(read_blob(file_info)) = 100
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)
    rows.zipWithIndex.foreach { case (row, idx) =>
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
      assertBytesContent(row.getAs[Array[Byte]]("data"), expectedOffset = idx * 100)
    }
  }
}
