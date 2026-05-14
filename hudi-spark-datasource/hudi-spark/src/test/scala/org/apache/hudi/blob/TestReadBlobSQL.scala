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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.blob.BlobTestHelpers._
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.exception.HoodieIOException
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.junit.Ignore
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

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

  // ------------------------------------------------------------------
  // Parquet DESCRIPTOR-mode interaction tests
  //
  // These exercise the per-query rewrite added by ReadBlobRule that
  // injects BLOB_INLINE_READ_FORCE_CONTENT_COLUMNS into the
  // LogicalRelation's options when a query uses read_blob(). The
  // contract: read_blob(col) always returns bytes; plain SELECT keeps
  // DESCRIPTOR's I/O savings (data sub-field is null) for the columns
  // that aren't referenced by read_blob().
  // ------------------------------------------------------------------

  /**
   * Helpers for the DESCRIPTOR-mode tests. Builds a Hudi table containing
   * one or two INLINE blob columns and returns the table path.
   */
  private def writeInlineBlobTable(name: String,
                                   tableType: HoodieTableType,
                                   payloads: Seq[Array[Byte]]): String = {
    val tablePath = s"$tempDir/$name"
    val rawDf = sparkSession.createDataFrame(
        payloads.zipWithIndex.map { case (bytes, i) => (i + 1, bytes) })
      .toDF("id", "bytes")
      .withColumn("payload", inlineBlobStructCol("payload", col("bytes")))
      .select("id", "payload")
    val canonicalSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("payload", BlobType().asInstanceOf[StructType], nullable = true, blobMetadata)
    ))
    val df = sparkSession.createDataFrame(rawDf.rdd, canonicalSchema)
    df.write.format("hudi")
      .option("hoodie.table.name", name)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), tableType.name())
      .option(DataSourceWriteOptions.OPERATION.key(), "bulk_insert")
      .mode("overwrite")
      .save(tablePath)
    tablePath
  }

  /**
   * Core contract: read_blob() always materializes bytes, even under
   * DESCRIPTOR mode, on both COW and MOR base files. Without this fix,
   * read_blob() would see a null `data` sub-field (column-pruned by
   * Parquet) and silently return null.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testReadBlobUnderDescriptorMaterializesBytes(tableType: HoodieTableType): Unit = {
    val payloads = Seq(
      Array.fill[Byte](128)(0x1.toByte),
      Array.fill[Byte](128)(0x2.toByte),
      Array.fill[Byte](128)(0x3.toByte))
    val tablePath = writeInlineBlobTable(
      s"read_blob_desc_${tableType.name().toLowerCase}", tableType, payloads)

    sparkSession.read.format("hudi")
      .option("hoodie.read.blob.inline.mode", "DESCRIPTOR")
      .load(tablePath)
      .createOrReplaceTempView("rb_desc_view")

    val rows = sparkSession.sql(
      "SELECT id, read_blob(payload) AS bytes FROM rb_desc_view ORDER BY id"
    ).collect()
    assertEquals(3, rows.length)
    rows.zip(payloads).foreach { case (row, expected) =>
      val bytes = row.getAs[Array[Byte]]("bytes")
      assertNotNull(bytes, s"read_blob() must materialize bytes under DESCRIPTOR (id=${row.getInt(0)})")
      assertArrayEquals(expected, bytes, s"bytes mismatch for id=${row.getInt(0)}")
    }
  }

  /**
   * DESCRIPTOR savings preserved when read_blob() is NOT in the query:
   * commit 1's column projection still strips `data`, and the rule writes
   * no force-content option.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testDescriptorWithoutReadBlobStillSkipsData(tableType: HoodieTableType): Unit = {
    val payloads = Seq(
      Array.fill[Byte](128)(0x1.toByte),
      Array.fill[Byte](128)(0x2.toByte))
    val tablePath = writeInlineBlobTable(
      s"desc_no_rb_${tableType.name().toLowerCase}", tableType, payloads)

    val rows = sparkSession.read.format("hudi")
      .option("hoodie.read.blob.inline.mode", "DESCRIPTOR")
      .load(tablePath)
      .select(col("id"), col("payload"))
      .orderBy(col("id"))
      .collect()

    assertEquals(2, rows.length)
    rows.foreach { row =>
      val payload = row.getStruct(row.fieldIndex("payload"))
      assertEquals(HoodieSchema.Blob.INLINE,
        payload.getString(payload.fieldIndex(HoodieSchema.Blob.TYPE)))
      assertTrue(payload.isNullAt(payload.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)),
        s"DESCRIPTOR should null-pad data when read_blob() is absent (id=${row.getInt(0)})")
      assertTrue(payload.isNullAt(payload.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)),
        "Parquet has no native byte-range descriptor; reference is null")
    }
  }

  /**
   * The per-column granularity claim. Query references read_blob(payload_a)
   * only — payload_a must materialize bytes, payload_b must remain
   * stripped (DESCRIPTOR savings preserved for the column the user
   * didn't ask about).
   *
   * Uses multiple rows with distinct per-row byte patterns so that any
   * row-iteration bug (e.g., reusing a row buffer without copying, or
   * mis-indexing the blob struct ordinal across rows) would surface as
   * mismatched bytes per row rather than slipping through a single-row
   * happy path.
   */
  @Test
  def testDescriptorPerColumnGranularity(): Unit = {
    val tablePath = s"$tempDir/desc_per_column"
    // Distinct fill byte AND distinct length per row, AND per-row distinction
    // between payload_a and payload_b — a cross-row or cross-column leak fails an assertion.
    val rows = Seq(
      (1, Array.fill[Byte](80)(0xA1.toByte), Array.fill[Byte](160)(0xB1.toByte)),
      (2, Array.fill[Byte](64)(0xA2.toByte), Array.fill[Byte](192)(0xB2.toByte)),
      (3, Array.fill[Byte](96)(0xA3.toByte), Array.fill[Byte](128)(0xB3.toByte))
    )
    val rawDf = sparkSession.createDataFrame(rows)
      .toDF("id", "bytes_a", "bytes_b")
      .withColumn("payload_a", inlineBlobStructCol("payload_a", col("bytes_a")))
      .withColumn("payload_b", inlineBlobStructCol("payload_b", col("bytes_b")))
      .select("id", "payload_a", "payload_b")
    val canonicalSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("payload_a", BlobType().asInstanceOf[StructType], nullable = true, blobMetadata),
      StructField("payload_b", BlobType().asInstanceOf[StructType], nullable = true, blobMetadata)
    ))
    sparkSession.createDataFrame(rawDf.rdd, canonicalSchema).write.format("hudi")
      .option("hoodie.table.name", "desc_per_column")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite")
      .save(tablePath)

    sparkSession.read.format("hudi")
      .option("hoodie.read.blob.inline.mode", "DESCRIPTOR")
      .load(tablePath)
      .createOrReplaceTempView("desc_per_column_view")

    val outRows = sparkSession.sql(
      "SELECT id, read_blob(payload_a) AS bytes_a, payload_b " +
        "FROM desc_per_column_view ORDER BY id"
    ).collect()
    assertEquals(rows.length, outRows.length)
    outRows.zip(rows).foreach { case (row, (expectedId, expectedA, expectedB)) =>
      assertEquals(expectedId, row.getInt(0))
      val bytesA = row.getAs[Array[Byte]]("bytes_a")
      assertArrayEquals(expectedA, bytesA,
        s"read_blob(payload_a) bytes mismatch at id=$expectedId — expected length ${expectedA.length}, " +
          s"got length ${if (bytesA == null) -1 else bytesA.length}")
      val payloadB = row.getStruct(row.fieldIndex("payload_b"))
      assertTrue(payloadB.isNullAt(payloadB.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)),
        s"DESCRIPTOR savings must be preserved for payload_b at id=$expectedId")
      // Sanity: payload_b's type marker survived even though `data` was stripped.
      assertEquals(HoodieSchema.Blob.INLINE,
        payloadB.getString(payloadB.fieldIndex(HoodieSchema.Blob.TYPE)),
        s"payload_b type marker must survive stripping at id=$expectedId")
      // Sanity: we did NOT smuggle bytes into payload_b under any name.
      val _ = expectedB // explicitly unused: the contract is that payload_b.data must be null
    }
  }

  /**
   * read_blob() in WHERE clause must also trigger the per-query rewrite.
   * ReadBlobRule's Filter case wraps the condition in BatchedBlobRead;
   * the second pass collects the blobAttr the same way.
   */
  @Test
  def testReadBlobInWhereClauseUnderDescriptor(): Unit = {
    val payloads = Seq(
      Array.fill[Byte](100)(0xA.toByte),
      Array.fill[Byte](200)(0xB.toByte),
      Array.fill[Byte](100)(0xC.toByte))
    val tablePath = writeInlineBlobTable(
      "desc_where_clause", HoodieTableType.COPY_ON_WRITE, payloads)

    sparkSession.read.format("hudi")
      .option("hoodie.read.blob.inline.mode", "DESCRIPTOR")
      .load(tablePath)
      .createOrReplaceTempView("desc_where_view")

    val rows = sparkSession.sql(
      "SELECT id, read_blob(payload) AS bytes FROM desc_where_view " +
        "WHERE length(read_blob(payload)) = 200"
    ).collect()
    assertEquals(1, rows.length)
    assertEquals(2, rows(0).getInt(0))
    assertArrayEquals(payloads(1), rows(0).getAs[Array[Byte]]("bytes"))
  }

  /**
   * JOIN of two Hudi Parquet tables, both in DESCRIPTOR mode, both with
   * a blob column. The query uses read_blob() on only the left side's
   * blob.
   *
   * This exercises ReadBlobRule's per-relation option routing: the
   * BLOB_INLINE_READ_FORCE_CONTENT_COLUMNS option must land on the
   * left table's LogicalRelation only, and the right table's payload
   * must come back with DESCRIPTOR's null `data`. A bug where the
   * rule writes the option to every Hudi LogicalRelation, or to
   * none, would fail exactly one of the two assertions below.
   */

  @Ignore //TODO to re-enable
  @Test
  def testReadBlobJoinUnderDescriptorRoutesOptionPerRelation(): Unit = {
    val leftPayloads = Seq(
      Array.fill[Byte](64)(0xA1.toByte),
      Array.fill[Byte](64)(0xA2.toByte))
    val rightPayloads = Seq(
      Array.fill[Byte](128)(0xB1.toByte),
      Array.fill[Byte](128)(0xB2.toByte))

    val leftPath = writeInlineBlobTable(
      "desc_join_left", HoodieTableType.COPY_ON_WRITE, leftPayloads)
    val rightPath = writeInlineBlobTable(
      "desc_join_right", HoodieTableType.COPY_ON_WRITE, rightPayloads)

    sparkSession.read.format("hudi")
      .option("hoodie.read.blob.inline.mode", "DESCRIPTOR")
      .load(leftPath)
      .createOrReplaceTempView("desc_join_left_v")
    sparkSession.read.format("hudi")
      .option("hoodie.read.blob.inline.mode", "DESCRIPTOR")
      .load(rightPath)
      .createOrReplaceTempView("desc_join_right_v")

    val rows = sparkSession.sql(
      "SELECT l.id AS id, read_blob(l.payload) AS left_bytes, r.payload AS right_payload " +
        "FROM desc_join_left_v l JOIN desc_join_right_v r ON l.id = r.id " +
        "ORDER BY l.id"
    ).collect()

    assertEquals(2, rows.length)
    rows.zipWithIndex.foreach { case (row, i) =>
      val id = i + 1
      assertEquals(id, row.getInt(0))
      // Left side: read_blob() must materialize bytes.
      val leftBytes = row.getAs[Array[Byte]]("left_bytes")
      assertNotNull(leftBytes, s"read_blob(l.payload) must return bytes at id=$id")
      assertArrayEquals(leftPayloads(i), leftBytes,
        s"read_blob(l.payload) bytes mismatch at id=$id")
      // Right side: DESCRIPTOR savings preserved — `data` is null.
      val rightPayload = row.getStruct(row.fieldIndex("right_payload"))
      assertEquals(HoodieSchema.Blob.INLINE,
        rightPayload.getString(rightPayload.fieldIndex(HoodieSchema.Blob.TYPE)),
        s"r.payload type marker must survive at id=$id")
      assertTrue(rightPayload.isNullAt(rightPayload.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)),
        s"r.payload.data must remain null under DESCRIPTOR at id=$id — read_blob() was not called on the right side")
    }
  }

  /**
   * Negative case: setting DESCRIPTOR on a table with no blob columns is
   * a no-op. The rule must not write the force-content option (nothing
   * to force), and the read must return rows untouched.
   */
  @Test
  def testDescriptorOnTableWithoutBlobColumns(): Unit = {
    val tablePath = s"$tempDir/desc_no_blob"
    sparkSession.createDataFrame(Seq((1, "a"), (2, "b")))
      .toDF("id", "name")
      .write.format("hudi")
      .option("hoodie.table.name", "desc_no_blob")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite")
      .save(tablePath)

    val rows = sparkSession.read.format("hudi")
      .option("hoodie.read.blob.inline.mode", "DESCRIPTOR")
      .load(tablePath)
      .select(col("id"), col("name"))
      .orderBy(col("id"))
      .collect()
    assertEquals(2, rows.length)
    assertEquals(1, rows(0).getInt(0))
    assertEquals("a", rows(0).getString(1))
    assertEquals(2, rows(1).getInt(0))
    assertEquals("b", rows(1).getString(1))
  }
}
